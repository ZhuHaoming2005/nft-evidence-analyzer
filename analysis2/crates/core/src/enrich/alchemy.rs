//! Alchemy NFT / transfers / prices / receipt-gas / native EXTERNAL clients.

use ahash::{AHashMap, AHashSet};
use num_bigint::BigUint;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, Notify, OnceCell};

use super::http::HttpClient;
use super::types::{
    DeploymentEvent, EvidenceObservation, EvidenceStatus, HolderRecord, PriceBucket,
    ProviderEndpoints, SaleEvent, TransferEvent, ValueFlowEdge, day_bucket, now_unix,
    status_from_count,
};

/// Parsed native EXTERNAL transfer from `alchemy_getAssetTransfers`.
#[derive(Clone, Debug, Default)]
pub struct NativeTransfer {
    pub tx_hash: String,
    pub event_id: Option<String>,
    pub from: String,
    pub to: String,
    pub value_native: Option<f64>,
    pub timestamp: Option<i64>,
    pub block_number: Option<u64>,
}

const ZERO: &str = "0x0000000000000000000000000000000000000000";
const MAX_COUNT_HEX: &str = "0x3e8";

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct FetchOutcome<T> {
    pub value: T,
    pub status: EvidenceStatus,
    pub observation: Option<EvidenceObservation>,
    pub failure: Option<String>,
    pub truncated: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PriceRequestKey {
    chain: String,
    symbols: Vec<String>,
    addresses: Vec<String>,
    require_native: bool,
}

type SharedPriceCell = Arc<OnceCell<FetchOutcome<Vec<PriceBucket>>>>;

/// Run-scoped price singleflight/cache. Identical symbol/address sets across
/// candidates share one provider request; incomplete outcomes are evicted so a
/// later candidate can retry independently.
#[derive(Clone, Default)]
pub struct PriceRequestCache {
    cells: Arc<AsyncMutex<AHashMap<PriceRequestKey, SharedPriceCell>>>,
}

impl PriceRequestCache {
    pub async fn fetch(
        &self,
        client: &HttpClient,
        endpoints: &ProviderEndpoints,
        api_key: Option<&str>,
        chain: &str,
        requested_symbols: &[String],
        requested_addresses: &[String],
    ) -> FetchOutcome<Vec<PriceBucket>> {
        let Some(api_key) = api_key else {
            return FetchOutcome::skipped("alchemy_prices");
        };
        let native = native_symbol(chain).to_owned();
        let native_outcome = self
            .fetch_subset(
                client,
                endpoints,
                api_key,
                chain,
                std::slice::from_ref(&native),
                &[],
                true,
            )
            .await;
        let mut extra_symbols: Vec<String> = requested_symbols
            .iter()
            .map(|symbol| symbol.trim().to_ascii_uppercase())
            .filter(|symbol| !symbol.is_empty() && !symbol.eq_ignore_ascii_case(&native))
            .collect();
        extra_symbols.sort();
        extra_symbols.dedup();
        let mut extra_addresses: Vec<String> = requested_addresses
            .iter()
            .map(|address| super::types::normalize_chain_address(chain, address))
            .filter(|address| !address.is_empty())
            .collect();
        extra_addresses.sort();
        extra_addresses.dedup();
        if extra_symbols.is_empty() && extra_addresses.is_empty() {
            return native_outcome;
        }
        let extras = self
            .fetch_subset(
                client,
                endpoints,
                api_key,
                chain,
                &extra_symbols,
                &extra_addresses,
                false,
            )
            .await;
        combine_price_outcomes(native_outcome, extras)
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_subset(
        &self,
        client: &HttpClient,
        endpoints: &ProviderEndpoints,
        api_key: &str,
        chain: &str,
        symbols: &[String],
        addresses: &[String],
        require_native: bool,
    ) -> FetchOutcome<Vec<PriceBucket>> {
        let key = price_request_key(chain, symbols, addresses, require_native);
        let cell = {
            let mut cells = self.cells.lock().await;
            cells
                .entry(key.clone())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let outcome = cell
            .get_or_init(|| async {
                fetch_price_subset(
                    client,
                    endpoints,
                    api_key,
                    chain,
                    symbols,
                    addresses,
                    require_native,
                )
                .await
            })
            .await
            .clone();
        if !matches!(
            outcome.status,
            EvidenceStatus::Complete | EvidenceStatus::Empty
        ) {
            let mut cells = self.cells.lock().await;
            if cells
                .get(&key)
                .is_some_and(|known| Arc::ptr_eq(known, &cell))
            {
                cells.remove(&key);
            }
        }
        outcome
    }
}

fn price_request_key(
    chain: &str,
    symbols: &[String],
    addresses: &[String],
    require_native: bool,
) -> PriceRequestKey {
    let mut symbols: Vec<String> = symbols
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect();
    symbols.sort();
    symbols.dedup();
    let mut addresses: Vec<String> = addresses
        .iter()
        .map(|address| super::types::normalize_chain_address(chain, address))
        .filter(|address| !address.is_empty())
        .collect();
    addresses.sort();
    addresses.dedup();
    PriceRequestKey {
        chain: chain.trim().to_ascii_lowercase(),
        symbols,
        addresses,
        require_native,
    }
}

fn combine_price_outcomes(
    mut native: FetchOutcome<Vec<PriceBucket>>,
    mut extras: FetchOutcome<Vec<PriceBucket>>,
) -> FetchOutcome<Vec<PriceBucket>> {
    if matches!(native.status, EvidenceStatus::NotRequested) {
        return native;
    }
    let native_ok = matches!(
        native.status,
        EvidenceStatus::Complete | EvidenceStatus::Empty
    );
    let extras_ok = matches!(
        extras.status,
        EvidenceStatus::Complete | EvidenceStatus::Empty
    );
    if !native_ok && !extras_ok {
        let failure = [native.failure.take(), extras.failure.take()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("; ");
        return FetchOutcome::failed("alchemy", "alchemy_prices", failure);
    }
    native.value.append(&mut extras.value);
    native.value.sort_by(|left, right| {
        (&left.chain, &left.symbol, &left.token_address).cmp(&(
            &right.chain,
            &right.symbol,
            &right.token_address,
        ))
    });
    native.value.dedup_by(|left, right| {
        left.chain == right.chain
            && left.symbol == right.symbol
            && left.token_address == right.token_address
    });
    let truncated = !native_ok || !extras_ok || native.truncated || extras.truncated;
    let failures = [native.failure.take(), extras.failure.take()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let count = native.value.len();
    let mut outcome = FetchOutcome::ok(native.value, count, truncated, "alchemy", "alchemy_prices");
    if !failures.is_empty() {
        outcome.failure = Some(failures.join("; "));
    }
    outcome
}

impl<T: Default> FetchOutcome<T> {
    pub fn skipped(request_key: &str) -> Self {
        Self {
            value: T::default(),
            status: EvidenceStatus::NotRequested,
            observation: Some(EvidenceObservation {
                source: "none".into(),
                request_key: request_key.into(),
                observed_at: now_unix(),
                status: EvidenceStatus::NotRequested,
            }),
            failure: None,
            truncated: false,
        }
    }

    pub fn failed(source: &str, request_key: &str, error: impl ToString) -> Self {
        let detail = error.to_string();
        let message = format!("{request_key}: {detail}");
        // Surface the concrete provider failure even when HTTP layer already
        // logged transport issues (JSON-RPC app errors, parse failures, etc.).
        super::http::print_provider_error(source, request_key, &detail);
        Self {
            value: T::default(),
            status: EvidenceStatus::Failed,
            observation: Some(EvidenceObservation {
                source: source.into(),
                request_key: request_key.into(),
                observed_at: now_unix(),
                status: EvidenceStatus::Failed,
            }),
            failure: Some(message),
            truncated: false,
        }
    }

    pub fn ok(value: T, count: usize, truncated: bool, source: &str, request_key: &str) -> Self {
        let status = status_from_count(count, truncated);
        Self {
            value,
            status,
            observation: Some(EvidenceObservation {
                source: source.into(),
                request_key: request_key.into(),
                observed_at: now_unix(),
                status,
            }),
            failure: None,
            truncated,
        }
    }
}

/// Fetch ERC-721/1155 transfers via `alchemy_getAssetTransfers`.
pub async fn fetch_transfers(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
    max_pages: usize,
) -> FetchOutcome<Vec<TransferEvent>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_transfers");
    };
    let Some(rpc) = endpoints.alchemy_rpc(chain, api_key) else {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_transfers",
            format!("unsupported alchemy network for {chain}"),
        );
    };

    let mut transfers = Vec::new();
    let mut page_key: Option<String> = None;
    let mut seen = std::collections::BTreeSet::new();
    let mut truncated = false;
    let mut partial_failure = None;
    let pages = max_pages.max(1);

    for page in 0..pages {
        let mut params = json!({
            "fromBlock": "0x0",
            "toBlock": "latest",
            "category": ["erc721", "erc1155"],
            "contractAddresses": [contract],
            "withMetadata": true,
            "excludeZeroValue": false,
            "maxCount": MAX_COUNT_HEX,
            "order": "asc"
        });
        if let Some(key) = &page_key {
            params["pageKey"] = Value::String(key.clone());
        }
        let body = json!({
            "jsonrpc": "2.0",
            "id": format!("transfers-{page}"),
            "method": "alchemy_getAssetTransfers",
            "params": [params]
        });
        let payload = match client.post_json_alchemy(&rpc, &[], &body).await {
            Ok(v) => v,
            Err(e) => {
                if transfers.is_empty() {
                    return FetchOutcome::failed("alchemy", "alchemy_transfers", e);
                }
                truncated = true;
                partial_failure = Some(format!("alchemy_transfers: partial page failure: {e}"));
                break;
            }
        };
        if let Some(error) = payload.get("error") {
            if transfers.is_empty() {
                return FetchOutcome::failed("alchemy", "alchemy_transfers", error.to_string());
            }
            truncated = true;
            partial_failure = Some(format!(
                "alchemy_transfers: partial provider error: {error}"
            ));
            break;
        }
        let result = payload.get("result").cloned().unwrap_or(Value::Null);
        let page_items = result
            .get("transfers")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for item in &page_items {
            transfers.extend(parse_alchemy_transfer(item, contract));
        }
        let next = result
            .get("pageKey")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .filter(|s| !s.is_empty());
        match next {
            Some(next) => {
                if !seen.insert(next.clone()) {
                    truncated = true;
                    partial_failure = Some("alchemy_transfers: repeated pagination cursor".into());
                    break;
                }
                page_key = Some(next);
                if page + 1 == pages {
                    truncated = true;
                }
            }
            None => break,
        }
    }

    let count = transfers.len();
    let mut outcome = FetchOutcome::ok(transfers, count, truncated, "alchemy", "alchemy_transfers");
    outcome.failure = partial_failure;
    outcome
}

/// Fetch owners via Alchemy NFT API `getOwnersForContract`.
///
/// Prefers `withTokenBalances=true` (per-token holders for economics). Large
/// collections often return multi-10MB JSON pages that exceed the HTTP client
/// body cap; in that case we automatically fall back to owner addresses only
/// and mark the result truncated.
pub async fn fetch_holders(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
    max_pages: usize,
) -> FetchOutcome<Vec<HolderRecord>> {
    let with_balances =
        fetch_holders_pages(client, endpoints, api_key, chain, contract, max_pages, true).await;
    if !holders_failed_due_to_oversize(&with_balances) {
        return with_balances;
    }
    eprintln!(
        "[api/warn] source=alchemy request_key=alchemy_holders \
         action=fallback_without_token_balances contract={contract} chain={chain} \
         reason=response_body_too_large"
    );
    let mut owners_only = fetch_holders_pages(
        client, endpoints, api_key, chain, contract, max_pages, false,
    )
    .await;
    // Lost per-token balances → always Truncated when any owners returned.
    if !owners_only.value.is_empty()
        && !matches!(
            owners_only.status,
            EvidenceStatus::Failed | EvidenceStatus::NotRequested
        )
    {
        owners_only.truncated = true;
        owners_only.status = EvidenceStatus::Truncated;
        if let Some(obs) = owners_only.observation.as_mut() {
            obs.status = EvidenceStatus::Truncated;
        }
        owners_only.failure = Some(
            "alchemy_holders: withTokenBalances response exceeded size limit; \
             returned owner addresses only"
                .into(),
        );
    }
    owners_only
}

fn holders_failed_due_to_oversize(outcome: &FetchOutcome<Vec<HolderRecord>>) -> bool {
    matches!(outcome.status, EvidenceStatus::Failed)
        && outcome
            .failure
            .as_deref()
            .is_some_and(|msg| msg.to_ascii_lowercase().contains("response exceeds"))
}

async fn fetch_holders_pages(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
    max_pages: usize,
    with_token_balances: bool,
) -> FetchOutcome<Vec<HolderRecord>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_holders");
    };
    let Some(mut url) = endpoints.alchemy_nft(chain, api_key, "getOwnersForContract") else {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_holders",
            format!("unsupported alchemy network for {chain}"),
        );
    };
    url.push_str(&format!(
        "{}contractAddress={}&withTokenBalances={}",
        if url.contains('?') { "&" } else { "?" },
        urlencoding_minimal(contract),
        if with_token_balances { "true" } else { "false" },
    ));

    let mut holders = Vec::new();
    let mut page_key: Option<String> = None;
    let mut seen = std::collections::BTreeSet::new();
    let mut truncated = false;
    let mut partial_failure = None;
    let pages = max_pages.max(1);

    for page in 0..pages {
        let mut page_url = url.clone();
        if let Some(key) = &page_key {
            page_url.push_str("&pageKey=");
            page_url.push_str(&urlencoding_minimal(key));
        }
        let payload = match client.get_json_alchemy(&page_url, &[]).await {
            Ok(v) => v,
            Err(e) => {
                if holders.is_empty() {
                    return FetchOutcome::failed("alchemy", "alchemy_holders", e);
                }
                truncated = true;
                partial_failure = Some(format!("alchemy_holders: partial page failure: {e}"));
                break;
            }
        };
        holders.extend(parse_holders(&payload));
        let next = payload
            .get("pageKey")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .filter(|s| !s.is_empty());
        match next {
            Some(next) => {
                if !seen.insert(next.clone()) {
                    truncated = true;
                    partial_failure = Some("alchemy_holders: repeated pagination cursor".into());
                    break;
                }
                page_key = Some(next);
                if page + 1 == pages {
                    truncated = true;
                }
            }
            None => break,
        }
    }

    let count = holders.len();
    let mut outcome = FetchOutcome::ok(holders, count, truncated, "alchemy", "alchemy_holders");
    outcome.failure = partial_failure;
    outcome
}

/// Fetch contract-scoped NFT sales through Alchemy `getNFTSales`.
///
/// The caller uses this only on networks where Alchemy exposes the endpoint.
pub async fn fetch_sales(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
    max_pages: usize,
) -> FetchOutcome<Vec<SaleEvent>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_sales");
    };
    let Some(base) = endpoints.alchemy_nft(chain, api_key, "getNFTSales") else {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_sales",
            format!("unsupported alchemy network for {chain}"),
        );
    };

    let mut sales = Vec::new();
    let mut page_key = None;
    let mut seen_page_keys = std::collections::BTreeSet::new();
    let mut truncated = false;
    let mut partial_failure = None;
    let pages = max_pages.max(1);
    for page in 0..pages {
        let mut url = format!(
            "{base}?fromBlock=0&toBlock=latest&order=asc&contractAddress={}",
            urlencoding_minimal(contract)
        );
        if let Some(key) = page_key.as_deref() {
            url.push_str("&pageKey=");
            url.push_str(&urlencoding_minimal(key));
        }
        let payload = match client.get_json_alchemy(&url, &[]).await {
            Ok(value) => value,
            Err(error) => {
                if sales.is_empty() {
                    return FetchOutcome::failed("alchemy", "alchemy_sales", error);
                }
                truncated = true;
                partial_failure = Some(format!("alchemy_sales: partial page failure: {error}"));
                break;
            }
        };
        if let Some(error) = payload.get("error") {
            if sales.is_empty() {
                return FetchOutcome::failed("alchemy", "alchemy_sales", error);
            }
            truncated = true;
            partial_failure = Some(format!("alchemy_sales: partial JSON-RPC failure: {error}"));
            break;
        }
        sales.extend(parse_nft_sales(&payload, chain));
        let next = payload
            .get("pageKey")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|key| !key.is_empty())
            .map(str::to_owned);
        let Some(next) = next else {
            break;
        };
        if !seen_page_keys.insert(next.clone()) {
            truncated = true;
            partial_failure = Some("alchemy_sales: repeated pagination cursor".into());
            break;
        }
        page_key = Some(next);
        if page + 1 == pages {
            truncated = true;
        }
    }
    let count = sales.len();
    let mut outcome = FetchOutcome::ok(sales, count, truncated, "alchemy", "alchemy_sales");
    outcome.failure = partial_failure;
    outcome
}

/// Fetch **current** (run-time) USD prices for the chain native token and
/// requested common payment symbols.
///
/// Historical day-bucket pricing is intentionally not used: Alchemy limits
/// `1d` historical ranges to 365 points, and cross-event valuation is simpler
/// and more stable with a single spot rate taken when enrich runs.
///
pub async fn fetch_prices(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    requested_symbols: &[String],
    requested_addresses: &[String],
) -> FetchOutcome<Vec<PriceBucket>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_prices");
    };
    let native = native_symbol(chain);
    let mut symbols = vec![native.to_owned()];
    for symbol in requested_symbols {
        let symbol = symbol.trim().to_ascii_uppercase();
        if !symbol.is_empty() && !symbols.iter().any(|known| known == &symbol) {
            symbols.push(symbol);
        }
    }
    fetch_price_subset(
        client,
        endpoints,
        api_key,
        chain,
        &symbols,
        requested_addresses,
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn fetch_price_subset(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: &str,
    chain: &str,
    symbols: &[String],
    requested_addresses: &[String],
    require_native: bool,
) -> FetchOutcome<Vec<PriceBucket>> {
    let native = native_symbol(chain);
    // Alchemy accepts at most 25 symbols per request. Keep broad payment-token
    // coverage by batching instead of letting one oversized request fail every
    // quote for the candidate.
    const SYMBOLS_PER_REQUEST: usize = 25;
    let mut prices = Vec::new();
    let mut symbol_fetch_succeeded = false;
    let mut symbol_fetch_failed = false;
    for chunk in symbols.chunks(SYMBOLS_PER_REQUEST) {
        let query = chunk.join("&symbols=");
        let url = format!(
            "{}/{}/tokens/by-symbol?symbols={}",
            endpoints.alchemy_prices.trim_end_matches('/'),
            api_key,
            query
        );
        let payload = match client.get_json_alchemy_uncached(&url, &[]).await {
            Ok(payload) => payload,
            Err(_) => {
                symbol_fetch_failed = true;
                continue;
            }
        };
        if payload.get("error").is_some() {
            symbol_fetch_failed = true;
            continue;
        }
        symbol_fetch_succeeded = true;
        prices.extend(chunk.iter().filter_map(|symbol| {
            parse_by_symbol_usd(&payload, symbol).map(|usd| PriceBucket {
                chain: chain.to_owned(),
                day_utc: day_bucket(now_unix()),
                symbol: symbol.clone(),
                token_address: None,
                usd_per_native: usd,
            })
        }));
    }
    if !symbols.is_empty() && !symbol_fetch_succeeded {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_prices",
            "all current symbol-price batches failed",
        );
    }
    let mut address_fetch_failed = false;
    if !requested_addresses.is_empty() {
        if let Some(network) = alchemy_price_network(chain) {
            let address_url = format!(
                "{}/{}/tokens/by-address",
                endpoints.alchemy_prices.trim_end_matches('/'),
                api_key
            );
            let body = json!({
                "addresses": requested_addresses
                    .iter()
                    .map(|address| json!({"network": network, "address": address}))
                    .collect::<Vec<_>>()
            });
            match client
                .post_json_alchemy_uncached(&address_url, &[], &body)
                .await
            {
                Ok(payload) if payload.get("error").is_none() => {
                    for address in requested_addresses {
                        if let Some(usd) = parse_by_address_usd(&payload, chain, address) {
                            prices.push(PriceBucket {
                                chain: chain.to_owned(),
                                day_utc: day_bucket(now_unix()),
                                symbol: String::new(),
                                token_address: Some(super::types::normalize_chain_address(
                                    chain, address,
                                )),
                                usd_per_native: usd,
                            });
                        }
                    }
                }
                _ => address_fetch_failed = true,
            }
        } else {
            address_fetch_failed = true;
        }
    }
    let native_missing = require_native
        && prices
            .iter()
            .all(|price| !price.symbol.eq_ignore_ascii_case(native));
    if prices.is_empty() {
        if !require_native && !symbol_fetch_failed && !address_fetch_failed {
            return FetchOutcome::ok(Vec::new(), 0, false, "alchemy", "alchemy_prices");
        }
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_prices",
            "no current USD price for any requested symbol",
        );
    }
    let count = prices.len();
    FetchOutcome::ok(
        prices,
        count,
        native_missing || address_fetch_failed || symbol_fetch_failed,
        "alchemy",
        "alchemy_prices",
    )
}

fn alchemy_price_network(chain: &str) -> Option<&'static str> {
    match chain.trim().to_ascii_lowercase().as_str() {
        "ethereum" => Some("eth-mainnet"),
        "base" => Some("base-mainnet"),
        "polygon" | "matic" => Some("polygon-mainnet"),
        "solana" => Some("solana-mainnet"),
        _ => None,
    }
}

pub(crate) fn parse_by_address_usd(payload: &Value, chain: &str, address: &str) -> Option<f64> {
    let expected = super::types::normalize_chain_address(chain, address);
    payload
        .get("data")
        .and_then(Value::as_array)?
        .iter()
        .find(|item| {
            item.get("address")
                .and_then(Value::as_str)
                .is_some_and(|actual| {
                    super::types::normalize_chain_address(chain, actual) == expected
                })
        })?
        .get("prices")
        .and_then(Value::as_array)?
        .iter()
        .find(|price| {
            price
                .get("currency")
                .and_then(Value::as_str)
                .is_some_and(|currency| currency.eq_ignore_ascii_case("usd"))
        })?
        .get("value")
        .and_then(|value| json_f64(Some(value)))
        .filter(|rate| rate.is_finite() && *rate > 0.0)
}

/// Parse Alchemy `tokens/by-symbol` current-price response.
pub(crate) fn parse_by_symbol_usd(payload: &Value, symbol: &str) -> Option<f64> {
    let mut matches = payload
        .get("data")
        .and_then(Value::as_array)?
        .iter()
        .filter(|item| {
            item.get("symbol")
                .and_then(Value::as_str)
                .is_some_and(|s| s.eq_ignore_ascii_case(symbol))
        })
        .filter_map(|item| {
            item.get("prices")
                .and_then(Value::as_array)?
                .iter()
                .find(|price| {
                    price
                        .get("currency")
                        .and_then(Value::as_str)
                        .is_some_and(|c| c.eq_ignore_ascii_case("usd"))
                })?
                .get("value")
                .and_then(|v| json_f64(Some(v)))
                .filter(|rate| rate.is_finite() && *rate > 0.0)
        });
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

pub fn parse_alchemy_transfer(item: &Value, fallback_contract: &str) -> Vec<TransferEvent> {
    let tx = item
        .get("hash")
        .or_else(|| item.get("transactionHash"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let from = item
        .get("from")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let to = item
        .get("to")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let timestamp = item
        .get("metadata")
        .and_then(|m| m.get("blockTimestamp"))
        .and_then(parse_timestamp);
    let block_number = item.get("blockNum").and_then(parse_block_number);
    let is_mint = from.is_empty() || from == ZERO;
    let _ = item
        .get("rawContract")
        .and_then(|c| c.get("address"))
        .and_then(Value::as_str)
        .unwrap_or(fallback_contract);
    transfer_token_ids(item)
        .into_iter()
        .filter(|id| !id.is_empty())
        .map(|token_id| TransferEvent {
            tx_hash: tx.clone(),
            token_id,
            from: from.clone(),
            to: to.clone(),
            timestamp,
            block_number,
            is_mint,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        })
        .collect()
}

pub fn parse_holders(payload: &Value) -> Vec<HolderRecord> {
    let mut out = Vec::new();
    for row in payload
        .get("owners")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let owner = row
            .get("ownerAddress")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_lowercase();
        if owner.is_empty() || owner == ZERO {
            continue;
        }
        let balances = row
            .get("tokenBalances")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if balances.is_empty() {
            out.push(HolderRecord {
                token_id: String::new(),
                owner: owner.clone(),
                balance: None,
            });
            continue;
        }
        for balance in balances {
            let token_id = normalize_token_id(balance.get("tokenId"));
            let bal = parse_i64(balance.get("balance"));
            out.push(HolderRecord {
                token_id,
                owner: owner.clone(),
                balance: bal,
            });
        }
    }
    out
}

#[derive(Clone, Debug)]
struct SaleFee {
    raw: BigUint,
    decimals: u32,
    amount: f64,
    symbol: Option<String>,
    address: Option<String>,
}

fn parse_sale_fee(value: Option<&Value>, chain: &str) -> Option<SaleFee> {
    let value = value?;
    let raw_text = value
        .get("amount")
        .or_else(|| value.get("quantity"))
        .and_then(|amount| {
            amount
                .as_str()
                .map(str::to_owned)
                .or_else(|| amount.as_u64().map(|number| number.to_string()))
        })?;
    let raw = raw_text.parse::<BigUint>().ok()?;
    let decimals = value.get("decimals").and_then(Value::as_u64).unwrap_or(18) as u32;
    let amount = raw_text.parse::<f64>().ok()? / 10f64.powi(decimals as i32);
    let symbol = value
        .get("symbol")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let address = value
        .get("contractAddress")
        .or_else(|| value.get("contract_address"))
        .or_else(|| value.get("tokenAddress"))
        .and_then(Value::as_str)
        .map(|address| super::types::normalize_chain_address(chain, address));
    Some(SaleFee {
        raw,
        decimals,
        amount,
        symbol,
        address,
    })
}

fn same_sale_currency(left: &SaleFee, right: &SaleFee) -> bool {
    left.decimals == right.decimals
        && match (left.address.as_deref(), right.address.as_deref()) {
            (Some(left), Some(right)) => left == right,
            (None, None) => left
                .symbol
                .as_deref()
                .zip(right.symbol.as_deref())
                .is_none_or(|(left, right)| left.eq_ignore_ascii_case(right)),
            _ => false,
        }
}

/// Parse Alchemy `getNFTSales` rows into the common sale model.
pub fn parse_nft_sales(payload: &Value, chain: &str) -> Vec<SaleEvent> {
    payload
        .get("nftSales")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let seller_fee = parse_sale_fee(item.get("sellerFee"), chain);
            let protocol_fee = parse_sale_fee(item.get("protocolFee"), chain);
            let royalty_fee = parse_sale_fee(item.get("royaltyFee"), chain);
            let components = [
                seller_fee.as_ref(),
                protocol_fee.as_ref(),
                royalty_fee.as_ref(),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
            let currency = components.first().copied();
            let comparable = currency.is_some_and(|first| {
                components
                    .iter()
                    .all(|component| same_sale_currency(first, component))
            });
            let native_amount =
                comparable.then(|| components.iter().map(|fee| fee.amount).sum::<f64>());
            let sale_price_raw = comparable.then(|| {
                components
                    .iter()
                    .fold(BigUint::default(), |sum, fee| sum + &fee.raw)
                    .to_string()
            });
            let seller = item
                .get("sellerAddress")
                .and_then(Value::as_str)
                .map(|address| super::types::normalize_chain_address(chain, address))
                .unwrap_or_default();
            let buyer = item
                .get("buyerAddress")
                .and_then(Value::as_str)
                .map(|address| super::types::normalize_chain_address(chain, address))
                .unwrap_or_default();
            let token_id = normalize_token_id(item.get("tokenId"));
            if seller.is_empty() || buyer.is_empty() || token_id.is_empty() {
                return None;
            }
            Some(SaleEvent {
                tx_hash: item
                    .get("transactionHash")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned(),
                token_id,
                seller,
                buyer,
                timestamp: item
                    .get("blockTimestamp")
                    .or_else(|| item.get("timestamp"))
                    .and_then(parse_timestamp),
                block_number: item
                    .get("blockNumber")
                    .or_else(|| item.get("blockNum"))
                    .and_then(parse_block_number),
                marketplace: item
                    .get("marketplace")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                native_amount,
                usd_amount: None,
                currency_symbol: currency
                    .and_then(|fee| fee.symbol.clone())
                    .or_else(|| Some(native_symbol(chain).to_owned())),
                currency_address: currency.and_then(|fee| fee.address.clone()),
                sale_price_raw,
                seller_proceeds_native: seller_fee.as_ref().map(|fee| fee.amount),
                seller_proceeds_usd: None,
                marketplace_fee_native: protocol_fee.as_ref().map(|fee| fee.amount),
                marketplace_fee_usd: None,
                marketplace_fee_currency_symbol: protocol_fee
                    .as_ref()
                    .and_then(|fee| fee.symbol.clone()),
                marketplace_fee_currency_address: protocol_fee
                    .as_ref()
                    .and_then(|fee| fee.address.clone()),
                royalty_fee_native: royalty_fee.as_ref().map(|fee| fee.amount),
                royalty_fee_usd: None,
                royalty_fee_currency_symbol: royalty_fee
                    .as_ref()
                    .and_then(|fee| fee.symbol.clone()),
                royalty_fee_currency_address: royalty_fee
                    .as_ref()
                    .and_then(|fee| fee.address.clone()),
                royalty_recipient: item
                    .get("royaltyFee")
                    .and_then(|fee| fee.get("recipient").or_else(|| fee.get("recipientAddress")))
                    .and_then(Value::as_str)
                    .map(|address| super::types::normalize_chain_address(chain, address)),
                gas_native: None,
                fee_payer: None,
            })
        })
        .collect()
}

fn transfer_token_ids(item: &Value) -> Vec<String> {
    let mut ids = Vec::new();
    if let Some(meta) = item.get("erc1155Metadata").and_then(Value::as_array) {
        for token in meta {
            let id = normalize_token_id(token.get("tokenId"));
            if !id.is_empty() {
                ids.push(id);
            }
        }
    }
    if ids.is_empty() {
        ids.push(normalize_token_id(
            item.get("erc721TokenId").or_else(|| item.get("tokenId")),
        ));
    }
    ids
}

pub fn normalize_token_id(raw: Option<&Value>) -> String {
    let Some(raw) = raw else {
        return String::new();
    };
    let text = raw
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| raw.to_string());
    let trimmed = text.trim();
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        hex_to_decimal(hex).unwrap_or_else(|| trimmed.to_owned())
    } else if trimmed.bytes().all(|byte| byte.is_ascii_digit()) {
        let normalized = trimmed.trim_start_matches('0');
        if normalized.is_empty() {
            "0".into()
        } else {
            normalized.into()
        }
    } else {
        trimmed.to_owned()
    }
}

fn hex_to_decimal(value: &str) -> Option<String> {
    if value.is_empty() || value.len() > 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let mut decimal_digits = vec![0_u8];
    for digit in value.bytes() {
        let nibble = match digit {
            b'0'..=b'9' => digit - b'0',
            b'a'..=b'f' => digit - b'a' + 10,
            b'A'..=b'F' => digit - b'A' + 10,
            _ => return None,
        };
        let mut carry = u16::from(nibble);
        for decimal in decimal_digits.iter_mut().rev() {
            let current = u16::from(*decimal) * 16 + carry;
            *decimal = (current % 10) as u8;
            carry = current / 10;
        }
        while carry > 0 {
            decimal_digits.insert(0, (carry % 10) as u8);
            carry /= 10;
        }
    }
    let first_nonzero = decimal_digits
        .iter()
        .position(|digit| *digit != 0)
        .unwrap_or(decimal_digits.len().saturating_sub(1));
    Some(
        decimal_digits[first_nonzero..]
            .iter()
            .map(|digit| char::from(b'0' + *digit))
            .collect(),
    )
}

fn parse_timestamp(value: &Value) -> Option<i64> {
    if let Some(n) = value.as_i64() {
        return Some(n);
    }
    let text = value.as_str()?.trim();
    if text.is_empty() {
        return None;
    }
    if let Ok(n) = text.parse::<i64>() {
        return Some(n);
    }
    parse_rfc3339(text)
}

fn parse_rfc3339(text: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|dt| dt.timestamp())
}

fn parse_block_number(value: &Value) -> Option<u64> {
    if let Some(n) = value.as_u64() {
        return Some(n);
    }
    let text = value.as_str()?.trim();
    if text.starts_with("0x") || text.starts_with("0X") {
        u64::from_str_radix(text.trim_start_matches(['0', 'x', 'X']), 16).ok()
    } else {
        text.parse().ok()
    }
}

fn parse_i64(value: Option<&Value>) -> Option<i64> {
    let value = value?;
    if let Some(n) = value.as_i64() {
        return Some(n);
    }
    let text = value.as_str()?.trim();
    if text.starts_with("0x") || text.starts_with("0X") {
        i64::from_str_radix(text.trim_start_matches(['0', 'x', 'X']), 16).ok()
    } else {
        text.parse().ok()
    }
}

fn json_f64(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|n| n as f64))
        .or_else(|| value.as_u64().map(|n| n as f64))
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .filter(|v| v.is_finite())
}

fn native_symbol(chain: &str) -> &'static str {
    match chain {
        "polygon" | "matic" => "POL",
        "solana" => "SOL",
        _ => "ETH",
    }
}

fn urlencoding_minimal(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                out.push(char::from(b"0123456789ABCDEF"[(byte >> 4) as usize]));
                out.push(char::from(b"0123456789ABCDEF"[(byte & 0xf) as usize]));
            }
        }
    }
    out
}

/// Parsed receipt fields used to fill transfer gas / fee payer.
#[derive(Clone, Debug, Default)]
pub struct ReceiptGas {
    pub gas_native: Option<f64>,
    pub fee_payer: Option<String>,
}

type ReceiptResult = Result<ReceiptGas, String>;
type ReceiptRow = (String, ReceiptResult);

#[derive(Default)]
struct ReceiptCell {
    value: AsyncMutex<Option<ReceiptResult>>,
    notify: Notify,
}

impl ReceiptCell {
    async fn set(&self, value: ReceiptResult) {
        *self.value.lock().await = Some(value);
        self.notify.notify_waiters();
    }

    async fn wait(&self) -> ReceiptResult {
        loop {
            let notified = self.notify.notified();
            if let Some(value) = self.value.lock().await.clone() {
                return value;
            }
            notified.await;
        }
    }
}

type SharedReceiptCell = Arc<ReceiptCell>;

/// Run-scoped receipt singleflight. Overlapping candidate and value-flow
/// transaction sets fetch each receipt at most once after a successful result.
#[derive(Clone, Default)]
pub struct ReceiptRequestCache {
    cells: Arc<AsyncMutex<AHashMap<(String, String), SharedReceiptCell>>>,
}

impl ReceiptRequestCache {
    pub async fn fetch(
        &self,
        client: &HttpClient,
        endpoints: &ProviderEndpoints,
        api_key: Option<&str>,
        chain: &str,
        tx_hashes: &[String],
    ) -> FetchOutcome<AHashMap<String, ReceiptGas>> {
        let Some(api_key) = api_key else {
            return FetchOutcome::skipped("alchemy_receipts");
        };
        if tx_hashes.is_empty() {
            return FetchOutcome::ok(AHashMap::new(), 0, false, "alchemy", "alchemy_receipts");
        }
        let chain_key = chain.trim().to_ascii_lowercase();
        let mut hashes: Vec<String> = tx_hashes
            .iter()
            .map(|hash| hash.trim().to_ascii_lowercase())
            .filter(|hash| !hash.is_empty())
            .collect();
        hashes.sort();
        hashes.dedup();
        let mut rows = Vec::with_capacity(hashes.len());
        let mut leaders = Vec::new();
        {
            let mut cells = self.cells.lock().await;
            for hash in &hashes {
                let key = (chain_key.clone(), hash.clone());
                if let Some(cell) = cells.get(&key) {
                    rows.push((key, cell.clone()));
                    continue;
                }
                let cell = Arc::new(ReceiptCell::default());
                cells.insert(key.clone(), cell.clone());
                leaders.push((hash.clone(), cell.clone()));
                rows.push((key, cell));
            }
        }
        if !leaders.is_empty() {
            let leader_hashes: Vec<String> = leaders.iter().map(|(hash, _)| hash.clone()).collect();
            let fetched =
                fetch_receipt_gas(client, endpoints, Some(api_key), chain, &leader_hashes).await;
            let missing_reason = fetched
                .failure
                .clone()
                .unwrap_or_else(|| "receipt unavailable without provider detail".to_owned());
            for (hash, cell) in leaders {
                let result = fetched
                    .value
                    .get(&hash)
                    .cloned()
                    .ok_or_else(|| missing_reason.clone());
                cell.set(result).await;
            }
        }

        let mut receipts = AHashMap::new();
        let mut failures = Vec::new();
        let mut failed_cells = Vec::new();
        for (key, cell) in &rows {
            match cell.wait().await {
                Ok(receipt) => {
                    receipts.insert(key.1.clone(), receipt.clone());
                }
                Err(error) => {
                    failures.push(format!("{}: {error}", key.1));
                    failed_cells.push((key.clone(), cell.clone()));
                }
            }
        }
        if !failed_cells.is_empty() {
            let mut cells = self.cells.lock().await;
            for (key, cell) in failed_cells {
                if cells
                    .get(&key)
                    .is_some_and(|known| Arc::ptr_eq(known, &cell))
                {
                    cells.remove(&key);
                }
            }
        }
        if receipts.is_empty() {
            return FetchOutcome::failed(
                "alchemy",
                "alchemy_receipts",
                summarize_receipt_failures("receipt cache", &failures, hashes.len()),
            );
        }
        let truncated = receipts.len() < hashes.len();
        let mut outcome = FetchOutcome::ok(
            receipts,
            hashes.len().saturating_sub(failures.len()),
            truncated,
            "alchemy",
            "alchemy_receipts",
        );
        if truncated {
            outcome.failure = Some(format!(
                "alchemy_receipts: partial failures ({}/{}): {}",
                failures.len(),
                hashes.len(),
                failures.into_iter().take(3).collect::<Vec<_>>().join("; ")
            ));
        }
        outcome
    }
}

/// Resolve missing positive-royalty recipients through ERC-2981 `royaltyInfo`.
pub async fn fetch_royalty_recipients(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
    sales: &[SaleEvent],
) -> FetchOutcome<AHashMap<(String, u64, String), String>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_royalty_recipients");
    };
    let token_blocks = sales
        .iter()
        .filter(|sale| {
            sale.royalty_recipient
                .as_deref()
                .is_none_or(|recipient| recipient.trim().is_empty())
                && (sale.royalty_fee_native.unwrap_or(0.0) > 0.0
                    || sale.royalty_fee_usd.unwrap_or(0.0) > 0.0)
        })
        .filter_map(|sale| {
            let block_number = sale.block_number?;
            let sale_price_raw = sale.sale_price_raw.as_deref()?;
            let token_word = token_id_to_abi_word(&sale.token_id)?;
            let sale_price_word = uint256_word_decimal(sale_price_raw)?;
            Some((
                (
                    sale.token_id.clone(),
                    block_number,
                    sale_price_raw.to_owned(),
                ),
                (token_word, sale_price_word),
            ))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    if token_blocks.is_empty() {
        return FetchOutcome::ok(
            AHashMap::new(),
            0,
            false,
            "alchemy",
            "alchemy_royalty_recipients",
        );
    }
    let Some(rpc) = endpoints.alchemy_rpc(chain, api_key) else {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_royalty_recipients",
            format!("unsupported alchemy network for {chain}"),
        );
    };
    let requests = token_blocks.into_iter().collect::<Vec<_>>();
    let mut recipients = AHashMap::new();
    let mut completed = 0usize;
    for (batch_index, chunk) in requests.chunks(RECEIPT_RPC_BATCH_SIZE).enumerate() {
        let body = Value::Array(
            chunk
                .iter()
                .enumerate()
                .map(
                    |(index, ((_, block_number, _), (token_word, sale_price_word)))| {
                        json!({
                            "jsonrpc": "2.0",
                            "id": format!("royalty-{batch_index}-{index}"),
                            "method": "eth_call",
                            "params": [{
                                "to": contract,
                                "data": format!("0x2a55205a{token_word}{sale_price_word}")
                            }, format!("0x{block_number:x}")]
                        })
                    },
                )
                .collect(),
        );
        let payload = match client.post_json_alchemy(&rpc, &[], &body).await {
            Ok(payload) => payload,
            Err(error) => {
                if completed == 0 {
                    return FetchOutcome::failed("alchemy", "alchemy_royalty_recipients", error);
                }
                break;
            }
        };
        let Some(rows) = payload.as_array() else {
            if completed == 0 {
                return FetchOutcome::failed(
                    "alchemy",
                    "alchemy_royalty_recipients",
                    "batch response was not an array",
                );
            }
            break;
        };
        let by_id = rows
            .iter()
            .filter_map(|row| {
                let id = row.get("id")?.as_str()?.to_owned();
                Some((id, row))
            })
            .collect::<AHashMap<_, _>>();
        for (index, ((token_id, block_number, sale_price_raw), _)) in chunk.iter().enumerate() {
            let id = format!("royalty-{batch_index}-{index}");
            let Some(row) = by_id.get(&id) else {
                continue;
            };
            completed += 1;
            if let Some(recipient) = row
                .get("result")
                .and_then(Value::as_str)
                .and_then(abi_first_word_address)
            {
                recipients.insert(
                    (token_id.clone(), *block_number, sale_price_raw.clone()),
                    recipient,
                );
            }
        }
    }
    let truncated = completed < requests.len();
    FetchOutcome::ok(
        recipients,
        completed,
        truncated,
        "alchemy",
        "alchemy_royalty_recipients",
    )
}

pub fn attach_royalty_recipients(
    sales: &mut [SaleEvent],
    recipients: &AHashMap<(String, u64, String), String>,
) {
    for sale in sales {
        if sale
            .royalty_recipient
            .as_deref()
            .is_none_or(|recipient| recipient.trim().is_empty())
            && let Some(recipient) = sale.block_number.and_then(|block| {
                sale.sale_price_raw.as_ref().and_then(|sale_price_raw| {
                    recipients.get(&(sale.token_id.clone(), block, sale_price_raw.clone()))
                })
            })
        {
            sale.royalty_recipient = Some(recipient.clone());
        }
    }
}

fn abi_first_word_address(value: &str) -> Option<String> {
    let hex = value
        .trim()
        .strip_prefix("0x")
        .or_else(|| value.trim().strip_prefix("0X"))
        .unwrap_or(value.trim());
    if hex.len() < 64 {
        return None;
    }
    let word = &hex[..64];
    if !word.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let address = format!("0x{}", &word[24..]).to_ascii_lowercase();
    (address != ZERO).then_some(address)
}

fn token_id_to_abi_word(token_id: &str) -> Option<String> {
    let trimmed = token_id.trim();
    if trimmed.is_empty() {
        return None;
    }
    let hex = if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        hex.to_owned()
    } else {
        decimal_to_hex(trimmed)?
    };
    (hex.len() <= 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| format!("{hex:0>64}").to_ascii_lowercase())
}

fn uint256_word_decimal(value: &str) -> Option<String> {
    let hex = decimal_to_hex(value.trim())?;
    (hex.len() <= 64).then(|| format!("{hex:0>64}"))
}

fn decimal_to_hex(value: &str) -> Option<String> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let mut digits = value.bytes().map(|byte| byte - b'0').collect::<Vec<_>>();
    let mut hex_digits = Vec::new();
    while digits.iter().any(|digit| *digit != 0) {
        let mut carry = 0u16;
        for digit in &mut digits {
            let current = carry * 10 + u16::from(*digit);
            *digit = (current / 16) as u8;
            carry = current % 16;
        }
        hex_digits.push(char::from_digit(u32::from(carry), 16)?);
        while digits.len() > 1 && digits.first() == Some(&0) {
            digits.remove(0);
        }
    }
    if hex_digits.is_empty() {
        Some("0".into())
    } else {
        Some(hex_digits.into_iter().rev().collect())
    }
}

/// Resolve the contract-creation receipt and block time from Alchemy.
///
/// A missing deployment block is treated as truncated evidence: deployment gas
/// is a required Setup cost and must not silently become zero.
pub async fn fetch_deployment(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
    deployed_block: Option<u64>,
) -> FetchOutcome<Option<DeploymentEvent>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_deployment");
    };
    let Some(block_number) = deployed_block else {
        let mut outcome = FetchOutcome::ok(None, 0, true, "alchemy", "alchemy_deployment");
        outcome.failure =
            Some("alchemy_deployment: contract metadata omitted deployedBlockNumber".into());
        return outcome;
    };
    let Some(rpc) = endpoints.alchemy_rpc(chain, api_key) else {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_deployment",
            format!("unsupported alchemy network for {chain}"),
        );
    };
    let block_hex = format!("0x{block_number:x}");
    let receipt_body = json!({
        "jsonrpc": "2.0",
        "id": "deployment-receipts",
        "method": "alchemy_getTransactionReceipts",
        "params": [{"blockNumber": block_hex}]
    });
    let receipt_payload = match client.post_json_alchemy(&rpc, &[], &receipt_body).await {
        Ok(payload) => payload,
        Err(error) => {
            return FetchOutcome::failed("alchemy", "alchemy_deployment", error);
        }
    };
    if let Some(error) = receipt_payload.get("error") {
        return FetchOutcome::failed("alchemy", "alchemy_deployment", error);
    }
    let normalized_contract = contract.trim().to_ascii_lowercase();
    let receipt = receipt_payload
        .pointer("/result/receipts")
        .and_then(Value::as_array)
        .and_then(|receipts| {
            receipts.iter().find(|receipt| {
                receipt
                    .get("contractAddress")
                    .and_then(Value::as_str)
                    .is_some_and(|address| {
                        address.trim().eq_ignore_ascii_case(&normalized_contract)
                    })
            })
        });
    let Some(receipt) = receipt else {
        let mut outcome = FetchOutcome::ok(None, 0, true, "alchemy", "alchemy_deployment");
        outcome.failure = Some(format!(
            "alchemy_deployment: no creation receipt for {contract} in block {block_number}"
        ));
        return outcome;
    };
    let Some(receipt_gas) = parse_receipt_gas(receipt) else {
        let mut outcome = FetchOutcome::ok(None, 0, true, "alchemy", "alchemy_deployment");
        outcome.failure =
            Some("alchemy_deployment: creation receipt omitted usable gas fields".into());
        return outcome;
    };

    let block_body = json!({
        "jsonrpc": "2.0",
        "id": "deployment-block",
        "method": "eth_getBlockByNumber",
        "params": [block_hex, false]
    });
    let timestamp = client
        .post_json_alchemy(&rpc, &[], &block_body)
        .await
        .ok()
        .and_then(|payload| {
            parse_u128(
                payload
                    .get("result")
                    .and_then(|result| result.get("timestamp")),
            )
        })
        .and_then(|timestamp| i64::try_from(timestamp).ok());
    let tx_hash = receipt
        .get("transactionHash")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if tx_hash.is_empty() {
        let mut outcome = FetchOutcome::ok(None, 0, true, "alchemy", "alchemy_deployment");
        outcome.failure =
            Some("alchemy_deployment: creation receipt omitted transactionHash".into());
        return outcome;
    }
    let deployment = DeploymentEvent {
        tx_hash,
        timestamp,
        gas_native: receipt_gas.gas_native,
        fee_payer: receipt_gas.fee_payer,
    };
    FetchOutcome::ok(Some(deployment), 1, false, "alchemy", "alchemy_deployment")
}

/// Collect unique non-empty tx hashes from transfers and sales (lowercase).
pub fn collect_unique_tx_hashes(transfers: &[TransferEvent], sales: &[SaleEvent]) -> Vec<String> {
    let mut set = std::collections::BTreeSet::new();
    for event in transfers {
        let hash = event.tx_hash.trim();
        if !hash.is_empty() {
            set.insert(hash.to_ascii_lowercase());
        }
    }
    for event in sales {
        let hash = event.tx_hash.trim();
        if !hash.is_empty() {
            set.insert(hash.to_ascii_lowercase());
        }
    }
    set.into_iter().collect()
}

/// JSON-RPC batch size for `eth_getTransactionReceipt` (Alchemy supports arrays).
const RECEIPT_RPC_BATCH_SIZE: usize = 80;

/// Fetch `eth_getTransactionReceipt` for unique tx hashes; parse gas fee in native units.
///
/// Uses JSON-RPC batches gated by the Alchemy lane concurrency (no nested
/// per-phase semaphore). Batch HTTP failures fall back to per-hash requests.
///
/// Status: NotRequested (no key) / Empty (no txs) / Complete (all ok) /
/// Truncated (partial) / Failed (all fail).
pub async fn fetch_receipt_gas(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    tx_hashes: &[String],
) -> FetchOutcome<AHashMap<String, ReceiptGas>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_receipts");
    };
    if tx_hashes.is_empty() {
        return FetchOutcome::ok(AHashMap::new(), 0, false, "alchemy", "alchemy_receipts");
    }
    let Some(rpc) = endpoints.alchemy_rpc(chain, api_key) else {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_receipts",
            format!("unsupported alchemy network for {chain}"),
        );
    };

    let mut handles = Vec::new();
    for (batch_idx, chunk) in tx_hashes.chunks(RECEIPT_RPC_BATCH_SIZE).enumerate() {
        let client = client.clone();
        let rpc = rpc.clone();
        let hashes: Vec<String> = chunk.to_vec();
        handles.push(tokio::spawn(async move {
            fetch_receipt_gas_batch(&client, &rpc, batch_idx, &hashes).await
        }));
    }

    let mut ok = AHashMap::new();
    let mut failures = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(batch_rows) => {
                for (hash, result) in batch_rows {
                    match result {
                        Ok(info) => {
                            ok.insert(hash, info);
                        }
                        Err(err) => failures.push(format!("{hash}: {err}")),
                    }
                }
            }
            Err(e) => failures.push(format!("receipt batch join failed: {e}")),
        }
    }

    let requested = tx_hashes.len();
    let succeeded = ok.len();
    if succeeded == 0 {
        let detail = summarize_receipt_failures("receipt fetch", &failures, requested);
        return FetchOutcome::failed("alchemy", "alchemy_receipts", detail);
    }
    let truncated = succeeded < requested;
    let mut outcome = FetchOutcome::ok(ok, succeeded, truncated, "alchemy", "alchemy_receipts");
    if truncated && !failures.is_empty() {
        outcome.failure = Some(format!(
            "alchemy_receipts: partial failures ({}/{}): {}",
            failures.len(),
            requested,
            failures.into_iter().take(3).collect::<Vec<_>>().join("; ")
        ));
    }
    outcome
}

async fn fetch_receipt_gas_batch(
    client: &HttpClient,
    rpc: &str,
    batch_idx: usize,
    hashes: &[String],
) -> Vec<ReceiptRow> {
    if hashes.is_empty() {
        return Vec::new();
    }
    let body = Value::Array(
        hashes
            .iter()
            .enumerate()
            .map(|(i, hash)| {
                json!({
                    "jsonrpc": "2.0",
                    "id": format!("receipt-{batch_idx}-{i}"),
                    "method": "eth_getTransactionReceipt",
                    "params": [hash]
                })
            })
            .collect(),
    );

    match client.post_json_alchemy(rpc, &[], &body).await {
        Ok(payload) => match parse_receipt_batch_payload(&payload, batch_idx, hashes) {
            Ok(rows) => rows,
            Err(_) => fetch_receipt_gas_singles(client, rpc, batch_idx, hashes).await,
        },
        Err(error) if super::http::is_rate_limited(&error) => hashes
            .iter()
            .cloned()
            .map(|hash| (hash, Err(error.to_string())))
            .collect(),
        Err(_) => fetch_receipt_gas_singles(client, rpc, batch_idx, hashes).await,
    }
}

fn summarize_receipt_failures(context: &str, failures: &[String], requested: usize) -> String {
    if failures.is_empty() {
        return format!("{context}: all {requested} receipt fetches failed without detail");
    }
    let examples = failures
        .iter()
        .take(3)
        .map(|failure| {
            const MAX_EXAMPLE_CHARS: usize = 320;
            let mut chars = failure.chars();
            let text = chars.by_ref().take(MAX_EXAMPLE_CHARS).collect::<String>();
            if chars.next().is_some() {
                format!("{text}…")
            } else {
                text
            }
        })
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "{context}: {}/{} receipt fetches failed; examples: {examples}",
        failures.len(),
        requested
    )
}

fn parse_receipt_batch_payload(
    payload: &Value,
    batch_idx: usize,
    hashes: &[String],
) -> Result<Vec<ReceiptRow>, ()> {
    let responses = payload.as_array().ok_or(())?;
    let mut by_id: AHashMap<String, &Value> = AHashMap::with_capacity(responses.len());
    for response in responses {
        let id = match response.get("id") {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Number(n)) => n.to_string(),
            _ => continue,
        };
        by_id.insert(id, response);
    }
    if by_id.is_empty() && responses.len() == hashes.len() {
        return Ok(hashes
            .iter()
            .zip(responses.iter())
            .map(|(hash, response)| (hash.clone(), receipt_from_rpc_response(response)))
            .collect());
    }
    let mut out = Vec::with_capacity(hashes.len());
    for (i, hash) in hashes.iter().enumerate() {
        let id = format!("receipt-{batch_idx}-{i}");
        match by_id.get(&id) {
            Some(response) => out.push((hash.clone(), receipt_from_rpc_response(response))),
            None => {
                // Positional fallback when the provider rewrites ids.
                if let Some(response) = responses.get(i) {
                    out.push((hash.clone(), receipt_from_rpc_response(response)));
                } else {
                    out.push((hash.clone(), Err("missing batch response".into())));
                }
            }
        }
    }
    Ok(out)
}

fn receipt_from_rpc_response(response: &Value) -> Result<ReceiptGas, String> {
    if let Some(error) = response.get("error") {
        return Err(error.to_string());
    }
    let Some(result) = response.get("result").filter(|v| !v.is_null()) else {
        return Err("null receipt result".into());
    };
    match parse_receipt_gas(result) {
        Some(info) if info.gas_native.is_some() => Ok(info),
        Some(_) | None => Err("missing gasUsed/effectiveGasPrice".into()),
    }
}

async fn fetch_receipt_gas_singles(
    client: &HttpClient,
    rpc: &str,
    batch_idx: usize,
    hashes: &[String],
) -> Vec<ReceiptRow> {
    let mut handles = Vec::with_capacity(hashes.len());
    for (i, hash) in hashes.iter().cloned().enumerate() {
        let client = client.clone();
        let rpc = rpc.to_owned();
        handles.push(tokio::spawn(async move {
            let body = json!({
                "jsonrpc": "2.0",
                "id": format!("receipt-{batch_idx}-{i}"),
                "method": "eth_getTransactionReceipt",
                "params": [hash]
            });
            let payload = match client.post_json_alchemy(&rpc, &[], &body).await {
                Ok(v) => v,
                Err(e) => return (hash, Err(e.to_string())),
            };
            (hash, receipt_from_rpc_response(&payload))
        }));
    }
    let mut out = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(row) => out.push(row),
            Err(e) => out.push((String::new(), Err(format!("receipt task join failed: {e}")))),
        }
    }
    out
}

/// Attach receipt gas / fee_payer onto matching transfers (by lowercase tx hash).
pub fn attach_receipt_gas(
    transfers: &mut [TransferEvent],
    receipts: &AHashMap<String, ReceiptGas>,
) {
    if receipts.is_empty() {
        return;
    }
    for transfer in transfers {
        let key = transfer.tx_hash.trim().to_ascii_lowercase();
        if key.is_empty() {
            continue;
        }
        let Some(info) = receipts.get(&key) else {
            continue;
        };
        if transfer.gas_native.is_none() {
            transfer.gas_native = info.gas_native;
        }
        if transfer.fee_payer.is_none()
            && let Some(payer) = info.fee_payer.clone()
        {
            transfer.fee_payer = Some(payer);
        }
    }
}

/// Attach receipt gas / fee payer onto matching sales.
pub fn attach_sale_receipt_gas(sales: &mut [SaleEvent], receipts: &AHashMap<String, ReceiptGas>) {
    for sale in sales {
        let key = sale.tx_hash.trim().to_ascii_lowercase();
        if let Some(receipt) = receipts.get(&key) {
            sale.gas_native = receipt.gas_native;
            sale.fee_payer = receipt.fee_payer.clone();
        }
    }
}

/// Collect and attach receipt fees for candidate-wide money-flow transactions.
pub fn value_flow_tx_hashes(edges: &[ValueFlowEdge]) -> Vec<String> {
    let mut hashes = AHashSet::new();
    for edge in edges {
        let hash = edge.tx_hash.trim();
        if !hash.is_empty() {
            hashes.insert(hash.to_ascii_lowercase());
        }
    }
    hashes.into_iter().collect()
}

pub fn attach_value_flow_receipt_gas(
    edges: &mut [ValueFlowEdge],
    receipts: &AHashMap<String, ReceiptGas>,
) {
    for edge in edges {
        let key = edge.tx_hash.trim().to_ascii_lowercase();
        if let Some(receipt) = receipts.get(&key) {
            edge.gas_native = receipt.gas_native;
            edge.fee_payer = receipt.fee_payer.clone();
        }
    }
}

pub fn parse_receipt_gas(result: &Value) -> Option<ReceiptGas> {
    let gas_used = parse_u128(result.get("gasUsed"))?;
    let gas_price = parse_u128(
        result
            .get("effectiveGasPrice")
            .or_else(|| result.get("gasPrice")),
    )?;
    let wei = gas_used.checked_mul(gas_price)?;
    let gas_native = (wei as f64) / 1e18;
    if !gas_native.is_finite() {
        return None;
    }
    let fee_payer = result
        .get("from")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_lowercase());
    Some(ReceiptGas {
        gas_native: Some(gas_native),
        fee_payer,
    })
}

fn parse_u128(value: Option<&Value>) -> Option<u128> {
    let value = value?;
    if let Some(n) = value.as_u64() {
        return Some(u128::from(n));
    }
    let text = value.as_str()?.trim();
    if text.is_empty() {
        return None;
    }
    if let Some(hex) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        if hex.is_empty() {
            return Some(0);
        }
        u128::from_str_radix(hex, 16).ok()
    } else {
        text.parse().ok()
    }
}

/// Fetch native EXTERNAL transfers for one address (`from` or `to`) in a block window.
///
/// `to_block == u64::MAX` means `"latest"`. One page only (`maxCount`); pageKey ⇒ Truncated.
#[allow(clippy::too_many_arguments)] // Mirrors the provider's directional block-window query.
pub async fn fetch_external_transfers(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    address: &str,
    direction: &str,
    from_block: u64,
    to_block: u64,
    request_id: usize,
) -> FetchOutcome<Vec<NativeTransfer>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("alchemy_external");
    };
    let Some(rpc) = endpoints.alchemy_rpc(chain, api_key) else {
        return FetchOutcome::failed(
            "alchemy",
            "alchemy_external",
            format!("unsupported alchemy network for {chain}"),
        );
    };
    let Some(address) = crate::seed::address::normalize_address(chain, address) else {
        return FetchOutcome::failed(
            "input",
            "alchemy_external_address",
            format!("invalid {chain} address excluded before provider request"),
        );
    };

    let to_block_value = if to_block == u64::MAX {
        Value::String("latest".into())
    } else {
        Value::String(format!("0x{to_block:x}"))
    };
    let mut params = json!({
        "fromBlock": format!("0x{from_block:x}"),
        "toBlock": to_block_value,
        "category": ["external"],
        "withMetadata": true,
        "excludeZeroValue": true,
        "maxCount": MAX_COUNT_HEX,
        "order": "asc"
    });
    match direction {
        "from" => params["fromAddress"] = Value::String(address.clone()),
        "to" => params["toAddress"] = Value::String(address),
        other => {
            return FetchOutcome::failed(
                "alchemy",
                "alchemy_external",
                format!("invalid direction {other}"),
            );
        }
    }
    let body = json!({
        "jsonrpc": "2.0",
        "id": format!("external-{direction}-{request_id}"),
        "method": "alchemy_getAssetTransfers",
        "params": [params]
    });
    let payload = match client.post_json_alchemy(&rpc, &[], &body).await {
        Ok(v) => v,
        Err(e) => return FetchOutcome::failed("alchemy", "alchemy_external", e),
    };
    if let Some(error) = payload.get("error") {
        return FetchOutcome::failed("alchemy", "alchemy_external", error.to_string());
    }
    let result = payload.get("result").cloned().unwrap_or(Value::Null);
    let mut transfers = Vec::new();
    for item in result
        .get("transfers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(row) = parse_native_transfer(item) {
            transfers.push(row);
        }
    }
    let truncated = result
        .get("pageKey")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .is_some();
    let count = transfers.len();
    FetchOutcome::ok(transfers, count, truncated, "alchemy", "alchemy_external")
}

/// Parse one Alchemy EXTERNAL transfer row; non-external categories are skipped.
pub fn parse_native_transfer(item: &Value) -> Option<NativeTransfer> {
    let category = item
        .get("category")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    if !category.is_empty() && category != "external" {
        return None;
    }
    let tx_hash = item
        .get("hash")
        .or_else(|| item.get("transactionHash"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())?
        .to_ascii_lowercase();
    let from = item
        .get("from")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    let to = item
        .get("to")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_ascii_lowercase();
    if from.is_empty() && to.is_empty() {
        return None;
    }
    let value_native = json_f64(item.get("value")).or_else(|| {
        let raw = item.get("rawContract")?;
        let wei = parse_u128(raw.get("value"))?;
        let decimals = parse_u128(raw.get("decimal"))
            .or_else(|| raw.get("decimals").and_then(Value::as_u64).map(u128::from))
            .unwrap_or(18) as i32;
        let amount = (wei as f64) / 10f64.powi(decimals);
        amount.is_finite().then_some(amount)
    });
    Some(NativeTransfer {
        tx_hash,
        event_id: item
            .get("uniqueId")
            .or_else(|| item.get("unique_id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
        from,
        to,
        value_native,
        timestamp: item
            .get("metadata")
            .and_then(|m| m.get("blockTimestamp"))
            .and_then(parse_timestamp),
        block_number: item.get("blockNum").and_then(parse_block_number),
    })
}

#[cfg(test)]
mod receipt_gas_tests {
    use super::*;
    use httpmock::{
        Method::{GET, POST},
        MockServer,
    };

    #[test]
    fn parse_u128_zero_hex() {
        assert_eq!(parse_u128(Some(&Value::String("0x0".into()))), Some(0));
        assert_eq!(parse_u128(Some(&Value::String("0x00".into()))), Some(0));
        assert_eq!(parse_u128(Some(&Value::String("0X0".into()))), Some(0));
    }

    #[tokio::test]
    async fn price_cache_reuses_equivalent_native_requests() {
        let server = MockServer::start_async().await;
        let prices = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/key/tokens/by-symbol")
                    .query_param("symbols", "ETH");
                then.status(200).json_body(json!({
                    "data": [{
                        "symbol": "ETH",
                        "prices": [{"currency": "usd", "value": "2000"}]
                    }]
                }));
            })
            .await;
        let endpoints = ProviderEndpoints {
            alchemy_prices: server.base_url(),
            ..ProviderEndpoints::default()
        };
        let client = HttpClient::with_retries(2, 0).unwrap();
        let cache = PriceRequestCache::default();
        let first = cache
            .fetch(&client, &endpoints, Some("key"), "ethereum", &[], &[])
            .await;
        let second = cache
            .fetch(
                &client,
                &endpoints,
                Some("key"),
                "ethereum",
                &["eth".into()],
                &[],
            )
            .await;

        assert_eq!(prices.hits(), 1);
        assert_eq!(first.value.len(), 1);
        assert_eq!(second.value.len(), 1);
    }

    #[tokio::test]
    async fn price_requests_do_not_reuse_durable_http_cache_across_clients() {
        let server = MockServer::start_async().await;
        let prices = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/key/tokens/by-symbol")
                    .query_param("symbols", "ETH");
                then.status(200).json_body(json!({
                    "data": [{
                        "symbol": "ETH",
                        "prices": [{"currency": "usd", "value": "2000"}]
                    }]
                }));
            })
            .await;
        let endpoints = ProviderEndpoints {
            alchemy_prices: server.base_url(),
            ..ProviderEndpoints::default()
        };
        let dir = std::env::temp_dir().join(format!(
            "analysis2_price_no_durable_cache_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        for _ in 0..2 {
            let client = HttpClient::with_retries_and_cache(1, 0, Some(dir.clone())).unwrap();
            let outcome =
                fetch_prices(&client, &endpoints, Some("key"), "ethereum", &[], &[]).await;
            assert_eq!(outcome.value.len(), 1);
        }

        assert_eq!(prices.hits(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn receipt_cache_reuses_transaction_across_candidates() {
        let server = MockServer::start_async().await;
        let rpc = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/rpc")
                    .body_contains("eth_getTransactionReceipt");
                then.status(200).json_body(json!([{
                    "jsonrpc": "2.0",
                    "id": "receipt-0-0",
                    "result": {
                        "from": "0x1111111111111111111111111111111111111111",
                        "gasUsed": "0x5208",
                        "effectiveGasPrice": "0x3b9aca00"
                    }
                }]));
            })
            .await;
        let endpoints = ProviderEndpoints {
            alchemy_rpc_template: format!("{}/rpc", server.base_url()),
            ..ProviderEndpoints::default()
        };
        let client = HttpClient::with_retries(2, 0).unwrap();
        let cache = ReceiptRequestCache::default();
        for _ in 0..2 {
            let outcome = cache
                .fetch(
                    &client,
                    &endpoints,
                    Some("key"),
                    "ethereum",
                    &["0xabc".into()],
                )
                .await;
            assert_eq!(outcome.value.len(), 1);
        }
        assert_eq!(rpc.hits(), 1);
    }

    #[tokio::test]
    async fn receipt_cache_preserves_rate_limit_without_single_request_fanout() {
        let server = MockServer::start_async().await;
        let rpc = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/rpc")
                    .body_contains("eth_getTransactionReceipt");
                then.status(429).header("retry-after", "0");
            })
            .await;
        let endpoints = ProviderEndpoints {
            alchemy_rpc_template: format!("{}/rpc", server.base_url()),
            ..ProviderEndpoints::default()
        };
        let client = HttpClient::with_retries(2, 0).unwrap();
        let hashes = (0..10)
            .map(|index| format!("0x{index:064x}"))
            .collect::<Vec<_>>();
        let outcome = ReceiptRequestCache::default()
            .fetch(&client, &endpoints, Some("key"), "ethereum", &hashes)
            .await;

        assert_eq!(rpc.hits(), 1, "429 batch must not fan out into singles");
        assert_eq!(outcome.status, EvidenceStatus::Failed);
        let failure = outcome.failure.expect("rate-limit failure detail");
        assert!(failure.contains("HTTP 429"));
        assert!(failure.len() < 2_000, "failure summary must stay bounded");
    }

    #[test]
    fn parse_receipt_uses_effective_gas_price() {
        let result = json!({
            "from": "0xAbC",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x3b9aca00"
        });
        let info = parse_receipt_gas(&result).unwrap();
        assert!((info.gas_native.unwrap() - 0.000021).abs() < 1e-12);
        assert_eq!(info.fee_payer.as_deref(), Some("0xabc"));
    }

    #[test]
    fn parse_receipt_falls_back_to_gas_price() {
        let result = json!({
            "gasUsed": "21000",
            "gasPrice": "1000000000"
        });
        let info = parse_receipt_gas(&result).unwrap();
        assert!((info.gas_native.unwrap() - 0.000021).abs() < 1e-12);
    }

    #[test]
    fn eip2981_helpers_support_decimal_tokens_and_first_word_recipient() {
        assert_eq!(
            token_id_to_abi_word("255").unwrap(),
            format!("{:0>64}", "ff")
        );
        let encoded = format!(
            "0x{:0>64}{:0>64}",
            "1234567890abcdef1234567890abcdef12345678", "64"
        );
        assert_eq!(
            abi_first_word_address(&encoded).as_deref(),
            Some("0x1234567890abcdef1234567890abcdef12345678")
        );
    }

    #[test]
    fn token_id_normalization_supports_full_uint256_and_decimal_leading_zeros() {
        assert_eq!(
            normalize_token_id(Some(&Value::String(
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".into()
            ))),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
        assert_eq!(
            normalize_token_id(Some(&Value::String("00000042".into()))),
            "42"
        );
        assert_eq!(normalize_token_id(Some(&Value::String("0x0".into()))), "0");
    }

    #[test]
    fn nft_sales_parser_sums_fee_components_and_preserves_breakdown() {
        let payload = json!({
            "nftSales": [{
                "transactionHash": "0xsale",
                "tokenId": "0x2a",
                "sellerAddress": "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "buyerAddress": "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
                "blockTimestamp": "1700000000",
                "blockNumber": "0x64",
                "marketplace": "seaport",
                "sellerFee": {
                    "amount": "1250000000000000000",
                    "decimals": 18,
                    "symbol": "ETH"
                },
                "protocolFee": {
                    "amount": "50000000000000000",
                    "decimals": 18,
                    "symbol": "ETH"
                },
                "royaltyFee": {
                    "amount": "50000000000000000",
                    "decimals": 18,
                    "symbol": "ETH",
                    "recipientAddress": "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
                }
            }]
        });

        let sales = parse_nft_sales(&payload, "ethereum");
        assert_eq!(sales.len(), 1);
        let sale = &sales[0];
        assert_eq!(sale.token_id, "42");
        assert_eq!(sale.seller, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(sale.buyer, "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        assert_eq!(sale.block_number, Some(100));
        assert_eq!(sale.timestamp, Some(1_700_000_000));
        assert_eq!(sale.sale_price_raw.as_deref(), Some("1350000000000000000"));
        assert!((sale.native_amount.unwrap() - 1.35).abs() < 1e-12);
        assert!((sale.seller_proceeds_native.unwrap() - 1.25).abs() < 1e-12);
        assert!((sale.marketplace_fee_native.unwrap() - 0.05).abs() < 1e-12);
        assert!((sale.royalty_fee_native.unwrap() - 0.05).abs() < 1e-12);
        assert_eq!(
            sale.royalty_recipient.as_deref(),
            Some("0xcccccccccccccccccccccccccccccccccccccccc")
        );
    }

    #[tokio::test]
    async fn royalty_lookup_uses_each_sale_historical_block_and_actual_price() {
        let server = MockServer::start_async().await;
        let rpc = server
            .mock_async(|when, then| {
                when.method(POST)
                    .path("/rpc/eth-mainnet/key")
                    .body_contains("\"eth_call\"")
                    .body_contains("0x2a55205a")
                    .body_contains(
                        "0000000000000000000000000000000000000000000000001bc16d674ec80000",
                    );
                then.status(200).json_body(json!([{
                    "jsonrpc": "2.0",
                    "id": "royalty-0-0",
                    "result": format!(
                        "0x{:0>64}{:0>64}",
                        "1234567890abcdef1234567890abcdef12345678", "64"
                    )
                }]));
            })
            .await;
        let endpoints = ProviderEndpoints {
            alchemy_rpc_template: format!("{}/rpc/{{network}}/{{key}}", server.base_url()),
            ..ProviderEndpoints::default()
        };
        let client = HttpClient::with_retries(1, 0).unwrap();
        let sales = vec![SaleEvent {
            token_id: "1".into(),
            block_number: Some(42),
            royalty_fee_native: Some(0.1),
            sale_price_raw: Some("2000000000000000000".into()),
            ..SaleEvent::default()
        }];
        let outcome = fetch_royalty_recipients(
            &client,
            &endpoints,
            Some("key"),
            "ethereum",
            "0xcontract",
            &sales,
        )
        .await;
        assert_eq!(outcome.status, EvidenceStatus::Complete);
        assert_eq!(
            outcome
                .value
                .get(&("1".into(), 42, "2000000000000000000".into()))
                .map(String::as_str),
            Some("0x1234567890abcdef1234567890abcdef12345678")
        );
        assert_eq!(rpc.hits(), 1);
    }

    #[test]
    fn collect_unique_hashes_from_transfers_and_sales() {
        let transfers = vec![TransferEvent {
            tx_hash: "0xAAA".into(),
            token_id: "1".into(),
            from: String::new(),
            to: String::new(),
            timestamp: None,
            block_number: None,
            is_mint: true,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        }];
        let sales = vec![SaleEvent {
            tx_hash: "0xaaa".into(),
            token_id: "1".into(),
            seller: String::new(),
            buyer: String::new(),
            timestamp: None,
            block_number: None,
            marketplace: None,
            native_amount: None,
            usd_amount: None,
            currency_symbol: None,
            currency_address: None,
            seller_proceeds_native: None,
            seller_proceeds_usd: None,
            ..SaleEvent::default()
        }];
        let hashes = collect_unique_tx_hashes(&transfers, &sales);
        assert_eq!(hashes, vec!["0xaaa".to_owned()]);
    }

    #[test]
    fn parse_by_symbol_usd_reads_current_price() {
        let payload = json!({
            "data": [{
                "symbol": "ETH",
                "prices": [
                    { "currency": "eur", "value": "2000" },
                    { "currency": "usd", "value": "2500.5" }
                ]
            }]
        });
        assert_eq!(parse_by_symbol_usd(&payload, "ETH"), Some(2500.5));
        assert_eq!(parse_by_symbol_usd(&payload, "SOL"), None);
    }

    #[test]
    fn symbol_price_is_rejected_when_multiple_tokens_share_the_symbol() {
        let payload = json!({
            "data": [
                {"symbol": "ABC", "prices": [{"currency": "usd", "value": "1"}]},
                {"symbol": "ABC", "prices": [{"currency": "usd", "value": "9"}]}
            ]
        });
        assert_eq!(parse_by_symbol_usd(&payload, "ABC"), None);
    }

    #[test]
    fn address_price_matches_chain_aware_token_identity() {
        let payload = json!({
            "data": [{
                "network": "solana-mainnet",
                "address": "AbC",
                "prices": [{"currency": "USD", "value": "2.5"}]
            }]
        });
        assert_eq!(parse_by_address_usd(&payload, "solana", "AbC"), Some(2.5));
        assert_eq!(parse_by_address_usd(&payload, "solana", "abc"), None);
    }

    #[test]
    fn oversize_holders_failure_is_detected_for_fallback() {
        let oversize = FetchOutcome::<Vec<HolderRecord>>::failed(
            "alchemy",
            "alchemy_holders",
            "http error: response exceeds 16777216 bytes body_len=25103313",
        );
        assert!(holders_failed_due_to_oversize(&oversize));
        let other = FetchOutcome::<Vec<HolderRecord>>::failed(
            "alchemy",
            "alchemy_holders",
            "http error: HTTP 500 endpoint=x",
        );
        assert!(!holders_failed_due_to_oversize(&other));
    }

    #[test]
    fn parse_holders_without_token_balances() {
        let payload = serde_json::json!({
            "owners": [
                { "ownerAddress": "0xAbC" },
                { "ownerAddress": "0x0000000000000000000000000000000000000000" }
            ]
        });
        let rows = parse_holders(&payload);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].owner, "0xabc");
        assert!(rows[0].token_id.is_empty());
    }
}

#[derive(Clone, Debug, Default)]
pub struct CollectionProfile {
    pub slug: Option<String>,
    pub open_license: bool,
}

pub(crate) fn is_open_license_payload(payload: &Value) -> bool {
    fn contains_marker(value: &Value) -> bool {
        match value {
            Value::Object(map) => map.values().any(contains_marker),
            Value::Array(items) => items.iter().any(contains_marker),
            Value::String(text) => {
                let text = text.to_ascii_lowercase();
                [
                    "cc0-1.0",
                    "license: cc0",
                    "creative commons zero",
                    "public domain",
                    "cc zero",
                ]
                .iter()
                .any(|marker| text.contains(marker))
            }
            _ => false,
        }
    }

    contains_marker(payload)
}

/// Alchemy NFT API: collection identity and seed-level license from the first NFT.
pub async fn fetch_collection_profile(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
) -> CollectionProfile {
    let Some(api_key) = api_key else {
        return CollectionProfile::default();
    };
    let Some(base) = endpoints.alchemy_nft(chain, api_key, "getNFTsForContract") else {
        return CollectionProfile::default();
    };
    let url = format!("{base}?contractAddress={contract}&withMetadata=true&limit=1");
    let Ok(payload) = client.get_json_alchemy(&url, &[]).await else {
        return CollectionProfile::default();
    };
    let slug = payload
        .get("nfts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find_map(|nft| {
            nft.get("collection")
                .and_then(|c| c.get("slug"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
        });
    CollectionProfile {
        slug,
        open_license: is_open_license_payload(&payload),
    }
}

/// Alchemy NFT API: collection slug from first NFT metadata (or None).
pub async fn fetch_collection_slug(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
) -> Option<String> {
    fetch_collection_profile(client, endpoints, api_key, chain, contract)
        .await
        .slug
}

#[cfg(test)]
mod open_license_tests {
    use super::is_open_license_payload;
    use serde_json::json;

    #[test]
    fn detects_supported_open_license_markers_recursively() {
        assert!(is_open_license_payload(&json!({
            "raw": {"metadata": {"license": "CC0-1.0"}}
        })));
        assert!(is_open_license_payload(&json!({
            "description": "Released into the public domain"
        })));
        assert!(!is_open_license_payload(&json!({
            "description": "All rights reserved"
        })));
    }
}

/// Alchemy NFT API: whether `wallet` currently holds any NFT of `contract`.
pub async fn is_holder_of_contract(
    client: &HttpClient,
    endpoints: &ProviderEndpoints,
    api_key: Option<&str>,
    chain: &str,
    wallet: &str,
    contract: &str,
) -> Result<Option<bool>, String> {
    let Some(api_key) = api_key else {
        return Ok(None);
    };
    let Some(base) = endpoints.alchemy_nft(chain, api_key, "isHolderOfContract") else {
        return Err(format!("unsupported alchemy network for {chain}"));
    };
    let url = format!("{base}?wallet={wallet}&contractAddress={contract}");
    let payload = client
        .get_json_alchemy(&url, &[])
        .await
        .map_err(|e| e.to_string())?;
    Ok(Some(
        payload
            .get("isHolderOfContract")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    ))
}
