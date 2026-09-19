//! Helius DAS helpers for Solana collection resolve + enrichment.
//!
//! History paths prefer DAS `getSignaturesForAsset` for ordinary and compressed
//! NFTs. When Helius reports `Tree not found` for an ordinary NFT, the bounded
//! `getSignaturesForAddress` fallback is retained as explicitly truncated
//! evidence. Both paths then feed deduped `getTransaction` jsonParsed decode
//! (standard SPL ownership + native SOL balance / transfer instructions).
//! Compressed NFT / Bubblegum full parity is intentionally out of MVP scope.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::error::AnalysisError;
use crate::progress::ProgressObserver;

use super::alchemy::{FetchOutcome, is_open_license_payload};
use super::controllers::solana_authorities_from_asset;
use super::http::HttpClient;
use super::roles::HolderSnapshot;
use super::types::{
    EvidenceStatus, HolderRecord, SaleEvent, TransferEvent, ValueFlowEdge, ValueFlowKind,
};

const DEFAULT_HELIUS_RPC: &str = "https://mainnet.helius-rpc.com/";
const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

/// One Solana asset from `getAssetsByGroup`.
#[derive(Clone, Debug, Default)]
pub struct SolanaAsset {
    pub mint: String,
    pub owner: Option<String>,
    pub compressed: bool,
}

#[derive(Clone, Debug, Default)]
pub struct SolanaAssetSnapshot {
    pub assets: Vec<SolanaAsset>,
    pub total: Option<usize>,
    pub truncated: bool,
    /// Whether sampled seed NFT metadata declares CC0 / public-domain use.
    pub open_license: bool,
    /// Collection updateAuthority (+ verified creators) extracted while paging assets.
    pub authority: Vec<String>,
    /// Asset ids whose DAS payload explicitly identifies a fungible asset.
    /// Missing/unknown interfaces are not included because they are not
    /// authoritative negative NFT identity evidence.
    pub rejected_non_nft_ids: Vec<String>,
    /// Direct getAsset recovery found collection grouping, so the returned ids
    /// cannot prove the full collection population.
    pub direct_grouped: bool,
}

enum DirectAssetRow {
    Asset(SolanaAsset, Vec<String>, bool, bool),
    Missing,
    ExplicitlyNonNft(String),
    Failed(String),
}

/// Resolve on-chain collection address for a mint via `getAsset`.
pub async fn resolve_collection_address(
    client: &HttpClient,
    rpc_url: &str,
    api_key: &str,
    mint: &str,
) -> Result<Option<String>, AnalysisError> {
    let mut url = rpc_url.trim_end_matches('/').to_owned();
    if !url.contains('?') {
        url.push_str("?api-key=");
        url.push_str(api_key);
    } else if !url.contains("api-key=") {
        url.push_str("&api-key=");
        url.push_str(api_key);
    }
    let body = json!({
        "jsonrpc": "2.0",
        "id": format!("seed-collection-{mint}"),
        "method": "getAsset",
        "params": {"id": mint}
    });
    let payload = client.post_json_helius(&url, &[], &body).await?;
    if let Some(error) = payload.get("error") {
        return Err(AnalysisError::http(format!(
            "Helius getAsset failed for {mint}: {error}"
        )));
    }
    let Some(result) = payload.get("result") else {
        return Err(AnalysisError::http(format!(
            "Helius getAsset omitted result for {mint}"
        )));
    };
    Ok(parse_collection_address(result))
}

/// Extract `grouping.group_value` where `group_key == "collection"`.
pub fn parse_collection_address(asset: &Value) -> Option<String> {
    let grouping = asset.get("grouping")?.as_array()?;
    for group in grouping {
        let key = group
            .get("group_key")
            .or_else(|| group.get("groupKey"))
            .and_then(Value::as_str)?;
        if key != "collection" {
            continue;
        }
        let value = group
            .get("group_value")
            .or_else(|| group.get("groupValue"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())?;
        if valid_solana_address(value) {
            return Some(value.to_owned());
        }
    }
    None
}

fn valid_solana_address(value: &str) -> bool {
    base58_decoded_len(value) == Some(32)
}

fn base58_decoded_len(value: &str) -> Option<usize> {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if value.is_empty() {
        return None;
    }
    let leading_zeroes = value.bytes().take_while(|b| *b == b'1').count();
    let mut decoded = vec![0_u8];
    for byte in value.bytes() {
        let digit = ALPHABET.iter().position(|c| *c == byte)? as u16;
        let mut carry = digit;
        for part in &mut decoded {
            let value = u16::from(*part) * 58 + carry;
            *part = value as u8;
            carry = value >> 8;
        }
        while carry > 0 {
            decoded.push(carry as u8);
            carry >>= 8;
        }
    }
    while decoded.last() == Some(&0) && decoded.len() > 1 {
        decoded.pop();
    }
    let body_len = if decoded == [0] { 0 } else { decoded.len() };
    Some(leading_zeroes + body_len)
}

pub fn default_rpc_url() -> &'static str {
    DEFAULT_HELIUS_RPC
}

/// Collection identity for legit matching without OpenSea: prefer DAS `symbol`,
/// then `name`, then the collection address itself.
pub async fn fetch_collection_identity(
    client: &HttpClient,
    rpc_url: &str,
    api_key: Option<&str>,
    collection: &str,
) -> Option<String> {
    let api_key = api_key?;
    let url = with_api_key(rpc_url, api_key);
    let body = json!({
        "jsonrpc": "2.0",
        "id": format!("collection-id-{collection}"),
        "method": "getAsset",
        "params": {"id": collection}
    });
    let payload = client.post_json_helius(&url, &[], &body).await.ok()?;
    if payload.get("error").is_some() {
        return None;
    }
    let result = payload.get("result")?;
    if let Some(id) = parse_collection_metadata_identity(result) {
        return Some(id);
    }
    // Fallback: stable on-chain collection address (same address = same collection).
    let trimmed = collection.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

/// Lightweight collection identity used by the legitimacy preflight. This
/// deliberately excludes collection-member assets; deep enrichment fetches
/// those only for candidates that survive the legitimacy gate.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct CollectionIdentityProbe {
    pub identity: Option<String>,
    pub authorities: Vec<String>,
}

fn collection_authorities_from_asset(asset: &Value, collection: &str) -> Vec<String> {
    let mut authorities = solana_authorities_from_asset(asset, asset, collection);
    if let Some(rows) = asset.get("authorities").and_then(Value::as_array) {
        for row in rows {
            if let Some(address) = row.get("address").and_then(Value::as_str) {
                let address = address.trim();
                if !address.is_empty() {
                    authorities.push(address.to_owned());
                }
            }
        }
    }
    authorities.sort();
    authorities.dedup();
    authorities
}

async fn fetch_collection_identity_probe(
    client: &HttpClient,
    rpc_url: &str,
    api_key: &str,
    collection: &str,
) -> CollectionIdentityProbe {
    let url = with_api_key(rpc_url, api_key);
    let body = json!({
        "jsonrpc": "2.0",
        "id": format!("collection-id-{collection}"),
        "method": "getAsset",
        "params": {"id": collection}
    });
    let Some(result) = client
        .post_json_helius(&url, &[], &body)
        .await
        .ok()
        .and_then(|payload| payload.get("result").cloned())
    else {
        return CollectionIdentityProbe::default();
    };
    CollectionIdentityProbe {
        identity: parse_collection_metadata_identity(&result)
            .or_else(|| Some(collection.to_owned())),
        authorities: collection_authorities_from_asset(&result, collection),
    }
}

/// Resolve many collection identities through the DAS `getAssetBatch`
/// endpoint. Missing or malformed batch members fall back to `getAsset`
/// individually, preserving the previous result semantics.
pub async fn fetch_collection_identities_batch(
    client: &HttpClient,
    rpc_url: &str,
    api_key: Option<&str>,
    collections: &[String],
    concurrency: usize,
    progress: &dyn ProgressObserver,
) -> AHashMap<String, CollectionIdentityProbe> {
    const COLLECTIONS_PER_BATCH: usize = 100;

    let mut out = AHashMap::with_capacity(collections.len());
    let Some(api_key) = api_key else {
        for collection in collections {
            out.insert(collection.clone(), CollectionIdentityProbe::default());
        }
        progress.add_completed(collections.len() as u64);
        return out;
    };
    let url = with_api_key(rpc_url, api_key);
    let jobs = collections
        .chunks(COLLECTIONS_PER_BATCH)
        .map(<[String]>::to_vec)
        .collect::<Vec<_>>();
    let mut batches = stream::iter(jobs.into_iter().map(|chunk| {
        let url = url.clone();
        async move {
            let mut rows_out = AHashMap::with_capacity(chunk.len());
            if chunk.len() == 1 {
                let collection = &chunk[0];
                rows_out.insert(
                    collection.clone(),
                    fetch_collection_identity_probe(client, rpc_url, api_key, collection).await,
                );
                return rows_out;
            }
            let body = json!({
                "jsonrpc": "2.0",
                "id": "collection-identities",
                "method": "getAssetBatch",
                "params": {"ids": &chunk}
            });
            let batch = client
                .post_json_helius(&url, &[], &body)
                .await
                .ok()
                .and_then(|payload| {
                    payload
                        .get("result")
                        .and_then(Value::as_array)
                        .or_else(|| payload.as_array())
                        .cloned()
                });
            let mut resolved = AHashSet::new();
            if let Some(rows) = batch {
                for row in rows {
                    let Some(id) = row.get("id").and_then(Value::as_str) else {
                        continue;
                    };
                    if !chunk.iter().any(|collection| collection == id) {
                        continue;
                    }
                    rows_out.insert(
                        id.to_owned(),
                        CollectionIdentityProbe {
                            identity: parse_collection_metadata_identity(&row)
                                .or_else(|| Some(id.to_owned())),
                            authorities: collection_authorities_from_asset(&row, id),
                        },
                    );
                    resolved.insert(id.to_owned());
                }
            }
            let unresolved = chunk
                .into_iter()
                .filter(|collection| !resolved.contains(collection))
                .collect::<Vec<_>>();
            let mut fallback = stream::iter(unresolved.into_iter().map(|collection| async move {
                let probe =
                    fetch_collection_identity_probe(client, rpc_url, api_key, &collection).await;
                (collection, probe)
            }))
            .buffer_unordered(concurrency.max(1));
            while let Some((collection, probe)) = fallback.next().await {
                rows_out.insert(collection, probe);
            }
            rows_out
        }
    }))
    .buffer_unordered(concurrency.max(1));
    while let Some(rows) = batches.next().await {
        progress.add_completed(rows.len() as u64);
        out.extend(rows);
    }
    out
}

fn parse_collection_metadata_identity(asset: &Value) -> Option<String> {
    let content = asset.get("content")?.get("metadata")?;
    for key in ["symbol", "name"] {
        if let Some(text) = content
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return Some(text.to_owned());
        }
    }
    None
}

fn with_api_key(rpc_url: &str, api_key: &str) -> String {
    let mut url = rpc_url.trim_end_matches('/').to_owned();
    if !url.contains('?') {
        url.push_str("?api-key=");
        url.push_str(api_key);
    } else if !url.contains("api-key=") {
        url.push_str("&api-key=");
        url.push_str(api_key);
    }
    url
}

/// Paginate `getAssetsByGroup` for a collection address.
pub async fn fetch_collection_assets(
    client: &HttpClient,
    rpc_url: &str,
    api_key: Option<&str>,
    collection: &str,
    max_assets: usize,
) -> FetchOutcome<SolanaAssetSnapshot> {
    fetch_collection_assets_with_visibility(client, rpc_url, api_key, collection, max_assets, false)
        .await
}

/// Retry collection discovery with unverified collection memberships visible.
/// This is used only when the verified collection query contradicts resident NFTs.
pub async fn fetch_collection_assets_including_unverified(
    client: &HttpClient,
    rpc_url: &str,
    api_key: Option<&str>,
    collection: &str,
    max_assets: usize,
) -> FetchOutcome<SolanaAssetSnapshot> {
    fetch_collection_assets_with_visibility(client, rpc_url, api_key, collection, max_assets, true)
        .await
}

async fn fetch_collection_assets_with_visibility(
    client: &HttpClient,
    rpc_url: &str,
    api_key: Option<&str>,
    collection: &str,
    max_assets: usize,
    show_unverified_collections: bool,
) -> FetchOutcome<SolanaAssetSnapshot> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("helius_assets");
    };
    let url = with_api_key(rpc_url, api_key);
    // Helius DAS supports up to 1,000 assets per page. Use the provider
    // maximum so the common 200-asset snapshot needs one request.
    let page_size = 1_000usize.min(max_assets.max(1));
    let mut snapshot = SolanaAssetSnapshot::default();
    let mut page = 1usize;
    let mut seen_mints = std::collections::BTreeSet::new();
    let mut seen_pages = std::collections::BTreeSet::new();

    loop {
        if snapshot.assets.len() >= max_assets {
            snapshot.truncated = true;
            break;
        }
        let limit = page_size.min(max_assets.saturating_sub(snapshot.assets.len()).max(1));
        let body = json!({
            "jsonrpc": "2.0",
            "id": format!("assets-{page}"),
            "method": "getAssetsByGroup",
            "params": {
                "groupKey": "collection",
                "groupValue": collection,
                "page": page,
                "limit": limit,
                "options": {
                    "showUnverifiedCollections": show_unverified_collections,
                    "showCollectionMetadata": true,
                    "showGrandTotal": true
                }
            }
        });
        let payload = match client.post_json_helius(&url, &[], &body).await {
            Ok(v) => v,
            Err(e) => {
                if snapshot.assets.is_empty() {
                    return FetchOutcome::failed("helius", "helius_assets", e);
                }
                snapshot.truncated = true;
                break;
            }
        };
        if let Some(error) = payload.get("error") {
            if snapshot.assets.is_empty() {
                return FetchOutcome::failed("helius", "helius_assets", error.to_string());
            }
            snapshot.truncated = true;
            break;
        }
        let Some(result) = payload.get("result") else {
            if snapshot.assets.is_empty() {
                return FetchOutcome::failed(
                    "helius",
                    "helius_assets",
                    "getAssetsByGroup omitted result",
                );
            }
            snapshot.truncated = true;
            break;
        };
        // Helius has returned page-sized/capped `total` values in production
        // (for example 1,000 for a 4,992-asset collection). Treat it as a hint
        // only and determine completeness from page exhaustion.
        let items = result
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if items.is_empty() {
            break;
        }
        let page_identity = items
            .iter()
            .filter_map(|item| item.get("id").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\0");
        if !page_identity.is_empty() && !seen_pages.insert(page_identity) {
            snapshot.truncated = true;
            break;
        }
        for item in &items {
            snapshot.open_license |= is_open_license_payload(item);
            if snapshot.authority.is_empty() {
                let authorities = solana_authorities_from_asset(item, result, collection);
                if !authorities.is_empty() {
                    snapshot.authority = authorities;
                }
            }
            if !helius_row_is_explicitly_non_nft(item)
                && let Some(asset) = parse_asset(item)
                && seen_mints.insert(asset.mint.clone())
            {
                snapshot.assets.push(asset);
                if snapshot.assets.len() >= max_assets {
                    snapshot.truncated = true;
                    break;
                }
            }
        }
        if items.len() < limit {
            break;
        }
        page += 1;
    }

    let count = snapshot.assets.len();
    let truncated = snapshot.truncated
        || snapshot
            .total
            .is_some_and(|total| total > snapshot.assets.len());
    snapshot.truncated = truncated;
    let request_key = if show_unverified_collections {
        "helius_assets_unverified"
    } else {
        "helius_assets"
    };
    FetchOutcome::ok(snapshot, count, truncated, "helius", request_key)
}

/// Recover resident singleton NFTs (or otherwise ungrouped assets) directly
/// through DAS `getAsset` when `getAssetsByGroup` cannot enumerate them.
///
/// A fully resolved set of explicitly ungrouped ids is Complete because the
/// ingester models those ids as the whole singleton analysis unit. Grouped,
/// capped, missing, or partially failed direct recovery remains Truncated.
pub async fn fetch_assets_by_ids(
    client: &HttpClient,
    rpc_url: &str,
    api_key: Option<&str>,
    asset_ids: &[String],
    max_assets: usize,
) -> FetchOutcome<SolanaAssetSnapshot> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("helius_assets_by_id");
    };
    let unique_ids: Vec<String> = asset_ids
        .iter()
        .map(|value| value.trim())
        .filter(|value| valid_solana_address(value))
        .map(str::to_owned)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if unique_ids.is_empty() {
        return FetchOutcome::ok(
            SolanaAssetSnapshot::default(),
            0,
            false,
            "helius",
            "helius_assets_by_id",
        );
    }

    let selected: Vec<String> = unique_ids.iter().take(max_assets.max(1)).cloned().collect();
    let selected_count = selected.len();
    let url = with_api_key(rpc_url, api_key);
    let rows: Vec<DirectAssetRow> = stream::iter(selected.into_iter().map(|mint| {
        let client = client.clone();
        let url = url.clone();
        async move {
            let body = json!({
                "jsonrpc": "2.0",
                "id": format!("asset-{mint}"),
                "method": "getAsset",
                "params": {"id": mint}
            });
            let payload = match client.post_json_helius(&url, &[], &body).await {
                Ok(payload) => payload,
                Err(error) => {
                    return DirectAssetRow::Failed(format!("getAsset {mint}: {error}"));
                }
            };
            if let Some(error) = payload.get("error") {
                return DirectAssetRow::Failed(format!("getAsset {mint}: JSON-RPC error {error}"));
            }
            let Some(result) = payload.get("result").filter(|value| !value.is_null()) else {
                return DirectAssetRow::Missing;
            };
            let mut nft_identity = is_nft_asset_payload(result);
            let interface = result
                .get("interface")
                .and_then(Value::as_str)
                .unwrap_or("missing");
            if !nft_identity
                && !is_explicit_fungible_interface(interface)
                && !is_explicit_non_nft_interface(interface)
            {
                nft_identity = custom_payload_is_single_nft(result)
                    || verify_single_nft_mint(&client, &url, &mint).await;
            }
            if !nft_identity {
                if is_explicit_fungible_interface(interface)
                    || is_explicit_non_nft_interface(interface)
                {
                    return DirectAssetRow::ExplicitlyNonNft(mint);
                }
                return DirectAssetRow::Failed(format!(
                    "getAsset {mint}: returned ambiguous digital asset identity interface {interface}"
                ));
            }
            let Some(asset) = parse_asset(result) else {
                return DirectAssetRow::Failed(format!(
                    "getAsset {mint}: missing valid asset identity"
                ));
            };
            let has_collection_group = result
                .get("grouping")
                .and_then(Value::as_array)
                .is_some_and(|groups| {
                    groups.iter().any(|group| {
                        group
                            .get("group_key")
                            .or_else(|| group.get("groupKey"))
                            .and_then(Value::as_str)
                            == Some("collection")
                    })
                });
            let collection_group = parse_collection_address(result);
            let collection = collection_group
                .clone()
                .unwrap_or_else(|| asset.mint.clone());
            let authorities = solana_authorities_from_asset(result, result, &collection);
            DirectAssetRow::Asset(
                asset,
                authorities,
                is_open_license_payload(result),
                has_collection_group,
            )
        }
    }))
    .buffer_unordered(32)
    .collect()
    .await;

    let mut snapshot = SolanaAssetSnapshot::default();
    let mut failures = Vec::new();
    let mut missing_count = 0usize;
    let mut any_grouped = false;
    for row in rows {
        match row {
            DirectAssetRow::Asset(asset, authorities, open_license, grouped) => {
                snapshot.assets.push(asset);
                snapshot.authority.extend(authorities);
                snapshot.open_license |= open_license;
                any_grouped |= grouped;
            }
            DirectAssetRow::Missing => missing_count += 1,
            DirectAssetRow::ExplicitlyNonNft(mint) => {
                snapshot.rejected_non_nft_ids.push(mint);
            }
            DirectAssetRow::Failed(error) => failures.push(error),
        }
    }
    snapshot.authority.sort();
    snapshot.authority.dedup();
    snapshot.rejected_non_nft_ids.sort();
    snapshot.rejected_non_nft_ids.dedup();
    snapshot.direct_grouped = any_grouped;

    if snapshot.assets.is_empty() && !failures.is_empty() {
        return FetchOutcome::failed(
            "helius",
            "helius_assets_by_id",
            format!(
                "{} getAsset request(s) failed: {}",
                failures.len(),
                failures.join("; ")
            ),
        );
    }

    let count = snapshot.assets.len();
    if count == 0 {
        return FetchOutcome::ok(snapshot, 0, false, "helius", "helius_assets_by_id");
    }

    let complete_ungrouped_unit = !any_grouped
        && selected_count == unique_ids.len()
        && count == unique_ids.len()
        && missing_count == 0
        && failures.is_empty()
        && snapshot.rejected_non_nft_ids.is_empty();
    if complete_ungrouped_unit {
        snapshot.total = Some(count);
    }
    snapshot.truncated = !complete_ungrouped_unit;
    let mut outcome = FetchOutcome::ok(
        snapshot,
        count,
        !complete_ungrouped_unit,
        "helius",
        "helius_assets_by_id",
    );
    if !failures.is_empty() {
        outcome.failure = Some(format!(
            "helius_assets_by_id: recovered {count} asset(s), but {} getAsset request(s) failed: {}",
            failures.len(),
            failures.join("; ")
        ));
    }
    outcome
}

/// Bounded history discovery for Solana NFTs → transfer/sale stubs.
///
/// DAS `getSignaturesForAsset` covers ordinary and compressed NFTs and avoids
/// the incomplete account-key semantics of `getSignaturesForAddress`.
///
/// Stubs alone are never Complete: callers must run [`decode_and_attach_transactions`]
/// and recompute field quality from decode stats.
pub async fn fetch_asset_histories(
    client: &HttpClient,
    rpc_url: &str,
    api_key: Option<&str>,
    assets: &[SolanaAsset],
    max_assets: usize,
    max_sigs_per_asset: usize,
) -> FetchOutcome<(Vec<TransferEvent>, Vec<SaleEvent>)> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("helius_histories");
    };
    if assets.is_empty() {
        return FetchOutcome::ok(
            (Vec::new(), Vec::new()),
            0,
            false,
            "helius",
            "helius_histories",
        );
    }
    let url = with_api_key(rpc_url, api_key);
    let selected: Vec<SolanaAsset> = assets.iter().take(max_assets.max(1)).cloned().collect();
    let mut truncated = assets.len() > max_assets;

    const SIGNATURE_RPC_BATCH_SIZE: usize = 10;
    let mut handles = Vec::with_capacity(selected.len().div_ceil(SIGNATURE_RPC_BATCH_SIZE).max(1));
    for (batch_idx, chunk) in selected.chunks(SIGNATURE_RPC_BATCH_SIZE).enumerate() {
        let client = client.clone();
        let url = url.clone();
        let assets = chunk.to_vec();
        let max_sigs = max_sigs_per_asset.max(1);
        handles.push(tokio::spawn(async move {
            fetch_asset_history_batch(&client, &url, batch_idx, &assets, max_sigs).await
        }));
    }

    let mut transfers = Vec::new();
    let mut sales = Vec::new();
    let mut any_ok = false;
    let mut failures = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(rows) => {
                for row in rows {
                    match row {
                        Ok((mut t, mut s, page_truncated)) => {
                            any_ok = true;
                            if page_truncated {
                                truncated = true;
                            }
                            transfers.append(&mut t);
                            sales.append(&mut s);
                        }
                        Err(error) => failures.push(error),
                    }
                }
            }
            Err(error) => failures.push(format!("history worker join failure: {error}")),
        }
    }

    if !any_ok && !failures.is_empty() {
        return FetchOutcome::failed(
            "helius",
            "helius_histories",
            format!(
                "{} signature discovery failure(s): {}",
                failures.len(),
                failures.join("; ")
            ),
        );
    }
    let count = transfers.len() + sales.len();
    // Only mark truncated for discovery caps; decode quality is decided later.
    let mut outcome = FetchOutcome::ok(
        (transfers, sales),
        count,
        truncated || !failures.is_empty(),
        "helius",
        "helius_histories",
    );
    if !failures.is_empty() {
        outcome.failure = Some(format!(
            "helius_histories: {} asset history failure(s): {}",
            failures.len(),
            failures.join("; ")
        ));
    }
    outcome
}

type AssetHistoryRow = Result<(Vec<TransferEvent>, Vec<SaleEvent>, bool), String>;

fn signature_request(asset: &SolanaAsset, request_id: String, max_sigs: usize) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "getSignaturesForAsset",
        "params": {
            "id": asset.mint,
            "page": 1,
            "limit": max_sigs
        }
    })
}

fn signatures_for_address_request(
    asset: &SolanaAsset,
    request_id: String,
    max_sigs: usize,
) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "getSignaturesForAddress",
        "params": [asset.mint, {"limit": max_sigs}]
    })
}

fn signature_method(_asset: &SolanaAsset) -> &'static str {
    "getSignaturesForAsset"
}

fn ordinary_asset_tree_not_found(asset: &SolanaAsset, payload: &Value) -> bool {
    if asset.compressed {
        return false;
    }
    let Some(error) = payload.get("error") else {
        return false;
    };
    error.get("code").and_then(Value::as_i64) == Some(-32000)
        && error
            .get("message")
            .and_then(Value::as_str)
            .is_some_and(|message| message.contains("Tree not found"))
}

async fn fetch_asset_history_single(
    client: &HttpClient,
    url: &str,
    asset: &SolanaAsset,
    max_sigs: usize,
) -> AssetHistoryRow {
    let body = signature_request(asset, format!("sigs-{}", asset.mint), max_sigs);
    let payload = client
        .post_json_helius(url, &[], &body)
        .await
        .map_err(|error| format!("{} {}: {error}", signature_method(asset), asset.mint))?;
    if ordinary_asset_tree_not_found(asset, &payload) {
        let fallback_body =
            signatures_for_address_request(asset, format!("address-sigs-{}", asset.mint), max_sigs);
        let fallback_payload = client
            .post_json_helius(url, &[], &fallback_body)
            .await
            .map_err(|error| format!("getSignaturesForAddress fallback {}: {error}", asset.mint))?;
        let (transfers, sales, _) = parse_address_history(asset, &fallback_payload, max_sigs)?;
        // A mint address is not guaranteed to appear in every SPL token-account
        // transfer. The fallback recovers observable history but cannot prove
        // complete coverage, even when fewer than `max_sigs` rows are returned.
        return Ok((transfers, sales, true));
    }
    parse_asset_history(asset, &payload, max_sigs)
}

async fn fetch_asset_history_batch(
    client: &HttpClient,
    url: &str,
    batch_idx: usize,
    assets: &[SolanaAsset],
    max_sigs: usize,
) -> Vec<AssetHistoryRow> {
    if assets.len() == 1 {
        return vec![fetch_asset_history_single(client, url, &assets[0], max_sigs).await];
    }

    let body = Value::Array(
        assets
            .iter()
            .enumerate()
            .map(|(idx, asset)| {
                signature_request(asset, format!("sigs-{batch_idx}-{idx}"), max_sigs)
            })
            .collect(),
    );
    if let Ok(payload) = client.post_json_helius(url, &[], &body).await
        && let Some(responses) = payload.as_array()
    {
        let by_id: AHashMap<&str, &Value> = responses
            .iter()
            .filter_map(|row| Some((row.get("id")?.as_str()?, row)))
            .collect();
        let rows: Vec<AssetHistoryRow> = assets
            .iter()
            .enumerate()
            .map(|(idx, asset)| {
                let id = format!("sigs-{batch_idx}-{idx}");
                by_id
                    .get(id.as_str())
                    .copied()
                    .ok_or_else(|| {
                        format!(
                            "{} {}: batch response omitted id {id}",
                            signature_method(asset),
                            asset.mint,
                        )
                    })
                    .and_then(|response| parse_asset_history(asset, response, max_sigs))
            })
            .collect();
        if rows.iter().all(Result::is_ok) {
            return rows;
        }
    }

    // A malformed/partial batch must not lose an asset history. Retry every
    // member independently so quality remains identical to the old path.
    let mut handles = Vec::with_capacity(assets.len());
    for asset in assets {
        let asset = asset.clone();
        let client = client.clone();
        let url = url.to_owned();
        handles.push(tokio::spawn(async move {
            fetch_asset_history_single(&client, &url, &asset, max_sigs).await
        }));
    }
    let mut rows = Vec::with_capacity(handles.len());
    for handle in handles {
        rows.push(
            handle
                .await
                .unwrap_or_else(|error| Err(format!("history worker join failure: {error}"))),
        );
    }
    rows
}

fn parse_asset_history(asset: &SolanaAsset, payload: &Value, max_sigs: usize) -> AssetHistoryRow {
    if let Some(error) = payload.get("error") {
        return Err(format!(
            "{} {}: JSON-RPC error {error}",
            signature_method(asset),
            asset.mint,
        ));
    }
    let items = payload
        .get("result")
        .and_then(|result| result.get("items"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            format!(
                "getSignaturesForAsset {}: response omitted result.items",
                asset.mint
            )
        })?;
    parse_history_items(asset, items, max_sigs, false)
}

fn parse_address_history(asset: &SolanaAsset, payload: &Value, max_sigs: usize) -> AssetHistoryRow {
    if let Some(error) = payload.get("error") {
        return Err(format!(
            "getSignaturesForAddress fallback {}: JSON-RPC error {error}",
            asset.mint
        ));
    }
    let items = payload
        .get("result")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            format!(
                "getSignaturesForAddress fallback {}: response omitted result array",
                asset.mint
            )
        })?;
    parse_history_items(asset, items, max_sigs, true)
}

fn parse_history_items(
    asset: &SolanaAsset,
    items: &[Value],
    max_sigs: usize,
    force_truncated: bool,
) -> AssetHistoryRow {
    let page_truncated = items.len() >= max_sigs;
    let mut transfers = Vec::new();
    let mut sales = Vec::new();
    for item in items {
        let (signature, event_type) = parse_signature_item(item);
        if signature.is_empty() {
            continue;
        }
        let kind = event_type.to_ascii_lowercase();
        if kind.contains("sale") || kind.contains("buy") || kind.contains("list") {
            sales.push(SaleEvent {
                tx_hash: signature,
                token_id: asset.mint.clone(),
                seller: String::new(),
                buyer: String::new(),
                marketplace: Some("helius".into()),
                currency_symbol: Some("SOL".into()),
                ..SaleEvent::default()
            });
        } else {
            transfers.push(TransferEvent {
                tx_hash: signature,
                token_id: asset.mint.clone(),
                from: String::new(),
                to: asset.owner.clone().unwrap_or_default(),
                timestamp: None,
                block_number: None,
                is_mint: kind.contains("mint") || kind.contains("create"),
                gas_native: None,
                fee_payer: None,
                mint_payment_native: None,
                mint_payment_usd: None,
                mint_payment_receiver: None,
            });
        }
    }
    Ok((transfers, sales, force_truncated || page_truncated))
}

#[derive(Default)]
struct TransactionCell {
    value: AsyncMutex<Option<TransactionResult>>,
    notify: Notify,
}

type TransactionResult = Result<Option<Value>, String>;
type TransactionRow = (String, TransactionResult);

impl TransactionCell {
    async fn set(&self, value: TransactionResult) {
        *self.value.lock().await = Some(value);
        self.notify.notify_waiters();
    }

    async fn wait(&self) -> TransactionResult {
        loop {
            let notified = self.notify.notified();
            if let Some(value) = self.value.lock().await.clone() {
                return value;
            }
            notified.await;
        }
    }
}

type SharedTransactionCell = Arc<TransactionCell>;

/// Run-scoped `getTransaction` singleflight keyed by the case-sensitive Solana
/// signature. Successful payloads are reused across related candidates.
#[derive(Clone, Default)]
pub struct TransactionRequestCache {
    cells: Arc<AsyncMutex<AHashMap<String, SharedTransactionCell>>>,
}

impl TransactionRequestCache {
    async fn fetch_many(
        &self,
        client: &HttpClient,
        url: &str,
        signatures: &[String],
    ) -> Vec<TransactionRow> {
        const TX_RPC_BATCH_SIZE: usize = 100;
        let mut rows = Vec::with_capacity(signatures.len());
        let mut leaders = Vec::new();
        {
            let mut cells = self.cells.lock().await;
            for signature in signatures {
                if let Some(cell) = cells.get(signature) {
                    rows.push((signature.clone(), cell.clone()));
                    continue;
                }
                let cell = Arc::new(TransactionCell::default());
                cells.insert(signature.clone(), cell.clone());
                leaders.push((signature.clone(), cell.clone()));
                rows.push((signature.clone(), cell));
            }
        }
        if !leaders.is_empty() {
            let mut handles = Vec::with_capacity(leaders.len().div_ceil(TX_RPC_BATCH_SIZE).max(1));
            for (batch_idx, chunk) in leaders.chunks(TX_RPC_BATCH_SIZE).enumerate() {
                let client = client.clone();
                let url = url.to_owned();
                let signatures: Vec<String> = chunk
                    .iter()
                    .map(|(signature, _)| signature.clone())
                    .collect();
                handles.push(tokio::spawn(async move {
                    fetch_transactions_batch(&client, &url, batch_idx, &signatures).await
                }));
            }
            let mut fetched = AHashMap::new();
            for handle in handles {
                if let Ok(batch) = handle.await {
                    fetched.extend(batch);
                }
            }
            for (signature, cell) in leaders {
                let result = fetched
                    .remove(&signature)
                    .unwrap_or_else(|| Err("getTransaction batch join failed".into()));
                cell.set(result).await;
            }
        }

        let mut out = Vec::with_capacity(rows.len());
        let mut evict = Vec::new();
        for (signature, cell) in rows {
            let result = cell.wait().await;
            if !matches!(result, Ok(Some(_))) {
                evict.push((signature.clone(), cell.clone()));
            }
            out.push((signature, result));
        }
        if !evict.is_empty() {
            let mut cells = self.cells.lock().await;
            for (signature, cell) in evict {
                if cells
                    .get(&signature)
                    .is_some_and(|known| Arc::ptr_eq(known, &cell))
                {
                    cells.remove(&signature);
                }
            }
        }
        out
    }
}

/// Per-signature decode bookkeeping used for quality upgrades.
#[derive(Clone, Debug, Default)]
pub struct DecodeStats {
    pub requested: usize,
    pub fetched_ok: usize,
    pub fetch_failed: usize,
    pub null_result: usize,
    pub transfers_complete: usize,
    pub transfers_total: usize,
    pub sales_complete: usize,
    pub sales_total: usize,
}

impl DecodeStats {
    pub fn all_fetch_failed(&self) -> bool {
        self.requested > 0 && self.fetch_failed == self.requested
    }

    pub fn any_fetch_ok(&self) -> bool {
        self.fetched_ok > 0
    }

    pub fn transfers_all_complete(&self) -> bool {
        self.transfers_total > 0 && self.transfers_complete == self.transfers_total
    }

    pub fn sales_all_complete(&self) -> bool {
        self.sales_total > 0 && self.sales_complete == self.sales_total
    }
}

/// Dedupe signatures → `getTransaction` jsonParsed; attach from/to/timestamp/fee;
/// extract SOL [`ValueFlowEdge`]s involving addresses classified as operators.
///
/// Returns `(gas_outcome, value_flows_outcome, stats)`.
pub struct DecodeContext<'a> {
    pub candidate: &'a str,
    pub controllers: &'a [String],
    pub holders: HolderSnapshot<'a>,
    pub transfer_discovery_complete: bool,
}

pub async fn decode_and_attach_transactions(
    client: &HttpClient,
    rpc_url: &str,
    api_key: Option<&str>,
    context: DecodeContext<'_>,
    transfers: &mut [TransferEvent],
    sales: &mut [SaleEvent],
) -> (
    FetchOutcome<()>,
    FetchOutcome<Vec<ValueFlowEdge>>,
    DecodeStats,
) {
    decode_and_attach_transactions_impl(client, rpc_url, api_key, context, transfers, sales, None)
        .await
}

pub async fn decode_and_attach_transactions_cached(
    client: &HttpClient,
    rpc_url: &str,
    api_key: Option<&str>,
    context: DecodeContext<'_>,
    transfers: &mut [TransferEvent],
    sales: &mut [SaleEvent],
    cache: &TransactionRequestCache,
) -> (
    FetchOutcome<()>,
    FetchOutcome<Vec<ValueFlowEdge>>,
    DecodeStats,
) {
    decode_and_attach_transactions_impl(
        client,
        rpc_url,
        api_key,
        context,
        transfers,
        sales,
        Some(cache),
    )
    .await
}

async fn decode_and_attach_transactions_impl(
    client: &HttpClient,
    rpc_url: &str,
    api_key: Option<&str>,
    context: DecodeContext<'_>,
    transfers: &mut [TransferEvent],
    sales: &mut [SaleEvent],
    cache: Option<&TransactionRequestCache>,
) -> (
    FetchOutcome<()>,
    FetchOutcome<Vec<ValueFlowEdge>>,
    DecodeStats,
) {
    let mut stats = DecodeStats {
        transfers_total: transfers.len(),
        sales_total: sales.len(),
        ..DecodeStats::default()
    };

    let Some(api_key) = api_key else {
        return (
            FetchOutcome::skipped("helius_get_transaction"),
            FetchOutcome::skipped("helius_value_flows"),
            stats,
        );
    };

    let mut sig_mints: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for t in transfers.iter() {
        let sig = t.tx_hash.trim();
        if !sig.is_empty() {
            sig_mints
                .entry(sig.to_owned())
                .or_default()
                .insert(t.token_id.clone());
        }
    }
    for s in sales.iter() {
        let sig = s.tx_hash.trim();
        if !sig.is_empty() {
            sig_mints
                .entry(sig.to_owned())
                .or_default()
                .insert(s.token_id.clone());
        }
    }

    if sig_mints.is_empty() {
        return (
            FetchOutcome::ok((), 0, false, "helius", "helius_get_transaction"),
            FetchOutcome::ok(Vec::new(), 0, false, "helius", "helius_value_flows"),
            stats,
        );
    }

    let url = with_api_key(rpc_url, api_key);
    let signatures: Vec<String> = sig_mints.keys().cloned().collect();
    stats.requested = signatures.len();

    // JSON-RPC batch (fallback to per-signature on parse/transport failure).
    // Cuts HTTP round-trips vs one request per signature for large histories.
    // Helius allows up to 100 getTransaction calls in one historical RPC
    // batch. The fallback below preserves per-signature results on a partial
    // or malformed batch response.
    const TX_RPC_BATCH_SIZE: usize = 100;
    let transaction_rows = if let Some(cache) = cache {
        cache.fetch_many(client, &url, &signatures).await
    } else {
        let mut handles = Vec::with_capacity(signatures.len().div_ceil(TX_RPC_BATCH_SIZE).max(1));
        for (batch_idx, chunk) in signatures.chunks(TX_RPC_BATCH_SIZE).enumerate() {
            let client = client.clone();
            let url = url.clone();
            let chunk: Vec<String> = chunk.to_vec();
            handles.push(tokio::spawn(async move {
                fetch_transactions_batch(&client, &url, batch_idx, &chunk).await
            }));
        }
        let mut transaction_rows = Vec::with_capacity(signatures.len());
        for handle in handles {
            match handle.await {
                Ok(batch) => transaction_rows.extend(batch),
                Err(error) => transaction_rows.push((
                    String::new(),
                    Err(format!("getTransaction join failed: {error}")),
                )),
            }
        }
        transaction_rows
    };

    let mut decoded: AHashMap<String, DecodedTx> = AHashMap::new();
    let mut failures = Vec::new();
    for (sig, result) in transaction_rows {
        match result {
            Ok(Some(payload)) => {
                stats.fetched_ok += 1;
                let mints = sig_mints.get(&sig).cloned().unwrap_or_default();
                decoded.insert(sig.clone(), parse_decoded_tx(&sig, &payload, &mints));
            }
            Ok(None) => {
                stats.null_result += 1;
            }
            Err(err) => {
                stats.fetch_failed += 1;
                failures.push(format!("{sig}: {err}"));
            }
        }
    }

    let mut preliminary_operators: BTreeSet<String> = BTreeSet::new();
    insert_sol_addr(&mut preliminary_operators, context.candidate);
    for c in context.controllers {
        insert_sol_addr(&mut preliminary_operators, c);
    }

    for transfer in transfers.iter_mut() {
        let Some(tx) = decoded.get(transfer.tx_hash.trim()) else {
            continue;
        };
        apply_transfer_decode(transfer, tx);
    }
    for sale in sales.iter_mut() {
        let Some(tx) = decoded.get(sale.tx_hash.trim()) else {
            continue;
        };
        apply_sale_decode(sale, tx);
    }
    stats.transfers_complete = transfers
        .iter()
        .filter(|t| {
            let Some(tx) = decoded.get(t.tx_hash.trim()) else {
                return false;
            };
            let had_owner_change = tx.owner_changes.contains_key(&t.token_id);
            transfer_fields_complete(t, had_owner_change)
        })
        .count();
    stats.sales_complete = sales.iter().filter(|s| sale_fields_complete(s)).count();
    // A decoded Solana mint transaction with one buyer-funded SOL receiver is
    // direct payment evidence even when the receiver is a Candy Machine or
    // treasury account that differs from the collection/update authority.
    // Current-holder evidence is attached by the outer bundle after decode, so
    // this preliminary value-flow query conservatively treats no address as a
    // confirmed still-holding buyer.
    let preliminary_flows = sol_value_flows(&decoded, &preliminary_operators);
    super::mint_payment::attach_mint_payments(
        transfers,
        &preliminary_flows,
        &[],
        "solana",
        &AHashMap::new(),
    );
    let operator_seeds = super::value_flow::derive_operator_seeds(
        "solana",
        context.candidate,
        context.controllers,
        None,
        transfers,
        sales,
        context.holders,
    );
    let mut operators = BTreeSet::new();
    for operator in operator_seeds {
        insert_sol_addr(&mut operators, &operator);
    }
    let value_flows = sol_value_flows(&decoded, &operators);

    let gas_outcome = gas_outcome_from_stats(&stats, transfers, &failures);
    let vf_outcome = value_flow_outcome(value_flows, &stats, &failures);

    (gas_outcome, vf_outcome, stats)
}

fn sol_value_flows(
    decoded: &AHashMap<String, DecodedTx>,
    operators: &BTreeSet<String>,
) -> Vec<ValueFlowEdge> {
    let mut value_flows = Vec::new();
    let mut seen_edges = HashSet::new();
    for tx in decoded.values() {
        for mv in &tx.native_moves {
            if let Some(edge) = classify_sol_edge(tx, mv, operators) {
                let key = (
                    edge.tx_hash.clone(),
                    edge.event_id.clone(),
                    edge.from.clone(),
                    edge.to.clone(),
                    edge.kind,
                );
                if seen_edges.insert(key) {
                    value_flows.push(edge);
                }
            }
        }
    }
    value_flows
}

fn get_transaction_params(signature: &str) -> Value {
    json!([
        signature,
        {
            "encoding": "jsonParsed",
            "commitment": "finalized",
            "maxSupportedTransactionVersion": 0
        }
    ])
}

async fn fetch_transactions_batch(
    client: &HttpClient,
    url: &str,
    batch_idx: usize,
    signatures: &[String],
) -> Vec<TransactionRow> {
    if signatures.is_empty() {
        return Vec::new();
    }
    let body = Value::Array(
        signatures
            .iter()
            .enumerate()
            .map(|(i, signature)| {
                json!({
                    "jsonrpc": "2.0",
                    "id": format!("tx-{batch_idx}-{i}"),
                    "method": "getTransaction",
                    "params": get_transaction_params(signature)
                })
            })
            .collect(),
    );
    match client.post_json_helius(url, &[], &body).await {
        Ok(payload) => match parse_transaction_batch_payload(&payload, batch_idx, signatures) {
            Ok(rows) => rows,
            Err(_) => fetch_transactions_singles(client, url, batch_idx, signatures).await,
        },
        Err(_) => fetch_transactions_singles(client, url, batch_idx, signatures).await,
    }
}

fn parse_transaction_batch_payload(
    payload: &Value,
    batch_idx: usize,
    signatures: &[String],
) -> Result<Vec<TransactionRow>, ()> {
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
    let mut out = Vec::with_capacity(signatures.len());
    for (i, signature) in signatures.iter().enumerate() {
        let id = format!("tx-{batch_idx}-{i}");
        let response = by_id.get(&id).copied().or_else(|| responses.get(i));
        match response {
            Some(response) => {
                out.push((signature.clone(), transaction_from_rpc_response(response)))
            }
            None => out.push((signature.clone(), Err("missing batch response".into()))),
        }
    }
    Ok(out)
}

fn transaction_from_rpc_response(response: &Value) -> Result<Option<Value>, String> {
    if let Some(error) = response.get("error") {
        return Err(error.to_string());
    }
    match response.get("result") {
        None | Some(Value::Null) => Ok(None),
        Some(result) => Ok(Some(result.clone())),
    }
}

async fn fetch_transactions_singles(
    client: &HttpClient,
    url: &str,
    batch_idx: usize,
    signatures: &[String],
) -> Vec<TransactionRow> {
    let mut handles = Vec::with_capacity(signatures.len());
    for (i, signature) in signatures.iter().cloned().enumerate() {
        let client = client.clone();
        let url = url.to_owned();
        handles.push(tokio::spawn(async move {
            let body = json!({
                "jsonrpc": "2.0",
                "id": format!("tx-{batch_idx}-{i}"),
                "method": "getTransaction",
                "params": get_transaction_params(&signature)
            });
            match client.post_json_helius(&url, &[], &body).await {
                Ok(payload) => (signature, transaction_from_rpc_response(&payload)),
                Err(e) => (signature, Err(e.to_string())),
            }
        }));
    }
    let mut out = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(row) => out.push(row),
            Err(e) => out.push((
                String::new(),
                Err(format!("getTransaction join failed: {e}")),
            )),
        }
    }
    out
}

/// Field quality after decode. Signature-only stubs never become Complete.
pub fn field_status_after_decode(
    empty: bool,
    page_truncated: bool,
    all_complete: bool,
    stats: &DecodeStats,
) -> EvidenceStatus {
    if empty && !page_truncated {
        return EvidenceStatus::Empty;
    }
    if stats.all_fetch_failed() {
        return EvidenceStatus::Failed;
    }
    if page_truncated || !all_complete || !stats.any_fetch_ok() {
        return EvidenceStatus::Truncated;
    }
    if empty {
        EvidenceStatus::Empty
    } else {
        EvidenceStatus::Complete
    }
}

/// Combined histories status across transfers + sales.
pub fn histories_status_after_decode(
    transfers_empty: bool,
    sales_empty: bool,
    page_truncated: bool,
    stats: &DecodeStats,
) -> EvidenceStatus {
    let empty = transfers_empty && sales_empty;
    let all_complete = (transfers_empty || stats.transfers_all_complete())
        && (sales_empty || stats.sales_all_complete());
    field_status_after_decode(empty, page_truncated, all_complete, stats)
}

fn gas_outcome_from_stats(
    stats: &DecodeStats,
    transfers: &[TransferEvent],
    failures: &[String],
) -> FetchOutcome<()> {
    if stats.requested == 0 {
        return FetchOutcome::ok((), 0, false, "helius", "helius_get_transaction");
    }
    if stats.all_fetch_failed() {
        let detail = if failures.is_empty() {
            "all getTransaction fetches failed".into()
        } else {
            failures.join("; ")
        };
        return FetchOutcome::failed("helius", "helius_get_transaction", detail);
    }
    let with_fee = transfers.iter().filter(|t| t.gas_native.is_some()).count();
    let fee_complete = !transfers.is_empty() && with_fee == transfers.len();
    let truncated = !fee_complete
        || stats.fetch_failed > 0
        || stats.null_result > 0
        || stats.fetched_ok < stats.requested;
    let count = if with_fee > 0 || stats.fetched_ok > 0 {
        with_fee.max(1)
    } else {
        0
    };
    let mut outcome = FetchOutcome::ok((), count, truncated, "helius", "helius_get_transaction");
    // Prefer gas Complete when every transfer has fee and every sig fetched.
    if fee_complete && stats.fetched_ok == stats.requested && stats.fetch_failed == 0 {
        outcome.status = EvidenceStatus::Complete;
        if let Some(obs) = outcome.observation.as_mut() {
            obs.status = EvidenceStatus::Complete;
        }
        outcome.truncated = false;
    }
    if truncated && !failures.is_empty() {
        outcome.failure = Some(format!(
            "helius_get_transaction: partial failures ({}/{}): {}",
            failures.len(),
            stats.requested,
            failures
                .iter()
                .take(3)
                .cloned()
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    outcome
}

fn value_flow_outcome(
    edges: Vec<ValueFlowEdge>,
    stats: &DecodeStats,
    failures: &[String],
) -> FetchOutcome<Vec<ValueFlowEdge>> {
    if stats.requested == 0 {
        return FetchOutcome::ok(edges, 0, false, "helius", "helius_value_flows");
    }
    if stats.all_fetch_failed() {
        let detail = if failures.is_empty() {
            "all getTransaction fetches failed".into()
        } else {
            failures.join("; ")
        };
        return FetchOutcome::failed("helius", "helius_value_flows", detail);
    }
    let truncated =
        stats.fetch_failed > 0 || stats.null_result > 0 || stats.fetched_ok < stats.requested;
    let count = edges.len();
    FetchOutcome::ok(edges, count, truncated, "helius", "helius_value_flows")
}

#[derive(Clone, Debug, Default)]
struct NativeSolMove {
    event_id: String,
    from: String,
    to: String,
    amount_sol: f64,
}

#[derive(Clone, Debug, Default)]
struct DecodedTx {
    signature: String,
    fee_payer: Option<String>,
    fee_sol: Option<f64>,
    timestamp: Option<i64>,
    slot: Option<u64>,
    /// mint → (from, to, is_mint)
    owner_changes: HashMap<String, (Option<String>, String, bool)>,
    native_moves: Vec<NativeSolMove>,
    failed: bool,
}

fn parse_decoded_tx(signature: &str, result: &Value, mints: &BTreeSet<String>) -> DecodedTx {
    let mut tx = DecodedTx {
        signature: signature.to_owned(),
        timestamp: result.get("blockTime").and_then(Value::as_i64),
        slot: result.get("slot").and_then(Value::as_u64),
        ..DecodedTx::default()
    };
    let meta = result.get("meta").unwrap_or(&Value::Null);
    if meta.get("err").is_some_and(|e| !e.is_null()) {
        tx.failed = true;
        return tx;
    }
    let transaction = result.get("transaction").unwrap_or(&Value::Null);
    let message = transaction.get("message").unwrap_or(&Value::Null);
    let accounts: Vec<String> = message
        .get("accountKeys")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(account_key)
        .collect();
    tx.fee_payer = accounts.first().cloned().filter(|s| !s.is_empty());
    if let Some(fee) = meta.get("fee").and_then(Value::as_i64) {
        if fee > 0 {
            tx.fee_sol = Some(fee as f64 / LAMPORTS_PER_SOL);
        } else if fee == 0 {
            tx.fee_sol = Some(0.0);
        }
    }
    for mint in mints {
        if let Some(change) = token_owner_change(meta, mint) {
            tx.owner_changes.insert(mint.clone(), change);
        }
    }

    tx.native_moves = parse_native_sol_moves(meta, message, &accounts, tx.fee_sol.unwrap_or(0.0));
    tx
}

fn apply_transfer_decode(transfer: &mut TransferEvent, tx: &DecodedTx) {
    if tx.failed {
        return;
    }
    if transfer.timestamp.is_none() {
        transfer.timestamp = tx.timestamp;
    }
    if transfer.block_number.is_none() {
        transfer.block_number = tx.slot;
    }
    if transfer.gas_native.is_none() {
        transfer.gas_native = tx.fee_sol;
    }
    if transfer.fee_payer.is_none() {
        transfer.fee_payer = tx.fee_payer.clone();
    }
    if let Some((from, to, is_mint)) = tx.owner_changes.get(&transfer.token_id) {
        transfer.is_mint = *is_mint || transfer.is_mint;
        if let Some(f) = from {
            if !f.is_empty() {
                transfer.from = f.clone();
            }
        } else if *is_mint {
            transfer.from.clear();
        }
        if !to.is_empty() {
            transfer.to = to.clone();
        }
    }
    if transfer.is_mint && !transfer.to.is_empty() {
        let mut total = 0.0;
        let mut receivers = BTreeSet::new();
        for movement in &tx.native_moves {
            if movement.from == transfer.to
                && movement.to != transfer.to
                && movement.amount_sol > 0.0
            {
                total += movement.amount_sol;
                receivers.insert(movement.to.clone());
            }
        }
        if total > 0.0 {
            transfer.mint_payment_native = Some(total);
            transfer.mint_payment_receiver = (receivers.len() == 1)
                .then(|| receivers.into_iter().next())
                .flatten();
        }
    }
}

fn apply_sale_decode(sale: &mut SaleEvent, tx: &DecodedTx) {
    if tx.failed {
        return;
    }
    if sale.timestamp.is_none() {
        sale.timestamp = tx.timestamp;
    }
    if sale.block_number.is_none() {
        sale.block_number = tx.slot;
    }
    let Some((from, to, is_mint)) = tx.owner_changes.get(&sale.token_id) else {
        return;
    };
    if *is_mint {
        return;
    }
    let seller = from.clone().unwrap_or_default();
    let buyer = to.clone();
    if seller.is_empty() || buyer.is_empty() || seller == buyer {
        return;
    }
    sale.seller = seller.clone();
    sale.buyer = buyer.clone();

    // Optional price: native SOL from buyer → seller.
    if sale.native_amount.is_none() {
        let paid: f64 = tx
            .native_moves
            .iter()
            .filter(|m| m.from == buyer && m.to == seller && m.amount_sol > 0.0)
            .map(|m| m.amount_sol)
            .sum();
        if paid > 0.0 {
            sale.native_amount = Some(paid);
            sale.seller_proceeds_native = Some(paid);
            if sale.currency_symbol.is_none() {
                sale.currency_symbol = Some("SOL".into());
            }
        }
    }
}

fn transfer_fields_complete(t: &TransferEvent, had_owner_change: bool) -> bool {
    // Mint/create must not be Complete without a successful ownership/token-balance
    // decode. Missing pre/postTokenBalances (Bubblegum/compressed) → Truncated.
    if !had_owner_change {
        return false;
    }
    let to_ok = !t.to.trim().is_empty();
    let from_ok = t.is_mint || !t.from.trim().is_empty();
    to_ok && from_ok && t.timestamp.is_some() && t.gas_native.is_some()
}

fn sale_fields_complete(s: &SaleEvent) -> bool {
    !s.seller.trim().is_empty() && !s.buyer.trim().is_empty() && s.timestamp.is_some()
}

fn classify_sol_edge(
    tx: &DecodedTx,
    mv: &NativeSolMove,
    operators: &BTreeSet<String>,
) -> Option<ValueFlowEdge> {
    if mv.from.is_empty() || mv.to.is_empty() || mv.from == mv.to || mv.amount_sol <= 0.0 {
        return None;
    }
    let from_op = operators.contains(&mv.from);
    let to_op = operators.contains(&mv.to);
    if !from_op && !to_op {
        return None;
    }
    let kind = match (from_op, to_op) {
        (false, true) => ValueFlowKind::Funding,
        (true, false) => ValueFlowKind::Withdrawal,
        (true, true) => ValueFlowKind::RevenueBackflow,
        (false, false) => return None,
    };
    Some(ValueFlowEdge {
        tx_hash: tx.signature.clone(),
        event_id: Some(mv.event_id.clone()),
        from: mv.from.clone(),
        to: mv.to.clone(),
        kind,
        native_amount: Some(mv.amount_sol),
        usd_amount: None,
        timestamp: tx.timestamp,
        gas_native: tx.fee_sol,
        fee_payer: tx.fee_payer.clone(),
    })
}

fn insert_sol_addr(set: &mut BTreeSet<String>, raw: &str) {
    let addr = raw.trim();
    if addr.is_empty() {
        return;
    }
    set.insert(addr.to_owned());
}

fn account_key(value: &Value) -> Option<String> {
    value
        .as_str()
        .or_else(|| value.get("pubkey").and_then(Value::as_str))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

#[derive(Clone, Debug, Default)]
struct TokenBalanceEntry {
    owner: String,
    amount: i128,
}

fn token_balances(rows: Option<&Value>, mint_address: &str) -> HashMap<u64, TokenBalanceEntry> {
    rows.and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|row| row.get("mint").and_then(Value::as_str) == Some(mint_address))
        .filter_map(|row| {
            let account_index = row.get("accountIndex").and_then(Value::as_u64)?;
            let amount = row
                .get("uiTokenAmount")
                .and_then(|amount| amount.get("amount"))
                .and_then(Value::as_str)
                .and_then(|amount| amount.parse::<i128>().ok())?;
            Some((
                account_index,
                TokenBalanceEntry {
                    owner: row
                        .get("owner")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string(),
                    amount,
                },
            ))
        })
        .collect()
}

fn token_owner_change(meta: &Value, mint_address: &str) -> Option<(Option<String>, String, bool)> {
    let before = token_balances(meta.get("preTokenBalances"), mint_address);
    let after = token_balances(meta.get("postTokenBalances"), mint_address);
    let total_before = before.values().map(|entry| entry.amount).sum::<i128>();
    let mut indexes = before
        .keys()
        .chain(after.keys())
        .copied()
        .collect::<HashSet<_>>();
    let mut source: Option<(i128, String)> = None;
    let mut destination: Option<(i128, String)> = None;
    for index in indexes.drain() {
        let pre = before.get(&index).cloned().unwrap_or_default();
        let post = after.get(&index).cloned().unwrap_or_default();
        let delta = post.amount - pre.amount;
        if delta < 0 && !pre.owner.is_empty() {
            if source.as_ref().is_none_or(|(amount, _)| -delta > *amount) {
                source = Some((-delta, pre.owner));
            }
        } else if delta > 0
            && !post.owner.is_empty()
            && destination
                .as_ref()
                .is_none_or(|(amount, _)| delta > *amount)
        {
            destination = Some((delta, post.owner));
        } else if delta == 0
            && pre.amount > 0
            && !pre.owner.is_empty()
            && !post.owner.is_empty()
            && pre.owner != post.owner
        {
            // Same token account index, ownership reassigned without amount delta.
            source = Some((pre.amount, pre.owner));
            destination = Some((post.amount, post.owner));
        }
    }
    let (_, to) = destination?;
    let is_mint = total_before == 0;
    let from = (!is_mint).then(|| source.map(|(_, owner)| owner)).flatten();
    if !is_mint && from.is_none() {
        return None;
    }
    Some((from, to, is_mint))
}

/// Native SOL movements: prefer parsed system transfers; fall back to pre/post balance deltas.
fn parse_native_sol_moves(
    meta: &Value,
    message: &Value,
    accounts: &[String],
    fee_sol: f64,
) -> Vec<NativeSolMove> {
    let mut moves = Vec::new();
    let mut instructions = message
        .get("instructions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    for group in meta
        .get("innerInstructions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        instructions.extend(
            group
                .get("instructions")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        );
    }
    for (instruction_index, instruction) in instructions.iter().enumerate() {
        let Some(parsed) = instruction.get("parsed") else {
            continue;
        };
        let instruction_type = parsed.get("type").and_then(Value::as_str).unwrap_or("");
        if !matches!(instruction_type, "transfer" | "transferWithSeed") {
            continue;
        }
        let info = parsed.get("info").unwrap_or(&Value::Null);
        let Some(lamports) = info.get("lamports").and_then(Value::as_u64) else {
            continue;
        };
        if lamports == 0 {
            continue;
        }
        let from = info
            .get("source")
            .or_else(|| info.get("from"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_owned();
        let to = info
            .get("destination")
            .or_else(|| info.get("to"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_owned();
        if from.is_empty() || to.is_empty() {
            continue;
        }
        moves.push(NativeSolMove {
            event_id: format!("instruction:{instruction_index}"),
            from,
            to,
            amount_sol: lamports as f64 / LAMPORTS_PER_SOL,
        });
    }
    if !moves.is_empty() {
        return moves;
    }

    // Fallback: balance deltas excluding fee payer's fee-only debit.
    let Some(pre) = meta.get("preBalances").and_then(Value::as_array) else {
        return moves;
    };
    let Some(post) = meta.get("postBalances").and_then(Value::as_array) else {
        return moves;
    };
    if pre.len() != post.len() || pre.len() != accounts.len() {
        return moves;
    }
    let mut deltas: Vec<(usize, i128)> = Vec::new();
    for (i, account) in accounts.iter().enumerate() {
        if account.is_empty() {
            continue;
        }
        let before = pre[i].as_u64().unwrap_or_default() as i128;
        let after = post[i].as_u64().unwrap_or_default() as i128;
        let mut delta = after - before;
        if i == 0 {
            // Remove fee debit so remaining delta is transferable SOL.
            delta += (fee_sol * LAMPORTS_PER_SOL).round() as i128;
        }
        if delta != 0 {
            deltas.push((i, delta));
        }
    }
    let sinks: Vec<_> = deltas.iter().filter(|(_, d)| *d > 0).copied().collect();
    let sources: Vec<_> = deltas.iter().filter(|(_, d)| *d < 0).copied().collect();
    if sources.len() == 1 && sinks.len() == 1 {
        let (si, sd) = sources[0];
        let (di, dd) = sinks[0];
        let amount = (-sd).min(dd) as f64 / LAMPORTS_PER_SOL;
        if amount > 0.0 {
            moves.push(NativeSolMove {
                event_id: format!("balance:{si}:{di}"),
                from: accounts[si].clone(),
                to: accounts[di].clone(),
                amount_sol: amount,
            });
        }
    }
    moves
}

pub fn holders_from_assets(assets: &[SolanaAsset]) -> Vec<HolderRecord> {
    assets
        .iter()
        .filter_map(|asset| {
            asset.owner.as_ref().map(|owner| HolderRecord {
                token_id: asset.mint.clone(),
                owner: owner.clone(),
                balance: Some(1),
            })
        })
        .collect()
}

fn is_nft_asset_payload(item: &Value) -> bool {
    let interface_is_nft = item
        .get("interface")
        .and_then(Value::as_str)
        .is_some_and(|interface| {
            matches!(
                interface.to_ascii_lowercase().as_str(),
                "v1_nft"
                    | "v1_print"
                    | "legacy_nft"
                    | "v2_nft"
                    | "programmablenft"
                    | "mplcoreasset"
                    | "mplbubblegumv2"
            )
        });
    let standard_is_nft = item
        .get("content")
        .and_then(|content| content.get("metadata"))
        .and_then(|metadata| metadata.get("token_standard"))
        .and_then(Value::as_str)
        .is_some_and(|standard| standard.to_ascii_lowercase().contains("nonfungible"));
    interface_is_nft || standard_is_nft
}

fn json_u64(value: Option<&Value>) -> Option<u64> {
    value.and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
    })
}

fn custom_payload_is_single_nft(item: &Value) -> bool {
    let token_info = item.get("token_info");
    let decimals = json_u64(token_info.and_then(|value| value.get("decimals")));
    let supply = json_u64(token_info.and_then(|value| value.get("supply")));
    let single_owner = item
        .get("ownership")
        .and_then(|value| value.get("ownership_model"))
        .and_then(Value::as_str)
        .is_none_or(|model| model.eq_ignore_ascii_case("single"));
    decimals == Some(0) && supply == Some(1) && single_owner
}

async fn verify_single_nft_mint(client: &HttpClient, url: &str, mint: &str) -> bool {
    let body = json!({
        "jsonrpc": "2.0",
        "id": format!("mint-identity-{mint}"),
        "method": "getAccountInfo",
        "params": [mint, {"encoding": "jsonParsed", "commitment": "confirmed"}]
    });
    let Ok(payload) = client.post_json_helius(url, &[], &body).await else {
        return false;
    };
    let info = payload
        .get("result")
        .and_then(|value| value.get("value"))
        .and_then(|value| value.get("data"))
        .and_then(|value| value.get("parsed"))
        .and_then(|value| value.get("info"));
    let decimals = json_u64(info.and_then(|value| value.get("decimals")));
    let supply = json_u64(info.and_then(|value| value.get("supply")));
    decimals == Some(0) && supply == Some(1)
}

fn is_explicit_fungible_interface(interface: &str) -> bool {
    matches!(
        interface.to_ascii_lowercase().as_str(),
        "fungibleasset" | "fungibletoken"
    )
}

fn is_explicit_non_nft_interface(interface: &str) -> bool {
    matches!(
        interface.to_ascii_lowercase().as_str(),
        "identity" | "executable" | "mplcorecollection" | "mplcoregroup"
    )
}

fn helius_row_is_explicitly_non_nft(item: &Value) -> bool {
    item.get("interface")
        .and_then(Value::as_str)
        .is_some_and(|interface| {
            is_explicit_fungible_interface(interface) || is_explicit_non_nft_interface(interface)
        })
}

fn parse_asset(item: &Value) -> Option<SolanaAsset> {
    let mint = item.get("id").and_then(Value::as_str)?.trim().to_owned();
    if mint.is_empty() {
        return None;
    }
    let owner = item
        .get("ownership")
        .and_then(|o| o.get("owner"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    let compressed = item
        .get("compression")
        .and_then(|c| c.get("compressed"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Some(SolanaAsset {
        mint,
        owner,
        compressed,
    })
}

fn parse_signature_item(item: &Value) -> (String, String) {
    if let Some(arr) = item.as_array() {
        let sig = arr.first().and_then(Value::as_str).unwrap_or("").to_owned();
        let event = arr
            .get(1)
            .and_then(Value::as_str)
            .unwrap_or("transfer")
            .to_owned();
        return (sig, event);
    }
    let sig = item
        .get("signature")
        .or_else(|| item.get("id"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let event = item
        .get("type")
        .or_else(|| item.get("event"))
        .and_then(Value::as_str)
        .unwrap_or("transfer")
        .to_owned();
    (sig, event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::{Method::POST, MockServer};
    use serde_json::json;

    use crate::enrich::types::{EvidenceStatus, status_from_count};
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn unverified_collection_retry_can_recover_resident_asset() {
        let server = MockServer::start_async().await;
        let assets = server
            .mock_async(|when, then| {
                when.method(POST)
                    .body_contains("getAssetsByGroup")
                    .body_contains("\"showUnverifiedCollections\":true");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "assets-1",
                    "result": {
                        "total": 1,
                        "items": [{
                            "id": "resident-mint",
                            "ownership": {"owner": "holder"},
                            "compression": {"compressed": false}
                        }]
                    }
                }));
            })
            .await;
        let client = HttpClient::with_retries(2, 0).unwrap();
        let outcome = fetch_collection_assets_including_unverified(
            &client,
            &server.base_url(),
            Some("key"),
            "collection",
            10,
        )
        .await;

        assert_eq!(assets.hits(), 1);
        assert_eq!(outcome.status, EvidenceStatus::Complete);
        assert_eq!(outcome.value.assets.len(), 1);
        assert_eq!(outcome.value.assets[0].mint, "resident-mint");
    }

    #[tokio::test]
    async fn direct_asset_retry_recovers_singleton_without_collection_grouping() {
        let server = MockServer::start_async().await;
        let mint = "So11111111111111111111111111111111111111112";
        let asset = server
            .mock_async(|when, then| {
                when.method(POST)
                    .body_contains("getAsset")
                    .body_contains("So11111111111111111111111111111111111111112");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "asset",
                    "result": {
                        "interface": "V1_NFT",
                        "id": "So11111111111111111111111111111111111111112",
                        "ownership": {"owner": "holder"},
                        "compression": {"compressed": false},
                        "grouping": [],
                        "creators": [{
                            "address": "creator",
                            "verified": true
                        }]
                    }
                }));
            })
            .await;
        let client = HttpClient::with_retries(2, 0).unwrap();
        let outcome = fetch_assets_by_ids(
            &client,
            &server.base_url(),
            Some("key"),
            &[mint.to_owned()],
            10,
        )
        .await;

        assert_eq!(asset.hits(), 1);
        assert_eq!(outcome.status, EvidenceStatus::Complete);
        assert!(!outcome.truncated);
        assert_eq!(outcome.value.assets.len(), 1);
        assert_eq!(outcome.value.total, Some(1));
        assert_eq!(outcome.value.assets[0].mint, mint);
        assert_eq!(outcome.value.authority, vec!["creator"]);
    }

    #[tokio::test]
    async fn direct_asset_explicit_fungible_is_identity_rejection_not_api_failure() {
        let server = MockServer::start_async().await;
        let mint = "So11111111111111111111111111111111111111112";
        let asset = server
            .mock_async(|when, then| {
                when.method(POST).body_contains("getAsset");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "asset",
                    "result": {
                        "interface": "FungibleAsset",
                        "id": mint
                    }
                }));
            })
            .await;
        let client = HttpClient::with_retries(2, 0).unwrap();
        let outcome = fetch_assets_by_ids(
            &client,
            &server.base_url(),
            Some("key"),
            &[mint.to_owned()],
            10,
        )
        .await;

        assert_eq!(asset.hits(), 1);
        assert_eq!(outcome.status, EvidenceStatus::Empty);
        assert!(outcome.failure.is_none());
        assert!(outcome.value.assets.is_empty());
        assert_eq!(outcome.value.rejected_non_nft_ids, vec![mint]);
    }

    #[tokio::test]
    async fn direct_custom_asset_uses_mint_shape_fallback() {
        let server = MockServer::start_async().await;
        let mint = "So11111111111111111111111111111111111111112";
        let asset = server
            .mock_async(|when, then| {
                when.method(POST).body_contains("getAsset");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "result": {
                        "interface": "Custom",
                        "id": mint,
                        "ownership": {"owner": "holder"},
                        "grouping": []
                    }
                }));
            })
            .await;
        let mint_info = server
            .mock_async(|when, then| {
                when.method(POST).body_contains("getAccountInfo");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "result": {"value": {"data": {"parsed": {"info": {
                        "decimals": 0,
                        "supply": "1"
                    }}}}}
                }));
            })
            .await;
        let client = HttpClient::with_retries(2, 0).unwrap();
        let outcome = fetch_assets_by_ids(
            &client,
            &server.base_url(),
            Some("key"),
            &[mint.to_owned()],
            10,
        )
        .await;

        assert_eq!(asset.hits(), 1);
        assert_eq!(mint_info.hits(), 1);
        assert_eq!(outcome.status, EvidenceStatus::Complete);
        assert_eq!(outcome.value.assets.len(), 1);
        assert!(outcome.failure.is_none());
    }

    #[tokio::test]
    async fn direct_grouped_asset_remains_truncated_without_collection_enumeration() {
        let server = MockServer::start_async().await;
        let mint = "So11111111111111111111111111111111111111112";
        let _asset = server
            .mock_async(|when, then| {
                when.method(POST).body_contains("getAsset");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "asset",
                    "result": {
                        "interface": "V1_NFT",
                        "id": mint,
                        "ownership": {"owner": "holder"},
                        "grouping": [{
                            "group_key": "collection",
                            "group_value": mint
                        }]
                    }
                }));
            })
            .await;
        let client = HttpClient::with_retries(2, 0).unwrap();
        let outcome = fetch_assets_by_ids(
            &client,
            &server.base_url(),
            Some("key"),
            &[mint.to_owned()],
            10,
        )
        .await;

        assert_eq!(outcome.status, EvidenceStatus::Truncated);
        assert!(outcome.truncated);
        assert_eq!(outcome.value.total, None);
    }

    #[test]
    fn direct_asset_identity_accepts_only_explicit_nft_payloads() {
        assert!(is_nft_asset_payload(
            &json!({"interface": "ProgrammableNFT"})
        ));
        assert!(is_nft_asset_payload(&json!({"interface": "MplCoreAsset"})));
        assert!(is_nft_asset_payload(&json!({
            "interface": "MplBubblegumV2"
        })));
        assert!(custom_payload_is_single_nft(&json!({
            "interface": "Custom",
            "token_info": {"decimals": 0, "supply": "1"},
            "ownership": {"ownership_model": "single"}
        })));
        assert!(is_nft_asset_payload(&json!({
            "content": {"metadata": {"token_standard": "NonFungible"}}
        })));
        assert!(!is_nft_asset_payload(
            &json!({"interface": "FungibleToken"})
        ));
        assert!(!is_nft_asset_payload(&json!({"id": "unknown"})));
    }

    #[test]
    fn asset_history_preserves_provider_error_detail() {
        let asset = SolanaAsset {
            mint: "mint-1".into(),
            ..SolanaAsset::default()
        };
        let error = parse_asset_history(
            &asset,
            &json!({"error": {"code": -32602, "message": "invalid asset"}}),
            10,
        )
        .unwrap_err();
        assert!(error.contains("mint-1"));
        assert!(error.contains("-32602"));
        assert!(error.contains("invalid asset"));
    }

    #[test]
    fn tree_not_found_fallback_requires_exact_ordinary_error() {
        let ordinary = SolanaAsset {
            mint: "ordinary".into(),
            compressed: false,
            ..SolanaAsset::default()
        };
        let compressed = SolanaAsset {
            mint: "compressed".into(),
            compressed: true,
            ..SolanaAsset::default()
        };
        let tree_not_found = json!({
            "error": {
                "code": -32000,
                "message": "Database Error: RecordNotFound Error: Tree not found"
            }
        });

        assert!(ordinary_asset_tree_not_found(&ordinary, &tree_not_found));
        assert!(!ordinary_asset_tree_not_found(&compressed, &tree_not_found));
        assert!(!ordinary_asset_tree_not_found(
            &ordinary,
            &json!({"error": {"code": -32602, "message": "Tree not found"}})
        ));
        assert!(!ordinary_asset_tree_not_found(
            &ordinary,
            &json!({"error": {"code": -32000, "message": "Record not found"}})
        ));
    }

    #[tokio::test]
    async fn asset_histories_batch_ten_assets_into_one_http_request() {
        let server = MockServer::start_async().await;
        let responses: Vec<Value> = (0..10)
            .map(|idx| {
                json!({
                    "jsonrpc": "2.0",
                    "id": format!("sigs-0-{idx}"),
                    "result": {
                        "items": [{
                            "signature": format!("signature-{idx}"),
                            "type": "TRANSFER"
                        }]
                    }
                })
            })
            .collect();
        let histories = server
            .mock_async(move |when, then| {
                when.method(POST).body_contains("getSignaturesForAsset");
                then.status(200).json_body(json!(responses));
            })
            .await;
        let assets: Vec<SolanaAsset> = (0..10)
            .map(|idx| SolanaAsset {
                mint: format!("mint-{idx}"),
                owner: Some(format!("owner-{idx}")),
                compressed: true,
            })
            .collect();
        let client = HttpClient::with_retries(4, 0).unwrap();
        let outcome =
            fetch_asset_histories(&client, &server.base_url(), Some("key"), &assets, 10, 1).await;

        assert_eq!(histories.hits(), 1);
        assert_eq!(outcome.value.0.len(), 10);
    }

    #[tokio::test]
    async fn ordinary_nft_history_uses_signatures_for_asset() {
        let server = MockServer::start_async().await;
        let histories = server
            .mock_async(|when, then| {
                when.method(POST)
                    .body_contains("getSignaturesForAsset")
                    .body_contains("ordinary-mint");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "sigs-ordinary-mint",
                    "result": {"items": [["ordinary-signature", "Transfer"]]}
                }));
            })
            .await;
        let asset = SolanaAsset {
            mint: "ordinary-mint".into(),
            owner: Some("owner".into()),
            compressed: false,
        };
        let client = HttpClient::with_retries(1, 0).unwrap();
        let outcome =
            fetch_asset_histories(&client, &server.base_url(), Some("key"), &[asset], 1, 10).await;

        assert_eq!(histories.hits(), 1);
        assert_eq!(outcome.value.0.len(), 1);
        assert_eq!(outcome.value.0[0].tx_hash, "ordinary-signature");
        assert!(outcome.value.1.is_empty());
    }

    #[tokio::test]
    async fn ordinary_nft_tree_not_found_falls_back_as_truncated() {
        let server = MockServer::start_async().await;
        let primary = server
            .mock_async(|when, then| {
                when.method(POST)
                    .body_contains("getSignaturesForAsset")
                    .body_contains("ordinary-tree-mint");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "sigs-ordinary-tree-mint",
                    "error": {
                        "code": -32000,
                        "message": "Database Error: RecordNotFound Error: Tree not found"
                    }
                }));
            })
            .await;
        let fallback = server
            .mock_async(|when, then| {
                when.method(POST)
                    .body_contains("getSignaturesForAddress")
                    .body_contains("ordinary-tree-mint");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "address-sigs-ordinary-tree-mint",
                    "result": [{
                        "signature": "fallback-signature",
                        "slot": 42,
                        "blockTime": 123
                    }]
                }));
            })
            .await;
        let asset = SolanaAsset {
            mint: "ordinary-tree-mint".into(),
            owner: Some("owner".into()),
            compressed: false,
        };
        let client = HttpClient::with_retries(1, 0).unwrap();
        let outcome =
            fetch_asset_histories(&client, &server.base_url(), Some("key"), &[asset], 1, 10).await;

        assert_eq!(primary.hits(), 1);
        assert_eq!(fallback.hits(), 1);
        assert_eq!(outcome.status, EvidenceStatus::Truncated);
        assert!(outcome.truncated);
        assert_eq!(outcome.value.0.len(), 1);
        assert_eq!(outcome.value.0[0].tx_hash, "fallback-signature");
        assert!(outcome.value.1.is_empty());
    }

    #[tokio::test]
    async fn compressed_nft_tree_not_found_does_not_fall_back() {
        let server = MockServer::start_async().await;
        let primary = server
            .mock_async(|when, then| {
                when.method(POST)
                    .body_contains("getSignaturesForAsset")
                    .body_contains("compressed-tree-mint");
                then.status(200).json_body(json!({
                    "jsonrpc": "2.0",
                    "id": "sigs-compressed-tree-mint",
                    "error": {
                        "code": -32000,
                        "message": "Database Error: RecordNotFound Error: Tree not found"
                    }
                }));
            })
            .await;
        let fallback = server
            .mock_async(|when, then| {
                when.method(POST)
                    .body_contains("getSignaturesForAddress")
                    .body_contains("compressed-tree-mint");
                then.status(500);
            })
            .await;
        let asset = SolanaAsset {
            mint: "compressed-tree-mint".into(),
            owner: Some("owner".into()),
            compressed: true,
        };
        let client = HttpClient::with_retries(1, 0).unwrap();
        let outcome =
            fetch_asset_histories(&client, &server.base_url(), Some("key"), &[asset], 1, 10).await;

        assert_eq!(primary.hits(), 1);
        assert_eq!(fallback.hits(), 0);
        assert_eq!(outcome.status, EvidenceStatus::Failed);
        assert!(outcome.value.0.is_empty());
        assert!(outcome.value.1.is_empty());
        assert!(
            outcome
                .failure
                .as_deref()
                .is_some_and(|failure| failure.contains("Tree not found"))
        );
    }

    #[tokio::test]
    async fn transaction_cache_reuses_signature_across_candidates() {
        let server = MockServer::start_async().await;
        let transaction = server
            .mock_async(|when, then| {
                when.method(POST).body_contains("getTransaction");
                then.status(200).json_body(json!([{
                    "jsonrpc": "2.0",
                    "id": "tx-0-0",
                    "result": {
                        "slot": 1,
                        "blockTime": 2,
                        "transaction": {"message": {"accountKeys": []}},
                        "meta": {"fee": 1}
                    }
                }]));
            })
            .await;
        let client = HttpClient::with_retries(4, 0).unwrap();
        let cache = TransactionRequestCache::default();
        for _ in 0..2 {
            let rows = cache
                .fetch_many(&client, &server.base_url(), &["signature".into()])
                .await;
            assert!(matches!(&rows[0].1, Ok(Some(_))));
        }
        assert_eq!(transaction.hits(), 1);
    }

    #[test]
    fn parses_collection_grouping() {
        let asset = json!({
            "grouping": [{
                "group_key": "collection",
                "group_value": "So11111111111111111111111111111111111111112"
            }]
        });
        assert_eq!(
            parse_collection_address(&asset).as_deref(),
            Some("So11111111111111111111111111111111111111112")
        );
    }

    #[test]
    fn returns_none_without_collection_group() {
        let asset = json!({
            "grouping": [{"group_key": "other", "group_value": "x"}]
        });
        assert_eq!(parse_collection_address(&asset), None);
    }

    #[test]
    fn collection_metadata_identity_prefers_symbol() {
        let asset = json!({
            "content": {
                "metadata": {
                    "name": "Cool Collection",
                    "symbol": "COOL"
                }
            }
        });
        assert_eq!(
            parse_collection_metadata_identity(&asset).as_deref(),
            Some("COOL")
        );
    }

    #[test]
    fn parses_asset_owner_and_compression() {
        let item = json!({
            "id": "Mint111111111111111111111111111111111111111",
            "ownership": {"owner": "Owner1111111111111111111111111111111111111"},
            "compression": {"compressed": true}
        });
        let asset = parse_asset(&item).unwrap();
        assert!(asset.compressed);
        assert_eq!(
            asset.owner.as_deref(),
            Some("Owner1111111111111111111111111111111111111")
        );
    }

    #[test]
    fn status_from_count_distinguishes_empty_complete_truncated() {
        assert_eq!(status_from_count(0, false), EvidenceStatus::Empty);
        assert_eq!(status_from_count(1, false), EvidenceStatus::Complete);
        assert_eq!(status_from_count(1, true), EvidenceStatus::Truncated);
    }

    #[test]
    fn solana_paid_mint_keeps_unique_same_transaction_treasury_payment() {
        let mint = "MintPaid111111111111111111111111111111111";
        let buyer = "BuyerPaid11111111111111111111111111111111";
        let treasury = "Treasury111111111111111111111111111111111";
        let tx = DecodedTx {
            signature: "SigPaid1111111111111111111111111111111111".into(),
            owner_changes: HashMap::from([(mint.into(), (None, buyer.into(), true))]),
            native_moves: vec![NativeSolMove {
                event_id: "instruction:0".into(),
                from: buyer.into(),
                to: treasury.into(),
                amount_sol: 1.25,
            }],
            ..DecodedTx::default()
        };
        let mut transfer = TransferEvent {
            tx_hash: tx.signature.clone(),
            token_id: mint.into(),
            from: String::new(),
            to: buyer.into(),
            timestamp: None,
            block_number: None,
            is_mint: true,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        };

        apply_transfer_decode(&mut transfer, &tx);

        assert_eq!(transfer.mint_payment_native, Some(1.25));
        assert_eq!(transfer.mint_payment_receiver.as_deref(), Some(treasury));
    }

    #[test]
    fn solana_sale_decode_uses_owner_change_and_buyer_to_seller_payment() {
        let mint = "MintSale111111111111111111111111111111111";
        let seller = "Seller1111111111111111111111111111111111";
        let buyer = "BuyerSale11111111111111111111111111111111";
        let tx = DecodedTx {
            signature: "SigSale1111111111111111111111111111111111".into(),
            timestamp: Some(1_700_000_000),
            slot: Some(42),
            owner_changes: HashMap::from([(
                mint.into(),
                (Some(seller.into()), buyer.into(), false),
            )]),
            native_moves: vec![NativeSolMove {
                event_id: "instruction:0".into(),
                from: buyer.into(),
                to: seller.into(),
                amount_sol: 2.5,
            }],
            ..DecodedTx::default()
        };
        let mut sale = SaleEvent {
            tx_hash: tx.signature.clone(),
            token_id: mint.into(),
            marketplace: Some("helius".into()),
            currency_symbol: Some("SOL".into()),
            ..SaleEvent::default()
        };

        apply_sale_decode(&mut sale, &tx);

        assert_eq!(sale.seller, seller);
        assert_eq!(sale.buyer, buyer);
        assert_eq!(sale.native_amount, Some(2.5));
        assert_eq!(sale.seller_proceeds_native, Some(2.5));
        assert_eq!(sale.timestamp, Some(1_700_000_000));
        assert_eq!(sale.block_number, Some(42));
        assert!(sale_fields_complete(&sale));
    }

    #[test]
    fn solana_value_flows_keep_distinct_instructions_with_same_endpoints() {
        let operator = "Operator111111111111111111111111111111111";
        let receiver = "Receiver111111111111111111111111111111111";
        let tx = DecodedTx {
            signature: "SigFlow1111111111111111111111111111111111".into(),
            native_moves: vec![
                NativeSolMove {
                    event_id: "instruction:0".into(),
                    from: operator.into(),
                    to: receiver.into(),
                    amount_sol: 1.0,
                },
                NativeSolMove {
                    event_id: "instruction:1".into(),
                    from: operator.into(),
                    to: receiver.into(),
                    amount_sol: 2.0,
                },
            ],
            ..DecodedTx::default()
        };
        let flows = sol_value_flows(
            &AHashMap::from([(tx.signature.clone(), tx)]),
            &BTreeSet::from([operator.into()]),
        );

        assert_eq!(flows.len(), 2);
        assert_eq!(
            flows
                .iter()
                .filter_map(|edge| edge.native_amount)
                .sum::<f64>(),
            3.0
        );
    }

    #[test]
    fn parses_standard_transfer_owner_change_and_fee() {
        let mint = "MintDecode1111111111111111111111111111111";
        let result = json!({
            "slot": 42,
            "blockTime": 1_700_000_000,
            "transaction": {
                "message": {
                    "accountKeys": [
                        {"pubkey": "FeePayer111111111111111111111111111111111", "signer": true},
                        {"pubkey": "Other111111111111111111111111111111111111", "signer": false}
                    ],
                    "instructions": [{
                        "parsed": {
                            "type": "transfer",
                            "info": {
                                "source": "BuyerFund1111111111111111111111111111111",
                                "destination": "FeePayer111111111111111111111111111111111",
                                "lamports": 2_000_000_000u64
                            }
                        }
                    }]
                }
            },
            "meta": {
                "err": null,
                "fee": 5000,
                "preTokenBalances": [{
                    "accountIndex": 1,
                    "mint": mint,
                    "owner": "Seller11111111111111111111111111111111111",
                    "uiTokenAmount": {"amount": "1", "decimals": 0}
                }],
                "postTokenBalances": [{
                    "accountIndex": 1,
                    "mint": mint,
                    "owner": "Buyer111111111111111111111111111111111111",
                    "uiTokenAmount": {"amount": "1", "decimals": 0}
                }]
            }
        });
        let mints = BTreeSet::from([mint.to_owned()]);
        let tx = parse_decoded_tx("SigDecode11111111111111111111111111111111", &result, &mints);
        assert_eq!(tx.fee_sol, Some(5000.0 / LAMPORTS_PER_SOL));
        assert_eq!(
            tx.fee_payer.as_deref(),
            Some("FeePayer111111111111111111111111111111111")
        );
        assert_eq!(tx.timestamp, Some(1_700_000_000));
        let (from, to, is_mint) = tx.owner_changes.get(mint).unwrap();
        assert_eq!(
            from.as_deref(),
            Some("Seller11111111111111111111111111111111111")
        );
        assert_eq!(to, "Buyer111111111111111111111111111111111111");
        assert!(!is_mint);
        assert_eq!(tx.native_moves.len(), 1);
        assert!((tx.native_moves[0].amount_sol - 2.0).abs() < 1e-12);
    }

    #[test]
    fn mint_without_token_balances_is_not_complete() {
        // Bubblegum/compressed-style: fee + timestamp present, no pre/postTokenBalances.
        let mint = "MintNoBal11111111111111111111111111111111";
        let result = json!({
            "slot": 7,
            "blockTime": 1_700_000_050i64,
            "transaction": {
                "message": {
                    "accountKeys": [
                        {"pubkey": "FeePayerMint11111111111111111111111111111", "signer": true}
                    ],
                    "instructions": []
                }
            },
            "meta": {
                "err": null,
                "fee": 5000
            }
        });
        let mints = BTreeSet::from([mint.to_owned()]);
        let tx = parse_decoded_tx("SigMintNoBal1111111111111111111111111111", &result, &mints);
        assert!(tx.owner_changes.is_empty());
        assert_eq!(tx.fee_sol, Some(5000.0 / LAMPORTS_PER_SOL));
        assert_eq!(tx.timestamp, Some(1_700_000_050));

        let mut transfer = TransferEvent {
            tx_hash: "SigMintNoBal1111111111111111111111111111".into(),
            token_id: mint.to_owned(),
            from: String::new(),
            // Stub may carry current asset owner as `to`.
            to: "OwnerStubMint111111111111111111111111111".into(),
            timestamp: None,
            block_number: None,
            is_mint: true,
            gas_native: None,
            fee_payer: None,
            mint_payment_native: None,
            mint_payment_usd: None,
            mint_payment_receiver: None,
        };
        apply_transfer_decode(&mut transfer, &tx);
        assert!(transfer.timestamp.is_some());
        assert!(transfer.gas_native.is_some());
        let had_owner_change = tx.owner_changes.contains_key(mint);
        assert!(!had_owner_change);
        assert!(
            !transfer_fields_complete(&transfer, had_owner_change),
            "mint with fee/timestamp but no owner_change must not be Complete"
        );
    }

    #[test]
    fn field_status_stubs_without_fetch_stay_truncated() {
        let stats = DecodeStats {
            requested: 2,
            fetched_ok: 0,
            null_result: 2,
            transfers_total: 1,
            transfers_complete: 0,
            ..DecodeStats::default()
        };
        assert_eq!(
            field_status_after_decode(false, false, false, &stats),
            EvidenceStatus::Truncated
        );
    }

    #[test]
    fn field_status_all_fetch_failed() {
        let stats = DecodeStats {
            requested: 1,
            fetch_failed: 1,
            transfers_total: 1,
            ..DecodeStats::default()
        };
        assert_eq!(
            field_status_after_decode(false, false, false, &stats),
            EvidenceStatus::Failed
        );
    }

    #[test]
    fn field_status_complete_when_decoded() {
        let stats = DecodeStats {
            requested: 1,
            fetched_ok: 1,
            transfers_total: 1,
            transfers_complete: 1,
            ..DecodeStats::default()
        };
        assert_eq!(
            field_status_after_decode(false, false, true, &stats),
            EvidenceStatus::Complete
        );
    }
}
