//! Etherscan NFT transfer fallback for EVM candidates.

use serde_json::Value;

use super::alchemy::{FetchOutcome, normalize_token_id};
use super::http::HttpClient;
use super::types::{EvidenceObservation, EvidenceStatus, TransferEvent, now_unix};

fn etherscan_chain_id(chain: &str) -> Option<&'static str> {
    match chain {
        "ethereum" => Some("1"),
        "base" => Some("8453"),
        "polygon" | "matic" => Some("137"),
        _ => None,
    }
}

/// Fetch ERC-721 and ERC-1155 transfers via Etherscan v2 (fallback only).
pub async fn fetch_transfers(
    client: &HttpClient,
    base_url: &str,
    api_key: Option<&str>,
    chain: &str,
    contract: &str,
    max_pages: usize,
) -> FetchOutcome<Vec<TransferEvent>> {
    let Some(api_key) = api_key else {
        return FetchOutcome::skipped("etherscan_transfers");
    };
    let Some(chain_id) = etherscan_chain_id(chain) else {
        return FetchOutcome::failed(
            "etherscan",
            "etherscan_transfers",
            format!("unsupported chain {chain}"),
        );
    };

    let (erc721, erc1155) = tokio::join!(
        fetch_transfer_action(
            client,
            base_url,
            api_key,
            chain_id,
            contract,
            max_pages,
            "tokennfttx"
        ),
        fetch_transfer_action(
            client,
            base_url,
            api_key,
            chain_id,
            contract,
            max_pages,
            "token1155tx"
        ),
    );
    let mut transfers = Vec::new();
    let mut truncated = false;
    let mut failures = Vec::new();
    for outcome in [erc721, erc1155] {
        match outcome {
            Ok((mut rows, partial, failure)) => {
                transfers.append(&mut rows);
                truncated |= partial;
                if let Some(failure) = failure {
                    failures.push(failure);
                }
            }
            Err(error) => {
                truncated = true;
                failures.push(error);
            }
        }
    }
    if transfers.is_empty() && failures.len() == 2 {
        return FetchOutcome::failed("etherscan", "etherscan_transfers", failures.join("; "));
    }

    let count = transfers.len();
    let status = if truncated {
        EvidenceStatus::Truncated
    } else if count == 0 {
        EvidenceStatus::Empty
    } else {
        EvidenceStatus::Complete
    };
    let mut outcome = FetchOutcome {
        value: transfers,
        status,
        observation: Some(EvidenceObservation {
            source: "etherscan".into(),
            request_key: "etherscan_transfers".into(),
            observed_at: now_unix(),
            status,
        }),
        failure: None,
        truncated,
    };
    if !failures.is_empty() {
        outcome.failure = Some(failures.join("; "));
    }
    outcome
}

async fn fetch_transfer_action(
    client: &HttpClient,
    base_url: &str,
    api_key: &str,
    chain_id: &str,
    contract: &str,
    max_pages: usize,
    action: &str,
) -> Result<(Vec<TransferEvent>, bool, Option<String>), String> {
    let mut transfers = Vec::new();
    let mut truncated = false;
    let mut failure = None;
    let pages = max_pages.max(1);
    for page in 1..=pages {
        let url = format!(
            "{}{}chainid={}&module=account&action={action}&contractaddress={}&page={page}&offset=1000&startblock=0&endblock=999999999&sort=asc&apikey={}",
            base_url.trim_end_matches('/'),
            if base_url.contains('?') { "&" } else { "?" },
            chain_id,
            contract,
            api_key
        );
        let payload = match client.get_json_etherscan(&url, &[]).await {
            Ok(payload) => payload,
            Err(error) => {
                if transfers.is_empty() {
                    return Err(format!("Etherscan {action}: {error}"));
                }
                truncated = true;
                failure = Some(format!("Etherscan {action}: partial page failure: {error}"));
                break;
            }
        };
        let Some(items) = payload.get("result").and_then(Value::as_array) else {
            let detail = etherscan_envelope_detail(&payload);
            if detail
                .to_ascii_lowercase()
                .contains("no transactions found")
            {
                break;
            }
            if transfers.is_empty() {
                return Err(format!("Etherscan {action}: {detail}"));
            }
            truncated = true;
            failure = Some(format!(
                "Etherscan {action}: partial provider error: {detail}"
            ));
            break;
        };
        if items.is_empty() {
            break;
        }
        transfers.extend(
            items
                .iter()
                .map(|item| parse_etherscan_transfer(item, contract)),
        );
        if items.len() < 1_000 {
            break;
        }
        if page == pages {
            truncated = true;
        }
    }
    Ok((transfers, truncated, failure))
}

fn etherscan_envelope_detail(payload: &Value) -> String {
    ["message", "result"]
        .into_iter()
        .filter_map(|key| payload.get(key))
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string())
        })
        .collect::<Vec<_>>()
        .join(": ")
}

pub fn parse_etherscan_transfer(item: &Value, fallback_contract: &str) -> TransferEvent {
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
    let is_mint = from.is_empty() || from == "0x0000000000000000000000000000000000000000";
    let _ = item
        .get("contractAddress")
        .and_then(Value::as_str)
        .unwrap_or(fallback_contract);
    TransferEvent {
        tx_hash: item
            .get("hash")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        token_id: normalize_token_id(item.get("tokenID").or_else(|| item.get("tokenId"))),
        from,
        to,
        timestamp: item
            .get("timeStamp")
            .and_then(Value::as_str)
            .and_then(|s| s.parse().ok())
            .or_else(|| item.get("timeStamp").and_then(Value::as_i64)),
        block_number: item
            .get("blockNumber")
            .and_then(Value::as_str)
            .and_then(|s| s.parse().ok())
            .or_else(|| item.get("blockNumber").and_then(Value::as_u64)),
        is_mint,
        gas_native: None,
        fee_payer: None,
        mint_payment_native: None,
        mint_payment_usd: None,
        mint_payment_receiver: None,
    }
}
