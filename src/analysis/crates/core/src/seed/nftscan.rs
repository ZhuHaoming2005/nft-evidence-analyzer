//! NFTScan Solana 30-day trade ranking used by `select-seeds`.

use serde_json::Value;

use crate::enrich::http::HttpClient;
use crate::error::AnalysisError;

use super::address::normalize_address;

const DEFAULT_NFTSCAN_BASE: &str = "https://solanaapi.nftscan.com";

#[derive(Clone, Debug, PartialEq)]
pub struct NftScanRankedCollection {
    pub address: String,
    pub name: String,
    pub volume: Option<f64>,
}

pub fn default_base_url() -> &'static str {
    DEFAULT_NFTSCAN_BASE
}

pub async fn fetch_top_collections(
    client: &HttpClient,
    base_url: &str,
    api_key: &str,
) -> Result<Vec<NftScanRankedCollection>, AnalysisError> {
    let base = base_url.trim_end_matches('/');
    let url = format!(
        "{base}/api/sol/statistics/ranking/trade?time=30d&sort_field=volume&sort_direction=desc"
    );
    let payload = client.get_json(&url, &[("x-api-key", api_key)]).await?;
    validate_envelope(&payload)?;
    let collections = parse_ranked_collections(&payload);
    if collections.is_empty() {
        return Err(AnalysisError::http(
            "NFTScan trade ranking returned no valid Solana collection addresses",
        ));
    }
    Ok(collections)
}

fn validate_envelope(payload: &Value) -> Result<(), AnalysisError> {
    let Some(code) = payload.get("code").and_then(json_i64) else {
        return Ok(());
    };
    if (200..300).contains(&code) {
        return Ok(());
    }
    let message = ["msg", "message", "error"]
        .into_iter()
        .find_map(|key| payload.get(key).and_then(Value::as_str))
        .unwrap_or("unknown error");
    Err(AnalysisError::http(format!(
        "NFTScan API error code={code}: {message}"
    )))
}

pub fn parse_ranked_collections(payload: &Value) -> Vec<NftScanRankedCollection> {
    let Some(rows) = ranked_rows(payload) else {
        return Vec::new();
    };
    let mut seen = std::collections::BTreeSet::new();
    rows.iter()
        .filter_map(|row| {
            let address = [
                "project_address",
                "collection_address",
                "contract_address",
                "collection_id",
                "collectionId",
                "address",
                "collection",
            ]
            .into_iter()
            .find_map(|key| {
                row.get(key)
                    .and_then(Value::as_str)
                    .and_then(|value| normalize_address("solana", value))
            })?;
            if !seen.insert(address.clone()) {
                return None;
            }
            let name = [
                "project_name",
                "collection_name",
                "collectionName",
                "name",
                "symbol",
            ]
            .into_iter()
            .find_map(|key| {
                row.get(key)
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or(&address)
            .to_owned();
            let volume = [
                "volume",
                "trade_volume",
                "tradeVolume",
                "volume_total",
                "total_volume",
            ]
            .into_iter()
            .find_map(|key| row.get(key).and_then(json_f64));
            Some(NftScanRankedCollection {
                address,
                name,
                volume,
            })
        })
        .collect()
}

fn ranked_rows(payload: &Value) -> Option<&Vec<Value>> {
    if let Some(rows) = payload.as_array() {
        return Some(rows);
    }
    for key in ["data", "collections", "results", "items", "list"] {
        let Some(value) = payload.get(key) else {
            continue;
        };
        if let Some(rows) = value.as_array() {
            return Some(rows);
        }
        for nested in ["content", "collections", "results", "items", "list"] {
            if let Some(rows) = value.get(nested).and_then(Value::as_array) {
                return Some(rows);
            }
        }
    }
    None
}

fn json_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn json_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|number| number as f64))
        .or_else(|| value.as_u64().map(|number| number as f64))
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .filter(|number| number.is_finite() && *number >= 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_common_solana_ranking_shapes_and_deduplicates() {
        let payload = json!({
            "code": 200,
            "data": {
                "list": [
                    {
                        "project_address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
                        "project_name": "Collection A",
                        "volume": "42.5"
                    },
                    {
                        "collection_address": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                        "collection_name": "Collection B",
                        "trade_volume": 21
                    },
                    {
                        "collection_address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
                        "name": "duplicate"
                    },
                    {"collection_address": "not-base58-0", "name": "invalid"}
                ]
            }
        });

        let rows = parse_ranked_collections(&payload);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "Collection A");
        assert_eq!(rows[0].volume, Some(42.5));
        assert_eq!(rows[1].name, "Collection B");
        assert_eq!(rows[1].volume, Some(21.0));
    }

    #[test]
    fn rejects_application_level_errors() {
        let error = validate_envelope(&json!({"code": 401, "msg": "Unauthorized"}))
            .unwrap_err()
            .to_string();
        assert!(error.contains("code=401"));
        assert!(error.contains("Unauthorized"));
    }
}
