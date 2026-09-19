//! `select_seeds` orchestration and `seeds.json` / `seeds.audit.json` writers.

use std::fs;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::enrich::http::HttpClient;
use crate::enrich::opensea::{self, OpenSeaRankedItem};
use crate::error::AnalysisError;

use super::address::is_evm_chain;
use super::nftscan::{self, NftScanRankedCollection};

/// Selected seed row written to `seeds.json` (compatible with run-dedup reader).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SeedRecord {
    pub chain: String,
    pub address: String,
    pub rank: u32,
    pub name: String,
    pub metric: String,
    pub window: String,
    pub source: String,
    pub collected_at: DateTime<Utc>,
}

/// Options for per-chain top-N seed selection.
#[derive(Clone, Debug)]
pub struct SelectSeedsOptions {
    pub chains: Vec<String>,
    pub seeds_per_chain: usize,
    pub opensea_api_key: Option<String>,
    pub nftscan_api_key: Option<String>,
    pub http_concurrency: usize,
    /// Override OpenSea base URL (tests / mirrors).
    pub opensea_base_url: Option<String>,
    /// Override NFTScan Solana API base URL.
    pub nftscan_base_url: Option<String>,
}

impl Default for SelectSeedsOptions {
    fn default() -> Self {
        Self {
            chains: vec![
                "ethereum".into(),
                "base".into(),
                "polygon".into(),
                "solana".into(),
            ],
            seeds_per_chain: 25,
            opensea_api_key: None,
            nftscan_api_key: None,
            http_concurrency: 32,
            opensea_base_url: None,
            nftscan_base_url: None,
        }
    }
}

/// Select top-N seeds per chain. Incomplete chains are recorded in audit and not
/// backfilled from other chains.
///
/// Creates a dedicated Tokio runtime. From an existing async context, call
/// [`select_seeds_async`] instead.
pub fn select_seeds(opts: &SelectSeedsOptions) -> Result<(Vec<SeedRecord>, Value), AnalysisError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(AnalysisError::invalid(
            "select_seeds cannot run inside an async runtime; use select_seeds_async",
        ));
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| AnalysisError::http(format!("tokio runtime: {e}")))?;
    runtime.block_on(select_seeds_async(opts))
}

/// Async entry for seed selection (preferred inside existing Tokio runtimes / tests).
pub async fn select_seeds_async(
    opts: &SelectSeedsOptions,
) -> Result<(Vec<SeedRecord>, Value), AnalysisError> {
    if opts.seeds_per_chain == 0 {
        return Err(AnalysisError::invalid("seeds_per_chain must be positive"));
    }
    let chains = normalize_chains(&opts.chains)?;
    if chains.is_empty() {
        return Err(AnalysisError::invalid("at least one chain is required"));
    }

    let client = HttpClient::new(opts.http_concurrency)?;
    let collected_at = Utc::now();
    let mut seeds = Vec::new();
    let mut chain_audit = serde_json::Map::new();

    for chain in &chains {
        let (chain_seeds, status) = select_chain(&client, opts, chain, collected_at).await?;
        seeds.extend(chain_seeds);
        chain_audit.insert(chain.clone(), status);
    }

    let audit = json!({
        "generated_at": collected_at,
        "seeds_per_chain": opts.seeds_per_chain,
        "chains": Value::Object(chain_audit),
    });
    Ok((seeds, audit))
}

async fn select_chain(
    client: &HttpClient,
    opts: &SelectSeedsOptions,
    chain: &str,
    collected_at: DateTime<Utc>,
) -> Result<(Vec<SeedRecord>, Value), AnalysisError> {
    let requested = opts.seeds_per_chain;
    if is_evm_chain(chain) {
        select_evm_chain(client, opts, chain, requested, collected_at).await
    } else if chain == "solana" {
        select_solana_chain(client, opts, requested, collected_at).await
    } else {
        Ok((
            Vec::new(),
            incomplete_status(requested, 0, format!("unsupported chain: {chain}")),
        ))
    }
}

async fn select_evm_chain(
    client: &HttpClient,
    opts: &SelectSeedsOptions,
    chain: &str,
    requested: usize,
    collected_at: DateTime<Utc>,
) -> Result<(Vec<SeedRecord>, Value), AnalysisError> {
    let Some(api_key) = opts
        .opensea_api_key
        .as_deref()
        .map(str::trim)
        .filter(|k| !k.is_empty())
    else {
        return Ok((
            Vec::new(),
            incomplete_status(requested, 0, "missing_opensea_api_key"),
        ));
    };
    let base = opts
        .opensea_base_url
        .as_deref()
        .unwrap_or(opensea::default_base_url());
    let ranked = match opensea::fetch_top_contracts(client, base, api_key, chain, requested).await {
        Ok(items) => items,
        Err(error) => {
            crate::enrich::print_provider_error(
                "opensea",
                "select_seeds_top_collections",
                &error.to_string(),
            );
            return Ok((
                Vec::new(),
                incomplete_status(requested, 0, format!("opensea_error: {error}")),
            ));
        }
    };
    let seeds = ranked
        .into_iter()
        .enumerate()
        .map(|(index, item)| evm_seed(item, (index + 1) as u32, collected_at))
        .collect::<Vec<_>>();
    let collected = seeds.len();
    let status = if collected >= requested {
        complete_status(requested, collected)
    } else {
        incomplete_status(
            requested,
            collected,
            format!("collected {collected} of {requested}"),
        )
    };
    Ok((seeds, status))
}

async fn select_solana_chain(
    client: &HttpClient,
    opts: &SelectSeedsOptions,
    requested: usize,
    collected_at: DateTime<Utc>,
) -> Result<(Vec<SeedRecord>, Value), AnalysisError> {
    let Some(api_key) = opts
        .nftscan_api_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
    else {
        return Ok((
            Vec::new(),
            incomplete_status(requested, 0, "missing_nftscan_api_key"),
        ));
    };
    let base = opts
        .nftscan_base_url
        .as_deref()
        .unwrap_or(nftscan::default_base_url());
    let collections = match nftscan::fetch_top_collections(client, base, api_key).await {
        Ok(items) => items,
        Err(error) => {
            crate::enrich::print_provider_error(
                "nftscan",
                "select_seeds_trade_ranking",
                &error.to_string(),
            );
            return Ok((
                Vec::new(),
                incomplete_status(requested, 0, format!("nftscan_error: {error}")),
            ));
        }
    };
    let seeds = collections
        .into_iter()
        .take(requested)
        .enumerate()
        .map(|(index, collection)| solana_seed(collection, (index + 1) as u32, collected_at))
        .collect::<Vec<_>>();

    let collected = seeds.len();
    let status = if collected >= requested {
        complete_status(requested, collected)
    } else {
        incomplete_status(
            requested,
            collected,
            format!("collected {collected} of {requested}"),
        )
    };
    Ok((seeds, status))
}

fn evm_seed(item: OpenSeaRankedItem, rank: u32, collected_at: DateTime<Utc>) -> SeedRecord {
    SeedRecord {
        chain: item.chain,
        address: item.address,
        rank,
        name: item.name,
        metric: "thirty_days_volume".into(),
        window: "30d".into(),
        source: "opensea".into(),
        collected_at,
    }
}

fn solana_seed(
    collection: NftScanRankedCollection,
    rank: u32,
    collected_at: DateTime<Utc>,
) -> SeedRecord {
    SeedRecord {
        chain: "solana".into(),
        address: collection.address,
        rank,
        name: collection.name,
        metric: "thirty_days_volume".into(),
        window: "30d".into(),
        source: "nftscan".into(),
        collected_at,
    }
}

fn complete_status(requested: usize, collected: usize) -> Value {
    json!({
        "requested": requested,
        "collected": collected,
        "complete": true,
    })
}

fn incomplete_status(requested: usize, collected: usize, reason: impl Into<String>) -> Value {
    json!({
        "requested": requested,
        "collected": collected,
        "complete": false,
        "reason": reason.into(),
    })
}

fn normalize_chains(chains: &[String]) -> Result<Vec<String>, AnalysisError> {
    let mut out = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for chain in chains {
        let normalized = chain.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }
        if normalized == "matic" {
            if seen.insert("polygon".into()) {
                out.push("polygon".into());
            }
            continue;
        }
        if !seen.insert(normalized.clone()) {
            continue;
        }
        out.push(normalized);
    }
    Ok(out)
}

/// Write `seeds.json` + `seeds.audit.json` under `output_dir`.
pub fn write_seed_outputs(
    output_dir: &Path,
    seeds: &[SeedRecord],
    audit: &Value,
) -> Result<(), AnalysisError> {
    fs::create_dir_all(output_dir)?;
    let seeds_path = output_dir.join("seeds.json");
    let audit_path = output_dir.join("seeds.audit.json");
    let seeds_json = serde_json::to_vec_pretty(seeds)
        .map_err(|e| AnalysisError::invalid(format!("serialize seeds.json: {e}")))?;
    let audit_json = serde_json::to_vec_pretty(audit)
        .map_err(|e| AnalysisError::invalid(format!("serialize seeds.audit.json: {e}")))?;
    fs::write(&seeds_path, seeds_json)?;
    fs::write(&audit_path, audit_json)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reporting::load_seeds_json;
    use httpmock::prelude::*;
    use serde_json::json;

    #[test]
    fn missing_opensea_key_records_incomplete_without_backfill() {
        let (seeds, audit) = select_seeds(&SelectSeedsOptions {
            chains: vec!["ethereum".into(), "base".into()],
            seeds_per_chain: 2,
            opensea_api_key: None,
            ..SelectSeedsOptions::default()
        })
        .unwrap();
        assert!(seeds.is_empty());
        assert_eq!(audit["chains"]["ethereum"]["complete"], false);
        assert_eq!(audit["chains"]["base"]["complete"], false);
        assert_eq!(
            audit["chains"]["ethereum"]["reason"],
            "missing_opensea_api_key"
        );
        // No backfill: ethereum shortfall does not inflate base.
        assert_eq!(audit["chains"]["ethereum"]["collected"], 0);
        assert_eq!(audit["chains"]["base"]["collected"], 0);
    }

    #[tokio::test]
    async fn missing_nftscan_key_records_audit_reason() {
        let (seeds, audit) = select_seeds_async(&SelectSeedsOptions {
            chains: vec!["solana".into()],
            seeds_per_chain: 1,
            nftscan_api_key: None,
            ..SelectSeedsOptions::default()
        })
        .await
        .unwrap();

        assert!(seeds.is_empty());
        assert_eq!(audit["chains"]["solana"]["complete"], false);
        assert_eq!(
            audit["chains"]["solana"]["reason"],
            "missing_nftscan_api_key"
        );
    }

    #[tokio::test]
    async fn nftscan_application_error_surfaces_in_audit_reason() {
        let server = MockServer::start_async().await;
        let _nftscan = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/api/sol/statistics/ranking/trade")
                    .header("x-api-key", "nftscan-key");
                then.status(200)
                    .json_body(json!({"code": 401, "msg": "Unauthorized"}));
            })
            .await;

        let (_, audit) = select_seeds_async(&SelectSeedsOptions {
            chains: vec!["solana".into()],
            seeds_per_chain: 1,
            nftscan_api_key: Some("nftscan-key".into()),
            nftscan_base_url: Some(server.base_url()),
            ..SelectSeedsOptions::default()
        })
        .await
        .unwrap();

        assert_eq!(audit["chains"]["solana"]["complete"], false);
        let reason = audit["chains"]["solana"]["reason"].as_str().unwrap();
        assert!(reason.contains("nftscan_error"), "reason={reason}");
        assert!(reason.contains("Unauthorized"), "reason={reason}");
    }

    #[tokio::test]
    async fn http_mock_opensea_and_nftscan_rankings() {
        let server = MockServer::start_async().await;

        let _opensea = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/api/v2/collections/top")
                    .query_param("chains", "ethereum")
                    .query_param("sort_by", "thirty_days_volume");
                then.status(200).json_body(json!({
                    "collections": [{
                        "name": "Alpha",
                        "collection": "alpha",
                        "thirty_days_volume": 100,
                        "contracts": [{
                            "chain": "ethereum",
                            "address": "0x1111111111111111111111111111111111111111"
                        }]
                    }]
                }));
            })
            .await;

        let _opensea_polygon = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/api/v2/collections/top")
                    .query_param("chains", "polygon")
                    .query_param("sort_by", "thirty_days_volume");
                then.status(200).json_body(json!({
                    "collections": [{
                        "name": "Poly",
                        "collection": "poly",
                        "thirty_days_volume": 50,
                        "contracts": [{
                            "chain": "matic",
                            "address": "0x2222222222222222222222222222222222222222"
                        }]
                    }]
                }));
            })
            .await;

        let _nftscan = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/api/sol/statistics/ranking/trade")
                    .query_param("time", "30d")
                    .query_param("sort_field", "volume")
                    .query_param("sort_direction", "desc")
                    .header("x-api-key", "nftscan-key");
                then.status(200).json_body(json!({
                    "code": 200,
                    "data": [
                        {
                            "project_address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
                            "project_name": "NFTScan One",
                            "volume": "99.5"
                        },
                        {
                            "collection_address": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                            "collection_name": "NFTScan Two",
                            "trade_volume": 50
                        }
                    ]
                }));
            })
            .await;

        let base = server.base_url();
        let (seeds, audit) = select_seeds_async(&SelectSeedsOptions {
            chains: vec!["ethereum".into(), "polygon".into(), "solana".into()],
            seeds_per_chain: 2,
            opensea_api_key: Some("os-key".into()),
            nftscan_api_key: Some("nftscan-key".into()),
            http_concurrency: 4,
            opensea_base_url: Some(base.clone()),
            nftscan_base_url: Some(base),
        })
        .await
        .unwrap();

        let eth: Vec<_> = seeds.iter().filter(|s| s.chain == "ethereum").collect();
        let poly: Vec<_> = seeds.iter().filter(|s| s.chain == "polygon").collect();
        let sol: Vec<_> = seeds.iter().filter(|s| s.chain == "solana").collect();
        assert_eq!(eth.len(), 1);
        assert_eq!(eth[0].address, "0x1111111111111111111111111111111111111111");
        assert_eq!(eth[0].source, "opensea");
        assert_eq!(eth[0].metric, "thirty_days_volume");
        assert_eq!(audit["chains"]["ethereum"]["complete"], false);
        assert_eq!(audit["chains"]["ethereum"]["collected"], 1);

        assert_eq!(poly.len(), 1);
        assert_eq!(
            poly[0].address,
            "0x2222222222222222222222222222222222222222"
        );
        assert_eq!(audit["chains"]["polygon"]["collected"], 1);

        assert_eq!(sol.len(), 2);
        assert_eq!(
            sol[0].address,
            "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
        );
        assert_eq!(sol[0].source, "nftscan");
        assert_eq!(sol[0].metric, "thirty_days_volume");
        assert_eq!(sol[0].window, "30d");
        assert_eq!(
            sol[1].address,
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        );
        assert_eq!(audit["chains"]["solana"]["complete"], true);

        let dir = std::env::temp_dir().join(format!("analysis-seed-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        write_seed_outputs(&dir, &seeds, &audit).unwrap();
        let loaded = load_seeds_json(&dir.join("seeds.json")).unwrap();
        assert_eq!(loaded.len(), seeds.len());
        assert_eq!(loaded[0].chain, seeds[0].chain);
        assert_eq!(loaded[0].address, seeds[0].address);
        assert_eq!(loaded[0].rank, Some(seeds[0].rank));
        assert!(dir.join("seeds.audit.json").is_file());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
