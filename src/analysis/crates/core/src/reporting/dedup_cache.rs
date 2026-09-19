//! Durable dedup checkpoint for `run` restarts.
//!
//! After seed-scoped URI/Name/Metadata queries finish, the pipeline writes a
//! portable JSON cache under the output directory. A later `run` automatically
//! reloads that cache (after loading identity from Parquet) and skips the
//! expensive query stages. Edges are stored with stable chain/address/token
//! identities so process-local `ContractId` / `NftId` values can be remapped.

use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::dedup::hits::{Dimension, HitEdge, HitGraph};
use crate::entity::{ContractId, ResidentStore};
use crate::error::AnalysisError;
use crate::reporting::json::SeedRecord;
use crate::reporting::manifest::FailureRecord;

pub const DEDUP_CACHE_VERSION: u32 = 3;
pub const DEFAULT_DEDUP_CACHE_FILE: &str = "dedup_cache.json";

/// Parameters that must match between the producing and reusing runs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DedupCacheParams {
    pub inputs: Vec<String>,
    /// Cheap ordered file identity used to reject stale caches even when the
    /// input paths themselves did not change.
    #[serde(default)]
    pub input_fingerprints: Vec<InputFileFingerprint>,
    pub chains: Vec<String>,
    pub evm_chains: Vec<String>,
    pub name_threshold: Option<f64>,
    pub metadata_threshold: f64,
    pub metadata_anchors: Option<usize>,
    /// SHA-256 over the ordered compressed seed-NFT cache files.
    #[serde(default)]
    pub seed_nft_fingerprint: String,
    /// Absolute or display path of the seeds file used when the cache was built.
    pub seeds_path: String,
    /// Seeds embedded for exact list comparison on reuse.
    pub seeds: Vec<SeedRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct InputFileFingerprint {
    pub path: String,
    pub byte_len: u64,
    pub modified_unix_nanos: u64,
}

/// One hit edge with stable identities (no process-local ids).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CachedHitEdge {
    pub candidate_chain: String,
    pub candidate_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_token_id: Option<String>,
    pub dimension: String,
    pub score: f64,
    pub primary_chain: String,
    pub secondary_chain: String,
}

/// One successfully queried seed and its hit edges.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CachedSeedHits {
    pub seed: SeedRecord,
    pub edges: Vec<CachedHitEdge>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CachedChainTotals {
    pub chain: String,
    pub contracts: u64,
    pub nfts: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CachedDatasetStats {
    pub rows_loaded: u64,
    pub chains: Vec<CachedChainTotals>,
}

/// On-disk dedup checkpoint.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DedupCacheFile {
    pub version: u32,
    pub params: DedupCacheParams,
    #[serde(default)]
    pub dataset: CachedDatasetStats,
    pub completed: Vec<CachedSeedHits>,
    #[serde(default)]
    pub failures: Vec<FailureRecord>,
}

/// Default cache path: `{output_dir}/intermediate/dedup_cache.json`.
pub fn default_dedup_cache_path(output_dir: &Path) -> PathBuf {
    super::layout::intermediate_path(output_dir, DEFAULT_DEDUP_CACHE_FILE)
}

fn dimension_label(d: Dimension) -> &'static str {
    match d {
        Dimension::Name => "name",
        Dimension::TokenUri => "token_uri",
        Dimension::ImageUri => "image_uri",
        Dimension::Metadata => "metadata",
    }
}

fn parse_dimension(label: &str) -> Result<Dimension, AnalysisError> {
    match label {
        "name" => Ok(Dimension::Name),
        "token_uri" => Ok(Dimension::TokenUri),
        "image_uri" => Ok(Dimension::ImageUri),
        "metadata" => Ok(Dimension::Metadata),
        other => Err(AnalysisError::invalid(format!(
            "dedup cache: unknown dimension {other:?}"
        ))),
    }
}

/// Build a portable cache from in-memory seed hit graphs.
pub fn build_dedup_cache(
    store: &ResidentStore,
    params: DedupCacheParams,
    completed: &[(SeedRecord, ContractId, HitGraph)],
    failures: &[FailureRecord],
) -> DedupCacheFile {
    let mut cached_completed = Vec::with_capacity(completed.len());
    for (seed, _seed_id, graph) in completed {
        let mut edges = Vec::with_capacity(graph.edges().len());
        for edge in graph.edges() {
            let cand = &store.contracts[edge.candidate_contract as usize];
            let candidate_token_id = edge.candidate_nft.and_then(|nft_id| {
                store
                    .nfts
                    .get(nft_id as usize)
                    .map(|nft| store.string(nft.token_id_id).to_owned())
            });
            edges.push(CachedHitEdge {
                candidate_chain: store.chain_name(cand.chain_id).to_owned(),
                candidate_address: cand.address.clone(),
                candidate_token_id,
                dimension: dimension_label(edge.dimension).to_owned(),
                score: edge.score,
                primary_chain: store.chain_name(edge.primary_chain).to_owned(),
                secondary_chain: store.chain_name(edge.secondary_chain).to_owned(),
            });
        }
        cached_completed.push(CachedSeedHits {
            seed: seed.clone(),
            edges,
        });
    }
    let mut chains = store
        .totals
        .iter()
        .map(|(&chain_id, totals)| CachedChainTotals {
            chain: store.chain_name(chain_id).to_owned(),
            contracts: totals.contracts,
            nfts: totals.nfts,
        })
        .collect::<Vec<_>>();
    chains.sort_unstable_by(|left, right| left.chain.cmp(&right.chain));
    DedupCacheFile {
        version: DEDUP_CACHE_VERSION,
        params,
        dataset: CachedDatasetStats {
            rows_loaded: store.rows_loaded,
            chains,
        },
        completed: cached_completed,
        failures: failures.to_vec(),
    }
}

/// Write cache JSON (compact, non-pretty) atomically via temp file + rename.
pub fn write_dedup_cache(path: &Path, cache: &DedupCacheFile) -> Result<(), AnalysisError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    {
        let file = File::create(&tmp)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, cache)
            .map_err(|e| AnalysisError::invalid(format!("serialize dedup cache: {e}")))?;
        writer.flush()?;
    }
    if let Err(error) = fs::rename(&tmp, path) {
        // Windows may fail rename over existing file; fall back to overwrite.
        fs::copy(&tmp, path).map_err(|e| {
            AnalysisError::invalid(format!(
                "write dedup cache {} (rename failed: {error}): {e}",
                path.display()
            ))
        })?;
        let _ = fs::remove_file(&tmp);
    }
    Ok(())
}

/// Load and parse a dedup cache file.
pub fn load_dedup_cache(path: &Path) -> Result<DedupCacheFile, AnalysisError> {
    let file = File::open(path)
        .map_err(|e| AnalysisError::invalid(format!("read dedup cache {}: {e}", path.display())))?;
    let cache: DedupCacheFile = serde_json::from_reader(BufReader::new(file)).map_err(|e| {
        AnalysisError::invalid(format!("parse dedup cache {}: {e}", path.display()))
    })?;
    if cache.version != DEDUP_CACHE_VERSION {
        return Err(AnalysisError::invalid(format!(
            "dedup cache version {} unsupported (expected {DEDUP_CACHE_VERSION})",
            cache.version
        )));
    }
    Ok(cache)
}

/// Ensure the cache was produced with equivalent run knobs / seeds.
pub fn validate_dedup_cache(
    cache: &DedupCacheFile,
    expected: &DedupCacheParams,
) -> Result<(), AnalysisError> {
    let got = &cache.params;
    if got.inputs != expected.inputs {
        return Err(AnalysisError::invalid(
            "dedup cache inputs do not match current --input list",
        ));
    }
    if got.input_fingerprints != expected.input_fingerprints {
        return Err(AnalysisError::invalid(
            "dedup cache input file fingerprints do not match current files",
        ));
    }
    if got.chains != expected.chains || got.evm_chains != expected.evm_chains {
        return Err(AnalysisError::invalid(
            "dedup cache chains/evm-chains do not match current flags",
        ));
    }
    let name_threshold_matches = match (got.name_threshold, expected.name_threshold) {
        (Some(got), Some(expected)) => (got - expected).abs() <= f64::EPSILON,
        (None, None) => true,
        _ => false,
    };
    if !name_threshold_matches
        || (got.metadata_threshold - expected.metadata_threshold).abs() > f64::EPSILON
        || got.metadata_anchors != expected.metadata_anchors
    {
        return Err(AnalysisError::invalid(
            "dedup cache thresholds/anchors do not match current flags",
        ));
    }
    if got.seeds != expected.seeds {
        return Err(AnalysisError::invalid(
            "dedup cache seeds list does not match current --seeds file",
        ));
    }
    if got.seed_nft_fingerprint != expected.seed_nft_fingerprint {
        return Err(AnalysisError::invalid(
            "dedup cache seed NFT snapshots do not match current compressed caches",
        ));
    }
    Ok(())
}

type RematerializedDedupBatch = (Vec<(SeedRecord, ContractId, HitGraph)>, Vec<FailureRecord>);

/// Rematerialize in-memory seed hit graphs from a validated cache.
pub fn rematerialize_dedup_batch(
    store: &ResidentStore,
    cache: &DedupCacheFile,
) -> Result<RematerializedDedupBatch, AnalysisError> {
    let mut completed = Vec::with_capacity(cache.completed.len());
    for entry in &cache.completed {
        let seed_id = store
            .contract_id(&entry.seed.chain, &entry.seed.address)
            .ok_or_else(|| {
                AnalysisError::invalid(format!(
                    "dedup cache seed not in snapshot: {} / {}",
                    entry.seed.chain, entry.seed.address
                ))
            })?;
        let mut graph = HitGraph::new();
        for edge in &entry.edges {
            let candidate = store
                .contract_id(&edge.candidate_chain, &edge.candidate_address)
                .ok_or_else(|| {
                    AnalysisError::invalid(format!(
                        "dedup cache candidate not in snapshot: {} / {}",
                        edge.candidate_chain, edge.candidate_address
                    ))
                })?;
            let candidate_nft = match &edge.candidate_token_id {
                None => None,
                Some(token_id) => {
                    let nft_id = store.nft_id(candidate, token_id).ok_or_else(|| {
                        AnalysisError::invalid(format!(
                            "dedup cache NFT not in snapshot: {} / {} / {token_id}",
                            edge.candidate_chain, edge.candidate_address
                        ))
                    })?;
                    Some(nft_id)
                }
            };
            let primary_chain = *store.chain_ids.get(&edge.primary_chain).ok_or_else(|| {
                AnalysisError::invalid(format!(
                    "dedup cache unknown primary_chain {}",
                    edge.primary_chain
                ))
            })?;
            let secondary_chain = *store.chain_ids.get(&edge.secondary_chain).ok_or_else(|| {
                AnalysisError::invalid(format!(
                    "dedup cache unknown secondary_chain {}",
                    edge.secondary_chain
                ))
            })?;
            graph.push(HitEdge {
                seed_contract: seed_id,
                candidate_contract: candidate,
                candidate_nft,
                dimension: parse_dimension(&edge.dimension)?,
                score: edge.score,
                primary_chain,
                secondary_chain,
            });
        }
        completed.push((entry.seed.clone(), seed_id, graph));
    }
    Ok((completed, cache.failures.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dedup::hits::Dimension;
    use crate::entity::{IdentityRow, SourceOrder};

    fn prepared() -> ResidentStore {
        let mut store = ResidentStore::new();
        for (n, (chain, addr, token)) in [("ethereum", "0xseed", "1"), ("base", "0xcand", "9")]
            .into_iter()
            .enumerate()
        {
            store
                .ingest_identity_row(IdentityRow {
                    chain: chain.into(),
                    contract_address: addr.into(),
                    token_id: token.into(),
                    name_norm: "n".into(),
                    token_uri_norm: format!("ipfs://{n}"),
                    image_uri_norm: String::new(),
                    source_order: SourceOrder {
                        file_ordinal: 0,
                        file_row_number: n as u64,
                    },
                })
                .unwrap();
        }
        store.rebuild_uri_csr();
        store
    }

    #[test]
    fn round_trip_cache_preserves_edges() {
        let store = prepared();
        let seed_id = store.contract_id("ethereum", "0xseed").unwrap();
        let cand = store.contract_id("base", "0xcand").unwrap();
        let nft = store.nft_id(cand, "9").unwrap();
        let mut graph = HitGraph::new();
        graph.push(HitEdge {
            seed_contract: seed_id,
            candidate_contract: cand,
            candidate_nft: Some(nft),
            dimension: Dimension::TokenUri,
            score: 1.0,
            primary_chain: store.chain_ids["ethereum"],
            secondary_chain: store.chain_ids["base"],
        });
        let seed = SeedRecord {
            chain: "ethereum".into(),
            address: "0xseed".into(),
            rank: Some(1),
        };
        let params = DedupCacheParams {
            inputs: vec!["a.parquet".into()],
            input_fingerprints: vec![],
            chains: vec!["ethereum".into(), "base".into()],
            evm_chains: vec!["ethereum".into(), "base".into()],
            name_threshold: Some(0.98),
            metadata_threshold: 0.6,
            metadata_anchors: Some(8),
            seed_nft_fingerprint: "fixture".into(),
            seeds_path: "seeds.json".into(),
            seeds: vec![seed.clone()],
        };
        let cache = build_dedup_cache(&store, params.clone(), &[(seed, seed_id, graph)], &[]);
        let dir = std::env::temp_dir().join(format!("analysis_dedup_cache_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dedup_cache.json");
        write_dedup_cache(&path, &cache).unwrap();
        let loaded = load_dedup_cache(&path).unwrap();
        validate_dedup_cache(&loaded, &params).unwrap();
        let mut name_disabled = params.clone();
        name_disabled.name_threshold = None;
        assert!(validate_dedup_cache(&loaded, &name_disabled).is_err());
        let (completed, failures) = rematerialize_dedup_batch(&store, &loaded).unwrap();
        assert!(failures.is_empty());
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].2.edges().len(), 1);
        assert_eq!(completed[0].2.edges()[0].candidate_contract, cand);
        assert_eq!(completed[0].2.edges()[0].candidate_nft, Some(nft));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
