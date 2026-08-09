//! Offline reporting: duplicate-scale aggregates, JSON/Markdown, run manifest.

pub mod aggregate;
pub mod dedup_cache;
pub mod evidence_cache;
pub mod json;
pub mod layout;
pub mod manifest;
pub mod markdown;
pub mod run;

pub use aggregate::{
    AllChainsRelationRef, ChainMatrixBlock, DuplicateScaleRow, ScopeNftCounts, ScopeScaleFilter,
    SeedDuplicateScale, build_all_chains_duplicate_scale, build_contract_nft_map,
    build_contract_nft_map_for_graphs, build_duplicate_scale_rows, build_scope_duplicate_scale,
    build_scope_duplicate_scale_for_chains, build_seed_duplicate_scale, count_scope_nfts,
    matrix_secondary_chains,
};
pub use dedup_cache::{
    CachedChainTotals, CachedDatasetStats, CachedHitEdge, CachedSeedHits, DEDUP_CACHE_VERSION,
    DEFAULT_DEDUP_CACHE_FILE, DedupCacheFile, DedupCacheParams, InputFileFingerprint,
    build_dedup_cache, default_dedup_cache_path, load_dedup_cache, rematerialize_dedup_batch,
    validate_dedup_cache, write_dedup_cache,
};
pub use evidence_cache::{
    DEFAULT_EVIDENCE_CACHE_BATCH, DEFAULT_EVIDENCE_CACHE_FILE, EVIDENCE_CACHE_VERSION,
    EvidenceCacheFile, EvidenceCacheMigrationStats, EvidenceCacheParams, EvidenceCacheSink,
    build_evidence_cache, default_evidence_cache_path, evidence_cache_artifacts_present,
    evidence_cache_params, load_evidence_cache, load_evidence_cache_resumable,
    migrate_evidence_cache_layout, rematerialize_evidence, rematerialize_evidence_owned,
    validate_evidence_cache, write_evidence_cache, write_evidence_cache_sharded,
};
pub use json::{
    DedupRunParams, SeedDedupReport, SeedRecord, SeedRelationJson, build_seed_dedup_report,
    load_seeds_json, resolve_seed_contract, seed_dir_name, write_dedup_outputs,
};
pub use layout::{
    DETAIL_CANDIDATES_REL, DETAIL_DIR, INTERMEDIATE_DIR, SCOPE_ALL_CHAINS, SCOPE_CHAIN_MATRIX,
    SCOPE_CROSS_CHAIN, SCOPE_INTRA_CHAIN, SCOPE_LABEL_ALL_CHAINS, SCOPE_LABEL_CROSS_CHAIN,
    SUMMARY_DIR, detail_candidates_dir, detail_dir, detail_seeds_dir, ensure_output_layout,
    intermediate_dir, intermediate_path, seed_report_dir, summary_dir, summary_scope_path,
};
pub use manifest::{FailureRecord, RunManifest, RunManifestSeeds, count_failed_seeds};
pub use run::{
    CandidateRef, EconomicsUsdRollup, RunSummaryScope, ScopeAnalysisSets, SeedAnalysisRollup,
    SeedFullReport, build_run_summary, build_seed_analysis_rollup, candidate_file_name,
    candidate_json_rel_path, scopes_complete_for_seed, serialize_candidate_json,
    write_candidate_json, write_candidate_json_bytes, write_run_outputs,
};
