//! NFT seed matching, evidence collection, and report generation.

pub mod analysis;
pub mod dedup;
pub mod enrich;
pub mod entity;
pub mod error;
mod normalize;
pub mod parquet;
pub mod progress;
pub mod reporting;
pub mod seed;
pub mod seed_nft;

pub use analysis::{
    AddressRole, BehaviorFacts, BehaviorInstance, BehaviorKind, CandidateAnalysis, LinkedLossEvent,
    PaperConfig, analyze_candidate,
};
pub use dedup::{
    CandidateRegistry, DEFAULT_METADATA_THRESHOLD, DEFAULT_NAME_THRESHOLD, Dimension, HitEdge,
    HitGraph, MetadataIndex, MetadataQueryScratch, NameQueryScratch, ScopeKind,
    SeedCandidateRelation, UriQueryScratch, finalize_metadata_index, finalize_name_index,
    query_metadata_for_seed, query_metadata_for_seed_with_scratch, query_name_for_seed,
    query_name_for_seed_with_scratch, query_uri_for_seed, query_uri_for_seed_with_scratch,
    set_inner_query_parallel,
};
pub use enrich::{
    ApiKeys, EvidenceBundle, EvidenceObservation, EvidenceQuality, EvidenceStatus, HolderRecord,
    HttpLimits, LegitSignals, PriceBucket, ProviderEndpoints, SaleEvent, TransferEvent,
    ValueFlowEdge, ValueFlowKind, enrich_candidates, enrich_candidates_with_hook,
    finalize_legit_signals, migrate_legacy_success_response_cache,
    migrate_legacy_success_response_cache_with_progress, refresh_cached_evm_holders,
    refresh_cached_prices, refresh_relation_legit,
};
pub use entity::{
    ChainId, ChainTotals, Contract, ContractId, CsrIndex, IdentityRow, MetadataRecord, Nft, NftId,
    ResidentStore, SourceOrder, StringId, StringPool, compare_token_ids, compare_token_ids_desc,
};
pub use error::AnalysisError;
pub use parquet::{
    LoadOptions, PendingDedupLoad, load_resident_store, load_resident_store_uri_ready,
};
pub use progress::{EwmaEta, NoopProgress, ProgressObserver};
pub use reporting::{
    CachedChainTotals, CachedDatasetStats, CachedHitEdge, CachedSeedHits, DEDUP_CACHE_VERSION,
    DEFAULT_DEDUP_CACHE_FILE, DEFAULT_EVIDENCE_CACHE_BATCH, DEFAULT_EVIDENCE_CACHE_FILE,
    DETAIL_CANDIDATES_REL, DETAIL_DIR, DedupCacheFile, DedupCacheParams, DedupRunParams,
    EVIDENCE_CACHE_VERSION, EvidenceCacheFile, EvidenceCacheMigrationStats, EvidenceCacheParams,
    EvidenceCacheSink, FailureRecord, INTERMEDIATE_DIR, InputFileFingerprint, SCOPE_ALL_CHAINS,
    SCOPE_CHAIN_MATRIX, SCOPE_CROSS_CHAIN, SCOPE_INTRA_CHAIN, SUMMARY_DIR, ScopeAnalysisSets,
    ScopeNftCounts, SeedDedupReport, SeedFullReport, SeedRecord, build_all_chains_duplicate_scale,
    build_contract_nft_map, build_contract_nft_map_for_graphs, build_dedup_cache,
    build_evidence_cache, build_seed_analysis_rollup, build_seed_dedup_report,
    candidate_json_rel_path, count_failed_seeds, count_scope_nfts, default_dedup_cache_path,
    default_evidence_cache_path, detail_candidates_dir, detail_dir, ensure_output_layout,
    evidence_cache_artifacts_present, evidence_cache_params, intermediate_dir, load_dedup_cache,
    load_evidence_cache, load_evidence_cache_resumable, load_seeds_json,
    migrate_evidence_cache_layout, rematerialize_dedup_batch, rematerialize_evidence,
    rematerialize_evidence_owned, resolve_seed_contract, scopes_complete_for_seed,
    serialize_candidate_json, summary_dir, validate_dedup_cache, validate_evidence_cache,
    write_candidate_json, write_candidate_json_bytes, write_dedup_cache, write_dedup_outputs,
    write_evidence_cache, write_evidence_cache_sharded, write_run_outputs,
};
pub use seed::{
    SeedRecord as SelectedSeed, SelectSeedsOptions, select_seeds, select_seeds_async,
    write_seed_outputs,
};
pub use seed_nft::{
    MAX_SEED_NFTS_PER_CONTRACT, SeedNftCacheRef, SeedNftDownloadOptions, SeedNftRecord,
    cache_fingerprint, for_each_cached_nft, prepare_seed_nft_caches, release_resident_seed_nfts,
};
