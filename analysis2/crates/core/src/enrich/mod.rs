//! Provider HTTP clients and candidate enrichment.

pub mod alchemy;
pub mod controllers;
pub mod etherscan;
pub mod helius;
pub mod http;
pub mod legit_detect;
pub mod mint_payment;
pub mod opensea;
pub mod orchestrator;
pub(crate) mod roles;
pub mod types;
pub mod value_flow;

pub use helius::{parse_collection_address, resolve_collection_address};
pub use http::{
    HttpClient, OPENSEA_RATE_LIMIT_BURST, OPENSEA_RATE_LIMIT_REFILL_MS, SuccessCacheMigrationStats,
    TokenBucketRateLimiter, is_http_not_found, migrate_legacy_success_response_cache,
    migrate_legacy_success_response_cache_with_progress, print_provider_error,
};
pub use legit_detect::attach_relation_legit;
pub use opensea::{OpenSeaRankedItem, parse_top_collections};
pub use orchestrator::{
    enrich_candidates, enrich_candidates_with_hook, refresh_cached_evm_holders,
    refresh_cached_prices, refresh_relation_legit,
};
pub use types::{
    ApiKeys, DeploymentEvent, EvidenceBundle, EvidenceObservation, EvidenceQuality, EvidenceStatus,
    HolderRecord, HttpLimits, LegitSignals, PriceBucket, ProviderEndpoints, SaleEvent,
    TransferEvent, ValueFlowEdge, ValueFlowKind, chain_addresses_equal, finalize_legit_signals,
    normalize_chain_address, normalize_chain_transaction,
};
