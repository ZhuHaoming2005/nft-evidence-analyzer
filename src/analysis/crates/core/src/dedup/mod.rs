//! Dedup hit graph and candidate registry.

use std::sync::atomic::{AtomicBool, Ordering};

pub mod candidates;
pub mod hits;
pub mod metadata;
pub mod name;
pub mod uri;

pub use candidates::{CandidateRegistry, SeedCandidateRelation};
pub use hits::{Dimension, HitEdge, HitGraph, ScopeKind};
pub use metadata::{
    DEFAULT_METADATA_THRESHOLD, MetadataIndex, MetadataQueryScratch, finalize_metadata_index,
    query_metadata_for_seed, query_metadata_for_seed_with_scratch,
};
pub use name::{
    DEFAULT_NAME_THRESHOLD, NameQueryScratch, finalize_name_index, query_name_for_seed,
    query_name_for_seed_with_scratch,
};
pub use uri::{UriQueryScratch, query_uri_for_seed, query_uri_for_seed_with_scratch};

/// When seed-level Rayon already saturates the pool, inner `par_chunks` only
/// adds work-stealing jitter. Pipeline stages toggle this around outer
/// `par_iter` over seeds.
static ALLOW_INNER_QUERY_PARALLEL: AtomicBool = AtomicBool::new(true);

/// Enable/disable nested query `par_chunks` (URI/Name/Metadata).
pub fn set_inner_query_parallel(allowed: bool) {
    ALLOW_INNER_QUERY_PARALLEL.store(allowed, Ordering::Relaxed);
}

#[inline]
pub(crate) fn inner_query_parallel_allowed() -> bool {
    ALLOW_INNER_QUERY_PARALLEL.load(Ordering::Relaxed)
}
