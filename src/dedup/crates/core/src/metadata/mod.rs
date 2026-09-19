mod bm25;
mod canonical_json;
mod direct;

pub(crate) use canonical_json::canonicalize_json as canonicalize_json_strict;
pub use direct::MetadataStats;

use crate::entity::EntityStore;
use crate::error::DedupError;
use crate::progress::ProgressObserver;
use crate::stats::SummaryAccumulator;

pub struct MetadataRunResult {
    pub stats: MetadataStats,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataSamplePool {
    IntraChain,
    CrossChain,
}

#[derive(Clone, Debug)]
pub struct MetadataSampleDownloadCandidate {
    pub id: u64,
    pub pool: MetadataSamplePool,
    pub sample: MetadataImagePairSample,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataSampleDownloadResult {
    pub id: u64,
    pub success: bool,
}

/// Asynchronous media sink used only by the opt-in Metadata sampler.
///
/// `submit` must preserve the supplied candidate IDs. `poll(false)` is
/// non-blocking; `poll(true)` waits until at least one submitted candidate
/// finishes or cancellation is observed.
pub trait MetadataSampleDownloadSink {
    fn submit(&mut self, candidates: &[MetadataSampleDownloadCandidate]) -> Result<(), DedupError>;

    fn poll(&mut self, wait: bool) -> Result<Vec<MetadataSampleDownloadResult>, DedupError>;

    fn retain_candidates(&mut self, _candidate_ids: &[u64]) -> Result<(), DedupError> {
        Ok(())
    }
}

pub struct MetadataFastSampleResult {
    pub intra_chain: Vec<MetadataImagePairSample>,
    pub cross_chain: Vec<MetadataImagePairSample>,
    pub scored_profile_pairs: u64,
    pub profile_visits: u64,
    pub planned_profile_visits: u64,
    pub sample_seed: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataImagePairSample {
    pub contract_a_chain: String,
    pub contract_a_address: String,
    pub token_id_a: String,
    pub image_uri_a: String,
    pub metadata_json_a: String,
    pub contract_b_chain: String,
    pub contract_b_address: String,
    pub token_id_b: String,
    pub image_uri_b: String,
    pub metadata_json_b: String,
}

pub fn run_metadata(
    store: &mut EntityStore,
    evm_chains: &std::collections::HashSet<String>,
    anchors_k: Option<usize>,
    content_threshold: f64,
    acc: &mut SummaryAccumulator,
    progress: &dyn ProgressObserver,
) -> Result<MetadataRunResult, DedupError> {
    progress.set_stage("metadata");
    let stats = direct::run_direct_releasing(
        store,
        evm_chains,
        anchors_k,
        content_threshold,
        acc,
        progress,
    )?;

    Ok(MetadataRunResult { stats })
}

#[allow(clippy::too_many_arguments)]
pub fn sample_metadata(
    store: &mut EntityStore,
    evm_chains: &std::collections::HashSet<String>,
    anchors_k: Option<usize>,
    content_threshold: f64,
    target_per_pool: usize,
    sample_seed: Option<u64>,
    progress: &dyn ProgressObserver,
    downloads: &mut dyn MetadataSampleDownloadSink,
) -> Result<MetadataFastSampleResult, DedupError> {
    progress.set_stage("sample_metadata");
    direct::sample_direct_releasing(
        store,
        evm_chains,
        anchors_k,
        content_threshold,
        target_per_pool,
        sample_seed,
        progress,
        downloads,
    )
}
