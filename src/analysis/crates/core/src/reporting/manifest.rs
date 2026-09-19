//! Run manifest and failure log writers.

use std::fs;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::AnalysisError;

use super::json::DedupRunParams;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunManifestSeeds {
    pub selected: u64,
    pub analyzed: u64,
    pub failed: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunManifest {
    pub status: String,
    pub command: String,
    pub params: DedupRunParams,
    pub snapshot: Value,
    pub seeds: RunManifestSeeds,
    pub completeness: Value,
    pub pricing_policy: String,
    pub stage_timings: Value,
    /// Directory split: intermediate / detail / summary + four scope names.
    #[serde(default = "default_output_layout")]
    pub output_layout: Value,
}

fn default_output_layout() -> Value {
    serde_json::json!({
        "intermediate": "intermediate",
        "detail": "detail",
        "summary": "summary",
        "scopes": [
            "intra_chain",
            "chain_matrix",
            "cross_chain_summary",
            "all_chains"
        ],
    })
}

/// One recoverable failure (seed / stage); siblings continue.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureRecord {
    pub seed_chain: String,
    pub seed_address: String,
    pub scope: String,
    pub stage: String,
    pub provider: String,
    pub retryable: bool,
    pub error: String,
}

impl FailureRecord {
    pub fn seed_stage(chain: &str, address: &str, stage: &str, error: impl Into<String>) -> Self {
        Self {
            seed_chain: chain.to_owned(),
            seed_address: address.to_owned(),
            scope: "seed".into(),
            stage: stage.to_owned(),
            provider: "local".into(),
            retryable: false,
            error: error.into(),
        }
    }

    pub fn candidate_stage(
        chain: &str,
        address: &str,
        stage: &str,
        error: impl Into<String>,
    ) -> Self {
        Self {
            seed_chain: chain.to_owned(),
            seed_address: address.to_owned(),
            scope: "candidate".into(),
            stage: stage.to_owned(),
            provider: "local".into(),
            retryable: false,
            error: error.into(),
        }
    }

    /// Provider/API failure attached to a candidate. These rows are diagnostic:
    /// they never remove the candidate or any related seed from result summaries.
    pub fn candidate_api(chain: &str, address: &str, error: impl Into<String>) -> Self {
        let error = error.into();
        let lower = error.to_ascii_lowercase();
        let provider = ["alchemy", "opensea", "helius", "etherscan", "magic_eden"]
            .into_iter()
            .find(|provider| lower.contains(provider))
            .unwrap_or("unknown");
        Self {
            seed_chain: chain.to_owned(),
            seed_address: address.to_owned(),
            scope: "candidate".into(),
            stage: "api_request".into(),
            provider: provider.into(),
            retryable: true,
            error,
        }
    }

    pub fn is_seed_scope(&self) -> bool {
        self.scope == "seed"
    }
}

/// Unique seeds that failed a seed-stage (resolve / dedup / incomplete seed path).
/// Candidate-stage rows in `failures.jsonl` do **not** inflate this count;
/// API/candidate-stage rows are diagnostic and never remove seed results.
pub fn count_failed_seeds(failures: &[FailureRecord]) -> u64 {
    let mut keys: Vec<(&str, &str)> = failures
        .iter()
        .filter(|f| f.is_seed_scope())
        .map(|f| (f.seed_chain.as_str(), f.seed_address.as_str()))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys.len() as u64
}

pub fn write_failures_jsonl(path: &Path, failures: &[FailureRecord]) -> Result<(), AnalysisError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::File::create(path)?;
    for fail in failures {
        let line = serde_json::to_string(fail)
            .map_err(|e| AnalysisError::invalid(format!("failures jsonl encode: {e}")))?;
        writeln!(file, "{line}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_api_error_is_retryable_and_keeps_provider() {
        let row = FailureRecord::candidate_api(
            "solana",
            "candidate",
            "opensea_sales: HTTP 404 contract not found",
        );
        assert_eq!(row.scope, "candidate");
        assert_eq!(row.stage, "api_request");
        assert_eq!(row.provider, "opensea");
        assert!(row.retryable);
    }
}
