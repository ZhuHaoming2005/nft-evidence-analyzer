//! Output directory layout for analysis runs.
//!
//! ```text
//! <output-dir>/
//!   intermediate/          # caches, failures, run manifest
//!   detail/                # per-seed + per-candidate full objects
//!     seeds/<chain>__<addr>/
//!     candidates/
//!   summary/
//!     intra_chain/<chain>.*
//!     intra_chain.*                 # all same-chain relations
//!     chain_pairs/<primary>_to_<secondary>.*
//!     chain_matrix.*                # all directional pairs
//!     cross_chain_by_source/<chain>.*
//!     cross_chain.*                 # all cross-chain relations
//!     all_chains.*                  # all relations
//! ```

use std::path::{Path, PathBuf};

/// Directory for caches, failures, and the run manifest.
pub const INTERMEDIATE_DIR: &str = "intermediate";
/// Directory for per-seed and per-candidate detail artifacts.
pub const DETAIL_DIR: &str = "detail";
/// Directory for four-scope summary rollups.
pub const SUMMARY_DIR: &str = "summary";

/// Scope file stem: single-chain.
pub const SCOPE_INTRA_CHAIN: &str = "intra_chain";
/// Scope file stem: directional chain matrix.
pub const SCOPE_CHAIN_MATRIX: &str = "chain_matrix";
/// Scope file stem: cross-chain summary (any other chain).
pub const SCOPE_CROSS_CHAIN: &str = "cross_chain";
/// Scope file stem: all-chains batch aggregate (fourth dimension).
pub const SCOPE_ALL_CHAINS: &str = "all_chains";

/// JSON scope label for cross-chain summary files.
pub const SCOPE_LABEL_CROSS_CHAIN: &str = "cross_chain_summary";
/// JSON scope label for all-chains aggregate.
pub const SCOPE_LABEL_ALL_CHAINS: &str = "all_chains";

/// Relative path prefix used inside seed reports for candidate pointers.
pub const DETAIL_CANDIDATES_REL: &str = "detail/candidates";

pub fn intermediate_dir(output_dir: &Path) -> PathBuf {
    output_dir.join(INTERMEDIATE_DIR)
}

pub fn detail_dir(output_dir: &Path) -> PathBuf {
    output_dir.join(DETAIL_DIR)
}

pub fn summary_dir(output_dir: &Path) -> PathBuf {
    output_dir.join(SUMMARY_DIR)
}

pub fn detail_seeds_dir(output_dir: &Path) -> PathBuf {
    detail_dir(output_dir).join("seeds")
}

pub fn detail_candidates_dir(output_dir: &Path) -> PathBuf {
    detail_dir(output_dir).join("candidates")
}

pub fn seed_report_dir(output_dir: &Path, seed_dir_name: &str) -> PathBuf {
    detail_seeds_dir(output_dir).join(seed_dir_name)
}

pub fn summary_scope_path(output_dir: &Path, scope_stem: &str, ext: &str) -> PathBuf {
    summary_dir(output_dir).join(format!("{scope_stem}.{ext}"))
}

pub fn intermediate_path(output_dir: &Path, file_name: &str) -> PathBuf {
    intermediate_dir(output_dir).join(file_name)
}

/// Ensure intermediate / detail / summary roots exist.
pub fn ensure_output_layout(output_dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(intermediate_dir(output_dir))?;
    std::fs::create_dir_all(detail_seeds_dir(output_dir))?;
    std::fs::create_dir_all(detail_candidates_dir(output_dir))?;
    std::fs::create_dir_all(summary_dir(output_dir))?;
    Ok(())
}
