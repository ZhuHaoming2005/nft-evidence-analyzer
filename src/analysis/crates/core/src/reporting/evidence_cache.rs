//! Durable enrich evidence checkpoint for `run` restarts.
//!
//! Network evidence is written **incrementally** while enrich runs:
//! - `evidence_cache.meta.json` — version + params
//! - `evidence_cache.entries/<sha-prefix>/<sha>.json.zst` — one atomic shard per candidate
//!
//! Legacy `evidence_cache.jsonl`/`evidence_cache.json` artifacts are loaded once,
//! converted to compressed shards, and removed only after the new layout is
//! complete. After an interrupt, the next run rematerializes durable shards and
//! only HTTP-fetches candidates still missing.
//! Pagination bounds must match. A stale pricing day triggers a price-only
//! refresh. Seed membership and provider-key presence do not discard
//! successfully collected candidate evidence. Producer versions remain
//! reusable only while their provider semantics are compatible.

use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::enrich::types::{ApiKeys, EvidenceBundle, HttpLimits, normalize_chain_address};
use crate::entity::{ContractId, ResidentStore};
use crate::error::AnalysisError;
use crate::reporting::json::SeedRecord;

// Version 15 invalidates older provider evidence: those bundles can contain a
// capped Helius `total`, address-based NFT history, indefinitely cached mutable
// responses, or incomplete Etherscan ERC-721-only transfer evidence.
pub const EVIDENCE_CACHE_VERSION: u32 = 15;
const MIN_REUSABLE_EVIDENCE_CACHE_VERSION: u32 = 15;
pub const DEFAULT_EVIDENCE_CACHE_FILE: &str = "evidence_cache.json";
/// How many finished candidates to buffer before writing atomic compressed shards.
pub const DEFAULT_EVIDENCE_CACHE_BATCH: usize = 16;
const SHARDED_STORAGE: &str = "sharded_zstd_v1";
const EVIDENCE_ZSTD_LEVEL: i32 = 3;
static EVIDENCE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Parameters that must match between the producing and reusing runs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvidenceCacheParams {
    /// Seeds recorded for provenance only. Candidate HTTP evidence is keyed by
    /// chain/address and remains reusable when the run's seed set changes.
    pub seeds: Vec<SeedRecord>,
    pub seeds_path: String,
    pub max_transfer_pages: usize,
    pub max_holder_pages: usize,
    pub max_sale_pages: usize,
    pub max_solana_assets: usize,
    pub max_history_assets: usize,
    pub max_signatures_per_asset: usize,
    /// UTC day whose run-time spot prices are embedded in cached bundles.
    #[serde(default)]
    pub pricing_day_utc: i64,
    /// Whether each provider key was present when the cache was built (not the secret).
    pub had_alchemy: bool,
    pub had_etherscan: bool,
    pub had_helius: bool,
    pub had_opensea: bool,
}

/// On-disk enrich checkpoint. Bundles use stable chain/address identity;
/// `contract_id` is rewritten on rematerialize.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceCacheFile {
    pub version: u32,
    pub params: EvidenceCacheParams,
    pub bundles: Vec<EvidenceBundle>,
}

/// Default cache path: `{output_dir}/intermediate/evidence_cache.json`.
pub fn default_evidence_cache_path(output_dir: &Path) -> PathBuf {
    super::layout::intermediate_path(output_dir, DEFAULT_EVIDENCE_CACHE_FILE)
}

fn companion_jsonl(path: &Path) -> PathBuf {
    path.with_extension("jsonl")
}

fn companion_meta(path: &Path) -> PathBuf {
    // evidence_cache.json → evidence_cache.meta.json
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("evidence_cache");
    path.with_file_name(format!("{stem}.meta.json"))
}

fn companion_entries_dir(path: &Path) -> PathBuf {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("evidence_cache");
    path.with_file_name(format!("{stem}.entries"))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EvidenceCacheMeta {
    version: u32,
    params: EvidenceCacheParams,
    #[serde(default)]
    storage: Option<String>,
}

/// True when the current sharded layout or a reusable legacy artifact exists.
pub fn evidence_cache_artifacts_present(path: &Path) -> bool {
    path.is_file()
        || (companion_meta(path).is_file()
            && (companion_jsonl(path).is_file() || companion_entries_dir(path).is_dir()))
}

/// Build params from the current run knobs (no secrets).
pub fn evidence_cache_params(
    seeds: &[SeedRecord],
    seeds_path: &str,
    keys: &ApiKeys,
    limits: &HttpLimits,
) -> EvidenceCacheParams {
    EvidenceCacheParams {
        seeds: seeds.to_vec(),
        seeds_path: seeds_path.to_owned(),
        max_transfer_pages: limits.max_transfer_pages,
        max_holder_pages: limits.max_holder_pages,
        max_sale_pages: limits.max_sale_pages,
        max_solana_assets: limits.max_solana_assets,
        max_history_assets: limits.max_history_assets,
        max_signatures_per_asset: limits.max_signatures_per_asset,
        pricing_day_utc: crate::enrich::types::now_unix().div_euclid(86_400) * 86_400,
        had_alchemy: keys.alchemy().is_some(),
        had_etherscan: keys.etherscan().is_some(),
        had_helius: keys.helius().is_some(),
        had_opensea: keys.opensea().is_some(),
    }
}

fn portable_bundle(bundle: &EvidenceBundle) -> EvidenceBundle {
    let mut b = bundle.clone();
    b.contract_id = 0;
    b
}

fn bundle_key(bundle: &EvidenceBundle) -> (String, String) {
    (
        bundle.chain.to_ascii_lowercase(),
        normalize_chain_address(&bundle.chain, &bundle.address),
    )
}

/// Build a portable cache from in-memory evidence (stable chain/address keys).
pub fn build_evidence_cache(
    params: EvidenceCacheParams,
    evidence: &AHashMap<ContractId, EvidenceBundle>,
) -> EvidenceCacheFile {
    let mut by_key: AHashMap<(String, String), EvidenceBundle> = AHashMap::new();
    for bundle in evidence.values() {
        let portable = portable_bundle(bundle);
        by_key.insert(bundle_key(&portable), portable);
    }
    let mut bundles: Vec<EvidenceBundle> = by_key.into_values().collect();
    bundles.sort_by(|a, b| {
        a.chain
            .cmp(&b.chain)
            .then_with(|| a.address.cmp(&b.address))
    });
    EvidenceCacheFile {
        version: EVIDENCE_CACHE_VERSION,
        params,
        bundles,
    }
}

/// Write cache JSON (compact, non-pretty) atomically via temp file + rename.
pub fn write_evidence_cache(path: &Path, cache: &EvidenceCacheFile) -> Result<(), AnalysisError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec(cache)
        .map_err(|e| AnalysisError::invalid(format!("serialize evidence cache: {e}")))?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, &body)?;
    if let Err(error) = fs::rename(&tmp, path) {
        fs::write(path, &body).map_err(|e| {
            AnalysisError::invalid(format!(
                "write evidence cache {} (rename failed: {error}): {e}",
                path.display()
            ))
        })?;
        let _ = fs::remove_file(&tmp);
    }
    Ok(())
}

/// Load and parse a full `evidence_cache.json` file.
pub fn load_evidence_cache(path: &Path) -> Result<EvidenceCacheFile, AnalysisError> {
    let text = fs::read_to_string(path).map_err(|e| {
        AnalysisError::invalid(format!("read evidence cache {}: {e}", path.display()))
    })?;
    let cache: EvidenceCacheFile = serde_json::from_str(&text).map_err(|e| {
        AnalysisError::invalid(format!("parse evidence cache {}: {e}", path.display()))
    })?;
    if !(MIN_REUSABLE_EVIDENCE_CACHE_VERSION..=EVIDENCE_CACHE_VERSION).contains(&cache.version) {
        return Err(AnalysisError::invalid(format!(
            "evidence cache version {} is incompatible; supported versions are {MIN_REUSABLE_EVIDENCE_CACHE_VERSION}..={EVIDENCE_CACHE_VERSION}",
            cache.version,
        )));
    }
    Ok(cache)
}

/// Load the current sharded-zstd cache, or import-compatible legacy JSONL/JSON.
/// The loader never parses more than one representation.
pub fn load_evidence_cache_resumable(path: &Path) -> Result<EvidenceCacheFile, AnalysisError> {
    let meta_path = companion_meta(path);
    let jsonl_path = companion_jsonl(path);
    let entries_dir = companion_entries_dir(path);
    let has_json = path.is_file();
    let has_jsonl = meta_path.is_file() && jsonl_path.is_file();
    let has_shards = meta_path.is_file() && entries_dir.is_dir();

    if !has_json && !has_jsonl && !has_shards {
        return Err(AnalysisError::invalid(format!(
            "evidence cache not found at {} (or legacy jsonl/current shard layout)",
            path.display(),
        )));
    }

    if meta_path.is_file() {
        let meta = read_evidence_meta(&meta_path)?;
        validate_cache_version(meta.version)?;
        if meta.storage.as_deref() == Some(SHARDED_STORAGE) {
            if !entries_dir.is_dir() {
                return Err(AnalysisError::invalid(format!(
                    "evidence cache meta selects {SHARDED_STORAGE}, but {} is missing",
                    entries_dir.display()
                )));
            }
            return load_sharded_evidence(&entries_dir, meta);
        }
        if has_jsonl {
            return load_legacy_jsonl(&jsonl_path, meta);
        }
    }

    let cache = load_evidence_cache(path)?;
    eprintln!(
        "evidence cache: loaded {} bundles from snapshot {}",
        cache.bundles.len(),
        path.display()
    );
    Ok(cache)
}

fn read_evidence_meta(path: &Path) -> Result<EvidenceCacheMeta, AnalysisError> {
    let body = fs::read(path).map_err(|e| {
        AnalysisError::invalid(format!("read evidence meta {}: {e}", path.display()))
    })?;
    serde_json::from_slice(&body)
        .map_err(|e| AnalysisError::invalid(format!("parse evidence meta {}: {e}", path.display())))
}

fn validate_cache_version(version: u32) -> Result<(), AnalysisError> {
    if !(MIN_REUSABLE_EVIDENCE_CACHE_VERSION..=EVIDENCE_CACHE_VERSION).contains(&version) {
        return Err(AnalysisError::invalid(format!(
            "evidence cache version {version} is incompatible; supported versions are {MIN_REUSABLE_EVIDENCE_CACHE_VERSION}..={EVIDENCE_CACHE_VERSION}",
        )));
    }
    Ok(())
}

fn load_legacy_jsonl(
    jsonl_path: &Path,
    meta: EvidenceCacheMeta,
) -> Result<EvidenceCacheFile, AnalysisError> {
    let mut by_key: AHashMap<(String, String), EvidenceBundle> = AHashMap::new();
    let file = File::open(jsonl_path).map_err(|e| {
        AnalysisError::invalid(format!("read evidence jsonl {}: {e}", jsonl_path.display()))
    })?;
    let mut line_count = 0_usize;
    for (line_no, line) in BufReader::new(file).lines().enumerate() {
        let line = line.map_err(|e| {
            AnalysisError::invalid(format!(
                "read evidence jsonl {}: line {}: {e}",
                jsonl_path.display(),
                line_no + 1
            ))
        })?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let bundle: EvidenceBundle = serde_json::from_str(line).map_err(|e| {
            AnalysisError::invalid(format!(
                "parse evidence jsonl {}: line {}: {e}",
                jsonl_path.display(),
                line_no + 1
            ))
        })?;
        by_key.insert(bundle_key(&bundle), bundle);
        line_count += 1;
    }
    eprintln!(
        "evidence cache: loaded {} unique bundles from legacy jsonl ({} lines) at {}",
        by_key.len(),
        line_count,
        jsonl_path.display()
    );
    Ok(cache_from_map(meta.version, meta.params, by_key))
}

fn load_sharded_evidence(
    entries_dir: &Path,
    meta: EvidenceCacheMeta,
) -> Result<EvidenceCacheFile, AnalysisError> {
    let mut paths = collect_evidence_shards(entries_dir)?;
    paths.sort();
    let mut by_key = AHashMap::with_capacity(paths.len());
    for path in &paths {
        let compressed = fs::read(path).map_err(|e| {
            AnalysisError::invalid(format!("read evidence shard {}: {e}", path.display()))
        })?;
        let body = zstd::stream::decode_all(std::io::Cursor::new(compressed)).map_err(|e| {
            AnalysisError::invalid(format!("decompress evidence shard {}: {e}", path.display()))
        })?;
        let bundle: EvidenceBundle = serde_json::from_slice(&body).map_err(|e| {
            AnalysisError::invalid(format!("parse evidence shard {}: {e}", path.display()))
        })?;
        let expected_path = evidence_shard_path(entries_dir, &bundle);
        if &expected_path != path {
            return Err(AnalysisError::invalid(format!(
                "evidence shard identity/path mismatch: expected {}, got {}",
                expected_path.display(),
                path.display()
            )));
        }
        by_key.insert(bundle_key(&bundle), bundle);
    }
    eprintln!(
        "evidence cache: loaded {} bundles from compressed shards at {}",
        by_key.len(),
        entries_dir.display()
    );
    Ok(cache_from_map(meta.version, meta.params, by_key))
}

fn cache_from_map(
    version: u32,
    params: EvidenceCacheParams,
    by_key: AHashMap<(String, String), EvidenceBundle>,
) -> EvidenceCacheFile {
    let mut bundles: Vec<EvidenceBundle> = by_key.into_values().collect();
    bundles.sort_by(|a, b| {
        a.chain
            .cmp(&b.chain)
            .then_with(|| a.address.cmp(&b.address))
    });
    EvidenceCacheFile {
        version,
        params,
        bundles,
    }
}

fn collect_evidence_shards(entries_dir: &Path) -> Result<Vec<PathBuf>, AnalysisError> {
    let mut files = Vec::new();
    let mut pending = vec![entries_dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let entries = fs::read_dir(&dir).map_err(|e| {
            AnalysisError::invalid(format!(
                "read evidence shard directory {}: {e}",
                dir.display()
            ))
        })?;
        for entry in entries {
            let entry = entry.map_err(|e| {
                AnalysisError::invalid(format!("read evidence shard entry {}: {e}", dir.display()))
            })?;
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("zst") {
                files.push(path);
            }
        }
    }
    Ok(files)
}

/// Ensure the cache was produced with equivalent evidence completeness bounds.
///
/// Seed membership is deliberately excluded: cached provider responses are
/// candidate-scoped and remain useful across seed changes.
pub fn validate_evidence_cache(
    cache: &EvidenceCacheFile,
    expected: &EvidenceCacheParams,
) -> Result<(), AnalysisError> {
    let got = &cache.params;
    if got.max_transfer_pages != expected.max_transfer_pages
        || got.max_holder_pages != expected.max_holder_pages
        || got.max_sale_pages != expected.max_sale_pages
        || got.max_solana_assets != expected.max_solana_assets
        || got.max_history_assets != expected.max_history_assets
        || got.max_signatures_per_asset != expected.max_signatures_per_asset
    {
        return Err(AnalysisError::invalid(
            "evidence cache pagination limits do not match current HttpLimits",
        ));
    }
    // Price age is handled by a price-only refresh in the pipeline. It must not
    // invalidate unrelated chain and market evidence.
    Ok(())
}

/// Rematerialize evidence keyed by process-local contract ids.
///
/// Address matching is case-insensitive for EVM and case-sensitive for
/// Solana. Bundles absent from the snapshot are skipped.
pub fn rematerialize_evidence(
    store: &ResidentStore,
    cache: &EvidenceCacheFile,
) -> Result<AHashMap<ContractId, EvidenceBundle>, AnalysisError> {
    rematerialize_evidence_owned(store, cache.clone())
}

/// Owned variant used by the production pipeline to avoid retaining and
/// cloning two decoded copies of a multi-gigabyte evidence cache.
pub fn rematerialize_evidence_owned(
    store: &ResidentStore,
    cache: EvidenceCacheFile,
) -> Result<AHashMap<ContractId, EvidenceBundle>, AnalysisError> {
    let mut by_identity: AHashMap<(String, String), ContractId> =
        AHashMap::with_capacity(store.contracts.len());
    for c in &store.contracts {
        let chain = store.chain_name(c.chain_id).to_ascii_lowercase();
        let addr = normalize_chain_address(&chain, &c.address);
        by_identity.insert((chain, addr), c.id);
    }

    let total = cache.bundles.len();
    let mut out = AHashMap::with_capacity(total);
    let mut skipped = 0_usize;
    for mut entry in cache.bundles {
        let key = (
            entry.chain.to_ascii_lowercase(),
            normalize_chain_address(&entry.chain, &entry.address),
        );
        let Some(&contract_id) = by_identity.get(&key) else {
            skipped += 1;
            continue;
        };
        entry.contract_id = contract_id;
        out.insert(contract_id, entry);
    }
    if skipped > 0 {
        eprintln!(
            "evidence cache: skipped {skipped}/{} bundles not present in current snapshot identity",
            total
        );
    } else {
        eprintln!(
            "evidence cache: rematerialized {}/{} bundles into resident contract ids",
            out.len(),
            total
        );
    }
    Ok(out)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EvidenceCacheMigrationStats {
    pub bundles: usize,
    pub legacy_files_removed: usize,
    pub legacy_bytes_removed: u64,
    pub compressed_bytes: u64,
}

/// Materialize a loaded legacy cache into the keyed compressed layout. The new
/// meta file is published only after every shard succeeds; legacy files are
/// removed afterwards, so interruption cannot make the existing cache unusable.
pub fn migrate_evidence_cache_layout(
    path: &Path,
    cache: &EvidenceCacheFile,
) -> Result<EvidenceCacheMigrationStats, AnalysisError> {
    validate_cache_version(cache.version)?;
    if sharded_layout_active(path) {
        let mut stats = EvidenceCacheMigrationStats {
            bundles: cache.bundles.len(),
            ..EvidenceCacheMigrationStats::default()
        };
        for legacy in [path.to_path_buf(), companion_jsonl(path)] {
            if legacy.is_file() {
                let len = fs::metadata(&legacy)
                    .map(|metadata| metadata.len())
                    .unwrap_or(0);
                fs::remove_file(&legacy)?;
                stats.legacy_files_removed += 1;
                stats.legacy_bytes_removed += len;
            }
        }
        return Ok(stats);
    }
    let entries_dir = companion_entries_dir(path);
    fs::create_dir_all(&entries_dir)?;
    let mut stats = EvidenceCacheMigrationStats {
        bundles: cache.bundles.len(),
        ..EvidenceCacheMigrationStats::default()
    };
    for bundle in &cache.bundles {
        stats.compressed_bytes += write_evidence_shard(&entries_dir, bundle)?;
    }
    write_evidence_meta(path, &cache.params)?;

    for legacy in [path.to_path_buf(), companion_jsonl(path)] {
        if legacy.is_file() {
            let len = fs::metadata(&legacy)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            fs::remove_file(&legacy).map_err(|e| {
                AnalysisError::invalid(format!(
                    "remove migrated evidence cache {}: {e}",
                    legacy.display()
                ))
            })?;
            stats.legacy_files_removed += 1;
            stats.legacy_bytes_removed += len;
        }
    }
    Ok(stats)
}

fn sharded_layout_active(path: &Path) -> bool {
    let meta_path = companion_meta(path);
    companion_entries_dir(path).is_dir()
        && read_evidence_meta(&meta_path)
            .ok()
            .and_then(|meta| meta.storage)
            .as_deref()
            == Some(SHARDED_STORAGE)
}

/// Write a complete cache directly in the current sharded-zstd format.
pub fn write_evidence_cache_sharded(
    path: &Path,
    cache: &EvidenceCacheFile,
) -> Result<EvidenceCacheMigrationStats, AnalysisError> {
    validate_cache_version(cache.version)?;
    let entries_dir = companion_entries_dir(path);
    let sequence = EVIDENCE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let staging =
        entries_dir.with_extension(format!("entries.{}.{}.tmp", std::process::id(), sequence));
    let backup =
        entries_dir.with_extension(format!("entries.{}.{}.bak", std::process::id(), sequence));
    if staging.exists() {
        fs::remove_dir_all(&staging)?;
    }
    fs::create_dir_all(&staging)?;
    let mut stats = EvidenceCacheMigrationStats {
        bundles: cache.bundles.len(),
        ..EvidenceCacheMigrationStats::default()
    };
    for bundle in &cache.bundles {
        stats.compressed_bytes += write_evidence_shard(&staging, bundle)?;
    }

    let had_entries = entries_dir.is_dir();
    if had_entries {
        fs::rename(&entries_dir, &backup)?;
    }
    if let Err(error) = fs::rename(&staging, &entries_dir) {
        if had_entries {
            let _ = fs::rename(&backup, &entries_dir);
        }
        return Err(error.into());
    }
    if let Err(error) = write_evidence_meta(path, &cache.params) {
        let failed = entries_dir.with_extension(format!(
            "entries.{}.{}.failed",
            std::process::id(),
            sequence
        ));
        let _ = fs::rename(&entries_dir, &failed);
        if had_entries {
            let _ = fs::rename(&backup, &entries_dir);
        }
        let _ = fs::remove_dir_all(failed);
        return Err(error);
    }
    if had_entries {
        fs::remove_dir_all(backup)?;
    }
    for legacy in [path.to_path_buf(), companion_jsonl(path)] {
        if legacy.is_file() {
            let len = fs::metadata(&legacy)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            fs::remove_file(&legacy)?;
            stats.legacy_files_removed += 1;
            stats.legacy_bytes_removed += len;
        }
    }
    Ok(stats)
}

fn write_evidence_meta(path: &Path, params: &EvidenceCacheParams) -> Result<(), AnalysisError> {
    let meta = EvidenceCacheMeta {
        version: EVIDENCE_CACHE_VERSION,
        params: params.clone(),
        storage: Some(SHARDED_STORAGE.into()),
    };
    let body = serde_json::to_vec(&meta)
        .map_err(|e| AnalysisError::invalid(format!("serialize evidence meta: {e}")))?;
    atomic_evidence_replace(&companion_meta(path), &body)
}

fn evidence_shard_path(entries_dir: &Path, bundle: &EvidenceBundle) -> PathBuf {
    let key = bundle_key(bundle);
    let identity = format!("{}\n{}", key.0, key.1);
    let digest = sha256_hex(identity.as_bytes());
    entries_dir
        .join(&digest[..2])
        .join(format!("{digest}.json.zst"))
}

fn write_evidence_shard(entries_dir: &Path, bundle: &EvidenceBundle) -> Result<u64, AnalysisError> {
    let portable = portable_bundle(bundle);
    let path = evidence_shard_path(entries_dir, &portable);
    let parent = path.parent().ok_or_else(|| {
        AnalysisError::invalid(format!("evidence shard has no parent: {}", path.display()))
    })?;
    fs::create_dir_all(parent)?;
    let body = serde_json::to_vec(&portable)
        .map_err(|e| AnalysisError::invalid(format!("serialize evidence shard: {e}")))?;
    let compressed = zstd::stream::encode_all(std::io::Cursor::new(body), EVIDENCE_ZSTD_LEVEL)
        .map_err(|e| AnalysisError::invalid(format!("compress evidence shard: {e}")))?;
    let len = compressed.len() as u64;
    atomic_evidence_replace(&path, &compressed)?;
    Ok(len)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn atomic_evidence_replace(path: &Path, body: &[u8]) -> Result<(), AnalysisError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let sequence = EVIDENCE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let tmp = path.with_extension(format!("{}.{}.tmp", std::process::id(), sequence));
    fs::write(&tmp, body)?;
    if !path.exists() {
        fs::rename(&tmp, path)?;
        return Ok(());
    }

    let backup = path.with_extension(format!("{}.{}.bak", std::process::id(), sequence));
    fs::rename(path, &backup)?;
    if let Err(error) = fs::rename(&tmp, path) {
        let _ = fs::rename(&backup, path);
        let _ = fs::remove_file(&tmp);
        return Err(AnalysisError::invalid(format!(
            "replace evidence cache {}: {error}",
            path.display()
        )));
    }
    let _ = fs::remove_file(backup);
    Ok(())
}

fn clear_evidence_artifacts(path: &Path) -> Result<(), AnalysisError> {
    for file in [
        path.to_path_buf(),
        companion_jsonl(path),
        companion_meta(path),
    ] {
        if file.is_file() {
            fs::remove_file(&file)?;
        }
    }
    let entries_dir = companion_entries_dir(path);
    if entries_dir.is_dir() {
        fs::remove_dir_all(entries_dir)?;
    }
    Ok(())
}

/// Incremental keyed writer. Each candidate is one independently compressed,
/// atomically replaced shard, so resume does not require an append log, a full
/// snapshot rewrite, or a second in-memory copy of every bundle.
pub struct EvidenceCacheSink {
    path: PathBuf,
    params: EvidenceCacheParams,
    entries_dir: PathBuf,
    batch_size: usize,
    pending: Vec<EvidenceBundle>,
    known_paths: AHashSet<PathBuf>,
}

impl EvidenceCacheSink {
    /// Create or resume a sink, importing a compatible legacy cache if needed.
    pub fn create(
        path: &Path,
        params: EvidenceCacheParams,
        batch_size: usize,
    ) -> Result<Self, AnalysisError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut compatible_existing = false;
        if sharded_layout_active(path) {
            let meta = read_evidence_meta(&companion_meta(path))?;
            validate_cache_version(meta.version)?;
            let header = EvidenceCacheFile {
                version: meta.version,
                params: meta.params,
                bundles: Vec::new(),
            };
            if validate_evidence_cache(&header, &params).is_ok() {
                compatible_existing = true;
            } else {
                eprintln!(
                    "evidence cache: params changed; replacing incompatible cache at {}",
                    path.display()
                );
                clear_evidence_artifacts(path)?;
            }
        } else if evidence_cache_artifacts_present(path) {
            match load_evidence_cache_resumable(path) {
                Ok(existing) if validate_evidence_cache(&existing, &params).is_ok() => {
                    migrate_evidence_cache_layout(path, &existing)?;
                    compatible_existing = true;
                }
                Ok(_) => {
                    eprintln!(
                        "evidence cache: params changed; replacing incompatible cache at {}",
                        path.display()
                    );
                    clear_evidence_artifacts(path)?;
                }
                Err(error) => {
                    eprintln!(
                        "evidence cache: unreadable/incompatible cache; replacing {}: {error}",
                        path.display()
                    );
                    clear_evidence_artifacts(path)?;
                }
            }
        }
        let entries_dir = companion_entries_dir(path);
        fs::create_dir_all(&entries_dir)?;
        // Keep the prior meta parameters until a selective refresh finishes.
        // If the process is interrupted, the next run will safely retry any
        // day-sensitive refresh instead of treating a mixed cache as current.
        if !compatible_existing {
            write_evidence_meta(path, &params)?;
        }
        let known_paths = collect_evidence_shards(&entries_dir)?.into_iter().collect();
        Ok(Self {
            path: path.to_path_buf(),
            params,
            entries_dir,
            batch_size: batch_size.max(1),
            pending: Vec::new(),
            known_paths,
        })
    }

    pub fn cached_count(&self) -> usize {
        self.known_paths.len()
    }

    /// Record the stable path for a bundle already known on disk.
    pub fn note_cached(&mut self, bundle: &EvidenceBundle) {
        let portable = portable_bundle(bundle);
        self.known_paths
            .insert(evidence_shard_path(&self.entries_dir, &portable));
    }

    /// Buffer one newly finished candidate; flush when the batch is full.
    pub fn push(&mut self, bundle: &EvidenceBundle) -> Result<(), AnalysisError> {
        let portable = portable_bundle(bundle);
        let key = bundle_key(&portable);
        if let Some(pending) = self.pending.iter_mut().find(|b| bundle_key(b) == key) {
            *pending = portable;
        } else {
            self.pending.push(portable);
        }
        if self.pending.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    /// Atomically replace pending candidate shards.
    pub fn flush(&mut self) -> Result<(), AnalysisError> {
        for bundle in std::mem::take(&mut self.pending) {
            write_evidence_shard(&self.entries_dir, &bundle)?;
            self.known_paths
                .insert(evidence_shard_path(&self.entries_dir, &bundle));
        }
        Ok(())
    }

    /// Flush remaining shards and return the number of durable candidate keys.
    pub fn finish(mut self) -> Result<usize, AnalysisError> {
        self.flush()?;
        write_evidence_meta(&self.path, &self.params)?;
        Ok(self.known_paths.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::{IdentityRow, SourceOrder};
    use crate::reporting::json::SeedRecord;
    use ahash::AHashSet;

    fn prepared() -> ResidentStore {
        let evm = ["ethereum"]
            .into_iter()
            .map(str::to_owned)
            .collect::<AHashSet<_>>();
        let mut store = ResidentStore::with_options(Some(8), &evm);
        store
            .ingest_identity_row(IdentityRow {
                chain: "ethereum".into(),
                contract_address: "0xabc".into(),
                token_id: "1".into(),
                name_norm: "n".into(),
                token_uri_norm: String::new(),
                image_uri_norm: String::new(),
                source_order: SourceOrder {
                    file_ordinal: 0,
                    file_row_number: 0,
                },
            })
            .unwrap();
        store
            .ingest_identity_row(IdentityRow {
                chain: "ethereum".into(),
                contract_address: "0xdef".into(),
                token_id: "1".into(),
                name_norm: "n".into(),
                token_uri_norm: String::new(),
                image_uri_norm: String::new(),
                source_order: SourceOrder {
                    file_ordinal: 0,
                    file_row_number: 1,
                },
            })
            .unwrap();
        store
    }

    fn params() -> EvidenceCacheParams {
        evidence_cache_params(
            &[SeedRecord {
                chain: "ethereum".into(),
                address: "0xseed".into(),
                rank: Some(1),
            }],
            "seeds.json",
            &ApiKeys::default(),
            &HttpLimits::default(),
        )
    }

    #[test]
    fn round_trip_remaps_contract_id() {
        let store = prepared();
        let cid = store.contract_id("ethereum", "0xabc").unwrap();
        let mut bundle = EvidenceBundle::empty(cid, "ethereum", "0xabc");
        bundle.controllers.push("0xop".into());
        let mut map = AHashMap::new();
        map.insert(cid, bundle);

        let p = params();
        let cache = build_evidence_cache(p.clone(), &map);
        assert_eq!(cache.bundles[0].contract_id, 0);

        let dir =
            std::env::temp_dir().join(format!("analysis_evidence_cache_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("evidence_cache.json");
        write_evidence_cache(&path, &cache).unwrap();
        let loaded = load_evidence_cache(&path).unwrap();
        validate_evidence_cache(&loaded, &p).unwrap();
        let remapped = rematerialize_evidence(&store, &loaded).unwrap();
        assert_eq!(remapped.len(), 1);
        let got = remapped.get(&cid).unwrap();
        assert_eq!(got.contract_id, cid);
        assert_eq!(got.controllers, vec!["0xop".to_owned()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn incompatible_historical_cache_is_rejected() {
        let mut cache = build_evidence_cache(params(), &AHashMap::new());
        cache.version = 1;
        let dir = std::env::temp_dir().join(format!(
            "analysis_evidence_cache_old_provider_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("evidence_cache.json");
        write_evidence_cache(&path, &cache).unwrap();

        let error = load_evidence_cache(&path).unwrap_err().to_string();
        assert!(error.contains("incompatible"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn future_cache_is_rejected_to_prevent_downgrade_misread() {
        let mut cache = build_evidence_cache(params(), &AHashMap::new());
        cache.version = EVIDENCE_CACHE_VERSION + 1;
        let dir = std::env::temp_dir().join(format!(
            "analysis_evidence_cache_previous_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("evidence_cache.json");
        write_evidence_cache(&path, &cache).unwrap();

        let error = load_evidence_cache(&path).unwrap_err().to_string();
        assert!(error.contains("incompatible"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn incremental_sink_survives_without_finish() {
        let store = prepared();
        let a = store.contract_id("ethereum", "0xabc").unwrap();
        let d = store.contract_id("ethereum", "0xdef").unwrap();
        let dir =
            std::env::temp_dir().join(format!("analysis_evidence_sink_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("evidence_cache.json");
        let p = params();

        {
            let mut sink = EvidenceCacheSink::create(&path, p.clone(), 2).unwrap();
            sink.push(&EvidenceBundle::empty(a, "ethereum", "0xabc"))
                .unwrap();
            // batch not full — force flush as if batch completed mid-run
            sink.push(&EvidenceBundle::empty(d, "ethereum", "0xdef"))
                .unwrap();
            // drop without finish — flush already ran at batch_size=2
        }

        // Mid-run durability comes from independently atomic compressed shards.
        assert!(!path.is_file());
        assert!(!companion_jsonl(&path).is_file());
        assert!(companion_entries_dir(&path).is_dir());
        let loaded = load_evidence_cache_resumable(&path).unwrap();
        validate_evidence_cache(&loaded, &p).unwrap();
        assert_eq!(loaded.bundles.len(), 2);
        let map = rematerialize_evidence(&store, &loaded).unwrap();
        assert!(map.contains_key(&a));
        assert!(map.contains_key(&d));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_snapshot_is_reused_then_replaced_by_compressed_shards() {
        let store = prepared();
        let a = store.contract_id("ethereum", "0xabc").unwrap();
        let dir = std::env::temp_dir().join(format!(
            "analysis_evidence_legacy_migration_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("evidence_cache.json");
        let mut evidence = AHashMap::new();
        evidence.insert(a, EvidenceBundle::empty(a, "ethereum", "0xabc"));
        let legacy = build_evidence_cache(params(), &evidence);
        write_evidence_cache(&path, &legacy).unwrap();

        let loaded = load_evidence_cache_resumable(&path).unwrap();
        let stats = migrate_evidence_cache_layout(&path, &loaded).unwrap();
        assert_eq!(stats.bundles, 1);
        assert_eq!(stats.legacy_files_removed, 1);
        assert!(!path.exists());
        assert!(companion_entries_dir(&path).is_dir());

        let migrated = load_evidence_cache_resumable(&path).unwrap();
        assert_eq!(migrated.bundles.len(), 1);
        assert_eq!(migrated.bundles[0].address, "0xabc");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_jsonl_prefers_latest_row_and_migrates_without_snapshot() {
        let store = prepared();
        let a = store.contract_id("ethereum", "0xabc").unwrap();
        let dir = std::env::temp_dir().join(format!(
            "analysis_evidence_legacy_jsonl_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("evidence_cache.json");
        let mut first = EvidenceBundle::empty(a, "ethereum", "0xabc");
        first.controllers.push("0xfirst".into());
        let mut latest = first.clone();
        latest.controllers = vec!["0xlatest".into()];
        let meta = EvidenceCacheMeta {
            version: EVIDENCE_CACHE_VERSION,
            params: params(),
            storage: None,
        };
        std::fs::write(companion_meta(&path), serde_json::to_vec(&meta).unwrap()).unwrap();
        std::fs::write(
            companion_jsonl(&path),
            format!(
                "{}\n{}\n",
                serde_json::to_string(&first).unwrap(),
                serde_json::to_string(&latest).unwrap()
            ),
        )
        .unwrap();

        let loaded = load_evidence_cache_resumable(&path).unwrap();
        assert_eq!(loaded.bundles.len(), 1);
        assert_eq!(loaded.bundles[0].controllers, vec!["0xlatest"]);
        let stats = migrate_evidence_cache_layout(&path, &loaded).unwrap();
        assert_eq!(stats.legacy_files_removed, 1);
        assert!(!companion_jsonl(&path).exists());
        assert_eq!(
            load_evidence_cache_resumable(&path).unwrap().bundles[0].controllers,
            vec!["0xlatest"]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn complete_sharded_write_replaces_obsolete_entries() {
        let store = prepared();
        let a = store.contract_id("ethereum", "0xabc").unwrap();
        let d = store.contract_id("ethereum", "0xdef").unwrap();
        let dir = std::env::temp_dir().join(format!(
            "analysis_evidence_complete_sharded_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("evidence_cache.json");
        let first = EvidenceCacheFile {
            version: EVIDENCE_CACHE_VERSION,
            params: params(),
            bundles: vec![
                EvidenceBundle::empty(a, "ethereum", "0xabc"),
                EvidenceBundle::empty(d, "ethereum", "0xdef"),
            ],
        };
        write_evidence_cache_sharded(&path, &first).unwrap();
        assert_eq!(
            load_evidence_cache_resumable(&path).unwrap().bundles.len(),
            2
        );

        let replacement = EvidenceCacheFile {
            version: EVIDENCE_CACHE_VERSION,
            params: params(),
            bundles: vec![EvidenceBundle::empty(a, "ethereum", "0xabc")],
        };
        write_evidence_cache_sharded(&path, &replacement).unwrap();
        let loaded = load_evidence_cache_resumable(&path).unwrap();
        assert_eq!(loaded.bundles.len(), 1);
        assert_eq!(loaded.bundles[0].address, "0xabc");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn interrupted_selective_refresh_keeps_previous_meta_day() {
        let store = prepared();
        let a = store.contract_id("ethereum", "0xabc").unwrap();
        let dir = std::env::temp_dir().join(format!(
            "analysis_evidence_refresh_meta_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("evidence_cache.json");
        let old_params = params();
        let mut initial = EvidenceCacheSink::create(&path, old_params.clone(), 1).unwrap();
        initial
            .push(&EvidenceBundle::empty(a, "ethereum", "0xabc"))
            .unwrap();
        initial.finish().unwrap();

        let mut current_params = old_params.clone();
        current_params.pricing_day_utc += 86_400;
        let mut interrupted = EvidenceCacheSink::create(&path, current_params.clone(), 1).unwrap();
        interrupted
            .push(&EvidenceBundle::empty(a, "ethereum", "0xabc"))
            .unwrap();
        drop(interrupted);
        assert_eq!(
            load_evidence_cache_resumable(&path)
                .unwrap()
                .params
                .pricing_day_utc,
            old_params.pricing_day_utc
        );

        EvidenceCacheSink::create(&path, current_params.clone(), 1)
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(
            load_evidence_cache_resumable(&path)
                .unwrap()
                .params
                .pricing_day_utc,
            current_params.pricing_day_utc
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn incremental_sink_keeps_bundles_when_seed_changes() {
        let store = prepared();
        let a = store.contract_id("ethereum", "0xabc").unwrap();
        let dir = std::env::temp_dir().join(format!(
            "analysis_evidence_sink_compat_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("evidence_cache.json");
        let cached_params = params();

        {
            let mut sink = EvidenceCacheSink::create(&path, cached_params.clone(), 1).unwrap();
            sink.push(&EvidenceBundle::empty(a, "ethereum", "0xabc"))
                .unwrap();
            sink.finish().unwrap();
        }

        let mut current_params = cached_params;
        current_params.seeds = vec![SeedRecord {
            chain: "base".into(),
            address: "0xnew-seed".into(),
            rank: None,
        }];
        let sink = EvidenceCacheSink::create(&path, current_params, 1).unwrap();
        assert_eq!(sink.cached_count(), 1);
        sink.finish().unwrap();

        let loaded = load_evidence_cache_resumable(&path).unwrap();
        assert_eq!(loaded.bundles.len(), 1);
        assert_eq!(loaded.bundles[0].address, "0xabc");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validate_accepts_seed_and_provider_key_changes() {
        let cached = evidence_cache_params(
            &[],
            "seeds.json",
            &ApiKeys::default(),
            &HttpLimits::default(),
        );
        let mut current = cached.clone();
        current.seeds = vec![SeedRecord {
            chain: "polygon".into(),
            address: "0xother-seed".into(),
            rank: Some(99),
        }];
        current.seeds_path = "different-seeds.json".into();
        current.had_alchemy = true;
        current.had_etherscan = true;
        current.had_helius = true;
        current.had_opensea = true;
        let cache = EvidenceCacheFile {
            version: EVIDENCE_CACHE_VERSION,
            params: cached,
            bundles: Vec::new(),
        };

        validate_evidence_cache(&cache, &current)
            .expect("provider-key presence must not discard successful cached evidence");

        let mut seed_only = cache.params.clone();
        seed_only.seeds = current.seeds;
        seed_only.seeds_path = current.seeds_path;
        validate_evidence_cache(&cache, &seed_only)
            .expect("candidate evidence must survive seed changes");
    }

    #[test]
    fn validate_still_rejects_pagination_changes() {
        let cached = evidence_cache_params(
            &[],
            "seeds.json",
            &ApiKeys::default(),
            &HttpLimits::default(),
        );
        let mut current = cached.clone();
        current.max_transfer_pages += 1;
        let cache = EvidenceCacheFile {
            version: EVIDENCE_CACHE_VERSION,
            params: cached,
            bundles: Vec::new(),
        };

        assert!(validate_evidence_cache(&cache, &current).is_err());
    }

    #[test]
    fn validate_accepts_stale_pricing_day_for_selective_refresh() {
        let cached = evidence_cache_params(
            &[],
            "seeds.json",
            &ApiKeys::default(),
            &HttpLimits::default(),
        );
        let mut current = cached.clone();
        current.pricing_day_utc += 86_400;
        let cache = EvidenceCacheFile {
            version: EVIDENCE_CACHE_VERSION,
            params: cached,
            bundles: Vec::new(),
        };
        validate_evidence_cache(&cache, &current)
            .expect("pricing age must not invalidate unrelated cached evidence");
    }
}
