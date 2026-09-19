//! Complete seed-contract NFT snapshots, resident for the hot path and persisted
//! to reusable Zstandard caches.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use ahash::{AHashMap, AHashSet};
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::AnalysisError;
use crate::enrich::http::{HttpClient, is_http_status};
use crate::enrich::types::{ApiKeys, ProviderEndpoints};
use crate::normalize::{normalize_name, normalize_url};
use crate::parquet::validated_metadata;
use crate::reporting::SeedRecord;

pub const MAX_SEED_NFTS_PER_CONTRACT: usize = 50_000;
// v2 invalidates snapshots that may have been published after a failed
// metadata-batch fallback and therefore contained identities only.
const CACHE_VERSION: u32 = 2;
const ALCHEMY_PAGE_SIZE: usize = 100;
const HELIUS_PAGE_SIZE: usize = 1_000;
const ZSTD_LEVEL: i32 = 3;

#[derive(Clone, Debug)]
pub struct SeedNftDownloadOptions {
    pub cache_dir: PathBuf,
    pub api_keys: ApiKeys,
    pub endpoints: ProviderEndpoints,
    pub concurrency: usize,
    pub retries: usize,
    pub refresh: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SeedNftRecord {
    pub token_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name_norm: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub token_uri_norm: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub image_uri_norm: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub metadata_json: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct CacheHeader {
    kind: String,
    version: u32,
    chain: String,
    address: String,
    max_nfts: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct CacheFooter {
    kind: String,
    item_count: usize,
    provider_total: Option<usize>,
    truncated: bool,
}

#[derive(Clone, Debug)]
pub struct SeedNftCacheRef {
    pub seed: SeedRecord,
    pub path: PathBuf,
    pub item_count: usize,
    pub provider_total: Option<usize>,
    pub truncated: bool,
    pub reused: bool,
    resident_records: Option<Vec<SeedNftRecord>>,
}

impl SeedNftCacheRef {
    /// Visit decoded rows without cloning when the cache is resident. Disk-only
    /// references fall back to one streaming decode.
    pub fn for_each_nft(
        &self,
        mut callback: impl FnMut(&SeedNftRecord) -> Result<(), AnalysisError>,
    ) -> Result<(), AnalysisError> {
        if let Some(records) = &self.resident_records {
            self.verify_count(records.len())?;
            for record in records {
                callback(record)?;
            }
            return Ok(());
        }
        let mut count = 0usize;
        read_cache(&self.path, |line_no, value| {
            if line_no == 0 || value.get("kind").and_then(Value::as_str) == Some("complete") {
                return Ok(());
            }
            let record = serde_json::from_value(value)
                .map_err(|e| AnalysisError::invalid(format!("parse seed NFT cache row: {e}")))?;
            callback(&record)?;
            count += 1;
            Ok(())
        })?;
        self.verify_count(count)
    }

    /// Consume decoded rows for their final pipeline use. Resident memory is
    /// detached before callbacks start, so it is freed promptly even on error.
    pub fn consume_nfts(
        &mut self,
        mut callback: impl FnMut(SeedNftRecord) -> Result<(), AnalysisError>,
    ) -> Result<(), AnalysisError> {
        if let Some(records) = self.resident_records.take() {
            let count = records.len();
            for record in records {
                callback(record)?;
            }
            return self.verify_count(count);
        }
        let mut count = 0usize;
        read_cache(&self.path, |line_no, value| {
            if line_no == 0 || value.get("kind").and_then(Value::as_str) == Some("complete") {
                return Ok(());
            }
            let record = serde_json::from_value(value)
                .map_err(|e| AnalysisError::invalid(format!("parse seed NFT cache row: {e}")))?;
            callback(record)?;
            count += 1;
            Ok(())
        })?;
        self.verify_count(count)
    }

    fn verify_count(&self, count: usize) -> Result<(), AnalysisError> {
        if count != self.item_count {
            return Err(AnalysisError::invalid(format!(
                "seed NFT cache count changed for {} / {}",
                self.seed.chain, self.seed.address
            )));
        }
        Ok(())
    }
}

struct CacheStreamWriter {
    encoder: zstd::stream::write::Encoder<'static, BufWriter<File>>,
    tmp: PathBuf,
    final_path: PathBuf,
    count: usize,
}

impl CacheStreamWriter {
    fn create(path: &Path, seed: &SeedRecord) -> Result<Self, AnalysisError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("jsonl.zst.tmp");
        let file = File::create(&tmp)?;
        let mut encoder = zstd::stream::write::Encoder::new(BufWriter::new(file), ZSTD_LEVEL)
            .map_err(|e| AnalysisError::invalid(format!("create seed NFT zstd cache: {e}")))?;
        write_json_line(
            &mut encoder,
            &CacheHeader {
                kind: "header".into(),
                version: CACHE_VERSION,
                chain: seed.chain.clone(),
                address: seed.address.clone(),
                max_nfts: MAX_SEED_NFTS_PER_CONTRACT,
            },
        )?;
        Ok(Self {
            encoder,
            tmp,
            final_path: path.to_owned(),
            count: 0,
        })
    }

    fn push(&mut self, record: &SeedNftRecord) -> Result<(), AnalysisError> {
        write_json_line(&mut self.encoder, record)?;
        self.count += 1;
        Ok(())
    }

    fn finish(
        mut self,
        provider_total: Option<usize>,
        truncated: bool,
    ) -> Result<CacheFooter, AnalysisError> {
        let footer = CacheFooter {
            kind: "complete".into(),
            item_count: self.count,
            provider_total,
            truncated,
        };
        write_json_line(&mut self.encoder, &footer)?;
        let mut output = self
            .encoder
            .finish()
            .map_err(|e| AnalysisError::invalid(format!("finish seed NFT zstd cache: {e}")))?;
        output.flush()?;
        drop(output);
        if self.final_path.is_file() {
            // Windows cannot atomically rename over an existing file. Preserve
            // the last valid cache until the new stream is in place.
            let backup = self.final_path.with_extension("jsonl.zst.bak");
            if backup.is_file() {
                fs::remove_file(&backup)?;
            }
            fs::rename(&self.final_path, &backup)?;
            if let Err(error) = fs::rename(&self.tmp, &self.final_path) {
                let _ = fs::rename(&backup, &self.final_path);
                return Err(AnalysisError::invalid(format!(
                    "publish seed NFT cache {}: {error}",
                    self.final_path.display()
                )));
            }
            fs::remove_file(backup)?;
        } else {
            fs::rename(&self.tmp, &self.final_path)?;
        }
        Ok(footer)
    }
}

fn write_json_line<T: Serialize>(writer: &mut impl Write, value: &T) -> Result<(), AnalysisError> {
    serde_json::to_writer(&mut *writer, value)
        .map_err(|e| AnalysisError::invalid(format!("serialize seed NFT cache row: {e}")))?;
    writer.write_all(b"\n")?;
    Ok(())
}

/// Download missing seed snapshots concurrently. Each contract is persisted to
/// its own compressed file and retained in memory for the immediate pipeline
/// stages, avoiding a second decompression/JSON parse on the hot path.
pub async fn prepare_seed_nft_caches(
    seeds: &[SeedRecord],
    options: &SeedNftDownloadOptions,
) -> Result<Vec<SeedNftCacheRef>, AnalysisError> {
    fs::create_dir_all(&options.cache_dir)?;
    let client = HttpClient::with_retries(options.concurrency.max(1), options.retries)?;
    let width = options.concurrency.max(1);
    let (solana, evm): (Vec<_>, Vec<_>) = seeds
        .iter()
        .cloned()
        .enumerate()
        .partition(|(_, seed)| seed.chain == "solana");
    let solana_downloads = prepare_provider_seed_caches(solana, &client, options, width);
    let evm_downloads = prepare_provider_seed_caches(evm, &client, options, width);
    let (solana_results, evm_results) = tokio::join!(solana_downloads, evm_downloads);
    let mut results = solana_results;
    results.extend(evm_results);
    let mut ordered = Vec::with_capacity(results.len());
    for result in results {
        ordered.push(result?);
    }
    ordered.sort_by_key(|(index, _)| *index);
    Ok(ordered.into_iter().map(|(_, cache)| cache).collect())
}

async fn prepare_provider_seed_caches(
    seeds: Vec<(usize, SeedRecord)>,
    client: &HttpClient,
    options: &SeedNftDownloadOptions,
    width: usize,
) -> Vec<Result<(usize, SeedNftCacheRef), AnalysisError>> {
    stream::iter(seeds)
        .map(|(index, seed)| {
            let client = client.clone();
            async move {
                let path = cache_path(&options.cache_dir, &seed);
                if !options.refresh
                    && let Ok(cached) = validate_cache(&path, &seed)
                {
                    return Ok((index, cached));
                }
                let cached = match seed.chain.as_str() {
                    "solana" => download_helius(&client, options, &seed, &path).await,
                    _ => download_alchemy(&client, options, &seed, &path).await,
                }?;
                Ok::<_, AnalysisError>((index, cached))
            }
        })
        .buffer_unordered(width)
        .collect::<Vec<_>>()
        .await
}

async fn download_alchemy(
    client: &HttpClient,
    options: &SeedNftDownloadOptions,
    seed: &SeedRecord,
    path: &Path,
) -> Result<SeedNftCacheRef, AnalysisError> {
    let key = options.api_keys.alchemy().ok_or_else(|| {
        AnalysisError::invalid(format!(
            "missing Alchemy API key and reusable seed NFT cache for {} / {}",
            seed.chain, seed.address
        ))
    })?;
    let base = options
        .endpoints
        .alchemy_nft(&seed.chain, key, "getNFTsForContract")
        .ok_or_else(|| {
            AnalysisError::invalid(format!("unsupported Alchemy chain {}", seed.chain))
        })?;
    let metadata_batch_url = options
        .endpoints
        .alchemy_nft(&seed.chain, key, "getNFTMetadataBatch")
        .ok_or_else(|| {
            AnalysisError::invalid(format!("unsupported Alchemy chain {}", seed.chain))
        })?;
    let mut writer = CacheStreamWriter::create(path, seed)?;
    let mut page_key: Option<String> = None;
    let mut seen_keys = AHashSet::new();
    let mut seen_tokens = AHashSet::new();
    let mut provider_total = None;
    let mut resident_records = Vec::new();
    let mut metadata_fallback = false;
    loop {
        let remaining = MAX_SEED_NFTS_PER_CONTRACT.saturating_sub(writer.count);
        if remaining == 0 {
            break;
        }
        let limit = ALCHEMY_PAGE_SIZE.min(remaining);
        let mut url = format!(
            "{base}?contractAddress={}&withMetadata={}&limit={limit}",
            encode_query(&seed.address),
            !metadata_fallback
        );
        if let Some(key) = &page_key {
            url.push_str("&pageKey=");
            url.push_str(&encode_query(key));
        }
        let payload = match client.get_json_alchemy(&url, &[]).await {
            Ok(payload) => payload,
            Err(error) if !metadata_fallback && is_alchemy_collection_metadata_error(&error) => {
                metadata_fallback = true;
                eprintln!(
                    "[seed_nfts/warn] Alchemy collection metadata failed for {} / {}; falling back to token listing + getNFTMetadataBatch",
                    seed.chain, seed.address
                );
                let fallback_url = url.replace("withMetadata=true", "withMetadata=false");
                client.get_json_alchemy(&fallback_url, &[]).await?
            }
            Err(error) => return Err(error),
        };
        if let Some(total) = json_usize(payload.get("totalCount").or_else(|| payload.get("total")))
        {
            provider_total = Some(total);
        }
        let rows = payload
            .get("nfts")
            .and_then(Value::as_array)
            .ok_or_else(|| AnalysisError::http("Alchemy getNFTsForContract omitted nfts"))?;
        let records = if metadata_fallback {
            hydrate_alchemy_page(client, &metadata_batch_url, seed, rows).await?
        } else {
            rows.iter()
                .map(|row| {
                    parse_alchemy_nft(row).ok_or_else(|| {
                        AnalysisError::http("Alchemy getNFTsForContract NFT omitted tokenId")
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        for record in records {
            if seen_tokens.insert(record.token_id.clone()) {
                writer.push(&record)?;
                resident_records.push(record);
                if writer.count >= MAX_SEED_NFTS_PER_CONTRACT {
                    break;
                }
            }
        }
        let next = payload
            .get("pageKey")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        if rows.is_empty() || next.is_none() {
            page_key = None;
            break;
        }
        let next = next.expect("checked above");
        if !seen_keys.insert(next.clone()) {
            return Err(AnalysisError::http(
                "Alchemy repeated getNFTsForContract pageKey",
            ));
        }
        page_key = Some(next);
    }
    let capped = writer.count >= MAX_SEED_NFTS_PER_CONTRACT;
    if let Some(total) = provider_total
        && (total < writer.count || (total > writer.count && !capped))
    {
        return Err(AnalysisError::http(format!(
            "Alchemy getNFTsForContract totalCount={total} disagrees with {} downloaded NFTs",
            writer.count
        )));
    }
    let truncated =
        capped && (page_key.is_some() || provider_total.is_some_and(|total| total > writer.count));
    let footer = writer.finish(provider_total, truncated)?;
    Ok(cache_ref(seed, path, footer, false, Some(resident_records)))
}

fn is_alchemy_collection_metadata_error(error: &AnalysisError) -> bool {
    let lower = error.to_string().to_ascii_lowercase();
    lower.contains("error fetching metadata for that collection")
        || (is_http_status(error, 500) && lower.contains("metadata"))
}

async fn hydrate_alchemy_page(
    client: &HttpClient,
    metadata_batch_url: &str,
    seed: &SeedRecord,
    rows: &[Value],
) -> Result<Vec<SeedNftRecord>, AnalysisError> {
    let base_records = rows
        .iter()
        .map(|row| {
            parse_alchemy_nft(row).ok_or_else(|| {
                AnalysisError::http("Alchemy getNFTsForContract NFT omitted tokenId")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if base_records.is_empty() {
        return Ok(base_records);
    }
    let tokens = base_records
        .iter()
        .map(|record| {
            json!({
                "contractAddress": seed.address,
                "tokenId": record.token_id,
            })
        })
        .collect::<Vec<_>>();
    // The dedicated v3 endpoint documents an object with `tokens`; an older
    // Alchemy batch guide documents the legacy bare array. Prefer the current
    // contract and fall back once for accounts still serving the legacy shape.
    let request = json!({"tokens": tokens, "refreshCache": false});
    let payload = match client
        .post_json_alchemy(metadata_batch_url, &[], &request)
        .await
    {
        Ok(payload) => payload,
        Err(primary) => client
            .post_json_alchemy(metadata_batch_url, &[], request.get("tokens").expect("present"))
            .await
            .map_err(|legacy| {
                AnalysisError::http(format!(
                    "Alchemy getNFTMetadataBatch failed with current and legacy request bodies: current={primary}; legacy={legacy}"
                ))
            })?,
    };
    let items = payload
        .as_array()
        .or_else(|| payload.get("nfts").and_then(Value::as_array));
    let items = items.ok_or_else(|| {
        AnalysisError::http(format!(
            "Alchemy metadata batch returned no NFT array for {} / {}",
            seed.chain, seed.address
        ))
    })?;
    let mut hydrated = AHashMap::with_capacity(items.len());
    for item in items {
        if let Some(record) = parse_alchemy_nft(item) {
            hydrated.insert(record.token_id.clone(), record);
        }
    }
    if base_records
        .iter()
        .any(|record| !hydrated.contains_key(&record.token_id))
    {
        return Err(AnalysisError::http(format!(
            "Alchemy metadata batch omitted one or more NFTs for {} / {}",
            seed.chain, seed.address
        )));
    }
    Ok(base_records
        .into_iter()
        .map(|base| hydrated.remove(&base.token_id).unwrap_or(base))
        .collect())
}

async fn download_helius(
    client: &HttpClient,
    options: &SeedNftDownloadOptions,
    seed: &SeedRecord,
    path: &Path,
) -> Result<SeedNftCacheRef, AnalysisError> {
    let key = options.api_keys.helius().ok_or_else(|| {
        AnalysisError::invalid(format!(
            "missing Helius API key and reusable seed NFT cache for {} / {}",
            seed.chain, seed.address
        ))
    })?;
    let url = with_api_key(&options.endpoints.helius, key);
    let mut writer = CacheStreamWriter::create(path, seed)?;
    let mut page = 1usize;
    let mut has_more;
    let mut seen_tokens = AHashSet::new();
    let mut seen_pages = AHashSet::new();
    let mut resident_records = Vec::new();
    loop {
        let remaining = MAX_SEED_NFTS_PER_CONTRACT.saturating_sub(writer.count);
        if remaining == 0 {
            has_more = true;
            break;
        }
        let limit = HELIUS_PAGE_SIZE;
        let body = json!({
            "jsonrpc": "2.0",
            "id": format!("seed-nfts-{page}"),
            "method": "getAssetsByGroup",
            "params": {
                "groupKey": "collection",
                "groupValue": seed.address,
                "page": page,
                "limit": limit,
                "options": {
                    "showUnverifiedCollections": false,
                    "showCollectionMetadata": false,
                    "showGrandTotal": false
                }
            }
        });
        let payload = client.post_json_helius(&url, &[], &body).await?;
        if let Some(error) = payload.get("error") {
            return Err(AnalysisError::http(format!(
                "Helius getAssetsByGroup: {error}"
            )));
        }
        let result = payload
            .get("result")
            .ok_or_else(|| AnalysisError::http("Helius getAssetsByGroup omitted result"))?;
        let rows = result
            .get("items")
            .and_then(Value::as_array)
            .ok_or_else(|| AnalysisError::http("Helius getAssetsByGroup omitted items"))?;
        let page_identity = rows
            .iter()
            .filter_map(|row| row.get("id").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\0");
        if !page_identity.is_empty() && !seen_pages.insert(page_identity) {
            return Err(AnalysisError::http(
                "Helius getAssetsByGroup repeated an asset page",
            ));
        }
        for row in rows {
            if helius_row_is_explicitly_non_nft(row) {
                continue;
            }
            let record = parse_helius_nft(row)
                .ok_or_else(|| AnalysisError::http("Helius getAssetsByGroup item omitted id"))?;
            if seen_tokens.insert(record.token_id.clone()) {
                writer.push(&record)?;
                resident_records.push(record);
                if writer.count >= MAX_SEED_NFTS_PER_CONTRACT {
                    break;
                }
            }
        }
        has_more = rows.len() >= limit;
        if rows.is_empty() || rows.len() < limit {
            break;
        }
        page += 1;
    }
    let capped = writer.count >= MAX_SEED_NFTS_PER_CONTRACT;
    let truncated = capped && has_more;
    let footer = writer.finish(None, truncated)?;
    Ok(cache_ref(seed, path, footer, false, Some(resident_records)))
}

fn helius_row_is_explicitly_non_nft(row: &Value) -> bool {
    row.get("interface")
        .and_then(Value::as_str)
        .is_some_and(|interface| {
            matches!(
                interface.to_ascii_lowercase().as_str(),
                "fungibleasset"
                    | "fungibletoken"
                    | "identity"
                    | "executable"
                    | "mplcorecollection"
                    | "mplcoregroup"
            )
        })
}

fn parse_alchemy_nft(row: &Value) -> Option<SeedNftRecord> {
    let token_id = text(row.get("tokenId"))?;
    let metadata = row.get("raw").and_then(|raw| raw.get("metadata"));
    Some(SeedNftRecord {
        token_id,
        name_norm: text(row.get("name"))
            .or_else(|| metadata.and_then(|value| text(value.get("name"))))
            .map(|value| normalize_name(&value))
            .unwrap_or_default(),
        token_uri_norm: text(row.get("raw").and_then(|raw| raw.get("tokenUri")))
            .or_else(|| text(row.get("tokenUri")))
            .and_then(|value| normalize_url(&value))
            .unwrap_or_default(),
        image_uri_norm: metadata
            .and_then(|value| text(value.get("image")))
            .or_else(|| text(row.get("image").and_then(|image| image.get("originalUrl"))))
            .or_else(|| text(row.get("image").and_then(|image| image.get("cachedUrl"))))
            .and_then(|value| normalize_url(&value))
            .unwrap_or_default(),
        metadata_json: canonical_metadata(metadata),
    })
}

fn parse_helius_nft(row: &Value) -> Option<SeedNftRecord> {
    let token_id = text(row.get("id"))?;
    let content = row.get("content");
    let metadata = content.and_then(|value| value.get("metadata"));
    let image = content
        .and_then(|value| value.get("links"))
        .and_then(|value| text(value.get("image")))
        .or_else(|| metadata.and_then(|value| text(value.get("image"))))
        .or_else(|| {
            content
                .and_then(|value| value.get("files"))
                .and_then(Value::as_array)
                .and_then(|files| files.first())
                .and_then(|file| text(file.get("uri")))
        })
        .unwrap_or_default();
    Some(SeedNftRecord {
        token_id,
        name_norm: metadata
            .and_then(|value| text(value.get("name")))
            .map(|value| normalize_name(&value))
            .unwrap_or_default(),
        token_uri_norm: content
            .and_then(|value| text(value.get("json_uri")))
            .and_then(|value| normalize_url(&value))
            .unwrap_or_default(),
        image_uri_norm: normalize_url(&image).unwrap_or_default(),
        metadata_json: canonical_metadata(metadata),
    })
}

fn canonical_metadata(value: Option<&Value>) -> String {
    let Some(value) = value else {
        return String::new();
    };
    if let Some(raw) = value.as_str() {
        return validated_metadata(raw).unwrap_or_default();
    }
    let Ok(raw) = serde_json::to_string(value) else {
        return String::new();
    };
    validated_metadata(&raw).unwrap_or_default()
}

fn text(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn json_usize(value: Option<&Value>) -> Option<usize> {
    value.and_then(|value| {
        value
            .as_u64()
            .and_then(|number| usize::try_from(number).ok())
            .or_else(|| value.as_str()?.parse().ok())
    })
}

fn encode_query(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

fn with_api_key(base: &str, key: &str) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    if base.contains("api-key=") {
        base.to_owned()
    } else {
        format!(
            "{}{separator}api-key={}",
            base.trim_end_matches('/'),
            encode_query(key)
        )
    }
}

fn cache_path(dir: &Path, seed: &SeedRecord) -> PathBuf {
    let mut hasher = Sha256::new();
    hasher.update(seed.chain.as_bytes());
    hasher.update([0]);
    hasher.update(seed.address.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    dir.join(format!("{}__{}.jsonl.zst", seed.chain, &digest[..20]))
}

fn cache_ref(
    seed: &SeedRecord,
    path: &Path,
    footer: CacheFooter,
    reused: bool,
    resident_records: Option<Vec<SeedNftRecord>>,
) -> SeedNftCacheRef {
    SeedNftCacheRef {
        seed: seed.clone(),
        path: path.to_owned(),
        item_count: footer.item_count,
        provider_total: footer.provider_total,
        truncated: footer.truncated,
        reused,
        resident_records,
    }
}

fn validate_cache(path: &Path, seed: &SeedRecord) -> Result<SeedNftCacheRef, AnalysisError> {
    let mut header = None;
    let mut footer = None;
    let mut resident_records = Vec::new();
    read_cache(path, |line_no, value| {
        if line_no == 0 {
            header = Some(serde_json::from_value::<CacheHeader>(value).map_err(|e| {
                AnalysisError::invalid(format!("parse seed NFT cache header: {e}"))
            })?);
        } else if value.get("kind").and_then(Value::as_str) == Some("complete") {
            footer = Some(serde_json::from_value::<CacheFooter>(value).map_err(|e| {
                AnalysisError::invalid(format!("parse seed NFT cache footer: {e}"))
            })?);
        } else {
            resident_records.push(
                serde_json::from_value::<SeedNftRecord>(value).map_err(|e| {
                    AnalysisError::invalid(format!("parse seed NFT cache row: {e}"))
                })?,
            );
        }
        Ok(())
    })?;
    let header = header.ok_or_else(|| AnalysisError::invalid("seed NFT cache missing header"))?;
    let footer = footer.ok_or_else(|| AnalysisError::invalid("seed NFT cache incomplete"))?;
    let count = resident_records.len();
    let invalid_completion = if footer.truncated {
        count != MAX_SEED_NFTS_PER_CONTRACT
            || footer.provider_total.is_some_and(|total| total <= count)
    } else {
        footer.provider_total.is_some_and(|total| total != count)
    };
    if header.kind != "header"
        || footer.kind != "complete"
        || header.version != CACHE_VERSION
        || header.chain != seed.chain
        || header.address != seed.address
        || header.max_nfts != MAX_SEED_NFTS_PER_CONTRACT
        || footer.item_count != count
        || count > MAX_SEED_NFTS_PER_CONTRACT
        || invalid_completion
    {
        return Err(AnalysisError::invalid(
            "seed NFT cache identity/count mismatch",
        ));
    }
    Ok(cache_ref(seed, path, footer, true, Some(resident_records)))
}

pub fn for_each_cached_nft(
    cache: &SeedNftCacheRef,
    mut callback: impl FnMut(SeedNftRecord) -> Result<(), AnalysisError>,
) -> Result<(), AnalysisError> {
    cache.for_each_nft(|record| callback(record.clone()))
}

/// Release decoded seed rows after their final consumer. The durable cache
/// reference and fingerprint remain usable.
pub fn release_resident_seed_nfts(caches: &mut [SeedNftCacheRef]) {
    for cache in caches {
        cache.resident_records = None;
    }
}

fn read_cache(
    path: &Path,
    mut callback: impl FnMut(usize, Value) -> Result<(), AnalysisError>,
) -> Result<(), AnalysisError> {
    let file = File::open(path)?;
    let decoder = zstd::stream::read::Decoder::new(BufReader::new(file))
        .map_err(|e| AnalysisError::invalid(format!("open seed NFT zstd cache: {e}")))?;
    let reader = BufReader::new(decoder);
    for (line_no, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value = serde_json::from_str(&line).map_err(|e| {
            AnalysisError::invalid(format!(
                "parse seed NFT cache {} line {}: {e}",
                path.display(),
                line_no + 1
            ))
        })?;
        callback(line_no, value)?;
    }
    Ok(())
}

pub fn cache_fingerprint(caches: &[SeedNftCacheRef]) -> Result<String, AnalysisError> {
    let mut hasher = Sha256::new();
    for cache in caches {
        hasher.update(cache.seed.chain.as_bytes());
        hasher.update([0]);
        hasher.update(cache.seed.address.as_bytes());
        hasher.update([0]);
        let mut file = File::open(&cache.path)?;
        let mut buffer = [0_u8; 128 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
    }
    Ok(format!("{:x}", hasher.finalize()))
}
