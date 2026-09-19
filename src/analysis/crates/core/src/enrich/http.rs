//! Shared HTTP client for seed selection and enrichment.

use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use crate::error::AnalysisError;

/// Total request timeout (connect + headers + body). Large Alchemy NFT pages
/// (e.g. `getOwnersForContract?withTokenBalances=true`) often exceed 30s.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(90);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_RETRIES: usize = 3;
const MAX_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;

pub const OPENSEA_RATE_LIMIT_BURST: usize = 4;
pub const OPENSEA_RATE_LIMIT_REFILL_MS: u64 = 300;
/// Shared cool-down applied to a provider bucket after HTTP 429 / rate-limit.
pub const RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(1);
const MAX_RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(30);
static CACHE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const SUCCESS_CACHE_FORMAT_VERSION: u32 = 3;
const MIN_SUCCESS_CACHE_FORMAT_VERSION: u32 = 2;
const SUCCESS_CACHE_ZSTD_LEVEL: i32 = 3;
const SUCCESS_CACHE_V2_DIR: &str = "v2";

#[derive(Clone, Debug)]
struct SuccessResponseCache {
    root: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct SuccessCacheEntry {
    request_identity: String,
    response: Value,
}

#[derive(Serialize, Deserialize)]
struct SuccessCacheEntryV2 {
    version: u32,
    #[serde(default)]
    cached_at: Option<i64>,
    response: Value,
}

#[derive(Serialize)]
struct SuccessCacheEntryV2Ref<'a> {
    version: u32,
    cached_at: i64,
    response: &'a Value,
}

/// Best-effort statistics for the one-time loose-JSON to sharded-zstd migration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SuccessCacheMigrationStats {
    pub scanned: u64,
    pub migrated: u64,
    pub already_present: u64,
    pub failed: u64,
    pub legacy_bytes_removed: u64,
    pub compressed_bytes: u64,
}

impl SuccessResponseCache {
    fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn legacy_path(&self, provider: &str, identity: &str) -> PathBuf {
        self.root
            .join(provider)
            .join(format!("{:016x}.json", stable_fnv1a64(identity.as_bytes())))
    }

    fn path(&self, provider: &str, identity: &str) -> PathBuf {
        let digest = sha256_hex(identity.as_bytes());
        self.root
            .join(SUCCESS_CACHE_V2_DIR)
            .join(provider)
            .join(&digest[..2])
            .join(format!("{digest}.json.zst"))
    }

    fn load(&self, provider: &str, identity: &str, min_cached_at: Option<i64>) -> Option<Value> {
        if let Some(response) = self.load_v2(provider, identity, min_cached_at) {
            return Some(response);
        }

        let legacy_path = self.legacy_path(provider, identity);
        let body = fs::read(&legacy_path).ok()?;
        let entry: SuccessCacheEntry = serde_json::from_slice(&body).ok()?;
        if entry.request_identity != identity {
            return None;
        }
        let cached_at = file_modified_unix(&legacy_path).unwrap_or(0);
        if min_cached_at.is_some_and(|minimum| cached_at < minimum) {
            return None;
        }
        let response = entry.response;
        if self
            .store_v2(provider, identity, &response, cached_at)
            .is_ok()
        {
            let _ = fs::remove_file(legacy_path);
        }
        Some(response)
    }

    fn store(&self, provider: &str, identity: &str, response: Value) {
        let _ = self.store_v2(provider, identity, &response, unix_now());
    }

    fn load_v2(&self, provider: &str, identity: &str, min_cached_at: Option<i64>) -> Option<Value> {
        let path = self.path(provider, identity);
        let body = fs::read(path).ok()?;
        let decoded = zstd::stream::decode_all(Cursor::new(body)).ok()?;
        let entry: SuccessCacheEntryV2 = serde_json::from_slice(&decoded).ok()?;
        if !(MIN_SUCCESS_CACHE_FORMAT_VERSION..=SUCCESS_CACHE_FORMAT_VERSION)
            .contains(&entry.version)
        {
            return None;
        }
        let cached_at = entry
            .cached_at
            .unwrap_or_else(|| file_modified_unix(&self.path(provider, identity)).unwrap_or(0));
        if min_cached_at.is_some_and(|minimum| cached_at < minimum) {
            return None;
        }
        Some(entry.response)
    }

    fn store_v2(
        &self,
        provider: &str,
        identity: &str,
        response: &Value,
        cached_at: i64,
    ) -> Result<u64, std::io::Error> {
        let path = self.path(provider, identity);
        let Some(parent) = path.parent() else {
            return Ok(0);
        };
        fs::create_dir_all(parent)?;
        let entry = SuccessCacheEntryV2Ref {
            version: SUCCESS_CACHE_FORMAT_VERSION,
            cached_at,
            response,
        };
        let body = serde_json::to_vec(&entry).map_err(std::io::Error::other)?;
        let compressed = zstd::stream::encode_all(Cursor::new(body), SUCCESS_CACHE_ZSTD_LEVEL)?;
        let len = compressed.len() as u64;
        atomic_replace(&path, &compressed)?;
        Ok(len)
    }
}

/// Convert every legacy provider-level `*.json` response under `root` into the
/// compressed sharded layout. Each legacy file is removed only after the new
/// entry is durably written and can be decoded, so interrupted migrations are
/// safely resumable and untouched entries remain readable by [`HttpClient`].
pub fn migrate_legacy_success_response_cache(root: &std::path::Path) -> SuccessCacheMigrationStats {
    migrate_legacy_success_response_cache_with_progress(root, || {})
}

pub fn migrate_legacy_success_response_cache_with_progress(
    root: &std::path::Path,
    mut on_scanned: impl FnMut(),
) -> SuccessCacheMigrationStats {
    let mut stats = SuccessCacheMigrationStats::default();
    let Ok(providers) = fs::read_dir(root) else {
        return stats;
    };
    let cache = SuccessResponseCache::new(root.to_path_buf());
    for provider_dir in providers.flatten() {
        let path = provider_dir.path();
        if !path.is_dir() || provider_dir.file_name() == SUCCESS_CACHE_V2_DIR {
            continue;
        }
        let Some(provider) = provider_dir.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Ok(files) = fs::read_dir(&path) else {
            stats.failed += 1;
            continue;
        };
        for file in files.flatten() {
            let legacy_path = file.path();
            if !legacy_path.is_file()
                || legacy_path.extension().and_then(|ext| ext.to_str()) != Some("json")
            {
                continue;
            }
            stats.scanned += 1;
            on_scanned();
            let legacy_len = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
            let result = fs::read(&legacy_path)
                .ok()
                .and_then(|body| serde_json::from_slice::<SuccessCacheEntry>(&body).ok())
                .and_then(|entry| {
                    let already_present = cache
                        .load_v2(&provider, &entry.request_identity, None)
                        .is_some();
                    let compressed_len = if already_present {
                        fs::metadata(cache.path(&provider, &entry.request_identity))
                            .ok()?
                            .len()
                    } else {
                        cache
                            .store_v2(
                                &provider,
                                &entry.request_identity,
                                &entry.response,
                                file_modified_unix(&legacy_path).unwrap_or(0),
                            )
                            .ok()?
                    };
                    cache
                        .load_v2(&provider, &entry.request_identity, None)
                        .map(|_| (already_present, compressed_len))
                });
            let Some((already_present, compressed_len)) = result else {
                stats.failed += 1;
                continue;
            };
            if fs::remove_file(&legacy_path).is_err() {
                stats.failed += 1;
                continue;
            }
            stats.migrated += 1;
            stats.already_present += u64::from(already_present);
            stats.legacy_bytes_removed += legacy_len;
            stats.compressed_bytes += compressed_len;
        }
    }
    stats
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

fn atomic_replace(path: &std::path::Path, body: &[u8]) -> Result<(), std::io::Error> {
    let sequence = CACHE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let tmp = path.with_extension(format!("{}.{}.tmp", std::process::id(), sequence));
    fs::write(&tmp, body)?;
    if !path.exists() {
        return fs::rename(tmp, path);
    }

    let backup = path.with_extension(format!("{}.{}.bak", std::process::id(), sequence));
    fs::rename(path, &backup)?;
    if let Err(error) = fs::rename(&tmp, path) {
        let _ = fs::rename(&backup, path);
        let _ = fs::remove_file(&tmp);
        return Err(error);
    }
    let _ = fs::remove_file(backup);
    Ok(())
}

fn stable_fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[derive(Debug)]
struct TokenBucketState {
    tokens: f64,
    last_refill: Instant,
    /// When set, every `acquire` waits until this instant (global 429 pause).
    cool_down_until: Option<Instant>,
}

/// Token-bucket rate gate without a background task (safe to construct outside
/// a Tokio runtime; waits only on `acquire`).
#[derive(Clone, Debug)]
pub struct TokenBucketRateLimiter {
    max_burst: f64,
    refill_interval: Duration,
    state: Arc<Mutex<TokenBucketState>>,
}

impl TokenBucketRateLimiter {
    pub fn new(max_burst: usize, refill_interval: Duration) -> Self {
        Self::with_initial_tokens(max_burst, refill_interval, 1.0)
    }

    /// Start with a full bucket (used when concurrency is the primary throttle).
    pub fn new_full(max_burst: usize, refill_interval: Duration) -> Self {
        let max_burst_f = max_burst.max(1) as f64;
        Self::with_initial_tokens(max_burst, refill_interval, max_burst_f)
    }

    fn with_initial_tokens(max_burst: usize, refill_interval: Duration, initial: f64) -> Self {
        let max_burst = max_burst.max(1) as f64;
        Self {
            max_burst,
            refill_interval: refill_interval.max(Duration::from_millis(1)),
            state: Arc::new(Mutex::new(TokenBucketState {
                tokens: initial.clamp(0.0, max_burst),
                last_refill: Instant::now(),
                cool_down_until: None,
            })),
        }
    }

    /// OpenSea default: 4 burst / 300 ms refill.
    pub fn opensea_default() -> Self {
        Self::new(
            OPENSEA_RATE_LIMIT_BURST,
            Duration::from_millis(OPENSEA_RATE_LIMIT_REFILL_MS),
        )
    }

    /// No RPS cap: concurrency semaphore is the throttle; still supports 429 cool-down.
    pub fn concurrency_only() -> Self {
        // Large burst + 1 ms refill ≈ unlimited steady-state RPS.
        Self::new_full(10_000, Duration::from_millis(1))
    }

    /// Block *all* subsequent acquires for at least `duration` (429 cool-down).
    ///
    /// Extends an existing cool-down if one is already active; drains tokens so
    /// traffic cannot immediately resume at full burst after the pause.
    pub fn note_rate_limited(&self, duration: Duration) {
        if let Ok(mut state) = self.state.lock() {
            let until = Instant::now() + duration;
            state.cool_down_until = Some(match state.cool_down_until {
                Some(prev) if prev > until => prev,
                _ => until,
            });
            state.tokens = 0.0;
            state.last_refill = Instant::now();
        }
    }

    /// Wait until cool-down (if any) ends and a rate token is available, then consume one.
    pub async fn acquire(&self) -> Result<(), AnalysisError> {
        loop {
            let wait = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| AnalysisError::http("rate limiter poisoned"))?;
                let now = Instant::now();

                // Global 429 cool-down: every concurrent caller parks until it ends.
                if let Some(until) = state.cool_down_until {
                    if now < until {
                        Some(until.saturating_duration_since(now))
                    } else {
                        // Cool-down finished: resume with a single token (not a full burst).
                        state.cool_down_until = None;
                        state.tokens = 1.0;
                        state.last_refill = now;
                        state.tokens -= 1.0;
                        None
                    }
                } else {
                    let elapsed = state.last_refill.elapsed();
                    if !elapsed.is_zero() {
                        let add = elapsed.as_secs_f64() / self.refill_interval.as_secs_f64();
                        state.tokens = (state.tokens + add).min(self.max_burst);
                        state.last_refill = Instant::now();
                    }
                    if state.tokens >= 1.0 {
                        state.tokens -= 1.0;
                        None
                    } else {
                        let need = 1.0 - state.tokens;
                        let wait_secs = need * self.refill_interval.as_secs_f64();
                        Some(Duration::from_secs_f64(wait_secs.max(0.001)))
                    }
                }
            };
            match wait {
                None => return Ok(()),
                Some(delay) => tokio::time::sleep(delay).await,
            }
        }
    }
}

/// One API provider's independent concurrency pool + rate / 429 cool-down gate.
///
/// Lanes never share semaphores or cool-downs: saturating Alchemy does not block
/// OpenSea / Helius / Etherscan, and a Helius 429 pause does not slow Alchemy.
#[derive(Clone, Debug)]
struct ProviderLane {
    name: &'static str,
    in_flight: Arc<Semaphore>,
    limiter: TokenBucketRateLimiter,
}

impl ProviderLane {
    fn new(name: &'static str, concurrency: usize, limiter: TokenBucketRateLimiter) -> Self {
        Self {
            name,
            in_flight: Arc::new(Semaphore::new(concurrency.max(1))),
            limiter,
        }
    }
}

/// Concurrent HTTP helper with **per-provider** concurrency and rate control.
#[derive(Clone)]
pub struct HttpClient {
    http: reqwest::Client,
    retries: usize,
    alchemy: ProviderLane,
    opensea: ProviderLane,
    helius: ProviderLane,
    etherscan: ProviderLane,
    /// Magic Eden / other non-primary providers.
    other: ProviderLane,
    success_cache: Option<SuccessResponseCache>,
    success_cache_min_unix: Option<i64>,
}

impl HttpClient {
    pub fn new(concurrency: usize) -> Result<Self, AnalysisError> {
        Self::with_retries(concurrency, DEFAULT_RETRIES)
    }

    pub fn with_retries(concurrency: usize, retries: usize) -> Result<Self, AnalysisError> {
        Self::with_retries_and_cache(concurrency, retries, None)
    }

    pub fn with_retries_and_cache(
        concurrency: usize,
        retries: usize,
        cache_dir: Option<PathBuf>,
    ) -> Result<Self, AnalysisError> {
        Self::with_retries_and_cache_since(concurrency, retries, cache_dir, None)
    }

    pub fn with_retries_and_cache_since(
        concurrency: usize,
        retries: usize,
        cache_dir: Option<PathBuf>,
        success_cache_min_unix: Option<i64>,
    ) -> Result<Self, AnalysisError> {
        let n = concurrency.max(1);
        // Each provider gets its own pool of size `n` (Alchemy uses the CLI
        // --http-concurrency value; others are independent and not shared).
        let opensea_n = n.max(OPENSEA_RATE_LIMIT_BURST);
        let helius_n = n;
        let etherscan_n = n;
        let other_n = n;
        let pool_idle = n
            .saturating_add(opensea_n)
            .saturating_add(helius_n)
            .saturating_add(etherscan_n)
            .saturating_add(other_n)
            .max(1);

        let http = reqwest::Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(pool_idle)
            .tcp_keepalive(Duration::from_secs(60))
            .tcp_nodelay(true)
            .build()
            .map_err(|e| AnalysisError::http(e.to_string()))?;
        Ok(Self {
            http,
            retries,
            alchemy: ProviderLane::new("alchemy", n, TokenBucketRateLimiter::concurrency_only()),
            opensea: ProviderLane::new(
                "opensea",
                opensea_n,
                TokenBucketRateLimiter::opensea_default(),
            ),
            helius: ProviderLane::new(
                "helius",
                helius_n,
                TokenBucketRateLimiter::concurrency_only(),
            ),
            etherscan: ProviderLane::new(
                "etherscan",
                etherscan_n,
                TokenBucketRateLimiter::concurrency_only(),
            ),
            other: ProviderLane::new("other", other_n, TokenBucketRateLimiter::concurrency_only()),
            success_cache: cache_dir.map(SuccessResponseCache::new),
            success_cache_min_unix,
        })
    }

    /// Generic GET on the independent "other" lane (NFTScan, misc).
    pub async fn get_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
    ) -> Result<Value, AnalysisError> {
        self.request_on_lane(reqwest::Method::GET, url, headers, None, &self.other, true)
            .await
    }

    pub async fn get_json_alchemy(
        &self,
        url: &str,
        headers: &[(&str, &str)],
    ) -> Result<Value, AnalysisError> {
        self.request_on_lane(
            reqwest::Method::GET,
            url,
            headers,
            None,
            &self.alchemy,
            true,
        )
        .await
    }

    /// Alchemy GET whose response must stay fresh across runs (spot prices).
    pub(crate) async fn get_json_alchemy_uncached(
        &self,
        url: &str,
        headers: &[(&str, &str)],
    ) -> Result<Value, AnalysisError> {
        self.request_on_lane(
            reqwest::Method::GET,
            url,
            headers,
            None,
            &self.alchemy,
            false,
        )
        .await
    }

    pub async fn post_json_alchemy(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<Value, AnalysisError> {
        self.request_on_lane(
            reqwest::Method::POST,
            url,
            headers,
            Some(body),
            &self.alchemy,
            true,
        )
        .await
    }

    /// Alchemy POST whose response must stay fresh across runs (spot prices).
    pub(crate) async fn post_json_alchemy_uncached(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<Value, AnalysisError> {
        self.request_on_lane(
            reqwest::Method::POST,
            url,
            headers,
            Some(body),
            &self.alchemy,
            false,
        )
        .await
    }

    /// GET on the OpenSea lane (≤ ~4 req/s + provider-local 429 cool-down).
    pub async fn get_json_opensea(
        &self,
        url: &str,
        headers: &[(&str, &str)],
    ) -> Result<Value, AnalysisError> {
        self.request_on_lane(
            reqwest::Method::GET,
            url,
            headers,
            None,
            &self.opensea,
            true,
        )
        .await
    }

    /// Generic POST on the "other" lane.
    pub async fn post_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<Value, AnalysisError> {
        self.request_on_lane(
            reqwest::Method::POST,
            url,
            headers,
            Some(body),
            &self.other,
            true,
        )
        .await
    }

    /// POST on the Helius lane (`--http-concurrency` + provider-local 429 cool-down).
    pub async fn post_json_helius(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<Value, AnalysisError> {
        self.request_on_lane(
            reqwest::Method::POST,
            url,
            headers,
            Some(body),
            &self.helius,
            true,
        )
        .await
    }

    pub async fn get_json_etherscan(
        &self,
        url: &str,
        headers: &[(&str, &str)],
    ) -> Result<Value, AnalysisError> {
        self.request_on_lane(
            reqwest::Method::GET,
            url,
            headers,
            None,
            &self.etherscan,
            true,
        )
        .await
    }

    /// Shared retry loop for one provider lane: rate token → concurrency permit → HTTP.
    async fn request_on_lane(
        &self,
        method: reqwest::Method,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&Value>,
        lane: &ProviderLane,
        use_success_cache: bool,
    ) -> Result<Value, AnalysisError> {
        let header_map = build_headers(headers)?;
        let endpoint = redact_endpoint(url);
        let cache_identity = success_cache_identity(&method, &endpoint, body);
        if use_success_cache && let Some(cache) = self.success_cache.clone() {
            let provider = lane.name;
            let identity = cache_identity.clone();
            let min_cached_at = self.success_cache_min_unix;
            if let Ok(Some(value)) =
                tokio::task::spawn_blocking(move || cache.load(provider, &identity, min_cached_at))
                    .await
            {
                return Ok(value);
            }
        }
        let mut last_error = None;
        for attempt in 0..=self.retries {
            // Rate / cool-down is provider-local.
            lane.limiter.acquire().await?;
            let _permit = lane.in_flight.acquire().await.map_err(|_| {
                AnalysisError::http(format!(
                    "HTTP concurrency pool closed provider={}",
                    lane.name
                ))
            })?;
            let mut builder = self
                .http
                .request(method.clone(), url)
                .headers(header_map.clone());
            if let Some(body) = body {
                builder = builder.json(body);
            }
            let result = match builder.send().await {
                Ok(response) => read_json_response(response, &endpoint).await,
                Err(error) => Err(AnalysisError::http(format_transport_error(
                    &method, &endpoint, &error,
                ))),
            };
            drop(_permit);
            match result {
                Ok(value) => {
                    // Several providers report throttling/transient failures in an
                    // HTTP 200 JSON envelope. Route those through the same finite
                    // retry/cool-down policy as transport-level failures.
                    if let Some(error) = application_retryable_error(&value, &endpoint) {
                        if !self
                            .handle_request_error(
                                &method,
                                &endpoint,
                                attempt,
                                error,
                                lane,
                                &mut last_error,
                            )
                            .await?
                        {
                            break;
                        }
                        continue;
                    }
                    if use_success_cache
                        && response_is_fully_successful(&value)
                        && let Some(cache) = self.success_cache.clone()
                    {
                        let provider = lane.name;
                        let identity = cache_identity.clone();
                        let cached_value = value.clone();
                        let _ = tokio::task::spawn_blocking(move || {
                            cache.store(provider, &identity, cached_value);
                        })
                        .await;
                    }
                    return Ok(value);
                }
                Err(error) => {
                    if !self
                        .handle_request_error(
                            &method,
                            &endpoint,
                            attempt,
                            error,
                            lane,
                            &mut last_error,
                        )
                        .await?
                    {
                        break;
                    }
                }
            }
        }
        let final_error = last_error.unwrap_or_else(|| AnalysisError::http("HTTP request failed"));
        // 404 is an expected miss for unknown collections — quiet one-liner, not full body dump.
        if is_http_not_found(&final_error) {
            eprintln!(
                "[api/miss] endpoint={endpoint} method={method} provider={} action=not_found",
                lane.name
            );
        } else if should_print_request_error(&final_error) {
            eprintln!(
                "[api/error] endpoint={endpoint} method={method} provider={} action=give_up error={}",
                lane.name,
                one_line_error(&final_error.to_string(), ERROR_LOG_CHARS)
            );
        }
        Err(final_error)
    }

    /// Log, cool-down, and sleep for one failed attempt.
    /// Returns `Ok(true)` when the caller should retry, `Ok(false)` to stop.
    async fn handle_request_error(
        &self,
        method: &reqwest::Method,
        endpoint: &str,
        attempt: usize,
        error: AnalysisError,
        lane: &ProviderLane,
        last_error: &mut Option<AnalysisError>,
    ) -> Result<bool, AnalysisError> {
        let rate_limited = is_rate_limited(&error);
        let not_found = is_http_not_found(&error);
        let retryable = !not_found && (rate_limited || is_retryable(&error));
        let will_retry = attempt < self.retries && retryable;
        // 429: cool down only this provider's limiter (not other providers).
        // 5xx: start from 500ms (503 connection resets need more space than 100ms).
        let backoff = if will_retry {
            if rate_limited {
                let delay = rate_limit_backoff(&error, attempt);
                lane.limiter.note_rate_limited(delay);
                Some(delay)
            } else if is_http_status(&error, 503)
                || is_http_status(&error, 502)
                || is_http_status(&error, 504)
            {
                Some(Duration::from_millis(
                    500u64.saturating_mul(1u64 << attempt.min(4)),
                ))
            } else {
                Some(Duration::from_millis(
                    100u64.saturating_mul(1u64 << attempt.min(8)),
                ))
            }
        } else {
            None
        };
        // Permanent 404 is logged once as api/miss. Rate limits are routine
        // flow-control signals and remain silent at every logging layer.
        if !not_found && should_print_request_error(&error) {
            print_request_error(
                method,
                endpoint,
                attempt + 1,
                self.retries + 1,
                backoff.map(|d| d.as_millis() as u64),
                &error,
            );
        }
        *last_error = Some(error);
        if !will_retry {
            return Ok(false);
        }
        if let Some(delay) = backoff {
            tokio::time::sleep(delay).await;
        }
        Ok(true)
    }
}

/// Max characters kept from error/response bodies in logs and error strings.
const ERROR_BODY_CHARS: usize = 800;
const ERROR_LOG_CHARS: usize = 1_200;

fn build_headers(headers: &[(&str, &str)]) -> Result<HeaderMap, AnalysisError> {
    let mut map = HeaderMap::new();
    map.insert(
        reqwest::header::ACCEPT,
        HeaderValue::from_static("application/json"),
    );
    map.insert(
        reqwest::header::USER_AGENT,
        HeaderValue::from_static("analysis-select-seeds/0.1"),
    );
    for (name, value) in headers {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| AnalysisError::http(format!("invalid header name {name}: {e}")))?;
        let header_value = HeaderValue::from_str(value)
            .map_err(|e| AnalysisError::http(format!("invalid header value: {e}")))?;
        map.insert(header_name, header_value);
    }
    Ok(map)
}

async fn read_json_response(
    response: reqwest::Response,
    endpoint: &str,
) -> Result<Value, AnalysisError> {
    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(parse_retry_after_header);
    let bytes = response.bytes().await.map_err(|e| {
        AnalysisError::http(format!(
            "read body failed endpoint={endpoint} status={status}: {e}"
        ))
    })?;
    if bytes.len() as u64 > MAX_RESPONSE_BYTES {
        return Err(AnalysisError::http(format!(
            "response exceeds {MAX_RESPONSE_BYTES} bytes endpoint={endpoint} status={status} \
             content_type={content_type} body_len={}",
            bytes.len()
        )));
    }
    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes);
        let snippet = one_line_error(&body, ERROR_BODY_CHARS);
        let retry_hint = retry_after
            .map(|delay| format!(" retry_after_ms={}", delay.as_millis()))
            .unwrap_or_default();
        return Err(AnalysisError::http(format!(
            "HTTP {status} endpoint={endpoint} content_type={content_type}{retry_hint} body={snippet}"
        )));
    }
    serde_json::from_slice(&bytes).map_err(|e| {
        let preview = String::from_utf8_lossy(&bytes);
        let snippet = one_line_error(&preview, ERROR_BODY_CHARS);
        AnalysisError::http(format!(
            "invalid JSON endpoint={endpoint} status={status} content_type={content_type} \
             parse_error={e} body={snippet}"
        ))
    })
}

fn parse_retry_after_header(value: &HeaderValue) -> Option<Duration> {
    let text = value.to_str().ok()?.trim();
    if let Ok(seconds) = text.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let at = httpdate::parse_http_date(text).ok()?;
    Some(
        at.duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO),
    )
}

fn retry_after_from_error(error: &AnalysisError) -> Option<Duration> {
    let AnalysisError::Http(message) = error else {
        return None;
    };
    let marker = "retry_after_ms=";
    let start = message.find(marker)? + marker.len();
    let digits: String = message[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    let milliseconds = digits.parse::<u64>().ok()?;
    Some(Duration::from_millis(milliseconds))
}

fn rate_limit_backoff(error: &AnalysisError, attempt: usize) -> Duration {
    let multiplier = 1u32 << attempt.min(5);
    let adaptive = RATE_LIMIT_COOLDOWN
        .saturating_mul(multiplier)
        .min(MAX_RATE_LIMIT_COOLDOWN);
    retry_after_from_error(error)
        .unwrap_or(Duration::ZERO)
        .max(adaptive)
}

fn format_transport_error(
    method: &reqwest::Method,
    endpoint: &str,
    error: &reqwest::Error,
) -> String {
    let mut parts = vec![
        "transport error".to_owned(),
        format!("method={method}"),
        format!("endpoint={endpoint}"),
    ];
    if error.is_timeout() {
        parts.push("kind=timeout".into());
    } else if error.is_connect() {
        parts.push("kind=connect".into());
    } else if error.is_request() {
        parts.push("kind=request".into());
    } else if error.is_body() {
        parts.push("kind=body".into());
    } else if error.is_decode() {
        parts.push("kind=decode".into());
    }
    if let Some(status) = error.status() {
        parts.push(format!("status={status}"));
    }
    // Keep the library message but strip raw secrets if any leaked in.
    // Prefer `to_string()` over `without_url()` so we can borrow `&Error`.
    let detail = one_line_error(&redact_sensitive_text(&error.to_string()), ERROR_BODY_CHARS);
    parts.push(format!("detail={detail}"));
    parts.join(" ")
}

fn is_retryable(error: &AnalysisError) -> bool {
    if is_rate_limited(error) {
        return true;
    }
    match error {
        AnalysisError::Http(message) => {
            let lower = message.to_ascii_lowercase();
            lower.contains("timeout")
                || lower.contains("timed out")
                || lower.contains("kind=timeout")
                || lower.contains("kind=connect")
                || lower.contains("kind=request")
                || lower.contains("kind=body")
                || lower.contains("kind=decode")
                || lower.contains("connection")
                || lower.contains("read body failed")
                || lower.contains("error decoding response body")
                || lower.contains("invalid json")
                || lower.contains("error sending request")
                || lower.contains("http 500")
                || lower.contains("http 502")
                || lower.contains("http 503")
                || lower.contains("http 504")
        }
        _ => false,
    }
}

/// True when the HTTP error message reports the given status (e.g. 429).
pub fn is_http_status(error: &AnalysisError, status: u16) -> bool {
    match error {
        AnalysisError::Http(message) => {
            let lower = message.to_ascii_lowercase();
            let code = status.to_string();
            // "HTTP 429 …", "HTTP 429 Too Many Requests …", "status=429 …"
            lower.contains(&format!("http {code}")) || lower.contains(&format!("status={code}"))
        }
        _ => false,
    }
}

/// Permanent client miss (no point retrying).
pub fn is_http_not_found(error: &AnalysisError) -> bool {
    is_http_status(error, 404)
}

/// Provider rate-limit signal: HTTP 429, transport status=429, or rate-limit text.
pub(crate) fn is_rate_limited(error: &AnalysisError) -> bool {
    match error {
        AnalysisError::Http(message) => is_rate_limit_message(message),
        _ => false,
    }
}

fn is_rate_limit_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("http 429")
        || lower.contains("status=429")
        || lower.contains("too many requests")
        || lower.contains("rate limit")
        || lower.contains("rate-limit")
        || lower.contains("ratelimited")
        || lower.contains("\"code\":-32429")
        || lower.contains("\"code\": -32429")
        || lower.contains("\"code\": 429")
        || lower.contains("\"code\":429")
        || lower.contains("\"code\":-32005")
        || lower.contains("\"code\": -32005")
}

fn should_print_request_error(error: &AnalysisError) -> bool {
    !is_rate_limited(error)
}

/// If a successful JSON-RPC payload is a rate-limit error, return a short body snippet.
fn jsonrpc_rate_limit_error(value: &Value) -> Option<String> {
    if let Some(rows) = value.as_array() {
        return rows.iter().find_map(jsonrpc_rate_limit_error);
    }
    let err = value.get("error")?;
    let code = err.get("code").and_then(|c| c.as_i64());
    let message = err
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let rate_limited = matches!(code, Some(429) | Some(-32429) | Some(-32005))
        || message.contains("rate limit")
        || message.contains("too many requests")
        || message.contains("rate limited");
    if rate_limited {
        Some(one_line_error(&err.to_string(), ERROR_BODY_CHARS))
    } else {
        None
    }
}

fn application_retryable_error(value: &Value, endpoint: &str) -> Option<AnalysisError> {
    if let Some(err) = jsonrpc_rate_limit_error(value) {
        return Some(AnalysisError::http(format!(
            "HTTP 429 endpoint={endpoint} content_type=application/json body={err}"
        )));
    }
    if let Some(rows) = value.as_array() {
        return rows
            .iter()
            .find_map(|row| application_retryable_error(row, endpoint));
    }

    let status_code = value.get("status").and_then(|status| {
        status
            .as_i64()
            .or_else(|| status.as_str().and_then(|text| text.parse().ok()))
    });
    let provider_code = value
        .get("code")
        .or_else(|| value.pointer("/error/code"))
        .and_then(|code| {
            code.as_i64()
                .or_else(|| code.as_str().and_then(|text| text.parse().ok()))
        });
    let message = ["message", "result", "msg", "error"]
        .into_iter()
        .filter_map(|key| value.get(key))
        .map(|part| {
            part.as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| part.to_string())
        })
        .collect::<Vec<_>>()
        .join(" ");
    let lower = message.to_ascii_lowercase();
    let rate_limited = lower.contains("rate limit")
        || lower.contains("rate-limit")
        || lower.contains("too many requests")
        || matches!(provider_code, Some(429));
    if rate_limited {
        return Some(AnalysisError::http(format!(
            "HTTP 429 endpoint={endpoint} content_type=application/json body={}",
            one_line_error(&message, ERROR_BODY_CHARS)
        )));
    }
    let transient = lower.contains("server busy")
        || lower.contains("internal error")
        || lower.contains("temporarily unavailable")
        || lower.contains("timeout occurred")
        || lower.contains("query timeout")
        || matches!(provider_code, Some(500..=599) | Some(-32603));
    let envelope_is_error =
        status_code == Some(0) || provider_code.is_some_and(|code| !(200..300).contains(&code));
    (transient && envelope_is_error).then(|| {
        AnalysisError::http(format!(
            "HTTP 503 endpoint={endpoint} content_type=application/json body={}",
            one_line_error(&message, ERROR_BODY_CHARS)
        ))
    })
}

fn print_request_error(
    method: &reqwest::Method,
    endpoint: &str,
    attempt: usize,
    max_attempts: usize,
    backoff_ms: Option<u64>,
    error: &AnalysisError,
) {
    // Error string already carries endpoint/status/body; still prefix for grepping.
    let message = one_line_error(&error.to_string(), ERROR_LOG_CHARS);
    match backoff_ms {
        Some(delay) => eprintln!(
            "[api/error] endpoint={endpoint} method={method} attempt={attempt}/{max_attempts} \
             action=retry backoff_ms={delay} error={message}"
        ),
        None => eprintln!(
            "[api/error] endpoint={endpoint} method={method} attempt={attempt}/{max_attempts} \
             action=continue error={message}"
        ),
    }
}

/// Query keys whose values are secrets (not bare substrings like `token` in
/// `withTokenBalances`).
fn is_secret_query_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "api-key"
            | "api_key"
            | "apikey"
            | "x-api-key"
            | "key"
            | "access_token"
            | "access-token"
            | "token"
            | "secret"
            | "password"
            | "authorization"
            | "auth"
    )
}

fn success_cache_identity(
    method: &reqwest::Method,
    redacted_endpoint: &str,
    body: Option<&Value>,
) -> String {
    let body = body
        .and_then(|value| serde_json::to_string(value).ok())
        .unwrap_or_default();
    format!("{method}\n{redacted_endpoint}\n{body}")
}

/// Accept only complete provider successes. HTTP failures never reach this
/// function; provider and JSON-RPC error envelopes remain retryable.
fn response_is_fully_successful(value: &Value) -> bool {
    if let Some(rows) = value.as_array() {
        return rows.iter().all(response_is_fully_successful);
    }
    let Some(object) = value.as_object() else {
        return true;
    };
    if object.contains_key("error") || object.contains_key("errors") {
        return false;
    }
    if object.contains_key("jsonrpc") {
        return object.contains_key("result");
    }
    true
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn file_modified_unix(path: &std::path::Path) -> Option<i64> {
    fs::metadata(path)
        .ok()?
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs() as i64)
}

/// Host + path + redacted query for logs (never includes API keys).
fn redact_endpoint(url: &str) -> String {
    // reqwest error strings wrap URLs in parentheses; peel them first.
    let trimmed = url
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim();
    let Ok(parsed) = reqwest::Url::parse(trimmed) else {
        return redact_path_secrets(trimmed);
    };
    let host = match (parsed.host_str(), parsed.port()) {
        (Some(host), Some(port)) => format!("{host}:{port}"),
        (Some(host), None) => host.to_owned(),
        _ => "unknown-host".to_owned(),
    };
    let path = parsed.path();
    let mut out = format!("{host}{path}");
    if let Some(query) = parsed.query() {
        let redacted = query
            .split('&')
            .map(|pair| {
                let mut parts = pair.splitn(2, '=');
                let key = parts.next().unwrap_or("");
                if is_secret_query_key(key) {
                    format!("{key}=***")
                } else {
                    pair.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("&");
        if !redacted.is_empty() {
            out.push('?');
            out.push_str(&redacted);
        }
    }
    // Alchemy / similar paths embed the key as a path segment: /v2/<key>
    redact_path_secrets(&out)
}

fn strip_wrapping_punct(s: &str) -> &str {
    s.trim_matches(|c: char| {
        matches!(
            c,
            '(' | ')' | '[' | ']' | '{' | '}' | '"' | '\'' | ',' | ';' | '.'
        )
    })
}

fn looks_like_api_key_segment(segment: &str) -> bool {
    let cleaned = strip_wrapping_punct(segment);
    cleaned.len() >= 12
        && cleaned
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn redact_path_secrets(endpoint: &str) -> String {
    // Replace long secret-looking path segments after API version prefixes:
    // Alchemy NFT `/v2/<key>`, prices `/prices/v1/<key>/tokens/...`.
    let mut parts: Vec<String> = endpoint.split('/').map(str::to_owned).collect();
    for i in 0..parts.len() {
        let head = strip_wrapping_punct(&parts[i]);
        let redact_next = matches!(head, "v1" | "v2" | "v3")
            || (head.eq_ignore_ascii_case("prices")
                && parts
                    .get(i + 1)
                    .map(|p| strip_wrapping_punct(p) == "v1")
                    .unwrap_or(false));
        if !redact_next {
            continue;
        }
        // For "prices"/"v1"/KEY skip one extra segment when head is prices.
        let key_idx = if head.eq_ignore_ascii_case("prices") {
            i + 2
        } else {
            i + 1
        };
        if let Some(next) = parts.get_mut(key_idx)
            && looks_like_api_key_segment(next)
        {
            let trailing: String = next
                .chars()
                .rev()
                .take_while(|c| !c.is_ascii_alphanumeric() && *c != '-' && *c != '_')
                .collect::<String>()
                .chars()
                .rev()
                .collect();
            *next = format!("***{trailing}");
        }
    }
    parts.join("/")
}

fn redact_sensitive_text(text: &str) -> String {
    // Best-effort: hide query api-key=... and path /v2/<long token>.
    let mut out = text.to_owned();
    for marker in ["api-key=", "api_key=", "apikey=", "x-api-key="] {
        let lower = out.to_ascii_lowercase();
        let mut search_from = 0;
        while let Some(rel) = lower[search_from..].find(marker) {
            let idx = search_from + rel;
            let start = idx + marker.len();
            let end = out[start..]
                .find(['&', ' ', '"', '\'', ')'])
                .map(|n| start + n)
                .unwrap_or(out.len());
            out.replace_range(start..end, "***");
            search_from = start + 3;
            if search_from >= out.len() {
                break;
            }
        }
    }
    redact_path_secrets(&out)
}

fn one_line_error(message: &str, max_chars: usize) -> String {
    message
        .chars()
        .take(max_chars)
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect()
}

/// Print a provider-layer failure (non-HTTP transport already logged above).
pub fn print_provider_error(source: &str, request_key: &str, error: &str) {
    if is_rate_limit_message(error) {
        return;
    }
    eprintln!(
        "[api/error] source={source} request_key={request_key} error={}",
        one_line_error(&redact_sensitive_text(error), ERROR_LOG_CHARS)
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::{
        Method::{GET, POST},
        MockServer,
    };

    #[test]
    fn endpoint_log_label_never_contains_path_or_api_key() {
        let url = "https://eth-mainnet.g.alchemy.com/v2/super-secret-key/getNFTs";
        let label = redact_endpoint(url);
        assert!(label.contains("eth-mainnet.g.alchemy.com"));
        assert!(label.contains("/v2/***/getNFTs") || label.contains("/v2/***"));
        let prices = "https://api.g.alchemy.com/prices/v1/test-api-key-redaction-marker/tokens/by-symbol?symbols=SOL";
        let prices_label = redact_endpoint(prices);
        assert!(
            !prices_label.contains("test-api-key-redaction-marker"),
            "prices path must redact key: {prices_label}"
        );
        assert!(prices_label.contains("/prices/v1/***/tokens/by-symbol"));
        assert!(!label.contains("super-secret-key"));
    }

    #[test]
    fn query_api_key_is_redacted() {
        let url = "https://mainnet.helius-rpc.com/?api-key=abc123secret";
        let label = redact_endpoint(url);
        assert!(label.contains("api-key=***"));
        assert!(!label.contains("abc123secret"));
    }

    #[test]
    fn with_token_balances_query_is_not_treated_as_secret() {
        let url = "https://eth-mainnet.g.alchemy.com/nft/v3/super-secret-key/getOwnersForContract?contractAddress=0xabc&withTokenBalances=true";
        let label = redact_endpoint(url);
        assert!(label.contains("withTokenBalances=true"));
        assert!(!label.contains("super-secret-key"));
    }

    #[test]
    fn redacts_key_inside_reqwest_error_parentheses() {
        let msg = "error sending request for url (https://base-mainnet.g.alchemy.com/v2/test-api-key-redaction-marker)";
        let redacted = redact_sensitive_text(msg);
        assert!(!redacted.contains("test-api-key-redaction-marker"));
        assert!(redacted.contains("/v2/***"));
    }

    #[test]
    fn error_log_message_is_single_line_and_bounded() {
        let message = format!("first\nsecond\r\n{}", "x".repeat(2000));
        let sanitized = one_line_error(&message, 500);
        assert!(!sanitized.contains('\n'));
        assert!(!sanitized.contains('\r'));
        assert_eq!(sanitized.chars().count(), 500);
    }

    #[test]
    fn retry_after_supports_delta_seconds_and_error_round_trip() {
        let header = HeaderValue::from_static("7");
        assert_eq!(
            parse_retry_after_header(&header),
            Some(Duration::from_secs(7))
        );
        let error =
            AnalysisError::http("HTTP 429 endpoint=example.test retry_after_ms=7000 body=limited");
        assert_eq!(retry_after_from_error(&error), Some(Duration::from_secs(7)));
        assert_eq!(rate_limit_backoff(&error, 0), Duration::from_secs(7));
    }

    #[test]
    fn rate_limit_backoff_grows_and_is_capped() {
        let error = AnalysisError::http("HTTP 429 endpoint=example.test");
        assert_eq!(rate_limit_backoff(&error, 0), Duration::from_secs(1));
        assert_eq!(rate_limit_backoff(&error, 1), Duration::from_secs(2));
        assert_eq!(rate_limit_backoff(&error, 4), Duration::from_secs(16));
        assert_eq!(rate_limit_backoff(&error, 8), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn opensea_token_bucket_starts_with_one_and_caps_burst() {
        let limiter = TokenBucketRateLimiter::new(4, Duration::from_millis(50));
        // Initial permit available.
        limiter.acquire().await.unwrap();
        // Immediate second acquire must wait for refill; with 50ms refill it should succeed.
        let start = std::time::Instant::now();
        limiter.acquire().await.unwrap();
        assert!(
            start.elapsed() >= Duration::from_millis(40),
            "second token should wait for refill"
        );
    }

    #[tokio::test]
    async fn provider_lanes_have_independent_concurrency_and_cooldown() {
        let client = HttpClient::with_retries(1, 0).unwrap();
        // Saturate Alchemy concurrency (1 slot) by holding the permit without
        // going through HTTP — acquire the semaphore directly via a long cool-down
        // is not enough; use rate cool-down on alchemy and ensure opensea still moves.
        client
            .alchemy
            .limiter
            .note_rate_limited(Duration::from_millis(400));
        // OpenSea must not wait for Alchemy's cool-down.
        let start = Instant::now();
        client.opensea.limiter.acquire().await.unwrap();
        assert!(
            start.elapsed() < Duration::from_millis(80),
            "opensea cool-down/rate must be independent of alchemy pause"
        );

        // Concurrency: hold Alchemy's only in-flight permit and ensure OpenSea
        // can still acquire its own permit immediately.
        let alchemy_permit = client
            .alchemy
            .in_flight
            .clone()
            .try_acquire_owned()
            .unwrap();
        let os_start = Instant::now();
        let _os_permit = client
            .opensea
            .in_flight
            .clone()
            .try_acquire_owned()
            .unwrap();
        assert!(
            os_start.elapsed() < Duration::from_millis(20),
            "opensea concurrency must not be blocked by a full alchemy pool"
        );
        drop(alchemy_permit);
    }

    #[test]
    fn helius_uses_the_same_configured_concurrency_as_alchemy() {
        let client = HttpClient::with_retries(7, 0).unwrap();
        assert_eq!(client.alchemy.in_flight.available_permits(), 7);
        assert_eq!(client.helius.in_flight.available_permits(), 7);
        assert_eq!(
            client.helius.limiter.max_burst,
            client.alchemy.limiter.max_burst
        );
        assert_eq!(
            client.helius.limiter.refill_interval,
            client.alchemy.limiter.refill_interval
        );
    }

    #[test]
    fn http_429_is_detected_for_adaptive_backoff() {
        let err = AnalysisError::http(
            "HTTP 429 Too Many Requests endpoint=example.com/ content_type=application/json body=rate limited",
        );
        assert!(is_http_status(&err, 429));
        assert!(is_rate_limited(&err));
        assert!(is_retryable(&err));
        assert!(!is_http_status(&err, 500));

        let transport = AnalysisError::http(
            "transport error method=POST endpoint=mainnet.helius-rpc.com/ status=429 detail=…",
        );
        assert!(is_rate_limited(&transport));
    }

    #[test]
    fn jsonrpc_rate_limit_payload_is_detected() {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "1",
            "error": { "code": -32429, "message": "rate limited" }
        });
        assert!(jsonrpc_rate_limit_error(&payload).is_some());
        let batch = serde_json::json!([
            {"jsonrpc":"2.0","id":"1","result":{}},
            {"jsonrpc":"2.0","id":"2","error":{"code":-32005,"message":"rate limited"}}
        ]);
        assert!(jsonrpc_rate_limit_error(&batch).is_some());
        let ok = serde_json::json!({"jsonrpc":"2.0","id":"1","result":{}});
        assert!(jsonrpc_rate_limit_error(&ok).is_none());
    }

    #[test]
    fn durable_cache_accepts_empty_successes_and_rejects_provider_errors() {
        assert!(response_is_fully_successful(&serde_json::json!({
            "jsonrpc":"2.0", "result": {"id":"asset"}
        })));
        assert!(response_is_fully_successful(&serde_json::json!({
            "jsonrpc":"2.0", "result": null
        })));
        assert!(response_is_fully_successful(&serde_json::json!([])));
        assert!(!response_is_fully_successful(&serde_json::json!([
            {"jsonrpc":"2.0", "result": {"ok":true}},
            {"jsonrpc":"2.0", "error": {"code":-32000}}
        ])));
        assert!(!response_is_fully_successful(&serde_json::json!({
            "errors": ["not found"]
        })));
    }

    #[test]
    fn rate_limit_errors_are_silent_at_request_and_provider_layers() {
        for message in [
            "HTTP 429 Too Many Requests endpoint=example.test",
            "transport error status=429",
            "JSON-RPC error {\"code\":-32005,\"message\":\"rate limited\"}",
        ] {
            let error = AnalysisError::http(message);
            assert!(!should_print_request_error(&error));
            assert!(is_rate_limit_message(message));
        }
        let error = AnalysisError::http("HTTP 500 endpoint=example.test");
        assert!(should_print_request_error(&error));
        assert!(!is_rate_limit_message(&error.to_string()));
    }

    #[tokio::test]
    async fn durable_success_cache_reuses_raw_response_across_clients_and_keys() {
        let server = MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method(POST).path("/");
                then.status(200)
                    .json_body(serde_json::json!({"jsonrpc":"2.0","result":{"id":"asset"}}));
            })
            .await;
        let dir = std::env::temp_dir().join(format!(
            "analysis_http_success_cache_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let body = serde_json::json!({
            "jsonrpc":"2.0",
            "method":"getTransaction",
            "params":["signature", {"commitment":"finalized"}]
        });

        let first = HttpClient::with_retries_and_cache(1, 0, Some(dir.clone())).unwrap();
        let first_value = first
            .post_json_helius(
                &format!("{}/?api-key=first-secret", server.base_url()),
                &[],
                &body,
            )
            .await
            .unwrap();
        let second = HttpClient::with_retries_and_cache(1, 0, Some(dir.clone())).unwrap();
        let second_value = second
            .post_json_helius(
                &format!("{}/?api-key=different-secret", server.base_url()),
                &[],
                &body,
            )
            .await
            .unwrap();

        assert_eq!(first_value, second_value);
        mock.assert_hits_async(1).await;
        let identity = success_cache_identity(
            &reqwest::Method::POST,
            &redact_endpoint(&format!("{}/?api-key=ignored", server.base_url())),
            Some(&body),
        );
        let cache = SuccessResponseCache::new(dir.clone());
        let cache_path = cache.path("helius", &identity);
        assert!(cache_path.is_file());
        let cache_text = String::from_utf8(
            zstd::stream::decode_all(Cursor::new(fs::read(cache_path).unwrap())).unwrap(),
        )
        .unwrap();
        assert!(!cache_text.contains("first-secret"));
        assert!(!cache_text.contains("different-secret"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn durable_success_cache_reuses_mutable_get_and_empty_payload() {
        let server = MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method(GET)
                    .path("/api/v2/events/collection/example")
                    .query_param("cursor", "page-1");
                then.status(200)
                    .json_body(serde_json::json!({"asset_events": [], "next": null}));
            })
            .await;
        let dir = std::env::temp_dir().join(format!(
            "analysis_http_success_cache_get_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let url = format!(
            "{}/api/v2/events/collection/example?cursor=page-1",
            server.base_url()
        );

        let first = HttpClient::with_retries_and_cache(1, 0, Some(dir.clone())).unwrap();
        let expected = first
            .get_json_opensea(&url, &[("x-api-key", "first")])
            .await
            .unwrap();
        let second = HttpClient::with_retries_and_cache(1, 0, Some(dir.clone())).unwrap();
        let reused = second
            .get_json_opensea(&url, &[("x-api-key", "second")])
            .await
            .unwrap();

        assert_eq!(expected, reused);
        mock.assert_hits_async(1).await;
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn uncached_alchemy_requests_never_read_or_write_durable_cache() {
        let server = MockServer::start_async().await;
        let get_mock = server
            .mock_async(|when, then| {
                when.method(GET).path("/prices");
                then.status(200)
                    .json_body(serde_json::json!({"price": "1.25"}));
            })
            .await;
        let post_mock = server
            .mock_async(|when, then| {
                when.method(POST).path("/prices-by-address");
                then.status(200)
                    .json_body(serde_json::json!({"price": "2.50"}));
            })
            .await;
        let dir = std::env::temp_dir().join(format!(
            "analysis_http_uncached_price_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let url = format!("{}/prices", server.base_url());
        let address_url = format!("{}/prices-by-address", server.base_url());
        let body = serde_json::json!({"addresses": ["0x1"]});
        let client = HttpClient::with_retries_and_cache(1, 0, Some(dir.clone())).unwrap();

        for _ in 0..2 {
            assert_eq!(
                client.get_json_alchemy_uncached(&url, &[]).await.unwrap(),
                serde_json::json!({"price": "1.25"})
            );
            assert_eq!(
                client
                    .post_json_alchemy_uncached(&address_url, &[], &body)
                    .await
                    .unwrap(),
                serde_json::json!({"price": "2.50"})
            );
        }

        get_mock.assert_hits_async(2).await;
        post_mock.assert_hits_async(2).await;
        assert!(!dir.join(SUCCESS_CACHE_V2_DIR).exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn refresh_cutoff_replaces_old_entry_once_then_reuses_new_success() {
        let server = MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method(GET).path("/snapshot");
                then.status(200)
                    .json_body(serde_json::json!({"value": "fresh"}));
            })
            .await;
        let dir = std::env::temp_dir().join(format!(
            "analysis_http_success_cache_refresh_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let url = format!("{}/snapshot", server.base_url());
        let identity = success_cache_identity(&reqwest::Method::GET, &redact_endpoint(&url), None);
        let cache = SuccessResponseCache::new(dir.clone());
        cache
            .store_v2(
                "opensea",
                &identity,
                &serde_json::json!({"value": "old"}),
                1,
            )
            .unwrap();
        let cutoff = unix_now();
        let client =
            HttpClient::with_retries_and_cache_since(1, 0, Some(dir.clone()), Some(cutoff))
                .unwrap();

        let first = client.get_json_opensea(&url, &[]).await.unwrap();
        let second = client.get_json_opensea(&url, &[]).await.unwrap();

        assert_eq!(first, serde_json::json!({"value": "fresh"}));
        assert_eq!(first, second);
        mock.assert_hits_async(1).await;
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn provider_error_response_is_never_cached() {
        let server = MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method(POST).path("/");
                then.status(200).json_body(serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": {"code": -32000, "message": "not found"}
                }));
            })
            .await;
        let dir = std::env::temp_dir().join(format!(
            "analysis_http_success_cache_error_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let client = HttpClient::with_retries_and_cache(1, 0, Some(dir.clone())).unwrap();
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "getAsset",
            "params": {"id": "missing"}
        });

        for _ in 0..2 {
            let value = client
                .post_json_helius(&server.base_url(), &[], &body)
                .await
                .unwrap();
            assert!(value.get("error").is_some());
        }
        mock.assert_hits_async(2).await;
        assert_eq!(
            fs::read_dir(dir.join(SUCCESS_CACHE_V2_DIR))
                .ok()
                .into_iter()
                .flatten()
                .count(),
            0
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn version_two_compressed_entry_remains_reusable() {
        let dir = std::env::temp_dir().join(format!(
            "analysis_http_success_cache_v2_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let cache = SuccessResponseCache::new(dir.clone());
        let identity = "GET\nexample.test/snapshot\n";
        let path = cache.path("opensea", identity);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let body = serde_json::to_vec(&SuccessCacheEntryV2 {
            version: 2,
            cached_at: None,
            response: serde_json::json!({"ok": true}),
        })
        .unwrap();
        fs::write(
            &path,
            zstd::stream::encode_all(Cursor::new(body), 3).unwrap(),
        )
        .unwrap();

        assert_eq!(
            cache.load("opensea", identity, None),
            Some(serde_json::json!({"ok": true}))
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_success_cache_is_reused_and_migrated_without_network() {
        let dir = std::env::temp_dir().join(format!(
            "analysis_http_success_cache_legacy_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let cache = SuccessResponseCache::new(dir.clone());
        let provider = "alchemy";
        let identity = "POST\nexample.test\n{\"method\":\"getTransaction\"}";
        let response = serde_json::json!({"jsonrpc":"2.0","result":{"ok":true}});
        let legacy_path = cache.legacy_path(provider, identity);
        fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
        fs::write(
            &legacy_path,
            serde_json::to_vec(&SuccessCacheEntry {
                request_identity: identity.into(),
                response: response.clone(),
            })
            .unwrap(),
        )
        .unwrap();

        assert_eq!(cache.load(provider, identity, None), Some(response));
        assert!(!legacy_path.exists());
        assert!(cache.path(provider, identity).is_file());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn bulk_success_cache_migration_preserves_corrupt_legacy_entries() {
        let dir = std::env::temp_dir().join(format!(
            "analysis_http_success_cache_bulk_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let cache = SuccessResponseCache::new(dir.clone());
        let identity = "GET\nexample.test\n";
        let legacy = cache.legacy_path("helius", identity);
        fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        fs::write(
            &legacy,
            serde_json::to_vec(&SuccessCacheEntry {
                request_identity: identity.into(),
                response: serde_json::json!({"result":1}),
            })
            .unwrap(),
        )
        .unwrap();
        let corrupt = legacy.parent().unwrap().join("corrupt.json");
        fs::write(&corrupt, b"not-json").unwrap();

        let stats = migrate_legacy_success_response_cache(&dir);
        assert_eq!(stats.scanned, 2);
        assert_eq!(stats.migrated, 1);
        assert_eq!(stats.failed, 1);
        assert!(!legacy.exists());
        assert!(corrupt.exists());
        assert_eq!(
            cache.load_v2("helius", identity, None),
            Some(serde_json::json!({"result":1}))
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn rate_limit_cooldown_blocks_other_acquires_for_one_second() {
        let limiter = TokenBucketRateLimiter::new(5, Duration::from_millis(200));
        // Consume the initial token so the next acquire must wait.
        limiter.acquire().await.unwrap();
        limiter.note_rate_limited(Duration::from_millis(250));
        let start = Instant::now();
        limiter.acquire().await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(240),
            "expected ~250ms cool-down, got {elapsed:?}"
        );
        // After cool-down, a single resume token was granted and consumed;
        // next acquire should wait for refill (~200ms), not block another full second.
        let start2 = Instant::now();
        limiter.acquire().await.unwrap();
        assert!(
            start2.elapsed() < Duration::from_millis(350),
            "post cool-down acquire should only wait for refill"
        );
    }
}
