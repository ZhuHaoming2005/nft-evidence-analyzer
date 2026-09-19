use crate::report::{metadata_sample_pair_reports, write_duplicate_pair_sample_files};
use base64::Engine;
use dedup_core::{
    DedupError, MetadataImagePairSample, MetadataSampleDownloadCandidate,
    MetadataSampleDownloadResult, MetadataSampleDownloadSink, MetadataSamplePool, ProgressObserver,
};
use reqwest::blocking::Client;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::header::{CONTENT_TYPE, LOCATION, RETRY_AFTER};
use reqwest::redirect::Policy;
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

const DOWNLOAD_WORKERS: usize = 32;
const MAX_IMAGE_BYTES: u64 = 50 * 1024 * 1024;
const MAX_ATTEMPTS: usize = 2;
const MAX_REDIRECTS: usize = 10;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const TOTAL_IMAGE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);
const DOWNLOAD_QUEUE_CAPACITY: usize = DOWNLOAD_WORKERS * 8;
const READY_URI_CACHE_CAPACITY: usize = DOWNLOAD_QUEUE_CAPACITY * 4;
const IMAGE_CACHE_DIR: &str = ".metadata-image-cache";
const TRANSACTION_ITEMS: &str = ".sample-transaction-items.json";
const TRANSACTION_STATE: &str = ".sample-transaction-state";
const TRANSACTION_BACKUP: &str = ".sample-transaction-backup";
const TRANSACTION_COMMITTING: &[u8] = b"committing\n";
const TRANSACTION_COMMITTED: &[u8] = b"committed\n";

#[derive(Debug)]
struct PublicDnsResolver;

impl Resolve for PublicDnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addresses = resolve_public_host(&host)
                .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })?;
            Ok(Box::new(addresses.into_iter()) as Addrs)
        })
    }
}

struct DownloadedImage {
    bytes: Vec<u8>,
    suffix: &'static str,
}

#[derive(Clone)]
struct CachedImage {
    file: PathBuf,
    suffix: &'static str,
}

#[derive(Serialize, Deserialize)]
struct CachedImageRecord {
    suffix: String,
}

#[derive(Clone, Copy)]
enum ImageSide {
    A,
    B,
}

struct ImageConsumer {
    candidate_id: u64,
    side: ImageSide,
}

enum UriState {
    Loading(Vec<ImageConsumer>),
    Ready(CachedImage),
}

struct ImageJob {
    uri: String,
    cache_key: String,
    attempt: usize,
    deadline: Option<Instant>,
}

struct ImageJobResult {
    uri: String,
    outcome: ImageJobOutcome,
}

enum ImageJobOutcome {
    Complete(Result<CachedImage, std::io::Error>),
    Retry { job: ImageJob, ready_at: Instant },
}

enum ImageDownloadAttempt {
    Complete(DownloadedImage),
    RetryAfter(Duration),
}

struct DelayedImageJob {
    ready_at: Instant,
    ordinal: u64,
    job: ImageJob,
}

impl PartialEq for DelayedImageJob {
    fn eq(&self, other: &Self) -> bool {
        self.ready_at == other.ready_at && self.ordinal == other.ordinal
    }
}

impl Eq for DelayedImageJob {}

impl PartialOrd for DelayedImageJob {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for DelayedImageJob {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .ready_at
            .cmp(&self.ready_at)
            .then_with(|| other.ordinal.cmp(&self.ordinal))
    }
}

struct PendingPair {
    candidate: MetadataSampleDownloadCandidate,
    row_dir: PathBuf,
    file_a: Option<(PathBuf, &'static str)>,
    file_b: Option<(PathBuf, &'static str)>,
}

struct WorkerProgress {
    parent: Arc<dyn ProgressObserver>,
    shutdown: Arc<AtomicBool>,
}

impl ProgressObserver for WorkerProgress {
    fn set_stage(&self, _stage: &str) {}
    fn begin_phase(&self, _phase: &str, _total: Option<u64>) {}
    fn set_total(&self, _total: Option<u64>) {}
    fn add_completed(&self, _delta: u64) {}

    fn check_cancelled(&self) -> Result<(), DedupError> {
        if self.shutdown.load(Ordering::SeqCst) {
            Err(DedupError::Interrupted)
        } else {
            self.parent.check_cancelled()
        }
    }
}

struct DownloadRow {
    index: usize,
    pool: &'static str,
    pool_index: usize,
    file_a: String,
    error_a: String,
    file_b: String,
    error_b: String,
    record_a: String,
    record_b: String,
}

struct StagedPair {
    candidate_id: u64,
    sample: MetadataImagePairSample,
    file_a: PathBuf,
    suffix_a: &'static str,
    file_b: PathBuf,
    suffix_b: &'static str,
}

#[derive(Serialize)]
struct NftSampleRecord<'a> {
    sample_pool: &'static str,
    chain: &'a str,
    contract_address: &'a str,
    token_id: &'a str,
    image_uri: &'a str,
    image_file: &'a str,
    metadata: serde_json::Value,
}

pub struct StreamingDownloadSession {
    output_dir: PathBuf,
    staging: Option<tempfile::TempDir>,
    cache_dir: PathBuf,
    progress: Arc<dyn ProgressObserver>,
    shutdown: Arc<AtomicBool>,
    job_tx: Option<SyncSender<ImageJob>>,
    unsent_jobs: VecDeque<ImageJob>,
    result_rx: Receiver<ImageJobResult>,
    workers: Vec<JoinHandle<()>>,
    delayed_retries: BinaryHeap<DelayedImageJob>,
    next_retry_ordinal: u64,
    uri_states: HashMap<String, UriState>,
    ready_uri_order: VecDeque<String>,
    pending_pairs: HashMap<u64, PendingPair>,
    completed: VecDeque<MetadataSampleDownloadResult>,
    intra_chain: Vec<StagedPair>,
    cross_chain: Vec<StagedPair>,
    attempted: usize,
    cache_hits: usize,
    coalesced_uris: usize,
    network_jobs: usize,
    failed_pairs: usize,
    sampling_seed: Option<u64>,
}

impl StreamingDownloadSession {
    pub fn new(
        output_dir: &Path,
        target_per_pool: usize,
        progress: Arc<dyn ProgressObserver>,
    ) -> Result<Self, std::io::Error> {
        fs::create_dir_all(output_dir)?;
        recover_sample_transactions(output_dir)?;
        let staging = tempfile::Builder::new()
            .prefix(".metadata-fast-sample-")
            .tempdir_in(output_dir)?;
        let cache_dir = output_dir.join(IMAGE_CACHE_DIR);
        fs::create_dir_all(&cache_dir)?;
        let client = build_http_client().map_err(std::io::Error::other)?;
        let (job_tx, job_rx) = mpsc::sync_channel(DOWNLOAD_QUEUE_CAPACITY);
        let job_rx = Arc::new(Mutex::new(job_rx));
        let (result_tx, result_rx) = mpsc::channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(DOWNLOAD_WORKERS);
        for worker_id in 0..DOWNLOAD_WORKERS {
            let client = client.clone();
            let cache_dir = cache_dir.clone();
            let worker_progress = WorkerProgress {
                parent: Arc::clone(&progress),
                shutdown: Arc::clone(&shutdown),
            };
            let job_rx = Arc::clone(&job_rx);
            let result_tx = result_tx.clone();
            workers.push(
                std::thread::Builder::new()
                    .name(format!("sample-image-{worker_id}"))
                    .spawn(move || {
                        image_download_worker(
                            &client,
                            &cache_dir,
                            &worker_progress,
                            &job_rx,
                            &result_tx,
                        );
                    })?,
            );
        }
        drop(result_tx);
        Ok(Self {
            output_dir: output_dir.to_owned(),
            staging: Some(staging),
            cache_dir,
            progress,
            shutdown,
            job_tx: Some(job_tx),
            unsent_jobs: VecDeque::new(),
            result_rx,
            workers,
            delayed_retries: BinaryHeap::new(),
            next_retry_ordinal: 0,
            uri_states: HashMap::new(),
            ready_uri_order: VecDeque::new(),
            pending_pairs: HashMap::new(),
            completed: VecDeque::new(),
            intra_chain: Vec::with_capacity(target_per_pool),
            cross_chain: Vec::with_capacity(target_per_pool),
            attempted: 0,
            cache_hits: 0,
            coalesced_uris: 0,
            network_jobs: 0,
            failed_pairs: 0,
            sampling_seed: None,
        })
    }

    pub fn set_sampling_seed(&mut self, seed: u64) {
        self.sampling_seed = Some(seed);
    }

    #[cfg(test)]
    pub fn try_batch(
        &mut self,
        candidates: &[(MetadataSamplePool, MetadataImagePairSample)],
        _progress: &dyn ProgressObserver,
    ) -> Result<Vec<bool>, std::io::Error> {
        let first_id = self.attempted as u64 + 1;
        let submitted = candidates
            .iter()
            .enumerate()
            .map(|(offset, (pool, sample))| MetadataSampleDownloadCandidate {
                id: first_id + offset as u64,
                pool: *pool,
                sample: sample.clone(),
            })
            .collect::<Vec<_>>();
        self.submit(&submitted).map_err(dedup_to_io)?;
        let mut by_id = HashMap::with_capacity(submitted.len());
        while by_id.len() < submitted.len() {
            for result in self.poll(true).map_err(dedup_to_io)? {
                by_id.insert(result.id, result.success);
            }
        }
        Ok(submitted
            .iter()
            .map(|candidate| by_id[&candidate.id])
            .collect())
    }

    pub fn finish(
        mut self,
    ) -> Result<Vec<MetadataImagePairSample>, Box<dyn std::error::Error + Send + Sync>> {
        self.shutdown_workers();
        let output_dir = self.output_dir.clone();
        let staging = self.staging.take().expect("staging directory is present");
        let mut successful = self
            .intra_chain
            .drain(..)
            .map(|pair| (MetadataSamplePool::IntraChain, pair))
            .collect::<Vec<_>>();
        successful.extend(
            self.cross_chain
                .drain(..)
                .map(|pair| (MetadataSamplePool::CrossChain, pair)),
        );
        successful.sort_unstable_by_key(|(_, pair)| pair.candidate_id);
        let staging_root = staging.keep();
        let published =
            publish_staged_pairs(&output_dir, &staging_root, successful, self.sampling_seed);
        if published.is_ok() || !staging_root.join(TRANSACTION_STATE).exists() {
            let _ = fs::remove_dir_all(&staging_root);
        }
        published
    }

    fn submit_candidate(
        &mut self,
        candidate: &MetadataSampleDownloadCandidate,
    ) -> Result<(), std::io::Error> {
        check_cancelled(self.progress.as_ref())?;
        self.attempted = self.attempted.saturating_add(1);
        let staging_root = self
            .staging
            .as_ref()
            .expect("staging directory is present")
            .path();
        let row_dir = staging_root.join(format!("candidate-{}", candidate.id));
        fs::create_dir(&row_dir)?;
        if self
            .pending_pairs
            .insert(
                candidate.id,
                PendingPair {
                    candidate: candidate.clone(),
                    row_dir,
                    file_a: None,
                    file_b: None,
                },
            )
            .is_some()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "duplicate media download candidate ID",
            ));
        }
        self.request_image(candidate.id, ImageSide::A, &candidate.sample.image_uri_a)?;
        self.request_image(candidate.id, ImageSide::B, &candidate.sample.image_uri_b)?;
        Ok(())
    }

    fn request_image(
        &mut self,
        candidate_id: u64,
        side: ImageSide,
        uri: &str,
    ) -> Result<(), std::io::Error> {
        let uri = uri.trim().to_owned();
        let consumer = ImageConsumer { candidate_id, side };
        let ready = match self.uri_states.get_mut(&uri) {
            Some(UriState::Loading(consumers)) => {
                self.coalesced_uris = self.coalesced_uris.saturating_add(1);
                consumers.push(consumer);
                return Ok(());
            }
            Some(UriState::Ready(image)) => {
                self.cache_hits = self.cache_hits.saturating_add(1);
                Some(image.clone())
            }
            None => None,
        };
        if let Some(image) = ready {
            self.deliver_image(consumer, Ok(&image));
            return Ok(());
        }
        let cache_key = image_cache_key(&uri);
        if let Some(image) = load_cached_image(&self.cache_dir, &cache_key)? {
            self.cache_hits = self.cache_hits.saturating_add(1);
            self.remember_ready_uri(uri, image.clone());
            self.deliver_image(consumer, Ok(&image));
            return Ok(());
        }
        self.uri_states
            .insert(uri.clone(), UriState::Loading(vec![consumer]));
        self.network_jobs = self.network_jobs.saturating_add(1);
        self.unsent_jobs.push_back(ImageJob {
            uri,
            cache_key,
            attempt: 0,
            deadline: None,
        });
        Ok(())
    }

    fn pump_jobs(&mut self) -> Result<(), std::io::Error> {
        check_cancelled(self.progress.as_ref())?;
        let sender = self
            .job_tx
            .as_ref()
            .ok_or_else(|| std::io::Error::other("image downloader is shutting down"))?;
        while let Some(job) = self.unsent_jobs.pop_front() {
            match sender.try_send(job) {
                Ok(()) => {}
                Err(TrySendError::Full(returned)) => {
                    self.unsent_jobs.push_front(returned);
                    break;
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err(std::io::Error::other(
                        "image download worker queue disconnected",
                    ));
                }
            }
        }
        Ok(())
    }

    fn receive_worker_result(&mut self, wait: bool) -> Result<bool, std::io::Error> {
        let result = if wait {
            loop {
                check_cancelled(self.progress.as_ref())?;
                self.submit_due_retries()?;
                self.pump_jobs()?;
                let retry_wait = self
                    .delayed_retries
                    .peek()
                    .map(|retry| retry.ready_at.saturating_duration_since(Instant::now()))
                    .unwrap_or(Duration::from_millis(100));
                match self
                    .result_rx
                    .recv_timeout(retry_wait.min(Duration::from_millis(100)))
                {
                    Ok(result) => break Some(result),
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break None,
                }
            }
        } else {
            self.submit_due_retries()?;
            self.pump_jobs()?;
            self.result_rx.try_recv().ok()
        };
        let Some(result) = result else {
            return Ok(false);
        };
        self.process_worker_result(result)?;
        Ok(true)
    }

    fn submit_due_retries(&mut self) -> Result<(), std::io::Error> {
        let now = Instant::now();
        while self
            .delayed_retries
            .peek()
            .is_some_and(|retry| retry.ready_at <= now)
        {
            let retry = self
                .delayed_retries
                .pop()
                .expect("due image retry is present");
            self.unsent_jobs.push_back(retry.job);
        }
        Ok(())
    }

    fn process_worker_result(&mut self, result: ImageJobResult) -> Result<(), std::io::Error> {
        let ImageJobResult { uri, outcome } = result;
        let result = match outcome {
            ImageJobOutcome::Retry { job, ready_at } => {
                if !matches!(self.uri_states.get(&uri), Some(UriState::Loading(_))) {
                    return Ok(());
                }
                let ordinal = self.next_retry_ordinal;
                self.next_retry_ordinal = self.next_retry_ordinal.wrapping_add(1);
                self.delayed_retries.push(DelayedImageJob {
                    ready_at,
                    ordinal,
                    job,
                });
                return Ok(());
            }
            ImageJobOutcome::Complete(result) => result,
        };
        let Some(UriState::Loading(consumers)) = self.uri_states.remove(&uri) else {
            return Ok(());
        };
        match result {
            Ok(image) => {
                self.remember_ready_uri(uri, image.clone());
                for consumer in consumers {
                    self.deliver_image(consumer, Ok(&image));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => return Err(error),
            Err(error) => {
                for consumer in consumers {
                    self.deliver_image(consumer, Err(&error));
                }
            }
        }
        Ok(())
    }

    fn remember_ready_uri(&mut self, uri: String, image: CachedImage) {
        self.uri_states.insert(uri.clone(), UriState::Ready(image));
        self.ready_uri_order.push_back(uri);
        while self.ready_uri_order.len() > READY_URI_CACHE_CAPACITY {
            let expired = self
                .ready_uri_order
                .pop_front()
                .expect("ready URI cache exceeds its capacity");
            if matches!(self.uri_states.get(&expired), Some(UriState::Ready(_))) {
                self.uri_states.remove(&expired);
            }
        }
    }

    fn deliver_image(
        &mut self,
        consumer: ImageConsumer,
        image: Result<&CachedImage, &std::io::Error>,
    ) {
        let Some(pair) = self.pending_pairs.get_mut(&consumer.candidate_id) else {
            return;
        };
        let delivered = image
            .map_err(|error| std::io::Error::new(error.kind(), error.to_string()))
            .and_then(|image| {
                let destination = pair.row_dir.join(match consumer.side {
                    ImageSide::A => format!("a{}", image.suffix),
                    ImageSide::B => format!("b{}", image.suffix),
                });
                materialize_cached_image(&image.file, &destination)?;
                Ok((destination, image.suffix))
            });
        match delivered {
            Ok(file) => match consumer.side {
                ImageSide::A => pair.file_a = Some(file),
                ImageSide::B => pair.file_b = Some(file),
            },
            Err(_) => {
                let failed = self
                    .pending_pairs
                    .remove(&consumer.candidate_id)
                    .expect("pending pair was just borrowed");
                let _ = fs::remove_dir_all(failed.row_dir);
                self.completed.push_back(MetadataSampleDownloadResult {
                    id: consumer.candidate_id,
                    success: false,
                });
                self.failed_pairs = self.failed_pairs.saturating_add(1);
                return;
            }
        }
        if pair.file_a.is_none() || pair.file_b.is_none() {
            return;
        }
        let completed = self
            .pending_pairs
            .remove(&consumer.candidate_id)
            .expect("completed pair exists");
        let (file_a, suffix_a) = completed.file_a.expect("A image completed");
        let (file_b, suffix_b) = completed.file_b.expect("B image completed");
        let pool = completed.candidate.pool;
        let staged = StagedPair {
            candidate_id: consumer.candidate_id,
            sample: completed.candidate.sample,
            file_a,
            suffix_a,
            file_b,
            suffix_b,
        };
        match pool {
            MetadataSamplePool::IntraChain => self.intra_chain.push(staged),
            MetadataSamplePool::CrossChain => self.cross_chain.push(staged),
        }
        self.completed.push_back(MetadataSampleDownloadResult {
            id: consumer.candidate_id,
            success: true,
        });
    }

    fn shutdown_workers(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.job_tx.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }

    pub fn summary(&self) -> String {
        format!(
            "downloaded_pairs={}, failed_pairs={}, submitted_pairs={}, network_uris={}, reused_uris={}, coalesced_uris={}",
            self.intra_chain.len() + self.cross_chain.len(),
            self.failed_pairs,
            self.attempted,
            self.network_jobs,
            self.cache_hits,
            self.coalesced_uris,
        )
    }

    fn update_progress_detail(&self) {
        let successful = self.intra_chain.len() + self.cross_chain.len();
        let resolved = successful.saturating_add(self.failed_pairs);
        let mut detail = format!("media={}/{}", resolved, self.attempted);
        let pending = self.pending_pairs.len();
        if pending != 0 {
            detail.push_str(&format!(" p={pending}"));
        }
        let queued = self.unsent_jobs.len();
        if queued != 0 {
            detail.push_str(&format!(" q={queued}"));
        }
        if self.failed_pairs != 0 {
            detail.push_str(&format!(" f={}", self.failed_pairs));
        }
        if self.network_jobs != 0 {
            detail.push_str(&format!(" net={}", self.network_jobs));
        }
        let reused = self.cache_hits.saturating_add(self.coalesced_uris);
        if reused != 0 {
            detail.push_str(&format!(" reused={reused}"));
        }
        self.progress.set_detail(&detail);
    }
}

impl MetadataSampleDownloadSink for StreamingDownloadSession {
    fn submit(&mut self, candidates: &[MetadataSampleDownloadCandidate]) -> Result<(), DedupError> {
        for candidate in candidates {
            self.submit_candidate(candidate).map_err(io_to_dedup)?;
        }
        self.pump_jobs().map_err(io_to_dedup)?;
        self.update_progress_detail();
        Ok(())
    }

    fn poll(&mut self, wait: bool) -> Result<Vec<MetadataSampleDownloadResult>, DedupError> {
        if self.completed.is_empty() {
            self.receive_worker_result(wait).map_err(io_to_dedup)?;
        }
        while self.receive_worker_result(false).map_err(io_to_dedup)? {}
        self.update_progress_detail();
        Ok(self.completed.drain(..).collect())
    }

    fn retain_candidates(&mut self, candidate_ids: &[u64]) -> Result<(), DedupError> {
        let selected = candidate_ids.iter().copied().collect::<HashSet<_>>();
        let retain_selected = |pairs: &mut Vec<StagedPair>| {
            pairs.retain(|pair| {
                if selected.contains(&pair.candidate_id) {
                    return true;
                }
                if let Some(row_dir) = pair.file_a.parent() {
                    let _ = fs::remove_dir_all(row_dir);
                }
                false
            });
        };
        retain_selected(&mut self.intra_chain);
        retain_selected(&mut self.cross_chain);
        let retained = self.intra_chain.len() + self.cross_chain.len();
        if retained != selected.len() {
            return Err(DedupError::invalid(
                "sample_metadata",
                "selected media candidates do not match completed downloads",
            ));
        }
        Ok(())
    }
}

impl Drop for StreamingDownloadSession {
    fn drop(&mut self) {
        self.shutdown_workers();
    }
}

fn image_download_worker(
    client: &Client,
    cache_dir: &Path,
    progress: &dyn ProgressObserver,
    job_rx: &Arc<Mutex<Receiver<ImageJob>>>,
    result_tx: &Sender<ImageJobResult>,
) {
    loop {
        let mut job = {
            let Ok(receiver) = job_rx.lock() else {
                return;
            };
            match receiver.recv() {
                Ok(job) => job,
                Err(_) => return,
            }
        };
        if progress.check_cancelled().is_err() {
            return;
        }
        let deadline = *job
            .deadline
            .get_or_insert_with(|| Instant::now() + TOTAL_IMAGE_TIMEOUT);
        let uri = job.uri.clone();
        let outcome = match load_cached_image(cache_dir, &job.cache_key) {
            Ok(Some(cached)) => ImageJobOutcome::Complete(Ok(cached)),
            Ok(None) => {
                match download_image_attempt(client, &job.uri, progress, deadline, job.attempt) {
                    Ok(ImageDownloadAttempt::Complete(image)) => ImageJobOutcome::Complete(
                        persist_cached_image(cache_dir, &job.cache_key, image),
                    ),
                    Ok(ImageDownloadAttempt::RetryAfter(delay)) => {
                        job.attempt += 1;
                        ImageJobOutcome::Retry {
                            ready_at: (Instant::now() + delay).min(deadline),
                            job,
                        }
                    }
                    Err(error) => ImageJobOutcome::Complete(Err(error)),
                }
            }
            Err(error) => ImageJobOutcome::Complete(Err(error)),
        };
        if result_tx.send(ImageJobResult { uri, outcome }).is_err() {
            return;
        }
    }
}

fn image_cache_key(uri: &str) -> String {
    let digest = Sha256::digest(uri.trim().as_bytes());
    let mut key = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(key, "{byte:02x}");
    }
    key
}

fn cache_paths(cache_dir: &Path, key: &str) -> (PathBuf, PathBuf) {
    (
        cache_dir.join(format!("{key}.media")),
        cache_dir.join(format!("{key}.json")),
    )
}

fn load_cached_image(cache_dir: &Path, key: &str) -> Result<Option<CachedImage>, std::io::Error> {
    let (media, record) = cache_paths(cache_dir, key);
    let Ok(metadata) = fs::metadata(&media) else {
        return Ok(None);
    };
    if metadata.len() > MAX_IMAGE_BYTES {
        return Ok(None);
    }
    let Ok(record) = fs::read(&record) else {
        return Ok(None);
    };
    let Ok(record) = serde_json::from_slice::<CachedImageRecord>(&record) else {
        return Ok(None);
    };
    let Some(suffix) = cached_suffix(&record.suffix) else {
        return Ok(None);
    };
    Ok(Some(CachedImage {
        file: media,
        suffix,
    }))
}

fn persist_cached_image(
    cache_dir: &Path,
    key: &str,
    image: DownloadedImage,
) -> Result<CachedImage, std::io::Error> {
    let (media, record) = cache_paths(cache_dir, key);
    atomic_write(&media, &image.bytes)?;
    let record_bytes = serde_json::to_vec(&CachedImageRecord {
        suffix: image.suffix.to_owned(),
    })
    .map_err(std::io::Error::other)?;
    atomic_write(&record, &record_bytes)?;
    Ok(CachedImage {
        file: media,
        suffix: image.suffix,
    })
}

fn cached_suffix(suffix: &str) -> Option<&'static str> {
    match suffix {
        ".png" => Some(".png"),
        ".jpg" => Some(".jpg"),
        ".gif" => Some(".gif"),
        ".webp" => Some(".webp"),
        ".svg" => Some(".svg"),
        ".avif" => Some(".avif"),
        ".bmp" => Some(".bmp"),
        ".tiff" => Some(".tiff"),
        ".ico" => Some(".ico"),
        _ => None,
    }
}

fn materialize_cached_image(source: &Path, destination: &Path) -> Result<(), std::io::Error> {
    match fs::hard_link(source, destination) {
        Ok(()) => Ok(()),
        Err(_) => {
            fs::copy(source, destination)?;
            Ok(())
        }
    }
}

fn io_to_dedup(error: std::io::Error) -> DedupError {
    if error.kind() == std::io::ErrorKind::Interrupted {
        DedupError::Interrupted
    } else {
        DedupError::Message(error.to_string())
    }
}

#[cfg(test)]
fn dedup_to_io(error: DedupError) -> std::io::Error {
    match error {
        DedupError::Interrupted => {
            std::io::Error::new(std::io::ErrorKind::Interrupted, "interrupted")
        }
        other => std::io::Error::other(other.to_string()),
    }
}

fn publish_staged_pairs(
    output_dir: &Path,
    staging_root: &Path,
    successful: Vec<(MetadataSamplePool, StagedPair)>,
    sampling_seed: Option<u64>,
) -> Result<Vec<MetadataImagePairSample>, Box<dyn std::error::Error + Send + Sync>> {
    let target = successful.len();
    let publish_root = staging_root.join("metadata_sample_images");
    fs::create_dir(&publish_root)?;
    fs::create_dir(publish_root.join("intra_chain"))?;
    fs::create_dir(publish_root.join("cross_chain"))?;
    let mut rows = Vec::with_capacity(target);
    let mut selected = Vec::with_capacity(target);
    let mut intra_index = 0_usize;
    let mut cross_index = 0_usize;
    for (offset, (pool, staged)) in successful.into_iter().enumerate() {
        let index = offset + 1;
        let (pool_name, pool_index) = match pool {
            MetadataSamplePool::IntraChain => {
                intra_index += 1;
                ("intra_chain", intra_index)
            }
            MetadataSamplePool::CrossChain => {
                cross_index += 1;
                ("cross_chain", cross_index)
            }
        };
        let row_dir = publish_root.join(pool_name).join(pool_index.to_string());
        fs::create_dir(&row_dir)?;
        let destination_a = row_dir.join(format!("{pool_index}a{}", staged.suffix_a));
        let destination_b = row_dir.join(format!("{pool_index}b{}", staged.suffix_b));
        fs::rename(staged.file_a, &destination_a)?;
        fs::rename(staged.file_b, &destination_b)?;
        let file_name_a = destination_a
            .file_name()
            .expect("published image always has a file name")
            .to_string_lossy();
        let file_name_b = destination_b
            .file_name()
            .expect("published image always has a file name")
            .to_string_lossy();
        write_nft_record(
            &row_dir.join(format!("{pool_index}a.json")),
            NftSampleRecord {
                sample_pool: pool_name,
                chain: &staged.sample.contract_a_chain,
                contract_address: &staged.sample.contract_a_address,
                token_id: &staged.sample.token_id_a,
                image_uri: &staged.sample.image_uri_a,
                image_file: &file_name_a,
                metadata: serde_json::from_str(&staged.sample.metadata_json_a)?,
            },
        )?;
        write_nft_record(
            &row_dir.join(format!("{pool_index}b.json")),
            NftSampleRecord {
                sample_pool: pool_name,
                chain: &staged.sample.contract_b_chain,
                contract_address: &staged.sample.contract_b_address,
                token_id: &staged.sample.token_id_b,
                image_uri: &staged.sample.image_uri_b,
                image_file: &file_name_b,
                metadata: serde_json::from_str(&staged.sample.metadata_json_b)?,
            },
        )?;
        rows.push(DownloadRow {
            index,
            pool: pool_name,
            pool_index,
            file_a: relative_image_path(pool_name, pool_index, &destination_a),
            error_a: String::new(),
            file_b: relative_image_path(pool_name, pool_index, &destination_b),
            error_b: String::new(),
            record_a: format!("metadata_sample_images/{pool_name}/{pool_index}/{pool_index}a.json"),
            record_b: format!("metadata_sample_images/{pool_name}/{pool_index}/{pool_index}b.json"),
        });
        selected.push(staged.sample);
    }

    write_manifest(staging_root, &selected, &rows, sampling_seed)?;
    let pairs = metadata_sample_pair_reports(&selected);
    let report_files =
        write_duplicate_pair_sample_files(staging_root, "metadata_duplicate_pairs.csv", &pairs)?;
    let mut bundle = vec![
        "metadata_sample_images".to_owned(),
        "metadata_image_samples.csv".to_owned(),
    ];
    bundle.extend(report_files);
    commit_sample_bundle(output_dir, staging_root, &bundle)?;
    Ok(selected)
}

fn commit_sample_bundle(
    output_dir: &Path,
    staging_root: &Path,
    items: &[String],
) -> Result<(), std::io::Error> {
    commit_sample_bundle_with(output_dir, staging_root, items, |_, _| Ok(()))
}

fn commit_sample_bundle_with(
    output_dir: &Path,
    staging_root: &Path,
    items: &[String],
    mut before_install: impl FnMut(usize, &str) -> Result<(), std::io::Error>,
) -> Result<(), std::io::Error> {
    for item in items {
        validate_transaction_item(item)?;
        if !staging_root.join(item).exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("staged sample output is missing: {item}"),
            ));
        }
    }
    atomic_write(
        &staging_root.join(TRANSACTION_ITEMS),
        &serde_json::to_vec(items).map_err(std::io::Error::other)?,
    )?;
    atomic_write(
        &staging_root.join(TRANSACTION_STATE),
        TRANSACTION_COMMITTING,
    )?;
    let backup = staging_root.join(TRANSACTION_BACKUP);
    fs::create_dir(&backup)?;
    let mut backed_up = Vec::new();
    for item in items {
        let target = output_dir.join(item);
        if target.exists() {
            if let Err(error) = fs::rename(&target, backup.join(item)) {
                if let Err(rollback) =
                    restore_sample_bundle(output_dir, staging_root, &backup, &[], &backed_up)
                {
                    return Err(std::io::Error::other(format!(
                        "failed to back up previous sample output {item}: {error}; rollback failed: {rollback}; recovery transaction remains in {}",
                        staging_root.display()
                    )));
                }
                return Err(std::io::Error::other(format!(
                    "failed to back up previous sample output {item}: {error}"
                )));
            }
            backed_up.push(item.as_str());
        }
    }
    let mut installed = Vec::new();
    for (index, item) in items.iter().enumerate() {
        let install = before_install(index, item)
            .and_then(|()| fs::rename(staging_root.join(item), output_dir.join(item)));
        if let Err(error) = install {
            if let Err(rollback) =
                restore_sample_bundle(output_dir, staging_root, &backup, &installed, &backed_up)
            {
                return Err(std::io::Error::other(format!(
                    "failed to install sample output {item}: {error}; rollback failed: {rollback}; recovery transaction remains in {}",
                    staging_root.display()
                )));
            }
            return Err(std::io::Error::other(format!(
                "failed to install sample output {item}; previous sample set was restored: {error}"
            )));
        }
        installed.push(item.as_str());
    }
    atomic_write(&staging_root.join(TRANSACTION_STATE), TRANSACTION_COMMITTED)?;
    Ok(())
}

fn restore_sample_bundle(
    output_dir: &Path,
    staging_root: &Path,
    backup_dir: &Path,
    installed: &[&str],
    backed_up: &[&str],
) -> Result<(), std::io::Error> {
    for item in installed.iter().rev() {
        fs::rename(output_dir.join(item), staging_root.join(item))?;
    }
    for item in backed_up.iter().rev() {
        fs::rename(backup_dir.join(item), output_dir.join(item))?;
    }
    Ok(())
}

fn validate_transaction_item(item: &str) -> Result<(), std::io::Error> {
    let path = Path::new(item);
    if item.is_empty()
        || path.components().count() != 1
        || !matches!(
            path.components().next(),
            Some(std::path::Component::Normal(_))
        )
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("sample output must be a top-level file or directory name: {item}"),
        ));
    }
    Ok(())
}

fn recover_sample_transactions(output_dir: &Path) -> Result<(), std::io::Error> {
    for entry in fs::read_dir(output_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir()
            || !entry
                .file_name()
                .to_string_lossy()
                .starts_with(".metadata-fast-sample-")
        {
            continue;
        }
        let root = entry.path();
        let state_path = root.join(TRANSACTION_STATE);
        if !state_path.is_file() {
            continue;
        }
        let state = fs::read_to_string(&state_path)?;
        if state.trim() == "committed" {
            fs::remove_dir_all(root)?;
            continue;
        }
        if state.trim() != "committing" {
            return Err(std::io::Error::other(format!(
                "unknown sample transaction state in {}",
                state_path.display()
            )));
        }
        let items: Vec<String> = serde_json::from_slice(&fs::read(root.join(TRANSACTION_ITEMS))?)
            .map_err(std::io::Error::other)?;
        for item in &items {
            validate_transaction_item(item)?;
        }
        recover_sample_transaction(output_dir, &root, &items)?;
        fs::remove_dir_all(root)?;
    }
    Ok(())
}

fn recover_sample_transaction(
    output_dir: &Path,
    staging_root: &Path,
    items: &[String],
) -> Result<(), std::io::Error> {
    let backup = staging_root.join(TRANSACTION_BACKUP);
    for item in items.iter().rev() {
        let staged = staging_root.join(item);
        let published = output_dir.join(item);
        let previous = backup.join(item);
        if previous.exists() {
            remove_path(&published)?;
            fs::rename(previous, published)?;
        } else if !staged.exists() {
            remove_path(&published)?;
        }
    }
    Ok(())
}

fn remove_path(path: &Path) -> Result<(), std::io::Error> {
    if path.is_dir() {
        fs::remove_dir_all(path)
    } else if path.exists() {
        fs::remove_file(path)
    } else {
        Ok(())
    }
}

fn build_http_client() -> Result<Client, reqwest::Error> {
    Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .user_agent("dedup-metadata-image-sampler/1.0")
        .redirect(Policy::none())
        .no_proxy()
        .dns_resolver(Arc::new(PublicDnsResolver))
        .build()
}

fn relative_image_path(pool_name: &str, pool_index: usize, path: &Path) -> String {
    let file_name = path
        .file_name()
        .expect("published image always has a file name")
        .to_string_lossy();
    format!("metadata_sample_images/{pool_name}/{pool_index}/{file_name}")
}

#[cfg(test)]
fn download_image(
    client: &Client,
    image_uri: &str,
    progress: &dyn ProgressObserver,
) -> Result<DownloadedImage, std::io::Error> {
    let deadline = Instant::now() + TOTAL_IMAGE_TIMEOUT;
    let mut attempt = 0;
    loop {
        match download_image_attempt(client, image_uri, progress, deadline, attempt)? {
            ImageDownloadAttempt::Complete(image) => return Ok(image),
            ImageDownloadAttempt::RetryAfter(delay) => {
                cancellable_sleep(delay, progress, deadline)?;
                attempt += 1;
            }
        }
    }
}

fn download_image_attempt(
    client: &Client,
    image_uri: &str,
    progress: &dyn ProgressObserver,
    deadline: Instant,
    attempt: usize,
) -> Result<ImageDownloadAttempt, std::io::Error> {
    check_cancelled(progress)?;
    check_deadline(deadline)?;
    if let Some(data) = image_uri.trim().strip_prefix("data:") {
        return decode_data_image(data).map(ImageDownloadAttempt::Complete);
    }
    let url = normalize_image_uri(image_uri)?;
    match send_public_get(client, &url, progress, deadline) {
        Ok((response, final_url)) if response.status().is_success() => {
            if response
                .content_length()
                .is_some_and(|size| size > MAX_IMAGE_BYTES)
            {
                return Err(std::io::Error::other("image exceeds 50 MiB limit"));
            }
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_owned();
            let bytes = read_response_body(response, progress, deadline)?;
            if bytes.len() as u64 > MAX_IMAGE_BYTES {
                return Err(std::io::Error::other("image exceeds 50 MiB limit"));
            }
            let suffix = infer_image_suffix(&content_type, &final_url, &bytes)?;
            Ok(ImageDownloadAttempt::Complete(DownloadedImage {
                bytes,
                suffix,
            }))
        }
        Ok((response, _)) => {
            let status = response.status();
            let retryable = status.as_u16() == 429 || status.is_server_error();
            if retryable && attempt + 1 < MAX_ATTEMPTS {
                let retry_after = response
                    .headers()
                    .get(RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok());
                return Ok(ImageDownloadAttempt::RetryAfter(Duration::from_secs(
                    retry_after
                        .unwrap_or(1_u64 << attempt.min(2))
                        .min(MAX_RETRY_DELAY.as_secs()),
                )));
            }
            Err(std::io::Error::other(format!("HTTP {status}")))
        }
        Err(error) => {
            if error.kind() == std::io::ErrorKind::PermissionDenied
                || error.kind() == std::io::ErrorKind::InvalidInput
                || error.kind() == std::io::ErrorKind::Interrupted
            {
                return Err(error);
            }
            if attempt + 1 == MAX_ATTEMPTS {
                return Err(std::io::Error::other(error.to_string()));
            }
            Ok(ImageDownloadAttempt::RetryAfter(
                Duration::from_secs(1_u64 << attempt.min(2)).min(MAX_RETRY_DELAY),
            ))
        }
    }
}

fn send_public_get(
    client: &Client,
    initial_url: &str,
    progress: &dyn ProgressObserver,
    deadline: Instant,
) -> Result<(reqwest::blocking::Response, String), std::io::Error> {
    let mut current = Url::parse(initial_url)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    for redirect in 0..=MAX_REDIRECTS {
        check_cancelled(progress)?;
        let remaining = remaining_before(deadline)?;
        validate_request_url(&current)?;
        let response = client
            .get(current.clone())
            .timeout(remaining.min(REQUEST_TIMEOUT))
            .send()
            .map_err(std::io::Error::other)?;
        if !is_followed_redirect(response.status()) {
            return Ok((response, current.to_string()));
        }
        if redirect == MAX_REDIRECTS {
            return Err(std::io::Error::other("too many image redirects"));
        }
        let location = response
            .headers()
            .get(LOCATION)
            .ok_or_else(|| std::io::Error::other("image redirect has no Location header"))?
            .to_str()
            .map_err(|error| {
                std::io::Error::other(format!("invalid redirect Location: {error}"))
            })?;
        current = current.join(location).map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid image redirect: {error}"),
            )
        })?;
    }
    unreachable!("redirect loop returns or errors at its bound")
}

fn read_response_body(
    response: reqwest::blocking::Response,
    progress: &dyn ProgressObserver,
    deadline: Instant,
) -> Result<Vec<u8>, std::io::Error> {
    let mut reader = response.take(MAX_IMAGE_BYTES + 1);
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        check_cancelled(progress)?;
        check_deadline(deadline)?;
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    Ok(bytes)
}

#[cfg(test)]
fn cancellable_sleep(
    duration: Duration,
    progress: &dyn ProgressObserver,
    request_deadline: Instant,
) -> Result<(), std::io::Error> {
    let deadline = (Instant::now() + duration).min(request_deadline);
    loop {
        check_cancelled(progress)?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return check_deadline(request_deadline);
        }
        std::thread::sleep(remaining.min(Duration::from_millis(100)));
    }
}

fn remaining_before(deadline: Instant) -> Result<Duration, std::io::Error> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "image download exceeded total deadline",
        ))
    } else {
        Ok(remaining)
    }
}

fn check_deadline(deadline: Instant) -> Result<(), std::io::Error> {
    remaining_before(deadline).map(|_| ())
}

fn check_cancelled(progress: &dyn ProgressObserver) -> Result<(), std::io::Error> {
    progress
        .check_cancelled()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::Interrupted, error.to_string()))
}

fn is_followed_redirect(status: StatusCode) -> bool {
    matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308)
}

fn validate_request_url(url: &Url) -> Result<(), std::io::Error> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "image URL must use HTTP or HTTPS",
        ));
    }
    let host = url.host().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "image URL has no host")
    })?;
    let allowed = match host {
        url::Host::Domain(_) => true,
        url::Host::Ipv4(address) => is_public_ipv4(address),
        url::Host::Ipv6(address) => is_public_ipv6(address),
    };
    if allowed {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "image URL resolves to a non-public network address",
        ))
    }
}

fn resolve_public_host(host: &str) -> Result<Vec<SocketAddr>, std::io::Error> {
    let addresses = (host, 0).to_socket_addrs()?.collect::<Vec<_>>();
    let public = addresses
        .into_iter()
        .filter(|address| is_public_ip(address.ip()))
        .collect::<Vec<_>>();
    if public.is_empty() {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("image host {host:?} has no public network address"),
        ))
    } else {
        Ok(public)
    }
}

fn is_public_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    !(a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2)
        || (a == 192 && b == 88 && c == 99)
        || (a == 192 && b == 168)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 224)
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    if let Some(mapped) = address.to_ipv4_mapped() {
        return is_public_ipv4(mapped);
    }
    let segments = address.segments();
    !(address.is_unspecified()
        || address.is_loopback()
        || address.is_multicast()
        || segments[0] & 0xe000 != 0x2000
        || segments[0] & 0xfe00 == 0xfc00
        || segments[0] & 0xffc0 == 0xfe80
        || segments[0] & 0xffc0 == 0xfec0
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || (segments[0] == 0x2001 && segments[1] == 0)
        || segments[0] == 0x2002
        || (segments[0] == 0x0064 && segments[1] == 0xff9b))
}

fn decode_data_image(data: &str) -> Result<DownloadedImage, std::io::Error> {
    let (metadata, payload) = data
        .split_once(',')
        .ok_or_else(|| std::io::Error::other("invalid data image URI"))?;
    let mime = metadata.split(';').next().unwrap_or("");
    if !metadata.split(';').any(|part| part == "base64") {
        return Err(std::io::Error::other(
            "only base64 data image URIs are supported",
        ));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .map_err(|error| std::io::Error::other(format!("invalid base64 image: {error}")))?;
    if bytes.len() as u64 > MAX_IMAGE_BYTES {
        return Err(std::io::Error::other("image exceeds 50 MiB limit"));
    }
    let suffix = infer_image_suffix(mime, "", &bytes)?;
    Ok(DownloadedImage { bytes, suffix })
}

fn normalize_image_uri(uri: &str) -> Result<String, std::io::Error> {
    let uri = uri.trim();
    let lowered = uri.to_ascii_lowercase();
    if lowered.starts_with("ipfs:") {
        let path = uri["ipfs:".len()..].trim_start_matches('/');
        let path = if path
            .get(.."ipfs/".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ipfs/"))
        {
            &path["ipfs/".len()..]
        } else {
            path
        };
        if path.is_empty() {
            return Err(std::io::Error::other("empty IPFS image URI"));
        }
        return Ok(format!(
            "https://ipfs.io/ipfs/{}",
            path.trim_start_matches('/')
        ));
    }
    if lowered.starts_with("ar:") {
        let path = uri["ar:".len()..].trim_start_matches('/');
        if path.is_empty() {
            return Err(std::io::Error::other("empty Arweave image URI"));
        }
        return Ok(format!(
            "https://arweave.net/{}",
            path.trim_start_matches('/')
        ));
    }
    if lowered.starts_with("http://") || lowered.starts_with("https://") {
        return Ok(uri.to_owned());
    }
    Err(std::io::Error::other("unsupported image URI scheme"))
}

fn infer_image_suffix(
    content_type: &str,
    url: &str,
    bytes: &[u8],
) -> Result<&'static str, std::io::Error> {
    let mime = content_type.split(';').next().unwrap_or("").trim();
    if let Some(suffix) = suffix_for_mime(mime) {
        return Ok(suffix);
    }
    let path = url
        .split(['?', '#'])
        .next()
        .unwrap_or(url)
        .to_ascii_lowercase();
    for suffix in [
        ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".avif", ".bmp", ".tif", ".tiff", ".ico",
    ] {
        if path.ends_with(suffix) {
            return Ok(match suffix {
                ".jpeg" => ".jpg",
                ".tif" => ".tiff",
                _ => suffix,
            });
        }
    }
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Ok(".png");
    }
    if bytes.starts_with(b"\xff\xd8\xff") {
        return Ok(".jpg");
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Ok(".gif");
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        return Ok(".webp");
    }
    if bytes.get(..1024.min(bytes.len())).is_some_and(|prefix| {
        String::from_utf8_lossy(prefix)
            .to_ascii_lowercase()
            .contains("<svg")
    }) {
        return Ok(".svg");
    }
    Err(std::io::Error::other("response is not a recognized image"))
}

fn suffix_for_mime(mime: &str) -> Option<&'static str> {
    match mime {
        "image/png" => Some(".png"),
        "image/jpeg" => Some(".jpg"),
        "image/gif" => Some(".gif"),
        "image/webp" => Some(".webp"),
        "image/svg+xml" => Some(".svg"),
        "image/avif" => Some(".avif"),
        "image/bmp" => Some(".bmp"),
        "image/tiff" => Some(".tiff"),
        "image/x-icon" => Some(".ico"),
        _ => None,
    }
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("image path has no parent"))?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn write_nft_record(
    path: &Path,
    record: NftSampleRecord<'_>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bytes = serde_json::to_vec_pretty(&record)?;
    atomic_write(path, &bytes)?;
    Ok(())
}

fn write_manifest(
    output_dir: &Path,
    samples: &[MetadataImagePairSample],
    rows: &[DownloadRow],
    sampling_seed: Option<u64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path = output_dir.join("metadata_image_samples.csv");
    let mut temporary = NamedTempFile::new_in(output_dir)?;
    {
        let mut writer = csv::Writer::from_writer(&mut temporary);
        writer.write_record([
            "row",
            "sampling_seed",
            "pool",
            "pool_row",
            "contract_a_chain",
            "contract_a_address",
            "token_id_a",
            "image_uri_a",
            "file_a",
            "error_a",
            "contract_b_chain",
            "contract_b_address",
            "token_id_b",
            "image_uri_b",
            "file_b",
            "error_b",
            "record_a",
            "record_b",
        ])?;
        for (sample, row) in samples.iter().zip(rows) {
            writer.write_record([
                row.index.to_string(),
                sampling_seed
                    .map(|seed| seed.to_string())
                    .unwrap_or_default(),
                row.pool.to_owned(),
                row.pool_index.to_string(),
                sample.contract_a_chain.clone(),
                sample.contract_a_address.clone(),
                sample.token_id_a.clone(),
                sample.image_uri_a.clone(),
                row.file_a.clone(),
                row.error_a.clone(),
                sample.contract_b_chain.clone(),
                sample.contract_b_address.clone(),
                sample.token_id_b.clone(),
                sample.image_uri_b.clone(),
                row.file_b.clone(),
                row.error_b.clone(),
                row.record_a.clone(),
                row.record_b.clone(),
            ])?;
        }
        writer.flush()?;
    }
    temporary.persist(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dedup_core::{DedupError, NoopProgress};

    struct CancelledProgress;

    impl ProgressObserver for CancelledProgress {
        fn set_stage(&self, _stage: &str) {}
        fn begin_phase(&self, _phase: &str, _total: Option<u64>) {}
        fn set_total(&self, _total: Option<u64>) {}
        fn add_completed(&self, _delta: u64) {}
        fn check_cancelled(&self) -> Result<(), DedupError> {
            Err(DedupError::Interrupted)
        }
    }

    fn sample(address: &str, image_uri: &str) -> MetadataImagePairSample {
        MetadataImagePairSample {
            contract_a_chain: "ethereum".to_owned(),
            contract_a_address: address.to_owned(),
            token_id_a: "1".to_owned(),
            image_uri_a: image_uri.to_owned(),
            metadata_json_a: r#"{"name":"left"}"#.to_owned(),
            contract_b_chain: "ethereum".to_owned(),
            contract_b_address: format!("{address}-peer"),
            token_id_b: "2".to_owned(),
            image_uri_b: image_uri.to_owned(),
            metadata_json_b: r#"{"name":"right"}"#.to_owned(),
        }
    }

    #[test]
    fn normalizes_decentralized_uris() {
        assert_eq!(
            normalize_image_uri("ipfs://ipfs/QmHash/image.png").unwrap(),
            "https://ipfs.io/ipfs/QmHash/image.png"
        );
        assert_eq!(
            normalize_image_uri("ar://transaction").unwrap(),
            "https://arweave.net/transaction"
        );
        assert_eq!(
            normalize_image_uri("ipfs:QmNormalized/image.png").unwrap(),
            "https://ipfs.io/ipfs/QmNormalized/image.png"
        );
        assert_eq!(
            normalize_image_uri("ar:normalized-transaction/image.png").unwrap(),
            "https://arweave.net/normalized-transaction/image.png"
        );
    }

    #[test]
    fn infers_image_types_without_transcoding() {
        assert_eq!(
            infer_image_suffix("image/jpeg", "https://x/a", b"raw").unwrap(),
            ".jpg"
        );
        assert_eq!(
            infer_image_suffix("", "https://x/a", b"\x89PNG\r\n\x1a\nrest").unwrap(),
            ".png"
        );
    }

    #[test]
    fn decodes_original_data_image_bytes() {
        let expected = b"\x89PNG\r\n\x1a\n".to_vec();
        let client = build_http_client().unwrap();
        let image =
            download_image(&client, "data:image/png;base64,iVBORw0KGgo=", &NoopProgress).unwrap();
        assert_eq!(image.suffix, ".png");
        assert_eq!(image.bytes, expected);
    }

    #[test]
    fn delayed_retry_releases_worker_and_is_requeued() {
        let output = tempfile::tempdir().unwrap();
        let mut session = StreamingDownloadSession::new(
            output.path(),
            1,
            Arc::new(NoopProgress) as Arc<dyn ProgressObserver>,
        )
        .unwrap();
        let uri = "data:image/png;base64,iVBORw0KGgo=".to_owned();
        session
            .uri_states
            .insert(uri.clone(), UriState::Loading(Vec::new()));
        session
            .process_worker_result(ImageJobResult {
                uri: uri.clone(),
                outcome: ImageJobOutcome::Retry {
                    job: ImageJob {
                        cache_key: image_cache_key(&uri),
                        uri: uri.clone(),
                        attempt: 1,
                        deadline: Some(Instant::now() + Duration::from_secs(5)),
                    },
                    ready_at: Instant::now(),
                },
            })
            .unwrap();

        assert_eq!(session.delayed_retries.len(), 1);
        while !matches!(session.uri_states.get(&uri), Some(UriState::Ready(_))) {
            assert!(session.receive_worker_result(true).unwrap());
        }
        assert!(session.delayed_retries.is_empty());
    }

    #[test]
    fn a_full_network_queue_does_not_block_candidate_submission() {
        let output = tempfile::tempdir().unwrap();
        let mut session = StreamingDownloadSession::new(
            output.path(),
            1,
            Arc::new(NoopProgress) as Arc<dyn ProgressObserver>,
        )
        .unwrap();
        let (blocked_tx, _blocked_rx) = mpsc::sync_channel(0);
        session.job_tx = Some(blocked_tx);
        session.unsent_jobs.push_back(ImageJob {
            uri: "data:image/png;base64,iVBORw0KGgo=".to_owned(),
            cache_key: "queued".to_owned(),
            attempt: 0,
            deadline: None,
        });

        let started = Instant::now();
        session.pump_jobs().unwrap();

        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(session.unsent_jobs.len(), 1);
    }

    #[test]
    fn completed_uri_state_is_bounded() {
        let output = tempfile::tempdir().unwrap();
        let mut session = StreamingDownloadSession::new(
            output.path(),
            1,
            Arc::new(NoopProgress) as Arc<dyn ProgressObserver>,
        )
        .unwrap();
        for index in 0..=READY_URI_CACHE_CAPACITY {
            session.remember_ready_uri(
                format!("uri-{index}"),
                CachedImage {
                    file: output.path().join(format!("cached-{index}")),
                    suffix: ".png",
                },
            );
        }

        assert_eq!(session.ready_uri_order.len(), READY_URI_CACHE_CAPACITY);
        assert_eq!(session.uri_states.len(), READY_URI_CACHE_CAPACITY);
        assert!(!session.uri_states.contains_key("uri-0"));
        assert!(
            session
                .uri_states
                .contains_key(&format!("uri-{READY_URI_CACHE_CAPACITY}"))
        );
    }

    #[test]
    fn rejects_non_public_literal_and_resolved_hosts() {
        for url in [
            "http://127.0.0.1/image.png",
            "http://10.0.0.1/image.png",
            "http://169.254.169.254/latest/meta-data",
            "http://[::1]/image.png",
            "http://[::127.0.0.1]/image.png",
            "http://[::ffff:127.0.0.1]/image.png",
            "http://[fe80::1]/image.png",
            "http://[fc00::1]/image.png",
        ] {
            let parsed = Url::parse(url).unwrap();
            assert_eq!(
                validate_request_url(&parsed).unwrap_err().kind(),
                std::io::ErrorKind::PermissionDenied,
                "unsafe URL was accepted: {url}"
            );
        }
        assert_eq!(
            resolve_public_host("localhost").unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        validate_request_url(&Url::parse("https://8.8.8.8/image.png").unwrap()).unwrap();

        let client = build_http_client().unwrap();
        assert_eq!(
            send_public_get(
                &client,
                "http://127.0.0.1/image.png",
                &NoopProgress,
                Instant::now() + TOTAL_IMAGE_TIMEOUT,
            )
            .unwrap_err()
            .kind(),
            std::io::ErrorKind::PermissionDenied
        );
        let redirect = Url::parse("https://example.com/image.png")
            .unwrap()
            .join("http://169.254.169.254/latest/meta-data")
            .unwrap();
        assert_eq!(
            validate_request_url(&redirect).unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn failed_pair_is_discarded_and_replaced_by_a_complete_pair() {
        let temp = tempfile::tempdir().unwrap();
        let good = "data:image/png;base64,iVBORw0KGgo=";
        let candidates = [
            sample("0xfail", "unsupported://image"),
            sample("0xok", good),
        ];
        let mut session =
            StreamingDownloadSession::new(temp.path(), 1, Arc::new(NoopProgress)).unwrap();
        let batch = candidates
            .into_iter()
            .map(|sample| (MetadataSamplePool::IntraChain, sample))
            .collect::<Vec<_>>();
        assert_eq!(
            session.try_batch(&batch, &NoopProgress).unwrap(),
            vec![false, true]
        );
        assert!(session.summary().contains("network_uris=2"));
        assert!(session.summary().contains("coalesced_uris=2"));
        assert_eq!(
            fs::read_dir(temp.path().join(IMAGE_CACHE_DIR))
                .unwrap()
                .count(),
            2
        );
        session.set_sampling_seed(42);
        let selected = session.finish().unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].contract_a_address, "0xok");
        assert!(
            temp.path()
                .join("metadata_sample_images/intra_chain/1/1a.png")
                .is_file()
        );
        assert!(
            temp.path()
                .join("metadata_sample_images/intra_chain/1/1b.png")
                .is_file()
        );
        let nft_a: serde_json::Value = serde_json::from_slice(
            &fs::read(
                temp.path()
                    .join("metadata_sample_images/intra_chain/1/1a.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let nft_b: serde_json::Value = serde_json::from_slice(
            &fs::read(
                temp.path()
                    .join("metadata_sample_images/intra_chain/1/1b.json"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(nft_a["contract_address"], "0xok");
        assert_eq!(nft_a["sample_pool"], "intra_chain");
        assert_eq!(nft_a["token_id"], "1");
        assert_eq!(nft_a["image_file"], "1a.png");
        assert_eq!(nft_a["metadata"]["name"], "left");
        assert_eq!(nft_b["contract_address"], "0xok-peer");
        assert_eq!(nft_b["sample_pool"], "intra_chain");
        assert_eq!(nft_b["token_id"], "2");
        assert_eq!(nft_b["image_file"], "1b.png");
        assert_eq!(nft_b["metadata"]["name"], "right");
        let mut manifest =
            csv::Reader::from_path(temp.path().join("metadata_image_samples.csv")).unwrap();
        assert!(
            manifest
                .headers()
                .unwrap()
                .iter()
                .any(|field| field == "record_a")
        );
        assert!(
            manifest
                .headers()
                .unwrap()
                .iter()
                .any(|field| field == "record_b")
        );
        let seed_column = manifest
            .headers()
            .unwrap()
            .iter()
            .position(|field| field == "sampling_seed")
            .unwrap();
        let records = manifest.records().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(&records[0][seed_column], "42");
    }

    #[test]
    fn only_random_order_winners_are_published_after_prefetch() {
        let temp = tempfile::tempdir().unwrap();
        let good = "data:image/png;base64,iVBORw0KGgo=";
        let candidates = [sample("0xfirst", good), sample("0xselected", good)];
        let mut session =
            StreamingDownloadSession::new(temp.path(), 1, Arc::new(NoopProgress)).unwrap();
        let batch = candidates
            .into_iter()
            .map(|sample| (MetadataSamplePool::IntraChain, sample))
            .collect::<Vec<_>>();
        assert_eq!(
            session.try_batch(&batch, &NoopProgress).unwrap(),
            vec![true, true]
        );

        session.retain_candidates(&[2]).unwrap();
        let selected = session.finish().unwrap();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].contract_a_address, "0xselected");
    }

    #[test]
    fn insufficient_candidates_do_not_publish_partial_output() {
        let temp = tempfile::tempdir().unwrap();
        let candidate = sample("0xok", "data:image/png;base64,iVBORw0KGgo=");
        let mut session =
            StreamingDownloadSession::new(temp.path(), 2, Arc::new(NoopProgress)).unwrap();
        assert_eq!(
            session
                .try_batch(
                    &[(MetadataSamplePool::IntraChain, candidate)],
                    &NoopProgress
                )
                .unwrap(),
            vec![true]
        );
        drop(session);
        assert!(!temp.path().join("metadata_image_samples.csv").exists());
        assert!(!temp.path().join("metadata_sample_images").exists());
    }

    #[test]
    fn sample_bundle_install_failure_restores_every_previous_output() {
        let output = tempfile::tempdir().unwrap();
        let staging = tempfile::tempdir_in(output.path()).unwrap();
        let items = vec![
            "metadata_sample_images".to_owned(),
            "metadata_image_samples.csv".to_owned(),
            "metadata_duplicate_pairs.csv".to_owned(),
        ];
        fs::create_dir(output.path().join(&items[0])).unwrap();
        fs::write(output.path().join(&items[0]).join("old.bin"), b"old-media").unwrap();
        fs::write(output.path().join(&items[1]), b"old-manifest").unwrap();
        fs::write(output.path().join(&items[2]), b"old-pairs").unwrap();
        fs::create_dir(staging.path().join(&items[0])).unwrap();
        fs::write(staging.path().join(&items[0]).join("new.bin"), b"new-media").unwrap();
        fs::write(staging.path().join(&items[1]), b"new-manifest").unwrap();
        fs::write(staging.path().join(&items[2]), b"new-pairs").unwrap();

        let error = commit_sample_bundle_with(output.path(), staging.path(), &items, |index, _| {
            if index == 2 {
                Err(std::io::Error::other("injected install failure"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("previous sample set was restored")
        );
        assert_eq!(
            fs::read(output.path().join(&items[0]).join("old.bin")).unwrap(),
            b"old-media"
        );
        assert_eq!(
            fs::read(output.path().join(&items[1])).unwrap(),
            b"old-manifest"
        );
        assert_eq!(
            fs::read(output.path().join(&items[2])).unwrap(),
            b"old-pairs"
        );
        assert!(!output.path().join(&items[0]).join("new.bin").exists());
    }

    #[test]
    fn next_session_recovers_a_crashed_partial_sample_commit() {
        let output = tempfile::tempdir().unwrap();
        let transaction = tempfile::Builder::new()
            .prefix(".metadata-fast-sample-")
            .tempdir_in(output.path())
            .unwrap();
        let items = vec![
            "metadata_sample_images".to_owned(),
            "metadata_image_samples.csv".to_owned(),
            "metadata_duplicate_pairs.csv".to_owned(),
        ];
        fs::create_dir(output.path().join(&items[0])).unwrap();
        fs::write(output.path().join(&items[0]).join("old.bin"), b"old-media").unwrap();
        fs::write(output.path().join(&items[1]), b"old-manifest").unwrap();
        fs::create_dir(transaction.path().join(&items[0])).unwrap();
        fs::write(
            transaction.path().join(&items[0]).join("new.bin"),
            b"new-media",
        )
        .unwrap();
        fs::write(transaction.path().join(&items[1]), b"new-manifest").unwrap();
        fs::write(transaction.path().join(&items[2]), b"new-pairs").unwrap();
        atomic_write(
            &transaction.path().join(TRANSACTION_ITEMS),
            &serde_json::to_vec(&items).unwrap(),
        )
        .unwrap();
        atomic_write(
            &transaction.path().join(TRANSACTION_STATE),
            TRANSACTION_COMMITTING,
        )
        .unwrap();
        let backup = transaction.path().join(TRANSACTION_BACKUP);
        fs::create_dir(&backup).unwrap();
        fs::rename(output.path().join(&items[0]), backup.join(&items[0])).unwrap();
        fs::rename(output.path().join(&items[1]), backup.join(&items[1])).unwrap();
        for item in &items {
            fs::rename(transaction.path().join(item), output.path().join(item)).unwrap();
        }
        let transaction_path = transaction.keep();

        recover_sample_transactions(output.path()).unwrap();

        assert!(!transaction_path.exists());
        assert_eq!(
            fs::read(output.path().join(&items[0]).join("old.bin")).unwrap(),
            b"old-media"
        );
        assert_eq!(
            fs::read(output.path().join(&items[1])).unwrap(),
            b"old-manifest"
        );
        assert!(!output.path().join(&items[2]).exists());
    }

    #[test]
    fn cancellation_stops_before_starting_candidate_downloads() {
        let temp = tempfile::tempdir().unwrap();
        let candidate = sample("0xcancelled", "data:image/png;base64,iVBORw0KGgo=");
        let mut session =
            StreamingDownloadSession::new(temp.path(), 1, Arc::new(CancelledProgress)).unwrap();
        let error = match session.try_batch(
            &[(MetadataSamplePool::IntraChain, candidate)],
            &CancelledProgress,
        ) {
            Ok(_) => panic!("cancelled download unexpectedly completed"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
        assert!(!temp.path().join("metadata_image_samples.csv").exists());
        assert!(!temp.path().join("metadata_sample_images").exists());
    }
}
