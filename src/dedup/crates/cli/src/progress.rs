use clap::ValueEnum;
use dedup_core::{DedupError, EwmaEta, ProgressObserver};
use serde::Serialize;
use std::io::{self, IsTerminal, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const EWMA_ALPHA: f64 = 0.25;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum ProgressMode {
    Auto,
    Tty,
    Json,
    Off,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EffectiveMode {
    Tty,
    Json,
    Off,
}

pub struct ProgressReporter {
    shared: Arc<Shared>,
    worker: Option<JoinHandle<()>>,
}

impl Clone for ProgressReporter {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            worker: None,
        }
    }
}

struct Shared {
    meta: Mutex<Meta>,
    completed: ProgressCounter,
    activity: ProgressCounter,
    stopping: AtomicBool,
    cancelled: AtomicBool,
    wake_lock: Mutex<()>,
    wake: Condvar,
    mode: EffectiveMode,
    interval: Duration,
}

#[repr(align(64))]
struct ProgressCounter(AtomicU64);

impl ProgressCounter {
    fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    fn load(&self, order: Ordering) -> u64 {
        self.0.load(order)
    }

    fn store(&self, value: u64, order: Ordering) {
        self.0.store(value, order);
    }

    fn add(&self, delta: u64) {
        self.0.fetch_add(delta, Ordering::Relaxed);
    }
}

struct Meta {
    stage: String,
    phase: String,
    total: Option<u64>,
    stage_started: Instant,
    phase_started: Instant,
    last_completed: u64,
    last_activity: u64,
    last_tick: Instant,
    eta: EwmaEta,
    activity_rate: EwmaEta,
    activity_label: Option<String>,
    detail: Option<String>,
    phase_history: Vec<PhaseTimingSnapshot>,
}

#[derive(Clone, Debug)]
pub struct PhaseTimingSnapshot {
    pub stage: String,
    pub phase: String,
    pub elapsed: Duration,
}

#[derive(Serialize)]
struct ProgressLine {
    stage: String,
    phase: String,
    completed: u64,
    total: Option<u64>,
    percent: Option<f64>,
    rate: Option<f64>,
    activity: u64,
    activity_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    activity_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    active: bool,
    eta_secs: Option<f64>,
    eta_confident: bool,
    eta_status: &'static str,
    phase_elapsed_secs: f64,
    stage_elapsed_secs: f64,
}

impl ProgressReporter {
    pub fn start(mode: ProgressMode, interval_ms: u64) -> Self {
        let effective = match mode {
            ProgressMode::Off => EffectiveMode::Off,
            ProgressMode::Tty => EffectiveMode::Tty,
            ProgressMode::Json => EffectiveMode::Json,
            ProgressMode::Auto => {
                if io::stderr().is_terminal() {
                    EffectiveMode::Tty
                } else {
                    EffectiveMode::Json
                }
            }
        };
        let now = Instant::now();
        let shared = Arc::new(Shared {
            meta: Mutex::new(Meta {
                stage: "idle".to_owned(),
                phase: String::new(),
                total: None,
                stage_started: now,
                phase_started: now,
                last_completed: 0,
                last_activity: 0,
                last_tick: now,
                eta: EwmaEta::new(EWMA_ALPHA),
                activity_rate: EwmaEta::new(EWMA_ALPHA),
                activity_label: None,
                detail: None,
                phase_history: Vec::new(),
            }),
            completed: ProgressCounter::new(),
            activity: ProgressCounter::new(),
            stopping: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            wake_lock: Mutex::new(()),
            wake: Condvar::new(),
            mode: effective,
            interval: Duration::from_millis(interval_ms.max(100)),
        });
        let worker = if effective == EffectiveMode::Off {
            None
        } else {
            let shared_worker = Arc::clone(&shared);
            Some(thread::spawn(move || reporter_loop(shared_worker)))
        };
        Self { shared, worker }
    }

    pub fn cancel_handle(&self) -> CancelHandle {
        CancelHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    pub fn finish(&mut self) {
        self.shared.stopping.store(true, Ordering::SeqCst);
        self.shared.wake.notify_all();
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
        self.emit_now();
        if self.shared.mode == EffectiveMode::Tty {
            let _ = writeln!(io::stderr());
        }
    }

    fn emit_now(&self) {
        emit_snapshot(&self.shared);
    }

    pub fn phase_timings(&self) -> Vec<PhaseTimingSnapshot> {
        let meta = self.shared.meta.lock().expect("progress lock");
        let mut timings = meta.phase_history.clone();
        if !meta.phase.is_empty() {
            timings.push(PhaseTimingSnapshot {
                stage: meta.stage.clone(),
                phase: meta.phase.clone(),
                elapsed: meta.phase_started.elapsed(),
            });
        }
        timings
    }
}

#[derive(Clone)]
pub struct CancelHandle {
    shared: Arc<Shared>,
}

impl CancelHandle {
    pub fn request_cancel(&self) {
        self.shared.cancelled.store(true, Ordering::SeqCst);
    }
}

impl ProgressObserver for ProgressReporter {
    fn set_stage(&self, stage: &str) {
        let mut meta = self.shared.meta.lock().expect("progress lock");
        let now = Instant::now();
        record_current_phase(&mut meta, now);
        meta.stage = stage.to_owned();
        meta.phase = String::new();
        meta.total = None;
        meta.stage_started = now;
        meta.phase_started = now;
        meta.last_completed = 0;
        meta.last_activity = 0;
        meta.last_tick = now;
        meta.eta = EwmaEta::new(EWMA_ALPHA);
        meta.activity_rate = EwmaEta::new(EWMA_ALPHA);
        meta.activity_label = None;
        meta.detail = None;
        self.shared.completed.store(0, Ordering::Relaxed);
        self.shared.activity.store(0, Ordering::Relaxed);
    }

    fn begin_phase(&self, phase: &str, total: Option<u64>) {
        let mut meta = self.shared.meta.lock().expect("progress lock");
        let now = Instant::now();
        record_current_phase(&mut meta, now);
        meta.phase = phase.to_owned();
        meta.total = total;
        meta.phase_started = now;
        meta.last_completed = 0;
        meta.last_activity = 0;
        meta.last_tick = now;
        meta.eta = EwmaEta::new(EWMA_ALPHA);
        meta.activity_rate = EwmaEta::new(EWMA_ALPHA);
        meta.activity_label = None;
        meta.detail = None;
        self.shared.completed.store(0, Ordering::Relaxed);
        self.shared.activity.store(0, Ordering::Relaxed);
    }

    fn set_total(&self, total: Option<u64>) {
        self.shared.meta.lock().expect("progress lock").total = total;
    }

    fn add_completed(&self, delta: u64) {
        if self.shared.mode != EffectiveMode::Off {
            self.shared.completed.add(delta);
        }
    }

    fn add_activity(&self, delta: u64) {
        if self.shared.mode != EffectiveMode::Off {
            self.shared.activity.add(delta);
        }
    }

    fn set_activity_label(&self, label: &str) {
        self.shared
            .meta
            .lock()
            .expect("progress lock")
            .activity_label = Some(label.to_owned());
    }

    fn set_detail(&self, detail: &str) {
        self.shared.meta.lock().expect("progress lock").detail =
            (!detail.is_empty()).then(|| detail.to_owned());
    }

    fn check_cancelled(&self) -> Result<(), DedupError> {
        if self.shared.cancelled.load(Ordering::SeqCst) {
            Err(DedupError::Interrupted)
        } else {
            Ok(())
        }
    }
}

fn record_current_phase(meta: &mut Meta, now: Instant) {
    if meta.phase.is_empty() {
        return;
    }
    meta.phase_history.push(PhaseTimingSnapshot {
        stage: meta.stage.clone(),
        phase: meta.phase.clone(),
        elapsed: now.duration_since(meta.phase_started),
    });
}

fn reporter_loop(shared: Arc<Shared>) {
    let Ok(mut guard) = shared.wake_lock.lock() else {
        return;
    };
    loop {
        if shared.stopping.load(Ordering::SeqCst) {
            break;
        }
        let Ok((next_guard, timeout)) = shared.wake.wait_timeout(guard, shared.interval) else {
            return;
        };
        guard = next_guard;
        if shared.stopping.load(Ordering::SeqCst) {
            break;
        }
        if !timeout.timed_out() {
            continue;
        }
        drop(guard);
        emit_snapshot(&shared);
        let Ok(next_guard) = shared.wake_lock.lock() else {
            return;
        };
        guard = next_guard;
    }
}

fn emit_snapshot(shared: &Shared) {
    if shared.mode == EffectiveMode::Off {
        return;
    }
    let mut meta = shared.meta.lock().expect("progress lock");
    // Read the phase metadata and its resettable counter under the same phase lock.
    // Workers still update the counter lock-free, while phase changes cannot pair a
    // new phase label with the previous phase's completed value.
    let completed = shared.completed.load(Ordering::Acquire);
    let activity = shared.activity.load(Ordering::Acquire);
    let now = Instant::now();
    let dt = now.duration_since(meta.last_tick).as_secs_f64().max(1e-6);
    let delta = completed.saturating_sub(meta.last_completed);
    let instant_rate = delta as f64 / dt;
    meta.eta.observe(instant_rate);
    let activity_delta = activity.saturating_sub(meta.last_activity);
    meta.activity_rate.observe(activity_delta as f64 / dt);
    meta.last_completed = completed;
    meta.last_activity = activity;
    meta.last_tick = now;

    let phase_elapsed_secs = meta.phase_started.elapsed().as_secs_f64().max(1e-6);
    let remaining = meta.total.map(|total| total.saturating_sub(completed));
    let percent = meta
        .total
        .and_then(|t| (t > 0).then_some(100.0 * completed as f64 / t as f64));
    // A positive-only EWMA is responsive while profiles complete, but would
    // otherwise leave a stale optimistic ETA during one unusually heavy profile.
    // Fall back to whole-phase throughput on zero-completion ticks so elapsed
    // time continues to influence both the displayed rate and ETA.
    let rate = effective_progress_rate(completed, delta, phase_elapsed_secs, meta.eta.rate());
    let eta_secs = remaining.and_then(|remaining| {
        rate.filter(|rate| *rate > 0.0)
            .map(|rate| remaining as f64 / rate)
    });
    let active = delta != 0 || activity_delta != 0;
    let unfinished = meta.total.is_none_or(|total| completed < total);
    let eta_status =
        progress_eta_status(eta_secs, meta.eta.confident(), active, unfinished, activity);
    let line = ProgressLine {
        stage: meta.stage.clone(),
        phase: meta.phase.clone(),
        completed,
        total: meta.total,
        percent,
        rate,
        activity,
        activity_rate: (activity_delta != 0)
            .then(|| meta.activity_rate.rate())
            .flatten(),
        activity_label: meta.activity_label.clone(),
        detail: meta.detail.clone(),
        active,
        eta_secs,
        eta_confident: meta.eta.confident(),
        eta_status,
        phase_elapsed_secs,
        stage_elapsed_secs: meta.stage_started.elapsed().as_secs_f64(),
    };
    drop(meta);

    match shared.mode {
        EffectiveMode::Json => {
            if let Ok(json) = serde_json::to_string(&line) {
                let _ = writeln!(io::stderr(), "{json}");
            }
        }
        EffectiveMode::Tty => {
            let label = if line.phase.is_empty() {
                line.stage.clone()
            } else {
                format!("{}/{}", line.stage, line.phase)
            };
            let progress = match line.total {
                Some(t) => format!("{}/{}", line.completed, t),
                None => format!("{} done", line.completed),
            };
            let pct = line
                .percent
                .map(|p| format!("{p:.1}%"))
                .unwrap_or_else(|| "--".to_owned());
            let rate = line
                .rate
                .map(|r| format!("{r:.0}/s"))
                .unwrap_or_else(|| "-/s".to_owned());
            let activity = if line.activity == 0 {
                String::new()
            } else {
                let activity_rate = line
                    .activity_rate
                    .map(|rate| format!(" {rate:.0}/s"))
                    .unwrap_or_default();
                format!(
                    " {}={}{}",
                    line.activity_label.as_deref().unwrap_or("candidates"),
                    line.activity,
                    activity_rate
                )
            };
            let detail = line
                .detail
                .as_deref()
                .map(|detail| format!(" {detail}"))
                .unwrap_or_default();
            let elapsed = format_duration(line.phase_elapsed_secs);
            let eta = match line.total {
                None => "n/a".to_owned(),
                Some(_) => match (line.eta_secs, line.eta_confident) {
                    (Some(secs), true) => format_duration(secs),
                    (Some(secs), false) => format!("~{}", format_duration(secs)),
                    (None, _) if line.eta_status == "working" => "warming".to_owned(),
                    (None, _) => "...".to_owned(),
                },
            };
            let _ = write!(
                io::stderr(),
                "\r[{label}] {progress} {pct} {rate}{activity}{detail} elapsed={elapsed} eta={eta}\x1b[K"
            );
            let _ = io::stderr().flush();
        }
        EffectiveMode::Off => {}
    }
}

fn effective_progress_rate(
    completed: u64,
    delta: u64,
    phase_elapsed_secs: f64,
    ewma_rate: Option<f64>,
) -> Option<f64> {
    let cumulative_rate = (completed != 0 && phase_elapsed_secs > 0.0)
        .then_some(completed as f64 / phase_elapsed_secs);
    if delta == 0 {
        cumulative_rate.or(ewma_rate)
    } else {
        ewma_rate.or(cumulative_rate)
    }
}

fn progress_eta_status(
    eta_secs: Option<f64>,
    confident: bool,
    active: bool,
    unfinished: bool,
    activity: u64,
) -> &'static str {
    if eta_secs.is_some() {
        if confident { "ready" } else { "estimating" }
    } else if active || (unfinished && activity != 0) {
        "working"
    } else {
        "waiting"
    }
}

fn format_duration(secs: f64) -> String {
    if !secs.is_finite() || secs < 0.0 {
        return "?".to_owned();
    }
    let total = secs.round() as u64;
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    if h > 0 {
        format!("{h}h{m:02}m{s:02}s")
    } else if m > 0 {
        format!("{m}m{s:02}s")
    } else {
        format!("{s}s")
    }
}

#[cfg(test)]
mod tests {
    use super::{ProgressMode, ProgressReporter, effective_progress_rate, progress_eta_status};
    use dedup_core::ProgressObserver;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    #[test]
    fn finish_wakes_a_reporter_with_a_long_interval() {
        let mut reporter = ProgressReporter::start(ProgressMode::Json, 60_000);
        let started = Instant::now();

        reporter.finish();

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "finish waited for the reporting interval"
        );
    }

    #[test]
    fn phase_changes_reset_fine_grained_activity() {
        let mut reporter = ProgressReporter::start(ProgressMode::Json, 60_000);
        reporter.begin_phase("direct_bm25", Some(10));
        reporter.add_activity(7);
        reporter.set_activity_label("candidate_slots");
        reporter.set_detail("media=1/2");
        assert_eq!(reporter.shared.activity.load(Ordering::Relaxed), 7);

        reporter.begin_phase("next", Some(1));
        assert_eq!(reporter.shared.activity.load(Ordering::Relaxed), 0);
        let meta = reporter.shared.meta.lock().expect("progress lock");
        assert!(meta.activity_label.is_none());
        assert!(meta.detail.is_none());
        drop(meta);
        reporter.finish();
    }

    #[test]
    fn off_mode_skips_hot_path_progress_atomics() {
        let mut reporter = ProgressReporter::start(ProgressMode::Off, 1_000);
        reporter.begin_phase("direct_bm25", Some(10));
        reporter.add_completed(3);
        reporter.add_activity(7);

        assert_eq!(reporter.shared.completed.load(Ordering::Relaxed), 0);
        assert_eq!(reporter.shared.activity.load(Ordering::Relaxed), 0);
        reporter.finish();
    }

    #[test]
    fn eta_rate_accounts_for_elapsed_time_during_a_heavy_profile() {
        assert_eq!(
            effective_progress_rate(100, 5, 10.0, Some(50.0)),
            Some(50.0)
        );
        assert_eq!(effective_progress_rate(100, 0, 20.0, Some(50.0)), Some(5.0));
        assert_eq!(effective_progress_rate(0, 0, 20.0, None), None);
    }

    #[test]
    fn candidate_activity_keeps_zero_profile_progress_visibly_working() {
        assert_eq!(
            progress_eta_status(None, false, false, true, 512),
            "working"
        );
        assert_eq!(progress_eta_status(None, false, false, true, 0), "waiting");
        assert_eq!(
            progress_eta_status(Some(10.0), false, true, true, 512),
            "estimating"
        );
        assert_eq!(
            progress_eta_status(Some(10.0), true, false, true, 512),
            "ready"
        );
    }
}
