use analysis_core::{AnalysisError, EwmaEta, ProgressObserver};
use clap::ValueEnum;
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
    worker: Mutex<Option<JoinHandle<()>>>,
}

struct Shared {
    meta: Mutex<Meta>,
    completed: AtomicU64,
    stopping: AtomicBool,
    cancelled: AtomicBool,
    wake_lock: Mutex<()>,
    wake: Condvar,
    mode: EffectiveMode,
    interval: Duration,
}

struct Meta {
    stage: String,
    phase: String,
    total: Option<u64>,
    stage_started: Instant,
    phase_started: Instant,
    last_completed: u64,
    last_tick: Instant,
    last_progress_at: Instant,
    eta: EwmaEta,
    phase_history: Vec<PhaseTimingSnapshot>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)] // consumed by later report/timing tasks
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
    eta_secs: Option<f64>,
    eta_confident: bool,
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
                last_tick: now,
                last_progress_at: now,
                eta: EwmaEta::new(EWMA_ALPHA),
                phase_history: Vec::new(),
            }),
            completed: AtomicU64::new(0),
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
        Self {
            shared,
            worker: Mutex::new(worker),
        }
    }

    #[allow(dead_code)] // wired when engines support Ctrl-C cancel
    pub fn cancel_handle(&self) -> CancelHandle {
        CancelHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    #[allow(dead_code)] // used by later timing/report tasks
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

    fn emit_now(&self) {
        emit_snapshot(&self.shared);
    }

    fn stop_worker(&self) {
        self.shared.stopping.store(true, Ordering::SeqCst);
        self.shared.wake.notify_all();
        if let Some(handle) = self.worker.lock().expect("progress worker lock").take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ProgressReporter {
    fn drop(&mut self) {
        if !self.shared.stopping.load(Ordering::SeqCst) {
            self.stop_worker();
        }
    }
}

#[derive(Clone)]
#[allow(dead_code)] // wired when engines support Ctrl-C cancel
pub struct CancelHandle {
    shared: Arc<Shared>,
}

impl CancelHandle {
    #[allow(dead_code)]
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
        meta.last_tick = now;
        meta.last_progress_at = now;
        meta.eta = EwmaEta::new(EWMA_ALPHA);
        self.shared.completed.store(0, Ordering::Relaxed);
    }

    fn begin_phase(&self, phase: &str, total: Option<u64>) {
        let mut meta = self.shared.meta.lock().expect("progress lock");
        let now = Instant::now();
        record_current_phase(&mut meta, now);
        meta.phase = phase.to_owned();
        meta.total = total;
        meta.phase_started = now;
        meta.last_completed = 0;
        meta.last_tick = now;
        meta.last_progress_at = now;
        meta.eta = EwmaEta::new(EWMA_ALPHA);
        self.shared.completed.store(0, Ordering::Relaxed);
    }

    fn add_completed(&self, n: u64) {
        self.shared.completed.fetch_add(n, Ordering::Relaxed);
    }

    fn check_cancelled(&self) -> Result<(), AnalysisError> {
        if self.shared.cancelled.load(Ordering::SeqCst) {
            Err(AnalysisError::Cancelled)
        } else {
            Ok(())
        }
    }

    fn finish(&self) {
        self.stop_worker();
        self.emit_now();
        if self.shared.mode == EffectiveMode::Tty {
            let _ = writeln!(io::stderr());
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
    let now = Instant::now();
    let dt = now.duration_since(meta.last_tick).as_secs_f64().max(1e-6);
    let delta = completed.saturating_sub(meta.last_completed);
    let instant_rate = delta as f64 / dt;
    if delta > 0 {
        meta.eta.observe(instant_rate);
        meta.last_progress_at = now;
    }
    meta.last_completed = completed;
    meta.last_tick = now;

    let remaining = meta.total.map(|total| total.saturating_sub(completed));
    let stalled = progress_is_stalled(remaining, meta.last_progress_at, now, shared.interval);
    let percent = meta
        .total
        .and_then(|t| (t > 0).then_some(100.0 * completed as f64 / t as f64));
    let line = ProgressLine {
        stage: meta.stage.clone(),
        phase: meta.phase.clone(),
        completed,
        total: meta.total,
        percent,
        rate: (!stalled).then(|| meta.eta.rate()).flatten(),
        eta_secs: (!stalled)
            .then(|| remaining.and_then(|r| meta.eta.eta_secs(r)))
            .flatten(),
        eta_confident: !stalled && meta.eta.confident(),
        phase_elapsed_secs: meta.phase_started.elapsed().as_secs_f64(),
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
            let elapsed = format_duration(line.phase_elapsed_secs);
            let eta = match line.total {
                None => "n/a".to_owned(),
                Some(_) => match (line.eta_secs, line.eta_confident) {
                    (Some(secs), true) => format_duration(secs),
                    (Some(secs), false) => format!("~{}", format_duration(secs)),
                    (None, _) => "...".to_owned(),
                },
            };
            let _ = write!(
                io::stderr(),
                "\r[{label}] {progress} {pct} {rate} elapsed={elapsed} eta={eta}\x1b[K"
            );
            let _ = io::stderr().flush();
        }
        EffectiveMode::Off => {}
    }
}

fn progress_is_stalled(
    remaining: Option<u64>,
    last_progress_at: Instant,
    now: Instant,
    interval: Duration,
) -> bool {
    remaining.is_some_and(|remaining| remaining > 0)
        && now.duration_since(last_progress_at)
            >= interval.saturating_mul(4).max(Duration::from_secs(2))
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
    use super::{ProgressMode, ProgressReporter, progress_is_stalled};
    use analysis_core::ProgressObserver;
    use std::time::{Duration, Instant};

    #[test]
    fn finish_wakes_a_reporter_with_a_long_interval() {
        let reporter = ProgressReporter::start(ProgressMode::Json, 60_000);
        let started = Instant::now();

        reporter.finish();

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "finish waited for the reporting interval"
        );
    }

    #[test]
    fn stale_tail_rate_is_not_reported_as_zero_eta() {
        let now = Instant::now();
        assert!(progress_is_stalled(
            Some(2),
            now - Duration::from_secs(10),
            now,
            Duration::from_millis(500),
        ));
        assert!(!progress_is_stalled(
            Some(0),
            now - Duration::from_secs(10),
            now,
            Duration::from_millis(500),
        ));
        assert!(!progress_is_stalled(
            Some(2),
            now - Duration::from_secs(1),
            now,
            Duration::from_millis(500),
        ));
    }
}
