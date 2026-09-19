//! Progress observer and EWMA ETA helpers.

use crate::AnalysisError;

/// Progress observer implemented by the CLI reporter.
pub trait ProgressObserver: Send + Sync {
    fn set_stage(&self, stage: &str);
    fn begin_phase(&self, phase: &str, total: Option<u64>);
    fn add_completed(&self, n: u64);
    fn check_cancelled(&self) -> Result<(), AnalysisError>;
    fn finish(&self);
}

/// No-op progress observer for tests and quiet runs.
#[derive(Debug, Default)]
pub struct NoopProgress;

impl ProgressObserver for NoopProgress {
    fn set_stage(&self, _stage: &str) {}
    fn begin_phase(&self, _phase: &str, _total: Option<u64>) {}
    fn add_completed(&self, _n: u64) {}
    fn check_cancelled(&self) -> Result<(), AnalysisError> {
        Ok(())
    }
    fn finish(&self) {}
}

/// EWMA throughput helper shared by the CLI reporter.
#[derive(Clone, Debug)]
pub struct EwmaEta {
    alpha: f64,
    rate: Option<f64>,
    positive_samples: u32,
}

impl EwmaEta {
    pub fn new(alpha: f64) -> Self {
        Self {
            alpha,
            rate: None,
            positive_samples: 0,
        }
    }

    pub fn observe(&mut self, items_per_sec: f64) {
        if !items_per_sec.is_finite() || items_per_sec <= 0.0 {
            return;
        }
        self.rate = Some(match self.rate {
            Some(prev) => self.alpha * items_per_sec + (1.0 - self.alpha) * prev,
            None => items_per_sec,
        });
        self.positive_samples = self.positive_samples.saturating_add(1);
    }

    pub fn confident(&self) -> bool {
        self.positive_samples >= 3
    }

    pub fn eta_secs(&self, remaining: u64) -> Option<f64> {
        let rate = self.rate?;
        if rate <= 0.0 {
            return None;
        }
        Some(remaining as f64 / rate)
    }

    pub fn rate(&self) -> Option<f64> {
        self.rate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ewma_needs_three_samples_for_confidence() {
        let mut eta = EwmaEta::new(0.25);
        eta.observe(100.0);
        eta.observe(100.0);
        assert!(!eta.confident());
        eta.observe(100.0);
        assert!(eta.confident());
        let secs = eta.eta_secs(200).unwrap();
        assert!((secs - 2.0).abs() < 0.01);
    }

    #[test]
    fn ewma_ignores_non_positive_samples() {
        let mut eta = EwmaEta::new(0.25);
        eta.observe(0.0);
        eta.observe(-1.0);
        eta.observe(f64::NAN);
        assert!(!eta.confident());
        assert!(eta.rate().is_none());
    }

    #[test]
    fn noop_progress_is_usable() {
        let p = NoopProgress;
        p.set_stage("load");
        p.begin_phase("read", Some(10));
        p.add_completed(3);
        assert!(p.check_cancelled().is_ok());
        p.finish();
    }
}
