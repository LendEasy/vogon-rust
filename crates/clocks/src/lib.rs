//! Injected monotonic timing shared by provider and transport state machines.
//! All public timestamps are seconds on one host clock, never provider wall time.
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Cloneable host epoch. Clones project onto the same monotonic coordinate.
#[derive(Clone, Debug)]
pub struct Clock {
    origin: Instant,
}
impl Default for Clock {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}
impl Clock {
    /// Seconds since this host epoch; use the same clock for a call's components.
    pub fn now(&self) -> f64 {
        self.origin.elapsed().as_secs_f64()
    }
}

/// Convert provider stream-relative word onset to the host monotonic coordinate.
pub fn stream_to_monotonic(epoch: f64, stream: Option<f64>) -> Option<f64> {
    stream.map(|t| epoch + t)
}

/// Smooth nonnegative mark latency observations for estimated playout completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetEstimator {
    pub alpha: f64,
    pub estimate: Option<f64>,
    pub samples: usize,
}
impl Default for OffsetEstimator {
    fn default() -> Self {
        Self {
            alpha: 0.3,
            estimate: None,
            samples: 0,
        }
    }
}
impl OffsetEstimator {
    /// Ignore negative measurements, matching the Python media clock.
    pub fn observe(&mut self, seconds: f64) {
        if seconds < 0.0 {
            return;
        }
        self.estimate = Some(
            self.estimate
                .map_or(seconds, |v| self.alpha * seconds + (1.0 - self.alpha) * v),
        );
        self.samples += 1;
    }
    /// Zero before the first accepted sample.
    pub fn value(&self) -> f64 {
        self.estimate.unwrap_or(0.0)
    }
    /// Estimate the actual event time from a delayed observation.
    pub fn project_back(&self, observed: f64) -> f64 {
        observed - self.value()
    }
}

pub mod call;
