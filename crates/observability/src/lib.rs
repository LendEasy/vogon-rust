//! Bounded process aggregates. These counters never influence call state, sequencing or time.
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
/// Fixed categories prevent caller IDs, transcripts or unbounded error labels becoming metrics.
#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Event {
    CallStarted,
    CallEnded,
    CallFailed,
    AdmissionRejected,
    ProviderError,
    Eager,
    Final,
    Resumed,
    FloorYield,
    StaleRejected,
    Clear,
    ArtifactFailed,
    AudioFrame,
    TurnRequest,
    TurnError,
}
const NAMES: [&str; 15] = [
    "call_started",
    "call_ended",
    "call_failed",
    "admission_rejected",
    "provider_error",
    "eager_eot",
    "final_eot",
    "utterance_resumed",
    "floor_yield",
    "stale_rejected",
    "playback_clear",
    "artifact_failed",
    "audio_frame",
    "turn_request",
    "turn_error",
];
const BUCKETS: [u64; 8] = [50, 100, 200, 500, 1000, 2000, 5000, u64::MAX];
/// Lock-free aggregate counters/histogram, shared only for operational reporting.
/// Call-local logical counters remain owned by their existing call state machines.
pub struct Metrics {
    counters: [AtomicU64; 15],
    first_chunk: [AtomicU64; 8],
    latency_sum: AtomicU64,
    raw_rms_sum: AtomicU64,
    clean_rms_sum: AtomicU64,
    turn_ms_sum: AtomicU64,
}
impl Default for Metrics {
    fn default() -> Self {
        Self {
            counters: std::array::from_fn(|_| AtomicU64::new(0)),
            first_chunk: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_sum: AtomicU64::new(0),
            raw_rms_sum: AtomicU64::new(0),
            clean_rms_sum: AtomicU64::new(0),
            turn_ms_sum: AtomicU64::new(0),
        }
    }
}
impl Metrics {
    /// Increment one predefined event category; no user-provided labels are accepted.
    pub fn increment(&self, event: Event) {
        self.counters[event as usize].fetch_add(1, Ordering::Relaxed);
    }
    /// Read an operational counter without mutating any clock or call.
    pub fn count(&self, event: Event) -> u64 {
        self.counters[event as usize].load(Ordering::Relaxed)
    }
    /// Scalar AEC input/output RMS summaries, with no audio or call identifiers retained.
    pub fn frame(&self, raw: i64, clean: i64) {
        self.increment(Event::AudioFrame);
        self.raw_rms_sum
            .fetch_add(raw.clamp(0, 32768) as u64, Ordering::Relaxed);
        self.clean_rms_sum
            .fetch_add(clean.clamp(0, 32768) as u64, Ordering::Relaxed);
    }
    /// Completed application request duration includes its ordering wait and retries.
    pub fn turn(&self, elapsed: std::time::Duration, ok: bool) {
        self.increment(Event::TurnRequest);
        if !ok {
            self.increment(Event::TurnError);
        }
        self.turn_ms_sum.fetch_add(
            elapsed.as_millis().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
    /// Record a bounded first-chunk latency observation in milliseconds.
    pub fn first_chunk(&self, ms: f64) {
        if !ms.is_finite() || ms < 0. || ms > 3600000. {
            return;
        }
        let n = ms.round() as u64;
        let bucket = BUCKETS.iter().position(|b| n <= *b).unwrap();
        self.first_chunk[bucket].fetch_add(1, Ordering::Relaxed);
        self.latency_sum.fetch_add(n, Ordering::Relaxed);
    }
    /// Classify existing diagnostics; never retain or log the payload/transcript.
    pub fn observe(&self, event: &Value) {
        match event["kind"].as_str().unwrap_or("") {
            "utterance_eager" => self.increment(Event::Eager),
            "utterance_final" => self.increment(Event::Final),
            "utterance_resumed" => self.increment(Event::Resumed),
            "provider_error" | "stt_error" | "tts_error" => self.increment(Event::ProviderError),
            "floor_result_rejected" | "playback_blocked" => self.increment(Event::StaleRejected),
            "floor_decision" if event["decision"] == "YIELD" => self.increment(Event::FloorYield),
            "tts_end" => {
                if let Some(ms) = event["first_chunk_ms"].as_f64() {
                    self.first_chunk(ms)
                }
            }
            _ => {}
        }
    }
    /// Prometheus text exposition; cardinality is constant regardless of call traffic.
    pub fn prometheus(
        &self,
        active: usize,
        max: usize,
        ready: bool,
        transfer_failures: u64,
    ) -> String {
        let mut out = String::from("# TYPE vogon_events_total counter\n");
        for (name, count) in NAMES.iter().zip(&self.counters) {
            out.push_str(&format!(
                "vogon_events_total{{event=\"{name}\"}} {}\n",
                count.load(Ordering::Relaxed)
            ));
        }
        out.push_str(&format!("# TYPE vogon_active_sockets gauge\nvogon_active_sockets {active}\n# TYPE vogon_call_capacity gauge\nvogon_call_capacity {max}\n# TYPE vogon_ready gauge\nvogon_ready {}\n# TYPE vogon_transfer_report_failures_total counter\nvogon_transfer_report_failures_total {transfer_failures}\n",u8::from(ready)));
        out.push_str("# TYPE vogon_tts_first_chunk_ms histogram\n");
        let mut sum = 0;
        for (limit, count) in BUCKETS.iter().zip(&self.first_chunk) {
            sum += count.load(Ordering::Relaxed);
            let le = if *limit == u64::MAX {
                "+Inf".into()
            } else {
                limit.to_string()
            };
            out.push_str(&format!(
                "vogon_tts_first_chunk_ms_bucket{{le=\"{le}\"}} {sum}\n"
            ));
        }
        out.push_str(&format!(
            "vogon_tts_first_chunk_ms_count {sum}\nvogon_tts_first_chunk_ms_sum {}\n",
            self.latency_sum.load(Ordering::Relaxed)
        ));
        out.push_str(&format!("vogon_audio_rms_sum{{stage=\"raw\"}} {}\nvogon_audio_rms_sum{{stage=\"clean\"}} {}\nvogon_turn_request_ms_sum {}\nvogon_turn_request_ms_count {}\n",self.raw_rms_sum.load(Ordering::Relaxed),self.clean_rms_sum.load(Ordering::Relaxed),self.turn_ms_sum.load(Ordering::Relaxed),self.count(Event::TurnRequest)));
        out
    }
}
/// Structured log with a fixed event/code vocabulary; callers must not pass provider bodies.
pub fn log(event: &'static str, code: &'static str) {
    eprintln!("{}", serde_json::json!({"event":event,"code":code}));
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parallel_aggregates_have_fixed_labels_and_no_transcript() {
        let m = std::sync::Arc::new(Metrics::default());
        let jobs: Vec<_> = (0..8)
            .map(|_| {
                let m = m.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        m.observe(
                            &serde_json::json!({"kind":"utterance_eager","text":"private speech"}),
                        );
                    }
                })
            })
            .collect();
        for j in jobs {
            j.join().unwrap();
        }
        assert_eq!(m.count(Event::Eager), 8000);
        m.first_chunk(90.);
        m.first_chunk(1000.);
        let text = m.prometheus(8, 64, true, 0);
        assert!(!text.contains("private"));
        assert!(text.contains("vogon_tts_first_chunk_ms_count 2"));
        assert!(text.contains("le=\"100\"} 1"));
    }
}
