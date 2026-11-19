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
/// Severity label of one log line: `info` is expected lifecycle, `warn` a degraded but
/// served call, `error` a failure someone has to act on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    Info,
    Warn,
    Error,
}
impl Level {
    fn label(self) -> &'static str {
        match self {
            Level::Info => "INFO",
            Level::Warn => "WARNING",
            Level::Error => "ERROR",
        }
    }
}
/// The one log line format of the gateway, the same shape the Python services write:
/// `2026-11-19 20:40:09,123 ERROR [vogon.artifacts] [<call id>] artifact_s3_error key=value ...`
/// The level is the second word, so an error is visible at a glance. `component` is the crate
/// that wrote it; the call id is `-` on lines that belong to no call. Fields carry identifiers
/// and fixed codes only; callers must not pass provider bodies, transcripts, credentials or
/// raw error text.
pub fn line(level: Level, component: &'static str, event: &'static str, fields: &Value) -> String {
    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    line_at(
        since_epoch.as_millis() as u64,
        level,
        component,
        event,
        fields,
    )
}
/// `line` with the clock passed in (milliseconds since the Unix epoch, printed in UTC).
pub fn line_at(
    epoch_ms: u64,
    level: Level,
    component: &'static str,
    event: &'static str,
    fields: &Value,
) -> String {
    let call = fields["call_id"].as_str().filter(|id| !id.is_empty());
    let mut out = format!(
        "{} {} [vogon.{component}] [{}] {event}",
        timestamp(epoch_ms),
        level.label(),
        call.unwrap_or("-")
    );
    if let Some(fields) = fields.as_object() {
        for (name, value) in fields {
            if name != "call_id" {
                pairs(&mut out, name, value);
            }
        }
    }
    out
}
/// Append ` name=value`; a nested object becomes `outer.inner=value`, an absent or empty value is left out.
fn pairs(out: &mut String, name: &str, value: &Value) {
    match value {
        Value::Null => {}
        Value::String(text) if text.is_empty() => {}
        Value::Object(inner) => {
            for (inner_name, inner_value) in inner {
                pairs(out, &format!("{name}.{inner_name}"), inner_value);
            }
        }
        Value::String(text)
            if !text
                .chars()
                .any(|c| c.is_whitespace() || c == '"' || c == '=') =>
        {
            out.push_str(&format!(" {name}={text}"));
        }
        other => out.push_str(&format!(" {name}={other}")),
    }
}
/// `YYYY-MM-DD HH:MM:SS,mmm` in UTC, the layout of Python's default `asctime`.
fn timestamp(epoch_ms: u64) -> String {
    let seconds = epoch_ms / 1000;
    let (days, of_day) = (seconds / 86_400, seconds % 86_400);
    // Civil date from a day count (Howard Hinnant's algorithm), valid for the whole Unix era.
    let shifted = days + 719_468;
    let era = shifted / 146_097;
    let day_of_era = shifted % 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + u64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02},{:03}",
        of_day / 3600,
        of_day % 3600 / 60,
        of_day % 60,
        epoch_ms % 1000
    )
}
/// Write one line to standard error.
pub fn emit(level: Level, component: &'static str, event: &'static str, fields: Value) {
    eprintln!("{}", line(level, component, event, &fields));
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
    #[test]
    fn a_line_reads_like_the_python_services_with_the_level_second() {
        let text = line_at(
            1_795_120_809_123,
            Level::Error,
            "artifacts",
            "artifact_s3_error",
            &serde_json::json!({"operation":"put","tenant_id":"t1","call_id":"c1","local_error":null,"bytes":4,"error":{"aws_code":"AccessDenied","http_status":403}}),
        );
        assert_eq!(
            text,
            "2026-11-19 20:40:09,123 ERROR [vogon.artifacts] [c1] artifact_s3_error operation=put tenant_id=t1 bytes=4 error.aws_code=AccessDenied error.http_status=403"
        );
    }
    #[test]
    fn a_line_for_no_call_marks_the_call_slot_and_quotes_awkward_values() {
        let text = line_at(
            0,
            Level::Info,
            "service",
            "gateway_listening",
            &serde_json::json!({"detail":"bind failed: in use","errors":["a","b"],"empty":""}),
        );
        assert_eq!(
            text,
            "1970-01-01 00:00:00,000 INFO [vogon.service] [-] gateway_listening detail=\"bind failed: in use\" errors=[\"a\",\"b\"]"
        );
    }
    #[test]
    fn the_calendar_handles_a_leap_day_and_a_year_end() {
        assert_eq!(timestamp(1_709_251_199_000), "2024-02-29 23:59:59,000");
        assert_eq!(timestamp(1_735_689_600_000), "2025-01-01 00:00:00,000");
    }
}
