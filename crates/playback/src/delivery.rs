//! Data and fallback estimates for reporting one response's delivery outcome.
//!
//! [`DeliveryReport`](crate::delivery::DeliveryReport) carries transport-supplied
//! counts/timings. The pure estimators infer duration and a whole-word prefix
//! when exact sentence/receipt evidence is unavailable. Estimates are not
//! proof of audibility; number-heavy text can take much longer to speak than
//! its whitespace-separated word count suggests.

use serde::{Deserialize, Serialize};

/// Fallback milliseconds per whitespace-separated word, retained from the gateway.
///
/// This estimate is deliberately secondary to sentence/mark evidence; it is
/// not a measured duration or a speech-provider timing guarantee.
pub const MS_PER_WORD: f64 = 375.0;
/// Split text on Rust standard Unicode whitespace for consistent duration/prefix estimates.
fn words(text: &str) -> Vec<&str> { text.split_whitespace().collect() }
/// Estimate full speech duration in milliseconds from word count.
///
/// Use at least one word, so empty/whitespace-only text returns 375 ms. This
/// preserves the fallback denominator without implying that empty text has audio.
pub fn estimate_total_ms(text: &str) -> f64 { words(text).len().max(1) as f64 * MS_PER_WORD }
/// Estimate a whole-word prefix from `audio_ms / total_ms`.
///
/// Unknown audio duration or a nonpositive total returns empty text. Clamp the
/// ratio to [0, 1], truncate the proportional word count and join selected words
/// with single spaces. The result is a delivery estimate, not mark-confirmed speech.
pub fn estimate_heard_text(text: &str, audio_ms: Option<f64>, total_ms: f64) -> String {
    let Some(audio) = audio_ms else { return String::new(); };
    if total_ms <= 0.0 { return String::new(); }
    let words = words(text);
    let fraction = (audio / total_ms).clamp(0.0,1.0);
    words[..(words.len() as f64 * fraction) as usize].join(" ")
}

/// Outcome of one host response/playout attempt.
///
/// The host populates this record; the type does not stream audio, observe marks
/// or emit metrics. A complete send may still have buffered audio awaiting a
/// playout mark, and a later drain clear is a separate event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryReport {
    /// Identity of the response audio associated with this report.
    pub audio_id: String,
    /// Host outcome, conventionally `complete`, `truncated` or `failed`; stored without validation.
    pub status: String,
    /// Number of audio chunks successfully dispatched by the transport.
    #[serde(default)] pub chunks_sent: usize,
    /// Duration streamed in milliseconds, or None when the transport cannot measure it.
    #[serde(default)] pub audio_ms: Option<f64>,
    /// Fallback estimate of the full intended text duration, in milliseconds.
    #[serde(default)] pub est_total_ms: f64,
    /// Whether terminal playback confirmation was still outstanding when reported.
    #[serde(default)] pub mark_pending: bool,
    /// Total planned sentence segments for this response.
    #[serde(default)] pub sentences_total: usize,
    /// Number of fully sent sentences according to the sending path, not receipt count.
    #[serde(default)] pub sentences_sent: usize,
    /// Send-based sentence prefix supplied by the host; this field alone does not prove audible playback.
    #[serde(default)] pub heard_text: String,
    /// Client-measured TTS first-chunk latency in milliseconds, when available.
    #[serde(default)] pub first_chunk_ms: Option<f64>,
    /// Monotonic seconds when the first audio headed toward the caller, when available.
    #[serde(default)] pub first_chunk_t: Option<f64>,
}
impl DeliveryReport {
    /// Return the reported audio/estimated-total ratio when both are usable.
    ///
    /// Unknown audio duration or a nonpositive estimate yields None. Only the upper
    /// bound is clamped to one; negative audio durations remain negative for existing
    /// behavior compatibility. This differs from the prefix estimator's two-sided clamp.
    pub fn heard_fraction(&self) -> Option<f64> {
        if self.est_total_ms <= 0.0 { None } else { self.audio_ms.map(|ms| (ms/self.est_total_ms).min(1.0)) }
    }
}
