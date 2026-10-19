//! TTS response demarcation and RIFF stripping match the existing streaming client.
use serde::{Deserialize, Serialize};

/// Non-secret Speak v1 options. Container is intentionally absent on WebSocket.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct TtsConfig {
    pub model: String,
    pub encoding: String,
    pub sample_rate: u32,
}
impl Default for TtsConfig {
    fn default() -> Self {
        Self {
            model: "aura-2-thalia-en".into(),
            encoding: "linear16".into(),
            sample_rate: 24000,
        }
    }
}
impl TtsConfig {
    /// Build the ordered Speak query without an HTTP-only container option.
    pub fn query(&self) -> Vec<(String, String)> {
        vec![
            ("model".into(), self.model.clone()),
            ("encoding".into(), self.encoding.clone()),
            ("sample_rate".into(), self.sample_rate.to_string()),
        ]
    }
}

/// Strip a complete RIFF prefix only. Preserve Python's current per-chunk behavior,
/// including no padding adjustment; this does not parse arbitrary fragmented WAV.
pub fn strip_wav_header(data: &[u8]) -> Vec<u8> {
    if data.len() < 12 || &data[..4] != b"RIFF" {
        return data.to_vec();
    }
    let mut offset = 12usize;
    while offset.checked_add(8).is_some_and(|n| n <= data.len()) {
        let id = &data[offset..offset + 4];
        let size = u32::from_le_bytes(data[offset + 4..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        if id == b"data" {
            return data[offset..].to_vec();
        }
        let Some(next) = offset.checked_add(size) else {
            return data.to_vec();
        };
        offset = next;
    }
    data.to_vec()
}

/// A chunk belongs to one utterance generation and one Flushed-delimited segment.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AudioChunk {
    pub segment: usize,
    pub data: Vec<u8>,
    pub first_chunk_ms: Option<f64>,
}

/// Pure response parser. The live client owns socket lifecycle and cancellation.
#[derive(Clone, Debug)]
pub struct TtsState {
    pub segments: usize,
    pub flushed: usize,
    pub first: bool,
    pub started: f64,
}
impl TtsState {
    /// Track one utterance with a fixed segment count and monotonic start time in seconds.
    pub fn new(segments: usize, started: f64) -> Self {
        Self {
            segments,
            flushed: 0,
            first: true,
            started,
        }
    }
    /// Return whether every expected segment has received its Flushed acknowledgement.
    pub fn done(&self) -> bool {
        self.flushed >= self.segments
    }
    /// Flushed closes a segment; metadata/other control frames do not advance it.
    pub fn control(&mut self, kind: &str) {
        if kind == "Flushed" && !self.done() {
            self.flushed += 1;
        }
    }
    /// Ignore empty audio; latency is emitted only on the first nonempty chunk.
    pub fn audio(&mut self, data: &[u8], now: f64) -> Option<AudioChunk> {
        if self.done() {
            return None;
        }
        let data = strip_wav_header(data);
        if data.is_empty() {
            return None;
        }
        let first_chunk_ms = if self.first {
            self.first = false;
            Some((now - self.started) * 1000.0)
        } else {
            None
        };
        Some(AudioChunk {
            segment: self.flushed,
            data,
            first_chunk_ms,
        })
    }
}
