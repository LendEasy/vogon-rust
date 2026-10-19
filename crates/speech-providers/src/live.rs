//! Optional GPT Live listener state. Only caller transcripts reach the transport;
//! model-generated audio is deliberately discarded, matching LiveSession.
use crate::{Error, Result, config::ApiKey, stt::Transcript};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
/// Existing gateway listener prompt, preserved verbatim.
pub const INSTRUCTIONS: &str = "You are the listening half of a phone call. A separate backend decides every reply and speaks it through its own audio path; you will be told what it said. Never speak, greet, acknowledge or backchannel. After every caller utterance, delegate to the backend immediately and wait silently.";
/// Listener format and background startup/quiet-fallback deadlines.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct LiveConfig {
    pub model: String,
    pub voice: String,
    pub encoding: String,
    pub sample_rate: u32,
    pub final_quiet_ms: u64,
    pub start_timeout_s: f64,
    pub start_wait_ms: u64,
}
impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            model: "gpt-live-1".into(),
            voice: "marin".into(),
            encoding: "mulaw".into(),
            sample_rate: 8000,
            final_quiet_ms: 900,
            start_timeout_s: 8.0,
            start_wait_ms: 300,
        }
    }
}
impl LiveConfig {
    /// Session.start payload; model belongs here, never in the socket URL.
    pub fn session(&self) -> Value {
        json!({"model":self.model,"instructions":INSTRUCTIONS,"audio":{"format":{"type":if self.encoding=="mulaw"{"audio/pcmu"}else{"audio/pcm"},"rate":if self.encoding=="mulaw"{8000}else{self.sample_rate}},"output":{"voice":self.voice}},"delegation":{"type":"client"}})
    }
    /// Reuse the same dotenv file with process values taking precedence; no file is written.
    pub fn load(path: &std::path::Path) -> Result<(Self, ApiKey)> {
        let file: std::collections::BTreeMap<String, String> = dotenvy::from_path_iter(path)
            .map_err(|_| Error::new("dotenv_read_failed"))?
            .collect::<std::result::Result<_, _>>()
            .map_err(|_| Error::new("dotenv_parse_failed"))?;
        let get = |name: &str| std::env::var(name).ok().or_else(|| file.get(name).cloned());
        let mut cfg = Self::default();
        if let Some(v) = get("LIVE_MODEL") {
            cfg.model = v;
        }
        if let Some(v) = get("LIVE_VOICE") {
            cfg.voice = v;
        }
        if let Some(v) = get("LIVE_FINAL_QUIET_MS") {
            cfg.final_quiet_ms = v.parse().map_err(|_| Error::new("invalid_live_settings"))?;
        }
        if let Some(v) = get("LIVE_START_WAIT_MS") {
            cfg.start_wait_ms = v.parse().map_err(|_| Error::new("invalid_live_settings"))?;
        }
        if let Some(v) = get("LIVE_START_TIMEOUT_S") {
            cfg.start_timeout_s = v.parse().map_err(|_| Error::new("invalid_live_settings"))?;
        }
        if !cfg.start_timeout_s.is_finite() || cfg.start_timeout_s <= 0.0 {
            return Err(Error::new("invalid_live_settings"));
        }
        Ok((cfg, ApiKey::new(get("OPENAI_API_KEY").unwrap_or_default())?))
    }
}
/// Deterministic delta/delegation/quiet state. Host supplies monotonic timestamps.
pub struct LiveState {
    pub config: LiveConfig,
    pub epoch: f64,
    pub connected: bool,
    pub session_id: String,
    pub deadline: Option<f64>,
    pub onset: Option<f64>,
    pub turns: usize,
    pub words: usize,
    pub fallback_finals: usize,
    pub errors: usize,
    pub usage_seconds: f64,
    pub open_ms: Option<f64>,
    buf: Vec<String>,
}
impl LiveState {
    /// Initialize listener transcript, readiness and quiet-final state at the host epoch.
    pub fn new(config: LiveConfig, epoch: f64) -> Self {
        Self {
            config,
            epoch,
            connected: false,
            session_id: String::new(),
            deadline: None,
            onset: None,
            turns: 0,
            words: 0,
            fallback_finals: 0,
            errors: 0,
            usage_seconds: 0.0,
            open_ms: None,
            buf: vec![],
        }
    }
    /// Ignore all output audio/text. Delegation commits at most one buffered final.
    pub fn message(&mut self, v: &Value, now: f64) -> Vec<Transcript> {
        match v["type"].as_str() {
            Some("session.started") => {
                self.connected = true;
                self.session_id = v["session"]["id"].as_str().unwrap_or("").into();
                self.open_ms = Some((now - self.epoch) * 1000.0);
                self.epoch = now;
                vec![]
            }
            Some("session.input_transcript.delta") => {
                if self.buf.is_empty() {
                    self.onset = v["start_ms"].as_f64().map(|v| v / 1000.0);
                }
                self.buf.push(v["delta"].as_str().unwrap_or("").into());
                self.deadline = Some(now + self.config.final_quiet_ms as f64 / 1000.0);
                vec![Transcript::Interim {
                    text: self.buf.concat().trim().into(),
                    confidence: 1.0,
                }]
            }
            Some("session.delegation.created") => {
                self.deadline = None;
                self.finalize()
            }
            Some("session.usage.updated") => {
                self.usage_seconds = v["usage"]["seconds"].as_f64().unwrap_or(0.0);
                vec![]
            }
            Some("session.closed") => {
                if !v["usage"].is_null() {
                    self.usage_seconds = v["usage"]["seconds"].as_f64().unwrap_or(0.0);
                }
                self.connected = false;
                vec![]
            }
            Some("error") => {
                self.errors += 1;
                vec![]
            }
            _ => vec![],
        }
    }
    /// Quiet fallback is independent of new network input; a later delegation is a no-op.
    pub fn tick(&mut self, now: f64) -> Vec<Transcript> {
        if self.deadline.is_some_and(|t| now >= t) {
            self.deadline = None;
            if !self.buf.is_empty() {
                self.fallback_finals += 1;
                return self.finalize();
            }
        }
        vec![]
    }
    fn finalize(&mut self) -> Vec<Transcript> {
        let text = std::mem::take(&mut self.buf).concat().trim().to_owned();
        if text.is_empty() {
            return vec![];
        }
        self.turns += 1;
        self.words += text.split_whitespace().count();
        vec![Transcript::Final {
            text,
            confidence: 1.0,
            elapsed_ms: 0.0,
            low_conf_words: vec![],
        }]
    }
    /// Project the last speech onset into the shared host clock, when available.
    pub fn onset_monotonic(&self) -> Option<f64> {
        self.onset.map(|t| self.epoch + t)
    }
    /// Quality deliberately omits fabricated word confidences for this provider.
    pub fn quality(&self, dropped: usize, error: Option<&str>) -> Value {
        json!({"total_words":self.words,"avg_confidence":null,"low_conf_fraction":null,"live_session_id":self.session_id,"live_turns":self.turns,"live_fallback_finals":self.fallback_finals,"live_errors":self.errors,"live_usage_seconds":self.usage_seconds,"live_open_ms":self.open_ms.map(f64::round_ties_even),"live_frames_dropped_before_open":dropped,"live_open_error":error})
    }
}
