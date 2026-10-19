//! Flux transcript routing and resumed-turn stitching, independent of sockets.
//! The host serializes messages and timer expiry; final elapsed time is captured
//! at receipt, not at the end of a legacy hold. Update does not cancel that hold.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use vogon_clocks::stream_to_monotonic;

/// Non-secret provider settings. Eager mode requires both consumer callbacks.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct SttConfig {
    pub model: String,
    pub encoding: String,
    pub sample_rate: u32,
    pub keyterms: Vec<String>,
    pub language: String,
    pub eot_threshold: f64,
    pub eot_timeout_ms: u64,
    pub eager_eot_threshold: Option<f64>,
    pub eager_callback: bool,
    pub resumed_callback: bool,
    pub hold_ms: u64,
    pub word_conf_threshold: f64,
}
impl Default for SttConfig {
    fn default() -> Self {
        Self {
            model: "flux-general-en".into(),
            encoding: "linear16".into(),
            sample_rate: 16000,
            keyterms: vec![],
            language: "en-US".into(),
            eot_threshold: 0.9,
            eot_timeout_ms: 1000,
            eager_eot_threshold: None,
            eager_callback: true,
            resumed_callback: true,
            hold_ms: 250,
            word_conf_threshold: 0.75,
        }
    }
}
impl SttConfig {
    /// The legacy path remains selected when either eager consumer is absent.
    pub fn eager_mode(&self) -> bool {
        self.eager_eot_threshold.is_some_and(|v| v > 0.0)
            && self.eager_callback
            && self.resumed_callback
    }
    /// Repeated keyterm parameters and clamped eager threshold match SDK kwargs.
    /// Language is accepted for parity but the Python Flux client does not send it.
    pub fn query(&self) -> Vec<(String, String)> {
        let mut query = vec![
            ("model".into(), self.model.clone()),
            ("encoding".into(), self.encoding.clone()),
            ("sample_rate".into(), self.sample_rate.to_string()),
            ("eot_threshold".into(), self.eot_threshold.to_string()),
            ("eot_timeout_ms".into(), self.eot_timeout_ms.to_string()),
        ];
        if self.eager_mode() {
            query.push((
                "eager_eot_threshold".into(),
                self.eager_eot_threshold
                    .unwrap()
                    .min(self.eot_threshold)
                    .to_string(),
            ));
        }
        query.extend(self.keyterms.iter().map(|k| ("keyterm".into(), k.clone())));
        query
    }
}

/// Consumer events preserve the four Python callback contracts.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Transcript {
    Interim {
        text: String,
        confidence: f64,
    },
    Eager {
        text: String,
        confidence: f64,
        elapsed_ms: f64,
        low_conf_words: Vec<String>,
    },
    Resumed,
    Final {
        text: String,
        confidence: f64,
        elapsed_ms: f64,
        low_conf_words: Vec<String>,
    },
}

/// Word timings are stream-relative; confidence is required by the provider.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Word {
    pub word: String,
    pub confidence: f64,
    #[serde(default)]
    pub start: Option<f64>,
}

/// Only TurnInfo fields that affect gateway behavior. Unknown fields are ignored.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TurnInfo {
    pub event: String,
    #[serde(default)]
    pub transcript: Option<String>,
    #[serde(default)]
    pub words: Vec<Word>,
}

/// Merge complete, overlapping or restarted transcripts without losing prior words.
pub fn merge_resumed_transcripts(prefix: &str, text: &str) -> String {
    if prefix.is_empty() {
        return text.into();
    }
    if text.is_empty() {
        return prefix.into();
    }
    let p: Vec<_> = prefix.split_whitespace().collect();
    let t: Vec<_> = text.split_whitespace().collect();
    let norm = |w: &&str| {
        w.to_lowercase()
            .trim_matches(['.', ',', '!', '?', ';', ':', '\'', '"'])
            .to_owned()
    };
    let pn: Vec<_> = p.iter().map(norm).collect();
    let tn: Vec<_> = t.iter().map(norm).collect();
    if pn.len() <= tn.len() && (0..=tn.len() - pn.len()).any(|i| tn[i..i + pn.len()] == pn) {
        return text.into();
    }
    for k in (1..=pn.len().min(tn.len())).rev() {
        if pn[pn.len() - k..] == tn[..k] {
            return p
                .iter()
                .chain(t[k..].iter())
                .copied()
                .collect::<Vec<_>>()
                .join(" ");
        }
    }
    p.iter()
        .chain(t.iter())
        .copied()
        .collect::<Vec<_>>()
        .join(" ")
}

/// One session's transcript state, pending close fallback, hold deadline and quality.
#[derive(Debug)]
pub struct SttState {
    pub config: SttConfig,
    pub epoch: f64,
    pub onset_stream: Option<f64>,
    pub pending_text: String,
    pub pending_confidence: f64,
    pub resumed_prefix: String,
    held: Option<(f64, Transcript)>,
    pub total_words: usize,
    pub conf_sum: f64,
    pub low_conf_count: usize,
}
impl SttState {
    /// Capture epoch at session construction, matching the original onset projection.
    pub fn new(config: SttConfig, epoch: f64) -> Self {
        Self {
            config,
            epoch,
            onset_stream: None,
            pending_text: String::new(),
            pending_confidence: 0.0,
            resumed_prefix: String::new(),
            held: None,
            total_words: 0,
            conf_sum: 0.0,
            low_conf_count: 0,
        }
    }
    /// Project the latest boundary's first-word start; retain prior onset if absent.
    pub fn last_onset_monotonic(&self) -> Option<f64> {
        stream_to_monotonic(self.epoch, self.onset_stream)
    }
    /// Next legacy final deadline, if there is one.
    pub fn deadline(&self) -> Option<f64> {
        self.held.as_ref().map(|v| v.0)
    }
    /// A requested shutdown cancels legacy dispatch without prematurely flushing eager text.
    pub fn cancel_hold(&mut self) {
        self.held = None;
    }
    /// Emit an expired hold once. The host decides ordering at an exact deadline tie.
    pub fn tick(&mut self, now: f64) -> Vec<Transcript> {
        if self.deadline().is_some_and(|at| now >= at) {
            vec![self.held.take().unwrap().1]
        } else {
            vec![]
        }
    }
    /// Route a provider message. Empty StartOfTurn is intentionally ignored like Python.
    pub fn message(&mut self, message: TurnInfo, now: f64) -> Vec<Transcript> {
        let event = message.event.as_str();
        let text = message.transcript.unwrap_or_default().trim().to_owned();
        let confidence = if message.words.is_empty() {
            1.0
        } else {
            message.words.iter().map(|w| w.confidence).sum::<f64>() / message.words.len() as f64
        };
        if text.is_empty()
            && event != "TurnResumed"
            && !(event == "EndOfTurn" && !self.resumed_prefix.is_empty())
        {
            return vec![];
        }
        let low: Vec<_> = message
            .words
            .iter()
            .filter(|w| w.confidence < self.config.word_conf_threshold)
            .map(|w| w.word.clone())
            .collect();
        if matches!(event, "EagerEndOfTurn" | "EndOfTurn")
            && let Some(start) = message.words.first().and_then(|w| w.start)
        {
            self.onset_stream = Some(start);
        }
        let elapsed_ms = (now - self.epoch) * 1000.0;
        match event {
            "EndOfTurn" => {
                let text = merge_resumed_transcripts(&self.resumed_prefix, &text);
                self.resumed_prefix.clear();
                for w in &message.words {
                    self.total_words += 1;
                    self.conf_sum += w.confidence;
                    if w.confidence < self.config.word_conf_threshold {
                        self.low_conf_count += 1;
                    }
                }
                self.pending_text.clear();
                self.pending_confidence = 0.0;
                self.held = None;
                let final_event = Transcript::Final {
                    text,
                    confidence,
                    elapsed_ms,
                    low_conf_words: low,
                };
                if self.config.eager_mode() {
                    vec![final_event]
                } else {
                    self.held = Some((now + self.config.hold_ms as f64 / 1000.0, final_event));
                    vec![]
                }
            }
            "EagerEndOfTurn" => {
                let text = merge_resumed_transcripts(&self.resumed_prefix, &text);
                self.pending_text = text.clone();
                self.pending_confidence = confidence;
                if self.config.eager_mode() {
                    vec![Transcript::Eager {
                        text,
                        confidence,
                        elapsed_ms,
                        low_conf_words: low,
                    }]
                } else {
                    vec![Transcript::Interim { text, confidence }]
                }
            }
            "TurnResumed" => {
                if !self.pending_text.is_empty() {
                    self.resumed_prefix =
                        merge_resumed_transcripts(&self.resumed_prefix, &self.pending_text);
                }
                self.held = None;
                self.pending_text.clear();
                self.pending_confidence = 0.0;
                if self.config.eager_mode() {
                    vec![Transcript::Resumed]
                } else if !text.is_empty() {
                    vec![Transcript::Interim { text, confidence }]
                } else {
                    vec![]
                }
            }
            _ => {
                if event == "StartOfTurn" {
                    self.held = None;
                }
                vec![Transcript::Interim { text, confidence }]
            }
        }
    }
    /// Socket close flushes pending eager text once. A requested close cancels a
    /// legacy hold first; a remote close allows its existing hold to finish.
    pub fn closed(&mut self, now: f64, requested: bool) -> Vec<Transcript> {
        if requested {
            self.held = None;
        }
        if self.pending_text.is_empty() {
            return vec![];
        }
        let text = std::mem::take(&mut self.pending_text);
        let confidence = std::mem::replace(&mut self.pending_confidence, 0.0);
        vec![Transcript::Final {
            text,
            confidence,
            elapsed_ms: (now - self.epoch) * 1000.0,
            low_conf_words: vec![],
        }]
    }
    /// Python's three-decimal reporting round, retaining banker rounding via formatting.
    pub fn quality_stats(&self) -> Value {
        if self.total_words == 0 {
            return json!({"total_words":0,"avg_confidence":null,"low_conf_fraction":null});
        }
        let round = |v: f64| format!("{v:.3}").parse::<f64>().unwrap();
        json!({"total_words":self.total_words,"avg_confidence":round(self.conf_sum/self.total_words as f64),
            "low_conf_fraction":round(self.low_conf_count as f64/self.total_words as f64)})
    }
    /// Read-only test/diagnostic snapshot. No credentials or provider payloads beyond transcripts.
    pub fn snapshot(&self) -> Value {
        json!({"pending_text":self.pending_text,"pending_confidence":self.pending_confidence,
        "resumed_prefix":self.resumed_prefix,"onset":self.last_onset_monotonic(),"deadline":self.deadline(),
        "quality":self.quality_stats(),"eager_mode":self.config.eager_mode()})
    }
}
