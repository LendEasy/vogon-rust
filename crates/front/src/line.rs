//! One-verdict line detector bound to the first qualifying STT turn, not acoustic episode.
use crate::{guard, jev};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
/// Call-config line thresholds and wait-for-quiet policy, in explicit seconds/milliseconds.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub enabled: bool,
    pub min_words: usize,
    pub machine_p: f64,
    pub human_p: f64,
    pub ask_window_s: f64,
    pub quiet_ms: f64,
    pub cap_s: f64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            min_words: 4,
            machine_p: 0.6,
            human_p: 0.3,
            ask_window_s: 2.,
            quiet_ms: 700.,
            cap_s: 8.,
        }
    }
}
/// Immutable observation from the floor controller; onset is host-monotonic seconds.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Episode {
    pub id: u64,
    pub onset: f64,
    pub text: String,
    #[serde(default)]
    pub echo: bool,
    #[serde(default)]
    pub possible_echo: bool,
}
/// One dispatched recording question; token protects against late completions after shutdown.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Request {
    pub token: u64,
    pub state: Value,
    pub text: String,
    pub final_event: bool,
    pub started: f64,
}
/// Ordered effects: caller-floor breakthrough precedes verdict, which precedes replayed final.
#[derive(Clone, Debug, Serialize)]
pub enum Effect {
    Breakthrough { episode: u64 },
    Event(Value),
}
/// Deterministic line state; the host owns ticking and the single in-flight instrument task.
pub struct Detector {
    pub config: Config,
    pub holder: String,
    pub verdict: Option<Value>,
    pub closed: bool,
    pub bound: bool,
    pub episode: Option<Episode>,
    pub onset_before_audio: Option<bool>,
    pub final_seen: bool,
    pub final_t: Option<f64>,
    pub last_text_t: Option<f64>,
    pub asked_text: Option<String>,
    pub pending: Option<(String, bool)>,
    pub last_p: Option<f64>,
    pub last_latency_ms: f64,
    pub asks: u64,
    pub failures: u64,
    pub first_audio_t: Option<f64>,
    pub opening_ended_t: Option<f64>,
    pub retire_at: Option<f64>,
    pub released: bool,
    flight: Option<Request>,
    next_token: u64,
    effects: Vec<Effect>,
}
/// Remove every sufficiently long consecutive echo run while retaining surviving punctuation.
pub fn strip_echo(text: &str, our_line: &str, min_run: usize) -> String {
    if text.is_empty() || our_line.is_empty() {
        return text.into();
    }
    let norm = |s: &str| {
        s.to_lowercase()
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '\'')
            .collect::<String>()
    };
    let ours: Vec<_> = our_line
        .split_whitespace()
        .map(norm)
        .filter(|s| !s.is_empty())
        .collect();
    let words: Vec<_> = text.split_whitespace().collect();
    let keys: Vec<_> = words.iter().map(|w| norm(w)).collect();
    let mut drop = vec![false; words.len()];
    let mut i = 0;
    while i < words.len() {
        let mut best = 0;
        if !keys[i].is_empty() {
            for j in 0..ours.len() {
                let mut k = 0;
                while i + k < keys.len()
                    && j + k < ours.len()
                    && !keys[i + k].is_empty()
                    && keys[i + k] == ours[j + k]
                {
                    k += 1;
                }
                best = best.max(k);
            }
        }
        if best >= min_run.max(1) {
            drop[i..i + best].fill(true);
            i += best;
        } else {
            i += 1;
        }
    }
    words
        .iter()
        .zip(drop)
        .filter_map(|(w, d)| (!d).then_some(*w))
        .collect::<Vec<_>>()
        .join(" ")
}
impl Detector {
    /// Start disabled or enabled from resolved call policy; no I/O or timers are spawned.
    pub fn new(config: Config, holder: String) -> Self {
        Self {
            config,
            holder,
            verdict: None,
            closed: false,
            bound: false,
            episode: None,
            onset_before_audio: None,
            final_seen: false,
            final_t: None,
            last_text_t: None,
            asked_text: None,
            pending: None,
            last_p: None,
            last_latency_ms: 0.,
            asks: 0,
            failures: 0,
            first_audio_t: None,
            opening_ended_t: None,
            retire_at: None,
            released: false,
            flight: None,
            next_token: 0,
            effects: vec![],
        }
    }
    /// Record the first actual playout's start once.
    pub fn playout_start(&mut self, now: f64) {
        self.first_audio_t.get_or_insert(now);
    }
    /// The first opening completion/clear starts the ask window.
    pub fn playout_end(&mut self, now: f64) {
        if self.opening_ended_t.is_none() && self.first_audio_t.is_some() {
            self.opening_ended_t = Some(now);
            self.retire_at = Some(now + self.config.ask_window_s);
        }
    }
    /// Observe post-AEC transcript revisions; closed verdicts still collect their turn's final for quiet holds.
    pub fn observe(
        &mut self,
        episode: Option<Episode>,
        final_event: bool,
        our_line: &str,
        now: f64,
    ) {
        let Some(ep) = episode else {
            return;
        };
        if !self.config.enabled || (self.bound && self.final_seen) {
            return;
        }
        if self.bound {
            self.last_text_t = Some(now);
            if final_event {
                self.final_seen = true;
                self.final_t = Some(now);
            }
        }
        if self.closed || ep.echo || ep.possible_echo {
            return;
        }
        let text = strip_echo(ep.text.trim(), our_line, 3);
        if text.split_whitespace().count() < self.config.min_words {
            return;
        }
        if !self.bound {
            self.bound = true;
            self.onset_before_audio = Some(self.first_audio_t.is_none_or(|t| ep.onset < t));
            self.episode = Some(ep);
            self.last_text_t = Some(now);
            if final_event {
                self.final_seen = true;
                self.final_t = Some(now);
            }
        }
        self.pending = Some((text, final_event));
    }
    /// Dispatch the newest pending revision; duplicate text reuses its probability without another request.
    pub fn begin_request(&mut self, now: f64) -> Option<Request> {
        if self.closed || self.flight.is_some() {
            return None;
        }
        let (text, final_event) = self.pending.take()?;
        if self.asked_text.as_ref() == Some(&text) && self.last_p.is_some() {
            self.evaluate(&text, final_event, self.last_p, now);
            return None;
        }
        self.asked_text = Some(text.clone());
        self.asks += 1;
        self.next_token += 1;
        let state = json!({"definitions":{"account_holder":"the person we are calling for","first_turn":"the first words heard after the line connected"},"account_holder":if self.holder.is_empty(){"the account holder"}else{&self.holder},"first_turn":text});
        let request = Request {
            token: self.next_token,
            state,
            text,
            final_event,
            started: now,
        };
        self.flight = Some(request.clone());
        Some(request)
    }
    /// Complete one instrument request. None means failure, not a guessed human verdict.
    pub fn complete(&mut self, token: u64, p: Option<f64>, now: f64) -> bool {
        if self.closed || self.flight.as_ref().is_none_or(|r| r.token != token) {
            return false;
        }
        let request = self.flight.take().unwrap();
        let p = p.filter(|p| p.is_finite());
        if p.is_none() {
            self.failures += 1;
        }
        self.last_p = p;
        self.last_latency_ms = (now - request.started) * 1000.;
        self.evaluate(&request.text, request.final_event, p, now);
        true
    }
    fn evaluate(&mut self, text: &str, final_event: bool, p: Option<f64>, now: f64) {
        if p.is_some_and(|p| p >= self.config.machine_p) {
            self.issue("machine", p.unwrap(), text, "machine_band", now);
        } else if final_event || self.final_seen {
            match p {
                Some(p) if p <= self.config.human_p => {
                    self.issue("human", p, text, "human_band", now)
                }
                Some(p) => self.issue("unknown", p, text, "overlap_band", now),
                None => self.issue("unknown", 0., text, "instrument_failed", now),
            }
        }
    }
    /// Retire after the ask window, allowing a bound unfinished turn twenty more seconds.
    pub fn tick(&mut self, now: f64) {
        let Some(retire) = self.retire_at else {
            return;
        };
        if self.closed || now < retire || self.flight.is_some() {
            return;
        }
        if self.bound && !self.final_seen && now < retire + 20. {
            return;
        }
        self.issue(
            "unknown",
            self.last_p.unwrap_or(0.),
            &self.asked_text.clone().unwrap_or_default(),
            "ask_window_expired",
            now,
        );
    }
    fn issue(&mut self, kind: &str, p: f64, text: &str, reason: &str, now: f64) {
        if self.closed {
            return;
        }
        self.closed = true;
        let source = if self.asks == 0 || (kind == "unknown" && reason == "instrument_failed") {
            "none"
        } else {
            "jev"
        };
        let verdict = json!({"kind":"line_verdict","answered_by":kind,"probability":p,"source":source,"transcript":text,"latency_ms":self.last_latency_ms,"at":now,"seq":0});
        self.verdict = Some(verdict.clone());
        if kind == "machine"
            && let Some(ep) = &self.episode
        {
            self.effects.push(Effect::Breakthrough { episode: ep.id });
        }
        self.effects.push(Effect::Event(verdict));
        if kind == "machine" && self.final_seen {
            self.effects.push(Effect::Event(json!({"kind":"utterance_final","text":text,"confidence":1.0,"low_conf_words":[],"heard":{"completed":[],"in_flight":null,"in_flight_heard_text":"","last_heard_text":"","unheard_text":""},"carry_forward":null,"onset_at":self.episode.as_ref().map_or(0.,|e|e.onset),"at":now,"seq":0})));
        }
    }
    /// Take ordered verdict/breakthrough effects exactly once.
    pub fn drain_effects(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.effects)
    }
    /// Evaluate hold release without sleeping; an external release wins over cap and quiet.
    pub fn quiet_reason(&self, now: f64) -> Option<&'static str> {
        let Some(verdict) = &self.verdict else {
            return Some("no_verdict");
        };
        if self.released {
            return Some("released");
        }
        if now >= verdict["at"].as_f64().unwrap() + self.config.cap_s {
            return Some("cap");
        }
        if self.final_seen
            && now
                - self
                    .last_text_t
                    .unwrap_or(0.)
                    .max(self.final_t.unwrap_or(0.))
                >= self.config.quiet_ms / 1000.
        {
            return Some("quiet");
        }
        None
    }
    /// Explicit teardown invalidates in-flight work and releases waiters without issuing a verdict.
    pub fn close(&mut self) {
        self.closed = true;
        self.flight = None;
        self.pending = None;
        self.released = true;
    }
    /// Read the state used by the Python differential oracle, excluding host task handles.
    pub fn snapshot(&self) -> Value {
        json!({"verdict":self.verdict,"closed":self.closed,"bound":self.bound,"episode":self.episode.as_ref().map(|e|e.id),"onset_before_audio":self.onset_before_audio,"final_seen":self.final_seen,"final_t":self.final_t,"last_text_t":self.last_text_t,"asked_text":self.asked_text,"pending":self.pending,"last_p":self.last_p,"last_latency_ms":self.last_latency_ms,"asks":self.asks,"failures":self.failures,"first_audio_t":self.first_audio_t,"opening_ended_t":self.opening_ended_t,"retire_at":self.retire_at,"worker_active":self.flight.is_some()})
    }
}
/// Ask the retained recording question; the line detector, not this client, applies thresholds.
pub async fn recording_probability(client: &jev::Client, state: Value) -> crate::Result<f64> {
    client
        .decide(
            state,
            json!({"recording":guard::constants()["recording_question"]}),
            None,
            "line_detector",
        )
        .await?
        .noul("recording")
}
