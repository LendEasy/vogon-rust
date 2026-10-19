//! Per-call front history, safe phrase admission and asynchronous generation ownership.
use crate::{
    config::{Artifact, Settings},
    guard, jev, judge, moves,
};
use rand::{SeedableRng, seq::SliceRandom};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
    time::{self, Duration, Instant},
};
use vogon_clocks::Clock;
/// Pure mutable history and policy. No call state is shared across source instances.
pub struct State {
    pub artifact: Artifact,
    pub settings: Settings,
    pub system: String,
    pub forbidden_names: Vec<String>,
    pub history: Vec<(String, String)>,
    pub lines_played: Vec<String>,
    pub metrics: BTreeMap<String, u64>,
    pub distress: bool,
    pub deliberate_silence: bool,
    pub generation: u64,
    pub primed: String,
    pub result: Option<String>,
    pub pending: bool,
    pools: BTreeMap<String, Vec<String>>,
    last: BTreeMap<String, String>,
    rng: rand::rngs::StdRng,
    last_line_t: f64,
    n: u64,
}
impl State {
    /// Create a call-local ledger; a seed supports repeatable rotation tests without copying Python RNG.
    pub fn new(
        artifact: Artifact,
        settings: Settings,
        agent: &str,
        lender: &str,
        borrower: &str,
        seed: u64,
    ) -> Self {
        let system = artifact.prompt.replace(
            "{agent_name}",
            if agent.is_empty() { "the agent" } else { agent },
        );
        let system = if lender.is_empty() {
            system
                .replace(" for {lender_name}", "")
                .replace("{lender_name}", "the lender")
        } else {
            system.replace("{lender_name}", lender)
        };
        Self {
            artifact,
            settings,
            system,
            forbidden_names: if borrower.is_empty() {
                vec![]
            } else {
                vec![borrower.into()]
            },
            history: vec![],
            lines_played: vec![],
            metrics: BTreeMap::new(),
            distress: false,
            deliberate_silence: false,
            generation: 0,
            primed: String::new(),
            result: None,
            pending: false,
            pools: BTreeMap::new(),
            last: BTreeMap::new(),
            rng: rand::rngs::StdRng::seed_from_u64(seed),
            last_line_t: 0.,
            n: 0,
        }
    }
    /// Increment a per-call decision counter.
    pub fn bump(&mut self, key: &str) {
        *self.metrics.entry(key.into()).or_default() += 1;
    }
    /// Record a caller final without changing generation or admitting speech.
    pub fn note_borrower(&mut self, text: &str) {
        if !text.trim().is_empty() {
            self.history.push(("Borrower".into(), text.trim().into()));
        }
    }
    /// Record delivered agent text; a desk answer clears the sticky distress latch.
    pub fn note_agent(&mut self, text: &str, front: bool) {
        if !text.trim().is_empty() {
            self.history.push((
                if front {
                    "Agent (your waiting line)"
                } else {
                    "Agent"
                }
                .into(),
                text.trim().into(),
            ));
        }
        if !front {
            self.distress = false;
        }
    }
    /// Mark a front line actually sent; selection alone must not update the played ledger.
    pub fn note_played(&mut self, text: &str) {
        self.lines_played.push(text.into());
        self.note_agent(text, true);
        self.bump("front_played");
    }
    /// Begin generation for new nonempty text. Repeated text is idempotent.
    pub fn prime(&mut self, text: &str) -> Option<u64> {
        if !text.is_empty() && text != self.primed {
            self.generation += 1;
        }
        let enabled = match self.settings.engine.as_str() {
            "off" => false,
            "jev" => self.artifact.can_read_moves(),
            _ => !self.artifact.prompt.is_empty(),
        };
        if !enabled || text.is_empty() || text == self.primed {
            return None;
        }
        self.primed = text.into();
        self.result = None;
        self.pending = true;
        self.deliberate_silence = false;
        Some(self.generation)
    }
    /// Invalidate every outstanding result and hold waiter when caller speech resumes.
    pub fn cancel(&mut self) {
        self.generation += 1;
        self.pending = false;
        self.result = None;
    }
    fn rotate(&mut self, key: &str, pool: &[String]) -> String {
        if pool.is_empty() {
            return String::new();
        }
        let q = self.pools.entry(key.into()).or_default();
        if q.is_empty() {
            *q = pool.to_vec();
            q.shuffle(&mut self.rng);
            if q.len() > 1 && self.last.get(key) == q.first() {
                q.rotate_left(1);
            }
        }
        let line = q.remove(0);
        self.last.insert(key.into(), line.clone());
        line
    }
    /// Draw a beat without repetition until the pool is exhausted, avoiding a cycle-boundary repeat.
    pub fn beat(&mut self, kind: &str) -> String {
        let pool = self.artifact.beats.get(kind).cloned().unwrap_or_default();
        if pool.is_empty() {
            return String::new();
        }
        let text = self.rotate(kind, &pool);
        self.bump(&format!("beat_{kind}"));
        text
    }
    /// Accept a Jev read only for the current generation; reasoned silence prevents fallback beats.
    pub fn complete_read(&mut self, generation: u64, mut read: moves::Read) -> bool {
        if generation != self.generation || !self.pending {
            return false;
        }
        self.pending = false;
        if !read.error.is_empty() {
            self.bump("front_error");
            self.result = Some(String::new());
            return true;
        }
        if read.rider("distress", 0.5) {
            self.distress = true;
        }
        if self.distress {
            read.riders.insert("distress".into(), 1.);
        }
        let tiers = self.artifact.tiers.clone();
        let spoken = self.history.iter().any(|(who, _)| who == "Agent");
        let floor = self.settings.move_floor;
        let mut pick = moves::pick(&read, &tiers, |k, p| self.rotate(k, p), spoken, floor);
        if pick.tier == "repeat" && !self.settings.repeat_enabled {
            pick = moves::Pick {
                tier: "silence".into(),
                text: String::new(),
                reason: "rider:repeat_disabled".into(),
            };
        }
        if pick.text.is_empty() {
            self.deliberate_silence = pick.reason.starts_with("rider:");
            self.bump("front_empty");
        } else {
            self.bump(&format!("front_{}", pick.tier));
            self.bump("front_generated");
        }
        self.result = Some(pick.text);
        true
    }
    /// Parse generated JSON's say field. Provider/schema failure is an empty result with an error counter.
    pub fn complete_text(&mut self, generation: u64, value: crate::Result<Value>) -> bool {
        if generation != self.generation || !self.pending {
            return false;
        }
        self.pending = false;
        let parsed = value.and_then(|v| match v.get("say") {
            Some(Value::String(s)) => Ok(s.trim().to_owned()),
            None | Some(Value::Null) | Some(Value::Bool(false)) => Ok(String::new()),
            Some(Value::Number(n)) if n.as_f64() == Some(0.) => Ok(String::new()),
            Some(Value::Array(a)) if a.is_empty() => Ok(String::new()),
            Some(Value::Object(o)) if o.is_empty() => Ok(String::new()),
            _ => Err(crate::Error::new("invalid_front_say")),
        });
        let text = match parsed {
            Ok(text) => {
                self.bump(if text.is_empty() {
                    "front_empty"
                } else {
                    "front_generated"
                });
                text
            }
            Err(_) => {
                self.bump("front_error");
                String::new()
            }
        };
        self.result = Some(text);
        true
    }
    /// Apply min-gap and structural guards and mint an audio ID; does not mark playback heard.
    pub fn admit(&mut self, text: &str, now: f64) -> (String, String) {
        if text.is_empty() {
            return (String::new(), String::new());
        }
        if self.last_line_t != 0. && now - self.last_line_t < self.settings.min_gap_sec {
            self.bump("front_guard_gap");
            return (String::new(), String::new());
        }
        let trips = guard::check(
            text,
            &guard::Config {
                max_words: self.settings.max_words,
                must_not_say: self.artifact.must_not_say.clone(),
                forbidden_names: self.forbidden_names.clone(),
                earlier: self.lines_played.clone(),
            },
        );
        if !trips.is_empty() {
            for t in trips {
                self.bump(&format!("front_guard_{}", t.split(':').next().unwrap()));
            }
            return (String::new(), String::new());
        }
        self.n += 1;
        self.last_line_t = now;
        (text.into(), format!("front-{}", self.n))
    }
    /// Build only the physically observed conversation and already-played front context.
    pub fn user_message(&self, utterance: &str) -> String {
        let lines = self
            .history
            .iter()
            .skip(self.history.len().saturating_sub(8))
            .map(|(who, what)| format!("{who}: {what}"))
            .collect::<Vec<_>>()
            .join("\n");
        let already = if self.lines_played.is_empty() {
            String::new()
        } else {
            format!(
                "ALREADY SAID THIS CALL (off limits, say it differently or say nothing):\n{}\n\n",
                self.lines_played
                    .iter()
                    .map(|s| format!("- {s}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        };
        format!(
            "VERIFIED: no\nCALLER FIRST NAME: (withheld)\n\nTRANSCRIPT (most recent last):\n{lines}\n\n{already}CALLER JUST SAID: \"{utterance}\""
        )
    }
    /// Stable diagnostic surface for Python differential traces; contains caller text.
    pub fn snapshot(&self) -> Value {
        json!({"system":self.system,"history":self.history,"lines_played":self.lines_played,"metrics":self.metrics,"distress":self.distress,"deliberate_silence":self.deliberate_silence,"last_line_t":self.last_line_t,"n":self.n})
    }
}
struct Inner {
    state: Mutex<State>,
    notify: Notify,
    clock: Clock,
}
/// Per-call async owner. Dropping it aborts generation; no detached late result survives.
pub struct Source {
    inner: Arc<Inner>,
    task: Mutex<Option<JoinHandle<()>>>,
    jev: Option<jev::Client>,
    llm: Option<judge::Client>,
}
impl Drop for Source {
    fn drop(&mut self) {
        if let Some(task) = self.task.get_mut().take() {
            task.abort();
        }
    }
}
impl Source {
    /// Inject reusable clients; absence is a handled provider failure, not a guessed read.
    pub fn new(
        state: State,
        clock: Clock,
        jev: Option<jev::Client>,
        llm: Option<judge::Client>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(state),
                notify: Notify::new(),
                clock,
            }),
            task: Mutex::new(None),
            jev,
            llm,
        }
    }
    /// Start background composition without blocking audio input or the hold deadline.
    pub async fn prime(&self, text: &str) {
        let mut task = self.task.lock().await;
        let mut state = self.inner.state.lock().await;
        let Some(token) = state.prime(text) else {
            self.inner.notify.notify_waiters();
            return;
        };
        if let Some(old) = task.take() {
            old.abort();
        }
        let engine = state.settings.engine.clone();
        let system = state.system.clone();
        let user = state.user_message(text);
        let effort = state.settings.model_effort.clone();
        let questions = moves::questions(
            &state.artifact.move_criteria,
            &state.artifact.move_riders,
            &state.artifact.move_question,
        );
        let agent = state
            .history
            .iter()
            .rev()
            .find(|(who, _)| who == "Agent")
            .map(|(_, s)| s.as_str())
            .unwrap_or("");
        let context = moves::state(agent, text, &state.history);
        let timeout = state.settings.beat_after_ms;
        let inner = Arc::downgrade(&self.inner);
        let jev = self.jev.clone();
        let llm = self.llm.clone();
        *task = Some(tokio::spawn(async move {
            if engine == "jev" {
                let result = async {
                    let q = questions?;
                    jev.ok_or_else(|| crate::Error::new("missing_jev_client"))?
                        .decide(context, q, Some(timeout), "front_move")
                        .await
                }
                .await;
                let read = result
                    .and_then(|r| moves::Read::from_result(&r))
                    .unwrap_or_else(|e| moves::Read {
                        error: e.code,
                        ..Default::default()
                    });
                if let Some(inner) = inner.upgrade() {
                    inner.state.lock().await.complete_read(token, read);
                    inner.notify.notify_waiters();
                }
            } else {
                let result = async {
                    llm.ok_or_else(|| crate::Error::new("missing_front_client"))?
                        .complete(&system, &user, 1024, 0.3, Some(&effort))
                        .await
                }
                .await;
                if let Some(inner) = inner.upgrade() {
                    inner.state.lock().await.complete_text(token, result);
                    inner.notify.notify_waiters();
                }
            }
        }));
        self.inner.notify.notify_waiters();
    }
    /// Invalidate pending speech immediately and abort its HTTP request/retry task.
    pub async fn cancel(&self) {
        let mut task = self.task.lock().await;
        self.inner.state.lock().await.cancel();
        if let Some(old) = task.take() {
            old.abort();
            let _ = old.await;
        }
        self.inner.notify.notify_waiters();
    }
    /// Await model text or a fallback beat; supersession cancels this hold's output.
    pub async fn hold_line(&self, deadline_s: f64, empty_s: Option<f64>) -> (String, String) {
        let start = Instant::now();
        let generation = self.inner.state.lock().await.generation;
        loop {
            let changed = self.inner.notify.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut state = self.inner.state.lock().await;
            if state.generation != generation {
                state.bump("front_beat_withheld");
                return (String::new(), String::new());
            }
            let elapsed = start.elapsed().as_secs_f64();
            if let Some(text) = state.result.clone() {
                if !text.is_empty() {
                    return state.admit(&text, self.inner.clock.now());
                }
                if state.deliberate_silence {
                    state.bump("front_silent_on_purpose");
                    return (String::new(), String::new());
                }
                if empty_s.is_none_or(|d| elapsed >= d.max(0.)) {
                    let beat = state.beat("hold");
                    return state.admit(&beat, self.inner.clock.now());
                }
            } else if !state.pending || elapsed >= deadline_s.max(0.) {
                if state.pending {
                    state.bump("front_late");
                }
                let beat = state.beat("hold");
                return state.admit(&beat, self.inner.clock.now());
            }
            let deadline = if state.result.is_some() {
                empty_s.unwrap_or(0.)
            } else {
                deadline_s
            };
            drop(state);
            tokio::select! {_=changed=>{},_=time::sleep(Duration::from_secs_f64((deadline-elapsed).max(0.)))=>{}}
        }
    }
    /// Admit at most one follow-up waiting line when the hold asks for it.
    pub async fn wait_line(&self) -> (String, String) {
        let mut s = self.inner.state.lock().await;
        let text = s.beat("wait");
        s.admit(&text, self.inner.clock.now())
    }
    /// Update the ledger only after the host's speech path returns a delivery result.
    pub async fn note_played(&self, text: &str) {
        self.inner.state.lock().await.note_played(text);
    }
    /// Record physical caller context; priming is deliberately a separate operation.
    pub async fn note_borrower(&self, text: &str) {
        self.inner.state.lock().await.note_borrower(text);
    }
    /// Record delivered desk speech and release sticky distress for its next turn.
    pub async fn note_agent(&self, text: &str) {
        self.inner.state.lock().await.note_agent(text, false);
    }
    /// Inspect per-call policy state for diagnostics and tests.
    pub async fn snapshot(&self) -> Value {
        self.inner.state.lock().await.snapshot()
    }
}
