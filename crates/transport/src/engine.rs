//! Per-call callback composition: transcript annotations, floor arbitration,
//! receipt accounting and timers. No provider or socket I/O occurs here.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::VecDeque;
use vogon_barge_in::{
    floor::{self, Controller, Decision, Effect, FinalWait},
    semantics::{Policy, should_swallow_absorbed_final},
};
use vogon_clocks::call::{CallClocks, Config as ClockConfig};
use vogon_playback::state::PlayoutState;
use vogon_speech_providers::stt::Transcript;

/// Wire family. Browser audio is PCM; Twilio media is 8 kHz mu-law.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Twilio,
    Browser,
    Text,
}
/// A deferred final retains its original generation and onset while the judge runs.
struct PendingFinal {
    wait: FinalWait,
    event: Transcript,
    onset: f64,
    at: f64,
    generation: u64,
}
/// Ordered host work. Effects are executed outside the state lock.
pub enum Action {
    Floor(Effect),
    Browser(Value),
    Cancel,
    Classifier(floor::ClassifierRequest),
    /// Recording question dispatched independently of the audio/transcript loop.
    Line(vogon_front::line::Request),
}
/// One call's policy and event sequence, shared only under the session state lock.
pub struct Engine {
    pub mode: Mode,
    pub stream_sid: Option<String>,
    pub playing: bool,
    pub playback: PlayoutState,
    pub floor: Option<Controller>,
    pub clocks: CallClocks,
    pub events: Vec<Value>,
    pub actions: Vec<Action>,
    pub gap_active: bool,
    pub typing_deadline: Option<f64>,
    pub typing_delay: f64,
    pub typing_index: i64,
    /// Optional first-turn line detector; disabled on non-phone adapters.
    pub line: Option<vogon_front::line::Detector>,
    line_episode: Option<floor::Episode>,
    seq: u64,
    onset: f64,
    interim_seen: bool,
    last_utterance: f64,
    recent_absorbed: Option<Value>,
    pending: VecDeque<PendingFinal>,
    next_clock_tick: f64,
}
impl Engine {
    /// Construct using call policy; phone floor control is always the existing module 4.
    pub fn new(
        mode: Mode,
        clocks: ClockConfig,
        floor_config: floor::Config,
        view: floor::Transport,
        now: f64,
    ) -> Self {
        let mut floor = (mode == Mode::Twilio).then(|| Controller::new(floor_config, view, now));
        if let Some(f) = &mut floor {
            f.start();
        }
        Self {
            mode,
            stream_sid: None,
            playing: false,
            playback: PlayoutState::default(),
            floor,
            clocks: CallClocks::new(clocks, now),
            events: vec![],
            actions: vec![],
            gap_active: false,
            typing_deadline: None,
            typing_delay: 0.0,
            typing_index: 0,
            line: None,
            line_episode: None,
            seq: 0,
            onset: 0.0,
            interim_seen: false,
            last_utterance: 0.0,
            recent_absorbed: None,
            pending: VecDeque::new(),
            next_clock_tick: now + 1.0,
        }
    }
    /// Stamp facts from every subsystem with one strictly increasing call sequence.
    pub fn emit(&mut self, mut event: Value) {
        self.seq += 1;
        event["seq"] = json!(self.seq);
        self.events.push(event);
    }
    /// Refresh observed playback without overwriting controller-owned absorb state.
    pub fn sync(&mut self, now: f64) {
        if let Some(f) = &mut self.floor {
            f.now = now;
            let v = &mut f.transport;
            v.tts_playing = self.playing;
            v.playback_draining = self.playback.draining(now);
            v.gap_active = self.gap_active;
            v.typing_arm_deadline = self.typing_deadline.unwrap_or(0.0);
            v.last_audio_id = (!self.playback.last_audio_id.is_empty())
                .then(|| self.playback.last_audio_id.clone());
            v.last_speak_text = self.playback.text.clone();
            v.protected_playout = self.playback.protected;
            v.absorb_playout = self.playback.absorb;
            (v.heard, v.unheard) = self.playback.heard_split(now);
            v.tts_ended_at = self.playback.ended_at;
        }
    }
    /// Move floor effects into host work, applying pure clock changes immediately.
    pub fn collect(&mut self) {
        self.collect_line();
        if let Some(f) = &mut self.floor {
            self.gap_active = f.transport.gap_active;
            if f.transport.typing_arm_deadline == 0.0 {
                self.typing_deadline = None;
            }
            for effect in f.drain_effects() {
                match effect {
                    Effect::PauseSilence => self.clocks.pause_silence(),
                    Effect::ResumeSilence => self.clocks.resume_silence(),
                    // A dispatched clear owns receipt/clock reconciliation; do not close a
                    // newer response here after the async cut has already been requested.
                    Effect::ClearPendingMarks
                    | Effect::CloseLedger { .. }
                    | Effect::NotePlayoutEnd { .. } => {}
                    other => self.actions.push(Action::Floor(other)),
                }
            }
            if let Some(request) = f.begin_classification() {
                self.actions.push(Action::Classifier(request));
            }
        }
        for event in std::mem::take(&mut self.playback.events) {
            self.emit(event);
        }
    }
    /// Forward acoustic evidence after native cleaning and the Python-compatible gate.
    pub fn frame(
        &mut self,
        now: f64,
        raw: i64,
        clean: i64,
        threshold: i64,
        duration_ms: f64,
        confirmed: bool,
    ) {
        self.sync(now);
        if let Some(f) = &mut self.floor {
            if confirmed {
                f.transport.candidate_raw_rms = raw;
                f.transport.candidate_clean_rms = clean;
            }
            f.frame(clean, threshold, duration_ms, confirmed);
        }
        self.collect();
    }
    /// Route STT callbacks. Eager never commits a final; resumed invalidates speculation.
    pub fn transcript(&mut self, event: Transcript, onset: Option<f64>, now: f64) {
        self.sync(now);
        match &event {
            Transcript::Interim { text, confidence } => {
                if let Some(f) = &mut self.floor {
                    f.transcript(text, false);
                }
                self.observe_line(false, now);
                if !self.playing && !self.playback.draining(now) {
                    self.clocks.caller_activity();
                }
                if !self.interim_seen {
                    self.interim_seen = true;
                    self.onset = now;
                }
                if self.mode == Mode::Browser {
                    if self.playing {
                        self.actions.push(Action::Cancel);
                    }
                    self.actions.push(Action::Browser(json!({"type":"server.transcript_interim","payload":{"text":text,"confidence":confidence,"is_final":false}})));
                }
            }
            Transcript::Resumed => {
                if let Some(f) = &mut self.floor {
                    f.resume();
                }
                self.clocks.caller_activity();
                self.emit(json!({"kind":"utterance_resumed","at":if self.mode==Mode::Twilio{now}else{0.0}}));
            }
            Transcript::Eager { .. } => {
                self.last_utterance = now;
                if let Some(onset) = onset {
                    self.onset = onset;
                }
                self.publish(event, self.onset, now, false, None, None);
            }
            Transcript::Final { text, .. } => {
                if self.last_utterance == 0.0 || self.mode != Mode::Twilio {
                    self.last_utterance = now;
                }
                if let Some(onset) = onset {
                    self.onset = onset;
                }
                self.interim_seen = false;
                let generation = self.floor.as_ref().map_or(0, |f| f.generation);
                let wait = self.floor.as_mut().and_then(|f| f.transcript(text, true));
                if let Some(wait) = wait {
                    self.pending.push_back(PendingFinal {
                        wait,
                        event,
                        onset: self.onset,
                        at: self.last_utterance,
                        generation,
                    });
                } else {
                    let episode = self.floor.as_ref().and_then(|f| f.current);
                    self.observe_line(true, now);
                    self.publish(
                        event,
                        self.onset,
                        self.last_utterance,
                        true,
                        Some(generation),
                        episode,
                    );
                }
            }
        }
        self.collect();
    }
    /// Publish a finalized callback after floor arbitration and absorb re-evaluation.
    fn publish(
        &mut self,
        event: Transcript,
        onset: f64,
        at: f64,
        final_event: bool,
        generation: Option<u64>,
        episode: Option<u64>,
    ) {
        let (text, confidence, low) = match event {
            Transcript::Eager {
                text,
                confidence,
                low_conf_words,
                ..
            }
            | Transcript::Final {
                text,
                confidence,
                low_conf_words,
                ..
            } => (text, confidence, low_conf_words),
            _ => return,
        };
        self.clocks.caller_activity();
        let mut generation = generation;
        if let Some(f) = &mut self.floor {
            if final_event {
                if let Some(ep) = episode.and_then(|id| f.get_episode(id)) {
                    if episode == f.current && ep.r#final {
                        generation = Some(f.generation);
                    }
                    if f.caller_floor || ep.deferred {
                        f.transport.absorbing_utterance = false;
                        f.transport.absorb_detail = None;
                    } else if ep.decision == Decision::Uncertain && f.active_playback() {
                        f.transport.absorbing_utterance = true;
                        f.transport.absorb_detail = Some(
                            json!({"text":text,"verdict":"timeout_absorb","source":"floor_controller","audio_id":f.transport.last_audio_id}),
                        );
                    }
                }
            }
            if f.transport.absorbing_utterance {
                let detail = f.transport.absorb_detail.clone().unwrap_or(json!({}));
                let verdict = detail["verdict"].as_str().unwrap_or("");
                let policy = Policy {
                    backchannel_max_tokens: f.cfg.semantic_barge_backchannel_max_tokens,
                    substantive_min_tokens: f.cfg.semantic_barge_substantive_min_tokens,
                    safety_phrases: f.transport.safety_phrases.clone(),
                    floor_phrases: f.transport.floor_phrases.clone(),
                    playout_text: self.playback.text.clone(),
                };
                let swallow =
                    !final_event || should_swallow_absorbed_final(verdict, &text, &policy);
                if final_event {
                    f.transport.absorbing_utterance = false;
                    f.transport.absorb_detail = None;
                }
                if swallow {
                    let saved = if final_event {
                        detail["text"]
                            .as_str()
                            .filter(|t| !t.is_empty())
                            .unwrap_or(&text)
                    } else {
                        &text
                    };
                    self.emit(json!({"kind":"utterance_absorbed","text":saved,"phase":if final_event{"final"}else{"eager"},"verdict":verdict,"source":detail["source"].as_str().unwrap_or(""),"window_ms":detail["window_ms"].as_f64().unwrap_or(0.0),"audio_id":detail["audio_id"].as_str().unwrap_or(""),"final_text":if final_event{&text}else{""},"onset_at":onset,"at":at}));
                    if final_event {
                        self.recent_absorbed = Some(
                            json!({"text":text,"verdict":verdict,"at":self.floor.as_ref().unwrap().now}),
                        );
                    }
                    return;
                }
            }
            if final_event {
                f.accepted_generation = generation;
            }
        }
        self.gap_active = true;
        self.typing_deadline = Some(self.floor.as_ref().map_or(at, |f| f.now) + self.typing_delay);
        if let Some(f) = &mut self.floor {
            f.transport.gap_active = true;
            f.transport.typing_arm_deadline = self.typing_deadline.unwrap();
        }
        let now = self.floor.as_ref().map_or(at, |f| f.now);
        if self
            .recent_absorbed
            .as_ref()
            .is_some_and(|v| now - v["at"].as_f64().unwrap_or(0.0) > 20.0)
        {
            self.recent_absorbed = None;
        }
        let carry = if final_event {
            self.recent_absorbed.take()
        } else {
            self.recent_absorbed.clone()
        };
        let phone = self.mode == Mode::Twilio;
        if self.mode == Mode::Browser && final_event {
            self.actions.push(Action::Browser(json!({"type":"server.transcript_final","payload":{"text":text,"confidence":confidence}})));
        }
        self.emit(json!({"kind":if final_event{"utterance_final"}else{"utterance_eager"},"text":text,"confidence":confidence,"low_conf_words":low,"onset_at":if phone{onset}else{0.0},"at":if phone||final_event{at}else{0.0},"heard":if phone{serde_json::to_value(self.playback.ledger.horizon(onset)).unwrap()}else{json!({"completed":[],"in_flight":null,"in_flight_heard_text":"","last_heard_text":"","unheard_text":""})},"carry_forward":carry,"playback_generation":if phone{generation.or_else(||self.floor.as_ref().map(|f|f.generation))}else{None}}));
    }
    /// Drive floor deadlines every 50 ms and the original call clock every second.
    pub fn tick(&mut self, now: f64) {
        self.sync(now);
        if let Some(line) = &mut self.line {
            line.tick(now);
        }
        if let Some(f) = &mut self.floor {
            f.classifier_deadline();
            f.tick();
        }
        while let Some(pending) = self.pending.front() {
            if !self.floor.as_mut().unwrap().poll_final(&pending.wait) {
                break;
            }
            let pending = self.pending.pop_front().unwrap();
            self.observe_line_episode(true, now, Some(pending.wait.episode_id));
            self.publish(
                pending.event,
                pending.onset,
                pending.at,
                true,
                Some(pending.generation),
                Some(pending.wait.episode_id),
            );
        }
        if now >= self.next_clock_tick {
            for event in self.clocks.tick(now) {
                self.emit(event);
            }
            self.next_clock_tick = now + 1.0;
        }
        self.collect();
    }
    /// Start delivery accounting after generation-aware admission succeeds.
    pub fn begin(&mut self, id: String, text: String, protected: bool, absorb: bool, now: f64) {
        self.playing = true;
        self.gap_active = false;
        self.typing_deadline = None;
        self.last_utterance = 0.0;
        if self.mode != Mode::Twilio {
            self.playback.last_audio_id = id;
            self.playback.text = text;
            self.playback.started_at = now;
            return;
        }
        self.playback
            .begin(id.clone(), text, now, protected, absorb);
        self.clocks.playout_start(is_turn_audio(&id));
        if let Some(line) = &mut self.line {
            line.playout_start(now);
        }
        self.sync(now);
    }
    /// A terminal echo is the phone's audible completion; sent bytes alone are not.
    pub fn mark(&mut self, name: &str, now: f64) {
        if self.playback.mark_echo(name, now) {
            if let Some(line) = &mut self.line {
                line.playout_end(now);
            }
            self.clocks
                .playout_end(is_turn_audio(&self.playback.last_audio_id));
            self.sync(now);
            if let Some(f) = &mut self.floor {
                f.playback_finished();
            }
        }
        self.collect();
    }
    /// Reconcile one successfully dispatched clear with marks, ledger and clocks.
    pub fn cleared(&mut self, now: f64) {
        if self.mode != Mode::Twilio {
            self.playing = false;
            self.playback.ended_at = now;
            return;
        }
        let active = self.clocks.playing_depth > 0;
        self.playing = false;
        self.playback.cleared(now);
        if active {
            self.clocks.playout_end(false);
            if let Some(line) = &mut self.line {
                line.playout_end(now);
            }
        }
        self.sync(now);
        self.collect();
    }
    /// Supply one tokenized instrument result and execute its verdict effects in order.
    pub fn line_result(&mut self, token: u64, probability: Option<f64>, now: f64) {
        self.sync(now);
        if let Some(line) = &mut self.line {
            line.complete(token, probability, now);
        }
        self.collect();
    }
    fn observe_line(&mut self, final_event: bool, now: f64) {
        self.observe_line_episode(final_event, now, None);
    }
    fn observe_line_episode(&mut self, final_event: bool, now: f64, episode_id: Option<u64>) {
        let episode = self
            .floor
            .as_ref()
            .and_then(|f| episode_id.or(f.current).and_then(|id| f.get_episode(id)))
            .cloned();
        if let Some(line) = &mut self.line {
            let was_bound = line.bound;
            line.observe(
                episode.as_ref().map(|e| vogon_front::line::Episode {
                    id: e.id,
                    onset: e.onset,
                    text: e.text.clone(),
                    echo: e.echo,
                    possible_echo: e.possible_echo,
                }),
                final_event,
                &self.playback.text,
                now,
            );
            if !was_bound && line.bound {
                self.line_episode = episode;
            }
        }
    }
    fn collect_line(&mut self) {
        let now = self.floor.as_ref().map_or(0.0, |f| f.now);
        let Some(line) = &mut self.line else {
            return;
        };
        if let Some(request) = line.begin_request(now) {
            self.actions.push(Action::Line(request));
        }
        for effect in line.drain_effects() {
            match effect {
                vogon_front::line::Effect::Breakthrough { .. } => {
                    if let (Some(floor), Some(episode)) = (&mut self.floor, &self.line_episode) {
                        floor.line_machine(episode.clone());
                    }
                }
                vogon_front::line::Effect::Event(mut event) => {
                    if event["kind"] == "utterance_final" {
                        event["playback_generation"] =
                            json!(self.floor.as_ref().map_or(0, |f| f.generation));
                    }
                    self.emit(event);
                }
            }
        }
    }
}
/// Silence probes pause idle accumulation but must not reset its window.
pub fn is_turn_audio(id: &str) -> bool {
    !id.starts_with("silence-probe") && !id.starts_with("silence-exit")
}
