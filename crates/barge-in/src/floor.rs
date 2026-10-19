//! Deterministic floor arbitration. Drive `tick` every 50 ms and after input;
//! dispatch `begin_classification` on the host executor, and return its token
//! on completion. A timed-out provider must finish before another can start.
//!
//! [`Controller`](crate::floor::Controller) owns per-call policy state and [`Episode`](crate::floor::Episode) history. [`Transport`](crate::floor::Transport)
//! is its synchronized host-state view; [`Effect`](crate::floor::Effect) describes work for the host.
//! [`ClassifierRequest`](crate::floor::ClassifierRequest)/[`ClassifierResult`](crate::floor::ClassifierResult) form the provider boundary, while
//! [`FinalWait`](crate::floor::FinalWait) and [`Admission`](crate::floor::Admission) preserve asynchronous continuations without
//! blocking frame processing. No type here performs I/O or starts a timer.
//!
//! Set [`Controller::now`](crate::floor::Controller::now) before each event. Drive classifier and final deadlines
//! separately from heartbeat ticks, drain effects in order, and stop delivering
//! input before closing the controller. Requests/tokens belong to one call only.
use std::collections::{BTreeMap, VecDeque};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use crate::semantics::{self, Policy};

/// Timing and text-policy limits for one floor controller.
///
/// Durations are milliseconds; the controller converts them to its injected
/// monotonic-seconds clock. Field names match gateway settings so hosts can
/// transfer configuration without duplicating naming conventions.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    /// Quiet gap that ends an acoustic episode and rearms the gate.
    pub barge_episode_gap_ms:f64,
    /// Qualified speech needed to yield when a recent unsuccessful attempt exists.
    pub barge_repeat_speech_ms:f64,
    /// Qualified speech duration that can force a yield even after a continue verdict.
    pub barge_persistent_speech_ms:f64,
    /// Retention window used when judging recent attempts and reporting failures.
    pub barge_attempt_history_ms:f64,
    /// Quiet time required after caller EOT before releasing the caller floor.
    pub barge_floor_silence_ms:f64,
    /// Elapsed episode time before unresolved text becomes an unsuccessful attempt.
    pub semantic_barge_window_ms:f64,
    /// Bound for each classifier request and each independent final-transcript wait.
    pub semantic_barge_llm_timeout_ms:f64,
    /// Maximum token count for the semantic backchannel fast path.
    pub semantic_barge_backchannel_max_tokens:i64,
    /// Minimum token count for the substantive-interruption fast path.
    pub semantic_barge_substantive_min_tokens:i64,
}
impl Default for Config {
    /// Construct the reference gateway timing/token defaults without reading environment settings.
    fn default()->Self {Self {barge_episode_gap_ms:350.0,barge_repeat_speech_ms:240.0,
        barge_persistent_speech_ms:1200.0,barge_attempt_history_ms:10000.0,
        barge_floor_silence_ms:500.0,semantic_barge_window_ms:900.0,
        semantic_barge_llm_timeout_ms:700.0,semantic_barge_backchannel_max_tokens:3,
        semantic_barge_substantive_min_tokens:6}}
}
/// Host transport state mirrored into, and updated by, the controller.
///
/// The host supplies playout, policy and audio evidence before each input. The
/// controller changes absorb/gap/typing/drain state and the streaming-cut end
/// timestamp as it emits effects; the host reconciles those changes with actual
/// I/O. This value contains no socket, audio buffer, VAD or provider client.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Transport {
    /// Whether an agent TTS stream is actively producing/sending audio.
    pub tts_playing:bool,
    /// Whether already-sent agent audio remains queued for audible playback.
    pub playback_draining:bool,
    /// Identity of the current/latest agent playout, included in stale-result checks.
    pub last_audio_id:Option<String>,
    /// Full agent line used for echo checks and classifier context.
    pub last_speak_text:String,
    /// Whether non-safety interruptions must defer until this span ends.
    pub protected_playout:bool,
    /// Whether protected introductory playout absorbs non-safety speech.
    pub absorb_playout:bool,
    /// Application-provided immediate-breakthrough substrings.
    pub safety_phrases:Vec<String>,
    /// Application-provided token phrases requesting the floor.
    pub floor_phrases:Vec<String>,
    /// Whether ambiguous qualified/final text may request remote classification.
    pub semantic_judge:bool,
    /// Current heard-text prefix, captured when a new acoustic episode opens.
    pub heard:String,
    /// Current unheard-text suffix, captured alongside the heard prefix.
    pub unheard:String,
    /// Provisional suppression state; every new transcript revision can overturn it.
    pub absorbing_utterance:bool,
    /// Suppression context for the current text, verdict reason and audio identity.
    pub absorb_detail:Option<Value>,
    /// Host gap/filler state cleared when the caller takes the floor.
    pub gap_active:bool,
    /// Host typing-sound arm deadline, cleared to zero when yielding.
    pub typing_arm_deadline:f64,
    /// Whether a host ambience mixer exists and needs a stop-typing effect.
    pub ambience:bool,
    /// Host playout-end timestamp in monotonic seconds; set on an immediate streaming cut.
    pub tts_ended_at:f64,
    /// Raw candidate-frame RMS forwarded to cut telemetry.
    pub candidate_raw_rms:i64,
    /// Cleaned candidate-frame RMS forwarded to cut telemetry.
    pub candidate_clean_rms:i64,
}
/// Semantic floor decision attached to an episode.
///
/// A yield verdict may still be deferred by protection; it does not alone mean
/// that an audible cut has happened.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all="UPPERCASE")]
pub enum Decision {
    /// Keep agent playback and provisionally absorb this transcript.
    Continue,
    /// Request caller ownership, subject to protected-span policy.
    Yield,
    /// Gather more evidence; deadlines and speech pressure still apply.
    Uncertain
}
/// Evidence and semantic state for one acoustic speech attempt.
///
/// An episode starts on new speech (or the first transcript) and ends after a
/// quiet gap. Transcript revisions do not create extra acoustic attempts.
/// Heard/unheard context is captured at onset; final callbacks and classifier
/// requests can keep an episode alive after the 16-entry history evicts it.
#[derive(Clone, Debug, Serialize)]
pub struct Episode {
    /// Per-call, monotonically allocated episode identity.
    pub id:u64,
    /// Monotonic time when this episode opened, in seconds.
    pub onset:f64,
    /// Floor generation when the episode opened; current ownership lives on the controller.
    pub generation:u64,
    /// Most recent above-threshold frame time, initialized to onset.
    pub last_speech:f64,
    /// Accumulated above-threshold duration; each frame contributes at most 100 ms.
    pub speech_ms:f64,
    /// Latest partial or final transcript associated with the attempt.
    pub text:String,
    /// Transcript/staleness revision; changed text and final deadlines advance it.
    pub revision:u64,
    /// Whether an acoustic gate confirmation qualified this episode.
    pub qualified:bool,
    /// Whether the acoustic quiet-gap boundary has closed this attempt.
    pub ended:bool,
    /// Most recently applied semantic decision, including a protected/deferred yield.
    pub decision:Decision,
    /// Monotonic time of the first unsuccessful attempt, when applicable.
    pub failed_at:Option<f64>,
    /// Whether the latest transcript was final; subsequent loud frames clear this episode flag.
    pub r#final:bool,
    /// Whether text is recognized as leaked agent speech.
    pub echo:bool,
    /// Whether a short partial may be agent echo and needs another revision or final.
    pub possible_echo:bool,
    /// Whether this episode awaits the end of protected playout.
    pub deferred:bool,
    /// Agent text considered heard when the episode opened.
    pub heard:String,
    /// Agent text still unheard when the episode opened.
    pub unheard:String,
}
/// Staleness key: `(episode_id, revision, floor_generation, audio_id)`.
///
/// All four parts must still match before a provider verdict can affect the floor.
/// This tuple tracks decision context; the separate request token tracks dispatch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Key(
    /// Episode identity within this call.
    pub u64,
    /// Episode revision at the time classification was queued.
    pub u64,
    /// Controller floor generation at the time classification was queued.
    pub u64,
    /// Agent audio identity at the time classification was queued.
    pub Option<String>
);
/// Immutable payload for one remote judge call, dispatched by the host.
///
/// The episode stores onset context; text and attempt history are read at
/// dispatch. The original queued key remains attached even if the episode text
/// changed in the meantime, allowing completion to reject stale work.
#[derive(Clone, Debug, Serialize)]
pub struct ClassifierRequest {
    /// Per-call dispatch token the host must return with a result/error.
    pub token:u64,
    /// Queued episode/revision/generation/audio context used to reject stale answers.
    pub key:Key,
    /// Episode transcript at dispatch time.
    pub text:String,
    /// Agent prefix captured at episode onset.
    pub heard:String,
    /// Agent suffix captured at episode onset.
    pub unheard:String,
    /// Full current agent line at dispatch time.
    pub agent_line:String,
    /// Recent other episodes, including decisions, unsuccessful flags and ages in milliseconds.
    pub attempts:Vec<Value>,
    /// Absolute monotonic-seconds deadline for the host classifier timer.
    pub deadline:f64,
}
/// Private record reserving the single physical classifier slot.
///
/// Timeout changes this record to draining rather than freeing the slot; the
/// host must report the eventual provider completion before dispatch can resume.
#[derive(Clone, Debug)]
struct Flight {
    /// Dispatched payload and its immutable identity/context.
    request:ClassifierRequest,
    /// Dispatch time used for classifier latency telemetry, in seconds.
    started:f64,
    /// True after timeout while the underlying provider operation is still outstanding.
    draining:bool
}
/// Host-reported provider outcome; processing is separate from executing I/O.
#[derive(Clone, Debug)]
pub enum ClassifierResult {
    /// Raw provider verdict; unknown strings become an uncertain decision.
    Verdict(String),
    /// Provider failure, mapped to an uncertain decision for a current request.
    Error,
    /// Deadline elapsed; the physical request stays reserved until a later result/error.
    Timeout
}
/// Ordered transport work produced by a controller transition.
///
/// The controller updates policy state immediately; the host performs these
/// actions in order and handles I/O completion/failure. Emitting an effect does
/// not itself prove that a clear packet was sent or a provider stopped.
#[derive(Clone, Debug, Serialize)]
#[serde(tag="kind",rename_all="snake_case")]
pub enum Effect {
    /// Append a structured audio-event record.
    Log {
        /// Injected monotonic time at the decision, in seconds.
        now:f64,
        /// Stable event name shared with the Python controller.
        event:String,
        /// Event-specific fields plus common episode/floor context.
        detail:Value
    },
    /// Pause the host silence policy while the caller owns the floor.
    PauseSilence,
    /// Resume the host silence policy after release or machine takeover.
    ResumeSilence,
    /// Rearm the acoustic gate for a new episode or after an echo revision.
    ResetGate,
    /// Stop ambience typing sounds because the caller is taking the floor.
    StopTyping,
    /// Record energy evidence for the audible interruption.
    CutTelemetry {
        /// Candidate RMS before echo cleaning.
        raw_rms:i64,
        /// Candidate RMS after echo cleaning.
        clean_rms:i64
    },
    /// Cancel active TTS, or clear buffered drain playback when streaming has ended.
    CutPlayback,
    /// Send an immediate transport clear without waiting for another TTS chunk.
    SendClear,
    /// Remove outstanding playout marks so discarded audio no longer appears queued.
    ClearPendingMarks,
    /// Close delivery accounting at the cut's estimated audible end.
    CloseLedger {
        /// Monotonic cut time in seconds.
        now:f64
    },
    /// Notify host playout clocks/listeners that agent audio ended.
    NotePlayoutEnd {
        /// Whether to reset the silence window; floor cuts preserve it with false.
        reset_window:bool
    },
}
/// Continuation handle for a bounded final-transcript/EOT decision.
///
/// Owned by the host and polled against the same controller. The controller
/// retains the canonical deadline and original worker identity; cloning or
/// re-polling the handle cannot complete another final wait.
#[derive(Clone, Debug)]
pub struct FinalWait {
    /// Episode whose final callback is awaiting arbitration.
    pub episode_id:u64,
    /// Absolute monotonic-seconds deadline exposed for host timer scheduling.
    pub deadline:f64,
    /// Private per-call final-wait identity used for idempotent completion.
    token:u64,
    /// Original classifier worker lifetime; a later worker cannot extend this wait.
    worker:u64
}
/// Continuation for one generation-aware playback admission attempt.
///
/// The host keeps this token while the result is waiting, then polls the same
/// token after release. Captured event epochs preserve a wakeup even when
/// protection or caller ownership changes again before the host resumes.
#[derive(Clone, Debug)]
pub struct Admission {
    /// Generation of the response requesting playback, if supplied.
    generation:Option<u64>,
    /// Requested audio identity used in admission telemetry.
    audio_id:Option<String>,
    /// Whether blocked playback holds for release or is immediately dropped.
    wait:bool,
    /// Continuation stage: 0 initial, 1 protection, 2 caller release, 3 admit, 4 allowed, 5 rejected.
    phase:u8,
    /// Protection-release counter observed when this request started waiting.
    protection_epoch:Option<u64>,
    /// Caller-floor release counter observed when this request started waiting.
    release_epoch:Option<u64>
}
/// Outcome of starting or resuming a playback admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all="lowercase")]
pub enum AdmissionResult {
    /// Playback may proceed for this request; this token remains terminally allowed.
    Allowed,
    /// Drop this request because it is stale or blocked without permission to wait.
    Rejected,
    /// Retain the admission token and poll it after the relevant release.
    Waiting
}

/// Per-call owner of episode history, floor policy and asynchronous continuations.
///
/// Methods advance state synchronously and enqueue effects; they do not block
/// on VAD, providers, sockets or timers. The host updates `now`, synchronizes
/// `transport`, records `accepted_generation` when a reply is accepted, and
/// serializes all calls. Treat other public state as observations: direct
/// mutation bypasses release epochs and stale-result invariants.
///
/// History exposes at most 16 episodes. Extra episodes survive only while
/// referenced by current state, pending/active classification or final waits.
pub struct Controller {
    /// Timing and semantic limits resolved for this call.
    pub cfg:Config,
    /// Synchronized host-state view, also carrying controller-induced state changes.
    pub transport:Transport,
    /// Host-supplied monotonic seconds; update before processing each event.
    pub now:f64,
    /// Episode storage, including objects retained beyond visible history by outstanding work.
    episodes:BTreeMap<u64,Episode>,
    /// Visible episode history in onset order, bounded to 16 identities.
    order:VecDeque<u64>,
    /// Current/latest episode identity, including an ended episode awaiting late revisions.
    pub current:Option<u64>,
    /// Per-call episode identity counter.
    pub serial:u64,
    /// Current floor generation used to invalidate replies and classifier results.
    pub generation:u64,
    /// Whether caller ownership currently prevents agent response admission.
    pub caller_floor:bool,
    /// Whether a final was dispatched and its response has not yet been admitted.
    pub awaiting_reply:bool,
    /// Most recent above-threshold frame time used by caller-silence release.
    pub last_speech:f64,
    /// Whether caller EOT permits release after silence; acoustic blips do not clear it.
    pub final_seen:bool,
    /// Current caller-floor release-event state, mirrored from Python asyncio semantics.
    pub released:bool,
    /// Current protected-span release-event state.
    pub protection_released:bool,
    /// Counter preserving caller-release pulses across delayed admission polling.
    release_epoch:u64,
    /// Counter preserving protection-release pulses across delayed admission polling.
    protection_epoch:u64,
    /// Worker-lifetime identity binding final waits to their original arbitration task.
    worker_serial:u64,
    /// Latest queued ambiguous revision; later revisions replace this work.
    pending:Option<Key>,
    /// Most recently dispatched classification key, used to avoid duplicate requests.
    pub requested:Option<Key>,
    /// Single physically outstanding provider call, including a timed-out draining call.
    flight:Option<Flight>,
    /// Whether the classifier worker is scheduled/active even before dispatch.
    worker_scheduled:bool,
    /// Generation of an accepted reply; clears failure history only after that reply plays.
    pub accepted_generation:Option<u64>,
    /// Late partial during an acoustic gap, to bind when the next episode opens.
    pub pending_text:Option<String>,
    /// Per-call physical provider dispatch counter.
    request_serial:u64,
    /// Canonical active final waits, indexed by their unique completion tokens.
    final_waits:BTreeMap<u64,FinalWait>,
    /// Per-call final-wait token counter.
    final_serial:u64,
    /// Ordered host actions accumulated since the last drain.
    effects:Vec<Effect>,
    /// Whether future classifier dispatch is disabled; the host must also stop new inputs.
    closed:bool,
}
impl Controller {
    /// Create a controller with an open floor, no episodes and no background work.
    ///
    /// `now` anchors the host clock; construction does not emit the policy log,
    /// start a heartbeat or call a provider. Use `start` to emit policy telemetry.
    pub fn new(cfg:Config,transport:Transport,now:f64)->Self {
        Self {cfg,transport,now,episodes:BTreeMap::new(),order:VecDeque::new(),current:None,
            serial:0,generation:0,caller_floor:false,awaiting_reply:false,last_speech:0.0,final_seen:false,
            released:true,protection_released:true,release_epoch:0,protection_epoch:0,worker_serial:0,pending:None,requested:None,flight:None,worker_scheduled:false,
            accepted_generation:None,pending_text:None,request_serial:0,final_waits:BTreeMap::new(),final_serial:0,effects:vec![],closed:false}
    }
    /// Set the protection event and preserve a release pulse for already-waiting admissions.
    fn release_protection(&mut self) {
        if !self.protection_released {self.protection_epoch+=1;}
        self.protection_released=true;
    }
    /// Set the caller-release event and retain its wakeup across later caller resumption.
    fn release_floor(&mut self) {
        if !self.released {self.release_epoch+=1;}
        self.released=true;
    }
    /// Return true while either agent streaming or buffered audible playback is active.
    pub fn active_playback(&self)->bool {self.transport.tts_playing||self.transport.playback_draining}
    /// Take all pending host actions in emission order, leaving the queue empty.
    ///
    /// The host owns execution and error handling after the actions are returned.
    pub fn drain_effects(&mut self)->Vec<Effect> {std::mem::take(&mut self.effects)}
    /// Iterate the visible history in onset order, excluding evicted-but-retained episodes.
    pub fn episodes(&self)->impl Iterator<Item=&Episode> {self.order.iter().map(|id|&self.episodes[id])}
    /// Look up stored evidence by identity, including episodes retained by outstanding work.
    pub fn get_episode(&self,id:u64)->Option<&Episode> {self.episodes.get(&id)}
    /// Report scheduled or physically in-flight classifier work, including timeout draining.
    pub fn worker_active(&self)->bool {self.worker_scheduled||self.flight.is_some()}
    /// Inspect the latest queued revision without dispatching or removing it.
    pub fn pending_key(&self)->Option<&Key> {self.pending.as_ref()}
    /// Build the current staleness key for an episode known to be present in storage.
    fn key(&self,id:u64)->Key {Key(id,self.episodes[&id].revision,self.generation,self.transport.last_audio_id.clone())}
    /// Collect visible episodes whose failure timestamp is within the configured history window.
    fn recent_failures(&self)->Vec<u64> {self.episodes().filter(|e|e.failed_at.is_some_and(|at|self.now-at<=self.cfg.barge_attempt_history_ms/1000.0)).map(|e|e.id).collect()}
    /// Queue a structured log with current episode, generation, playout and failure context.
    ///
    /// An omitted episode uses `current`; `extra` must be a JSON object and may
    /// override common fields. Speech milliseconds use Python-compatible ties-to-even rounding.
    fn emit(&mut self,event:&str,reason:&str,id:Option<u64>,extra:Value) {
        let e=id.or(self.current).and_then(|id|self.episodes.get(&id));
        let mut detail=json!({"schema_version":1,"reason":reason,"episode_id":e.map(|e|e.id),
            "revision":e.map(|e|e.revision),"generation":self.generation,"audio_id":self.transport.last_audio_id,
            "floor":if self.caller_floor {"caller"} else if self.active_playback() {"agent"} else {"open"},
            "protected":self.transport.protected_playout,"intro_absorb":self.transport.absorb_playout,
            "unsuccessful_attempts":self.recent_failures(),"speech_ms":e.map_or(0,|e|e.speech_ms.round_ties_even() as i64)});
        detail.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        self.effects.push(Effect::Log {now:self.now,event:event.into(),detail});
    }
    /// Emit the resolved floor-policy limits.
    ///
    /// Unlike the Python async controller, this method starts no timer. The host
    /// must schedule the 50 ms heartbeat and classifier/final deadlines separately.
    pub fn start(&mut self) {self.emit("floor_policy","controller_started",None,json!({
        "decision_ms":self.cfg.semantic_barge_window_ms,"repeat_ms":self.cfg.barge_repeat_speech_ms,
        "persistent_ms":self.cfg.barge_persistent_speech_ms,"episode_gap_ms":self.cfg.barge_episode_gap_ms,
        "silence_ms":self.cfg.barge_floor_silence_ms}));}
    /// Discard pending work and final waits, forget the active request, and reclaim
    /// unreferenced history. Late completions are ignored because their slot is gone.
    ///
    /// This is not a guard on every input method: the host must stop delivering
    /// frames/transcripts and drop admission tokens when the call closes. Cancel
    /// host timers/waiters and drain any physical provider operation separately;
    /// dropping its future does not stop underlying I/O.
    pub fn close(&mut self) {self.closed=true;self.pending=None;self.worker_scheduled=false;self.flight=None;self.final_waits.clear();self.gc();}
    /// Reclaim evicted episodes only after current, pending, in-flight and final-wait references end.
    fn gc(&mut self) {
        self.episodes.retain(|id,_|self.order.contains(id)||self.current==Some(*id)||
            self.pending.as_ref().is_some_and(|k|k.0==*id)||self.flight.as_ref().is_some_and(|f|f.request.key.0==*id)||self.final_waits.values().any(|w|w.episode_id==*id));
    }
    /// Reuse the open episode or create a new acoustic attempt with captured playout context.
    ///
    /// Bind any delayed partial, enforce the 16-entry visible history limit and
    /// emit onset telemetry. Opening an acoustic episode never acquires the floor
    /// or increments a generation by itself.
    fn episode(&mut self)->u64 {
        if let Some(id)=self.current {if !self.episodes[&id].ended {return id;}}
        self.serial+=1;let id=self.serial;
        let text=self.pending_text.take();
        let e=Episode {id,onset:self.now,generation:self.generation,last_speech:self.now,
            speech_ms:0.0,revision:if text.is_some(){1}else{0},text:text.unwrap_or_default(),qualified:false,
            ended:false,decision:Decision::Uncertain,failed_at:None,r#final:false,echo:false,possible_echo:false,
            deferred:false,heard:self.transport.heard.clone(),unheard:self.transport.unheard.clone()};
        self.episodes.insert(id,e);self.order.push_back(id);if self.order.len()>16 {self.order.pop_front();}
        self.current=Some(id);self.emit("speech_episode_started","new_speech_attempt",Some(id),json!({}));self.gc();id
    }
    /// Apply explicit caller resumption from words or a turn-resumed event.
    ///
    /// Resumption after a final invalidates its reply by advancing the generation.
    /// With no active agent playback, acquire caller ownership and pause silence;
    /// energy frames alone must never call this method.
    pub fn resume(&mut self) {
        if self.caller_floor {
            if self.final_seen {self.generation+=1;self.emit("floor_acquired","caller_resumed_after_final",None,json!({}));}
            self.final_seen=false;
        } else if !self.active_playback() {
            self.generation+=1;self.caller_floor=true;self.effects.push(Effect::PauseSilence);
            self.final_seen=false;self.released=false;self.emit("floor_acquired","caller_resumed_before_response",None,json!({}));
        }
    }
    /// Accumulate acoustic evidence and evaluate time-based floor pressure.
    ///
    /// `duration_ms` contributes at most 100 ms per above-threshold frame. A first
    /// gate confirmation qualifies the episode and reviews its text. Quiet gaps
    /// end episodes and rearm the gate. Loud frames update the silence anchor but
    /// do not clear controller-level EOT or independently acquire caller ownership.
    pub fn frame(&mut self,rms:i64,threshold:i64,duration_ms:f64,confirmed:bool) {
        if rms>threshold {
            let id=self.episode();let e=self.episodes.get_mut(&id).unwrap();
            e.speech_ms+=duration_ms.min(100.0);e.r#final=false;e.last_speech=self.now;self.last_speech=self.now;
            if confirmed && !e.qualified {
                e.qualified=true;self.emit("speech_episode_qualified","energy_gate_confirmed",Some(id),json!({"clean_rms":rms,"threshold":threshold}));self.review(id,false);
            }
        } else if let Some(id)=self.current {
            if !self.episodes[&id].ended && self.now-self.episodes[&id].last_speech>=self.cfg.barge_episode_gap_ms/1000.0 {
                self.episodes.get_mut(&id).unwrap().ended=true;
                self.emit("speech_episode_ended","speech_gap",Some(id),json!({}));self.effects.push(Effect::ResetGate);
            }
        }
        self.tick();
    }
    /// Apply a partial/final revision, reset stale echo qualification and review policy.
    ///
    /// A delayed partial on an ended episode also seeds the next acoustic attempt.
    /// A final marks a reply as outstanding and can enable caller-silence release.
    /// `None` means this callback requires no arbitration wait; `Some` identifies
    /// a bounded EOT continuation to poll on worker completion or deadline.
    pub fn transcript(&mut self,text:&str,is_final:bool)->Option<FinalWait> {
        let id=match self.current {Some(id)=>id,None=>self.episode()};
        if self.episodes[&id].ended && !is_final {self.pending_text=Some(text.into());}
        if is_final {self.pending_text=None;}
        let changed=text!=self.episodes[&id].text;
        if !text.is_empty() && (!is_final||changed) && (self.caller_floor||(self.awaiting_reply&&!self.active_playback())) {self.resume();}
        if changed {
            let e=self.episodes.get_mut(&id).unwrap();
            if e.echo {e.speech_ms=0.0;e.qualified=false;self.effects.push(Effect::ResetGate);}
            e.text=text.into();e.revision+=1;e.decision=Decision::Uncertain;e.echo=false;
            self.emit("speech_revision",if is_final{"final"}else{"partial"},Some(id),json!({"text":text}));
        }
        self.episodes.get_mut(&id).unwrap().r#final=is_final;
        if is_final {self.awaiting_reply=true;}
        if self.caller_floor&&is_final {self.final_seen=true;}
        if is_final||changed {self.review(id,is_final);}
        if is_final {
            if self.episodes[&id].decision==Decision::Uncertain && self.worker_active() {
                self.final_serial+=1;
                let wait=FinalWait {episode_id:id,deadline:self.now+self.cfg.semantic_barge_llm_timeout_ms/1000.0,token:self.final_serial,worker:self.worker_serial};
                self.final_waits.insert(wait.token,wait.clone());
                return Some(wait);
            }
            self.finish_final(id);
        }
        None
    }
    /// Resume one EOT continuation; return true when it is finished or already consumed.
    ///
    /// Return false only while its original worker remains active before the
    /// deadline. Expiry increments the episode revision to invalidate late cuts,
    /// then commits the bounded final outcome. It does not cancel the provider or
    /// wait for a newer worker. Unknown/consumed tokens are idempotently complete.
    pub fn poll_final(&mut self,wait:&FinalWait)->bool {
        let Some(wait)=self.final_waits.get(&wait.token).cloned() else {return true;};
        if self.worker_active() && wait.worker==self.worker_serial {
            if self.now<wait.deadline {return false;}
            self.episodes.get_mut(&wait.episode_id).unwrap().revision+=1;
            self.emit("floor_result_rejected","final_decision_deadline",Some(wait.episode_id),json!({}));
        }
        self.finish_final(wait.episode_id);
        self.final_waits.remove(&wait.token);
        self.gc();true
    }
    /// Mark a still-unresolved overlap as unsuccessful when appropriate, then run a floor tick.
    fn finish_final(&mut self,id:u64) {
        if self.episodes[&id].decision==Decision::Uncertain && self.active_playback()&&!self.caller_floor {self.fail(id,"eot_unresolved");}
        self.tick();
    }
    /// Record the first failure timestamp and log it, excluding definite and possible echo.
    fn fail(&mut self,id:u64,reason:&str) {
        let e=self.episodes.get_mut(&id).unwrap();
        if e.failed_at.is_none()&&!e.echo&&!e.possible_echo {e.failed_at=Some(self.now);self.emit("speech_attempt_unsuccessful",reason,Some(id),json!({}));}
    }
    /// Build recent-other-attempt classifier context for the supplied episode identity.
    ///
    /// The history window uses last-speech time, while reported age uses onset.
    /// Ages are milliseconds rounded with Python-compatible ties-to-even behavior.
    pub fn history(&self,id:u64)->Vec<Value> {
        self.episodes().filter(|e|e.id!=id&&self.now-e.last_speech<=self.cfg.barge_attempt_history_ms/1000.0)
            .map(|e|json!({"episode_id":e.id,"text":e.text,"decision":e.decision,"unsuccessful":e.failed_at.is_some(),"age_ms":((self.now-e.onset)*1000.0).round_ties_even() as i64})).collect()
    }
    /// Apply text policy to an existing episode while agent playback is active.
    ///
    /// Check echo, tentative partial echo, safety breakthrough, protected intro,
    /// energy qualification and decisive heuristics in that order. Ambiguity
    /// either defers behind protection, queues/replaces judge work, or stays
    /// uncertain when classification is disabled. No provider is executed here.
    fn review(&mut self,id:u64,is_final:bool) {
        if self.episodes[&id].text.is_empty()||!self.active_playback()||self.caller_floor {return;}
        self.episodes.get_mut(&id).unwrap().possible_echo=false;
        if semantics::is_echo_of(&self.episodes[&id].text,&self.transport.last_speak_text,4) {
            let e=self.episodes.get_mut(&id).unwrap();e.echo=true;e.failed_at=None;
            self.decide(id,Decision::Continue,"agent_echo",false,true);return;
        }
        let policy=Policy {backchannel_max_tokens:self.cfg.semantic_barge_backchannel_max_tokens,
            substantive_min_tokens:self.cfg.semantic_barge_substantive_min_tokens,
            safety_phrases:self.transport.safety_phrases.clone(),floor_phrases:self.transport.floor_phrases.clone(),playout_text:self.transport.last_speak_text.clone()};
        let h=semantics::heuristic(&self.episodes[&id].text,&policy);
        if !is_final&&h=="ambiguous"&&semantics::is_echo_of(&self.episodes[&id].text,&self.transport.last_speak_text,2) {
            let e=self.episodes.get_mut(&id).unwrap();e.possible_echo=true;e.decision=Decision::Uncertain;
            self.emit("floor_decision","possible_agent_echo",Some(id),json!({"decision":"UNCERTAIN","action":"GATHER_MORE"}));return;
        }
        if h=="safety" {self.decide(id,Decision::Yield,"policy_breakthrough",true,false);return;}
        if self.transport.protected_playout&&self.transport.absorb_playout {self.decide(id,Decision::Continue,"protected_intro_absorb",false,true);return;}
        if !(self.episodes[&id].qualified||is_final) {
            self.emit("floor_decision","waiting_for_energy_gate",Some(id),json!({"decision":"UNCERTAIN","action":"GATHER_MORE"}));return;
        }
        match h {
            "backchannel"=>self.decide(id,Decision::Continue,"cooperative_backchannel",false,false),
            "floor_request"=>self.decide(id,Decision::Yield,"policy_floor_request",false,false),
            "substantive_interrupt"=>self.decide(id,Decision::Yield,"substantive_speech",false,false),
            _=> {
                if self.transport.protected_playout {
                    self.episodes.get_mut(&id).unwrap().deferred=true;self.protection_released=false;
                    self.emit("floor_decision","protected_span",Some(id),json!({"decision":"UNCERTAIN","action":"DEFER"}));
                } else if self.transport.semantic_judge {
                    let key=self.key(id);
                    if self.requested.as_ref()!=Some(&key) {
                        if !self.worker_active() {self.worker_serial+=1;}
                        self.pending=Some(key);self.worker_scheduled=true;
                    }
                } else {self.decide(id,Decision::Uncertain,"classifier_disabled",false,false);}
            }
        }
    }
    /// Reserve and return the next provider request, or None if closed, busy or empty.
    ///
    /// Call on the next host executor turn after queuing and after completion.
    /// The returned deadline starts at dispatch. A timed-out physical call still
    /// occupies the slot; no pending request can pass it until its result/error arrives.
    pub fn begin_classification(&mut self)->Option<ClassifierRequest> {
        if self.closed||self.flight.is_some() {return None;}
        let Some(key)=self.pending.take() else {self.worker_scheduled=false;return None;};
        self.requested=Some(key.clone());let e=self.episodes[&key.0].clone();
        self.emit("floor_classifier_requested","ambiguous_speech",Some(e.id),json!({"heard_text":e.heard,"unheard_text":e.unheard,"text":e.text,"decision":"UNCERTAIN","action":"GATHER_MORE"}));
        self.request_serial+=1;
        let request=ClassifierRequest {token:self.request_serial,key,text:e.text,heard:e.heard,unheard:e.unheard,
            agent_line:self.transport.last_speak_text.clone(),attempts:self.history(e.id),deadline:self.now+self.cfg.semantic_barge_llm_timeout_ms/1000.0};
        self.flight=Some(Flight {request:request.clone(),started:self.now,draining:false});self.worker_scheduled=true;Some(request)
    }
    /// Process a token-matched provider outcome and queue resulting decisions/effects.
    ///
    /// False means no matching active token (or an already-applied timeout).
    /// True means the outcome was consumed, not necessarily that its verdict was
    /// applied: changed revision/generation/audio, caller ownership or ended
    /// playback reject the answer. Invalid verdict strings become uncertain.
    /// A completion after timeout only drains the slot and emits late-result
    /// telemetry. Dispatch of pending work remains a separate host operation.
    pub fn classifier_result(&mut self,token:u64,result:ClassifierResult)->bool {
        let Some(f)=self.flight.clone() else {return false;};
        if f.request.token!=token {return false;}
        let id=f.request.key.0;
        if f.draining {
            if matches!(result,ClassifierResult::Timeout) {return false;}
            self.emit("floor_result_rejected","result_after_deadline",Some(id),json!({}));
        } else {
            let (mut decision,mut reason)=match &result {
                ClassifierResult::Timeout=>("UNCERTAIN","classifier_timeout"),
                ClassifierResult::Error=>("UNCERTAIN","classifier_error"),
                ClassifierResult::Verdict(v)=>(v.as_str(),"classifier_result"),
            };
            if f.request.key!=self.key(id)||self.caller_floor||!self.active_playback() {
                self.emit("floor_result_rejected","stale_revision_or_generation",Some(id),json!({"request_revision":f.request.key.1,"request_generation":f.request.key.2}));
            } else {
                if !matches!(decision,"CONTINUE"|"YIELD"|"UNCERTAIN") {decision="UNCERTAIN";reason="invalid_classifier_result";}
                self.emit("floor_classifier_result",reason,Some(id),json!({"decision":decision,"latency_ms":((self.now-f.started)*1000.0).round_ties_even() as i64}));
                let decision=match decision {"CONTINUE"=>Decision::Continue,"YIELD"=>Decision::Yield,_=>Decision::Uncertain};
                self.decide(id,decision,reason,false,false);
            }
            if matches!(result,ClassifierResult::Timeout) {self.flight.as_mut().unwrap().draining=true;return true;}
        }
        self.flight=None;self.worker_scheduled=self.pending.is_some();self.gc();true
    }
    /// Check the active request against `now` and apply timeout once if due.
    ///
    /// This is separate from `tick`; the host schedules the deadline. The request
    /// remains physically outstanding until its eventual result/error is reported.
    pub fn classifier_deadline(&mut self) {
        if let Some(f)=&self.flight {if !f.draining && self.now>=f.request.deadline {let token=f.request.token;self.classifier_result(token,ClassifierResult::Timeout);}}
    }
    /// Apply the line detector's machine breakthrough to its originally bound episode.
    /// The detector retains this evidence even after visible acoustic history evicts it.
    pub fn line_machine(&mut self, episode: Episode) {
        let id = episode.id;
        self.episodes.entry(id).or_insert(episode);
        self.decide(id, Decision::Yield, "line_machine", true, false);
    }
    /// Apply a semantic verdict to an episode and update provisional suppression state.
    ///
    /// Protected non-breakthrough yields defer without cutting. Continue clears
    /// that episode's failure/deferral; uncertain keeps gathering evidence.
    /// Ordinary decisions then run a tick so speech pressure remains effective.
    fn decide(&mut self,id:u64,decision:Decision,reason:&str,breakthrough:bool,suppress:bool) {
        self.episodes.get_mut(&id).unwrap().decision=decision;
        if decision==Decision::Yield {
            if self.transport.protected_playout&&!breakthrough {
                self.episodes.get_mut(&id).unwrap().deferred=true;self.protection_released=false;self.fail(id,"protected_span");
                self.emit("floor_decision","protected_span",Some(id),json!({"decision":"YIELD","action":"DEFER"}));return;
            }
            self.yield_floor(id,reason,true);
        } else {
            if decision==Decision::Continue {
                let e=self.episodes.get_mut(&id).unwrap();e.failed_at=None;e.deferred=false;
                if !self.episodes().any(|e|e.deferred) {self.release_protection();}
            }
            self.emit("floor_decision",reason,Some(id),json!({"decision":decision,"action":if decision==Decision::Continue{"KEEP_PLAYING"}else{"GATHER_MORE"}}));
            self.transport.absorbing_utterance=decision==Decision::Continue;
            let e=&self.episodes[&id];
            self.transport.absorb_detail=Some(json!({"text":e.text,"verdict":if e.echo{"echo"}else if suppress{"intro_absorb"}else{"backchannel"},"source":"floor_controller","audio_id":self.transport.last_audio_id}));
        }
        self.tick();
    }
    /// Clear protected-span deferrals on every episode in visible history.
    fn clear_deferred(&mut self) {for id in &self.order {self.episodes.get_mut(id).unwrap().deferred=false;}}
    /// Acquire caller ownership once and enqueue ordered transport work.
    ///
    /// A cut advances the generation, invalidates queued classification, clears
    /// absorb/gap/typing state and immediately clears an active stream. A deferred
    /// protected-span handoff uses `cut = false`: no generation increment or audio
    /// cut is emitted. Existing caller ownership makes this operation a no-op.
    fn yield_floor(&mut self,id:u64,reason:&str,cut:bool) {
        if self.caller_floor {return;}
        self.caller_floor=true;self.effects.push(Effect::PauseSilence);
        let e=&self.episodes[&id];self.final_seen=e.r#final&&!self.episodes().any(|p|p.last_speech>e.last_speech);
        if cut {self.generation+=1;}
        self.pending=None;self.clear_deferred();self.release_protection();self.released=false;
        self.transport.absorbing_utterance=false;self.transport.absorb_detail=None;self.transport.gap_active=false;self.transport.typing_arm_deadline=0.0;
        if self.transport.ambience {self.effects.push(Effect::StopTyping);}
        self.emit("floor_decision",reason,Some(id),json!({"decision":"YIELD","action":if cut{"CUT"}else{"TAKE_FLOOR"}}));
        if cut {
            self.effects.push(Effect::CutTelemetry {raw_rms:self.transport.candidate_raw_rms,clean_rms:self.transport.candidate_clean_rms});
            self.effects.push(Effect::CutPlayback);
            if !self.transport.tts_playing {self.transport.playback_draining=false;}
            if self.transport.tts_playing {
                self.effects.push(Effect::SendClear);self.effects.push(Effect::ClearPendingMarks);
                self.transport.playback_draining=false;self.transport.tts_ended_at=self.now;
                self.effects.push(Effect::CloseLedger {now:self.now});self.effects.push(Effect::NotePlayoutEnd {reset_window:false});
            }
        }
    }
    /// Evaluate caller release, protected completion and episode pressure at `now`.
    ///
    /// Caller EOT plus sufficient silence releases the floor. Otherwise unresolved
    /// text can become a failed attempt, and qualified recent speech can force a
    /// repeat/persistent yield. Echo and protection suppress those pressure cuts.
    /// This heartbeat neither dispatches providers nor expires classifier/final waits.
    pub fn tick(&mut self) {
        if self.caller_floor {
            if self.final_seen && self.now-self.last_speech>=self.cfg.barge_floor_silence_ms/1000.0 {
                self.caller_floor=false;self.effects.push(Effect::ResumeSilence);self.release_floor();
                self.emit("floor_released","caller_eot_and_silence",None,json!({}));
            }
            return;
        }
        if !self.active_playback()&&!self.protection_released {self.playback_finished();return;}
        let Some(id)=self.current else {return;};
        let e=&self.episodes[&id];if !self.active_playback()||e.echo||e.possible_echo {return;}
        if !e.text.is_empty()&&e.decision==Decision::Uncertain&&self.now-e.onset>=self.cfg.semantic_barge_window_ms/1000.0 {self.fail(id,"decision_deadline");}
        let e=&self.episodes[&id];if self.transport.protected_playout||!e.qualified||self.now-e.last_speech>0.1 {return;}
        let repeat=self.episodes().any(|p|p.id!=id&&p.failed_at.is_some_and(|t|self.now-t<=self.cfg.barge_attempt_history_ms/1000.0));
        if repeat && e.speech_ms>=self.cfg.barge_repeat_speech_ms {self.yield_floor(id,"repeated_speech",true);}
        else if !e.text.is_empty()&&e.speech_ms>=self.cfg.barge_persistent_speech_ms {self.yield_floor(id,"persistent_speech",true);}
    }
    /// Release caller/protection holds so machine-directed playout can proceed.
    ///
    /// Clear waiting-for-reply/EOT state and log the requested audio identity;
    /// preserve the generation and existing classifier work, matching Python policy.
    pub fn take_floor_from_machine(&mut self,audio_id:Option<String>) {
        let was=self.caller_floor;self.caller_floor=false;self.awaiting_reply=false;self.final_seen=false;
        if was {self.effects.push(Effect::ResumeSilence);}
        self.clear_deferred();self.release_floor();self.release_protection();
        self.emit("floor_decision","line_machine_playout",None,json!({"decision":"AGENT","action":"TAKE_FLOOR","requested_audio_id":audio_id,"was_caller_floor":was}));
    }
    /// Resolve protected handoff or clear history after an accepted reply actually plays.
    ///
    /// The latest visible deferred episode takes the floor without an audible cut.
    /// Otherwise failures clear only when `accepted_generation` matches the current
    /// generation and the caller does not own the floor.
    pub fn playback_finished(&mut self) {
        self.release_protection();
        let pending=self.order.iter().rev().find(|id|self.episodes[id].deferred).copied();
        if let Some(id)=pending {self.clear_deferred();self.yield_floor(id,"protected_span_completed",false);self.tick();}
        else if !self.caller_floor&&self.accepted_generation==Some(self.generation) {
            for id in &self.order {self.episodes.get_mut(id).unwrap().failed_at=None;}
            self.accepted_generation=None;self.emit("speech_history_cleared","response_played_after_caller_turn",None,json!({}));
        }
    }
    /// Start playback admission after a floor tick, returning a token and initial result.
    ///
    /// Check protection, response generation and caller ownership in order. With
    /// `wait = false`, blocked requests are rejected; otherwise retain the token
    /// and poll it on release. The method itself never waits or starts playback.
    pub fn allow_playback(&mut self,generation:Option<u64>,wait:bool,audio_id:Option<String>)->(Admission,AdmissionResult) {
        self.tick();let mut a=Admission {generation,wait,audio_id,phase:0,protection_epoch:None,release_epoch:None};
        let result=self.poll_admission(&mut a);(a,result)
    }
    /// Resume a held admission without duplicating HOLD logs or losing release pulses.
    ///
    /// Revalidate generation after the relevant wait. Terminal results are
    /// idempotent. For Python parity, `generation = None` skips the initial stale
    /// check but is rejected after a caller-floor hold. Admission clears
    /// `awaiting_reply`; actual playback remains the host's responsibility.
    pub fn poll_admission(&mut self,a:&mut Admission)->AdmissionResult {
        if a.phase==4 {return AdmissionResult::Allowed;} if a.phase==5 {return AdmissionResult::Rejected;}
        if a.phase==0 {
            if !self.protection_released {
                self.emit("playback_blocked","protected_span_pending",None,json!({"action":if a.wait{"HOLD"}else{"DROP"},"requested_audio_id":a.audio_id}));
                if !a.wait {a.phase=5;return AdmissionResult::Rejected;}
                a.protection_epoch=Some(self.protection_epoch);
            }
            a.phase=1;
        }
        if a.phase==1 {
            if !self.protection_released && a.protection_epoch==Some(self.protection_epoch) {return AdmissionResult::Waiting;}
            if a.generation.is_some_and(|g|g!=self.generation) {
                self.emit("playback_blocked","stale_generation",None,json!({"request_generation":a.generation,"requested_audio_id":a.audio_id}));a.phase=5;return AdmissionResult::Rejected;
            }
            if self.caller_floor {
                self.emit("playback_blocked","caller_owns_floor",None,json!({"action":if a.wait{"HOLD"}else{"DROP"},"requested_audio_id":a.audio_id}));
                if !a.wait {a.phase=5;return AdmissionResult::Rejected;}a.phase=2;a.release_epoch=Some(self.release_epoch);
            } else {a.phase=3;}
        }
        if a.phase==2 {
            if !self.released && a.release_epoch==Some(self.release_epoch) {return AdmissionResult::Waiting;}
            if a.generation!=Some(self.generation) {
                self.emit("playback_blocked","stale_generation_after_hold",None,json!({"request_generation":a.generation,"requested_audio_id":a.audio_id}));a.phase=5;return AdmissionResult::Rejected;
            }
            a.phase=3;
        }
        self.awaiting_reply=false;self.emit("playback_allowed","floor_available",None,json!({"requested_audio_id":a.audio_id}));a.phase=4;AdmissionResult::Allowed
    }
}
