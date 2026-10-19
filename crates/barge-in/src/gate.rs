//! Acoustic evidence gate, independent of semantic floor ownership and transport I/O.
//!
//! [`Config`](crate::gate::Config) supplies energy/VAD policy; [`Gate`](crate::gate::Gate) owns one call's noise estimate,
//! streak and buffered provisional window. [`FrameVerdict`](crate::gate::FrameVerdict) reports the resulting
//! evidence and ordered log events. Confirmation latches until an explicit reset.
//! The host supplies monotonic seconds and a stateful retained VAD callback; this
//! module neither converts audio bytes nor decides whether to cancel playback.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Energy debounce, adaptive threshold and optional VAD confirmation policy.
///
/// Defaults match Python `BargeGateConfig`. Thresholds use the input RMS scale;
/// durations ending in `_sec` use seconds. Configuration is accepted as supplied,
/// so the host retains responsibility for deployment validation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    /// Fixed RMS threshold used when adaptation is disabled or warming up.
    pub rms_threshold: i64,
    /// Consecutive frames strictly above threshold required to open/confirm a candidate.
    pub consecutive_frames: i64,
    /// Time after TTS starts during which a mature energy streak cannot open a candidate.
    pub grace_sec: f64,
    /// Whether idle, sub-threshold frames update the noise-floor estimate.
    pub adaptive_enabled: bool,
    /// Exponential moving-average weight assigned to each new noise observation.
    pub adaptive_alpha: f64,
    /// Multiplier applied to the learned noise floor after warmup.
    pub adaptive_margin: f64,
    /// Lower clamp for the adaptive RMS threshold.
    pub adaptive_min: i64,
    /// Upper clamp for the adaptive RMS threshold.
    pub adaptive_max: i64,
    /// Number of eligible noise observations before adaptation controls the threshold.
    pub adaptive_warmup_frames: i64,
    /// Confirmation mode: `off` uses energy alone; `local_vad` uses the injected predicate.
    pub confirm: String,
    /// Required positive VAD votes within the recent confirmation window.
    pub confirm_frames_m: i64,
    /// Recent VAD window length; zero retains Python slicing behavior (all votes).
    pub confirm_frames_n: i64,
    /// Maximum elapsed provisional-window time before abort, unless this frame confirms.
    pub confirm_timeout_sec: f64,
    /// Consecutive sub-threshold provisional frames that abort a candidate.
    pub confirm_energy_floor_frames: i64,
}
impl Default for Config {
    /// Construct Python-compatible gate defaults; no environment/settings are read.
    fn default() -> Self { Self { rms_threshold:1200, consecutive_frames:3, grace_sec:0.1,
        adaptive_enabled:false, adaptive_alpha:0.05, adaptive_margin:7.0, adaptive_min:600,
        adaptive_max:2600, adaptive_warmup_frames:50, confirm:"off".into(), confirm_frames_m:3,
        confirm_frames_n:5, confirm_timeout_sec:0.4, confirm_energy_floor_frames:3 } }
}
/// Lifecycle of acoustic confirmation, separate from the semantic caller/agent floor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all="lowercase")]
pub enum State {
    /// Accumulating energy evidence; no provisional audio is buffered.
    Idle,
    /// Buffering onset frames while waiting for sufficient VAD confirmation.
    Provisional,
    /// Confirmation is latched; subsequent frames are forwarded until reset.
    Confirmed
}
/// Result of one frame evaluation, consumed by the transport/floor controller.
///
/// `confirmed_now` is acoustic evidence, not an unconditional cut instruction.
/// The open STT path continues independently of `forward_frames`; that buffer
/// preserves compatibility with the optional legacy gated feed.
#[derive(Default, Debug, Serialize)]
pub struct FrameVerdict {
    /// True only on the frame that transitions into confirmed state.
    pub confirmed_now: bool,
    /// Opaque frame copies to forward, including the buffered onset when VAD confirms.
    pub forward_frames: Vec<Vec<u8>>,
    /// Whether this frame counts as background/noise for transport statistics.
    pub noise_frame: bool,
    /// Ordered event-name/detail pairs for the host audio log.
    pub events: Vec<(&'static str, Value)>,
}
/// Per-call owner of acoustic gate state and provisional onset audio.
///
/// Resetting confirmation preserves the learned noise floor. Starting TTS drops
/// an unfinished provisional window but deliberately does not unlatch a confirmed
/// gate. The host owns the VAD instance and calls these methods serially.
#[derive(Debug, Serialize)]
pub struct Gate {
    /// Effective configuration, including energy-only fallback when local VAD is unavailable.
    pub cfg: Config,
    /// Current acoustic gate lifecycle state.
    pub state: State,
    /// Current consecutive above-threshold frame count.
    pub streak: i64,
    /// Monotonic timestamp anchoring the TTS grace window, in seconds.
    tts_started_at: f64,
    /// Learned RMS noise floor; zero until an eligible observation arrives.
    floor: f64,
    /// Number of eligible observations used for adaptive warmup.
    floor_frames: i64,
    /// Monotonic start time of the current provisional window, or zero when absent.
    prov_opened_at: f64,
    /// Unmodified onset frames retained until confirmation or abort.
    prov_buffer: Vec<Vec<u8>>,
    /// VAD votes for the provisional window, stored as zero or one.
    prov_vad_flags: Vec<i64>,
    /// Consecutive sub-threshold frames within the provisional window.
    prov_quiet_run: i64,
}
impl Gate {
    /// Create an idle gate with no learned noise or buffered audio.
    ///
    /// If `local_vad` was requested but `vad_available` is false, change the
    /// effective mode to `off` so speech can still confirm through energy alone.
    pub fn new(mut cfg: Config, vad_available: bool) -> Self {
        if cfg.confirm=="local_vad" && !vad_available { cfg.confirm="off".into(); }
        Self {cfg, state:State::Idle, streak:0, tts_started_at:0.0, floor:0.0, floor_frames:0,
            prov_opened_at:0.0, prov_buffer:vec![], prov_vad_flags:vec![], prov_quiet_run:0}
    }
    /// Return whether acoustic confirmation is latched; provisional candidates return false.
    pub fn in_progress(&self) -> bool {self.state==State::Confirmed}
    /// Return the learned RMS noise estimate without advancing adaptation.
    pub fn noise_floor(&self) -> f64 {self.floor}
    /// Return the fixed or warmed-up adaptive threshold.
    ///
    /// The adaptive value is clamped and truncated to an integer like Python; a
    /// frame must be strictly above this threshold to count toward a streak.
    pub fn threshold(&self) -> i64 {
        if !self.cfg.adaptive_enabled || self.floor_frames<self.cfg.adaptive_warmup_frames {return self.cfg.rms_threshold;}
        (self.cfg.adaptive_margin*self.floor).max(self.cfg.adaptive_min as f64).min(self.cfg.adaptive_max as f64) as i64
    }
    /// Discard only provisional audio, VAD votes, quiet-run count and start time.
    fn drop_provisional(&mut self) {self.prov_buffer.clear();self.prov_vad_flags.clear();self.prov_quiet_run=0;self.prov_opened_at=0.0;}
    /// Rearm the gate to idle, clearing the streak and provisional window.
    ///
    /// The adaptive noise estimate and TTS grace anchor survive this reset.
    pub fn reset(&mut self) {self.state=State::Idle;self.streak=0;self.drop_provisional();}
    /// Anchor grace at `now` (monotonic seconds), reset the streak and abandon
    /// any provisional candidate. A previously confirmed gate remains latched.
    pub fn on_tts_start(&mut self, now:f64) {
        self.tts_started_at=now;self.streak=0;
        if self.state==State::Provisional {self.drop_provisional();self.state=State::Idle;}
    }
    /// Evaluate one frame and return acoustic evidence, buffered bytes and log events.
    ///
    /// `now` is monotonic seconds, `clean_rms` is the caller-audio energy measure,
    /// and `frame` is opaque audio passed unchanged to the callback and output.
    /// Only idle sub-threshold frames teach the noise floor. During a provisional
    /// window, confirmation is checked before timeout/quiet abort, preserving
    /// Python behavior at simultaneous boundaries. No transport I/O occurs.
    ///
    /// The retained VAD predicate runs only when opening/updating a provisional
    /// window. The host must preserve its state across calls, as in Python.
    pub fn on_frame(&mut self, now:f64, clean_rms:i64, tts_playing:bool, frame:&[u8],
                    mut confirm:impl FnMut(&[u8])->bool) -> FrameVerdict {
        let mut v=FrameVerdict::default(); let thr=self.threshold();let over=clean_rms>thr;
        if self.state==State::Confirmed {v.forward_frames.push(frame.to_vec());return v;}
        if self.cfg.adaptive_enabled && !over && self.state==State::Idle {
            self.floor=if self.floor_frames==0 {clean_rms as f64} else {
                self.cfg.adaptive_alpha*clean_rms as f64+(1.0-self.cfg.adaptive_alpha)*self.floor};
            self.floor_frames+=1;
        }
        if over {
            self.streak+=1;
            if self.streak==1 && self.state==State::Idle {v.events.push(("barge_streak_start",json!({"clean_rms":clean_rms,"threshold":thr})));}
        } else {
            if self.streak>0 && self.state==State::Idle {v.events.push(("barge_suppressed",json!({"streak":self.streak,"clean_rms":clean_rms})));}
            self.streak=0;
        }
        if self.state==State::Provisional {
            self.prov_buffer.push(frame.to_vec());self.prov_vad_flags.push(i64::from(confirm(frame)));
            self.prov_quiet_run=if over {0} else {self.prov_quiet_run+1};
            let len=self.prov_vad_flags.len();let n=self.cfg.confirm_frames_n;
            // Python flags[-n:] includes the whole window when n == 0.
            let start=if n>0 {len.saturating_sub(n as usize)} else {(n.unsigned_abs() as usize).min(len)};
            let positive:i64=self.prov_vad_flags[start..].iter().sum();
            if positive>=self.cfg.confirm_frames_m {
                v.confirmed_now=true;v.forward_frames=std::mem::take(&mut self.prov_buffer);
                v.events.push(("barge_confirmed",json!({"clean_rms":clean_rms,"confirm":"local_vad",
                    "confirm_latency_ms":((now-self.prov_opened_at)*1000.0).round_ties_even() as i64,
                    "vad_positive":positive,"window":len})));
                self.state=State::Confirmed;self.drop_provisional();return v;
            }
            let timeout=now-self.prov_opened_at>=self.cfg.confirm_timeout_sec;
            if timeout || self.prov_quiet_run>=self.cfg.confirm_energy_floor_frames {
                v.events.push(("barge_abort",json!({"reason":if timeout {"timeout"} else {"energy_floor"},
                    "window":len,"vad_positive":self.prov_vad_flags.iter().sum::<i64>()})));
                self.state=State::Idle;self.streak=0;self.drop_provisional();v.noise_frame=!over;
            }
            return v;
        }
        let mature=self.streak>=self.cfg.consecutive_frames;
        if mature && tts_playing && now-self.tts_started_at<self.cfg.grace_sec {
            if self.streak==self.cfg.consecutive_frames {v.events.push(("barge_grace_blocked",json!({"streak":self.streak,"clean_rms":clean_rms})));}
            return v;
        }
        if mature {
            if self.cfg.confirm=="off" {
                v.confirmed_now=true;v.forward_frames.push(frame.to_vec());
                v.events.push(("barge_confirmed",json!({"clean_rms":clean_rms,"confirm":"off","streak":self.streak})));
                self.state=State::Confirmed;
            } else {
                self.state=State::Provisional;self.prov_opened_at=now;
                self.prov_buffer=vec![frame.to_vec()];self.prov_vad_flags=vec![i64::from(confirm(frame))];self.prov_quiet_run=0;
                v.events.push(("barge_provisional_open",json!({"clean_rms":clean_rms,"streak":self.streak,"threshold":thr})));
            }
            return v;
        }
        v.noise_frame=!over;v
    }
}
