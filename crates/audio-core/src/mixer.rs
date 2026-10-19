//! Native Rust port of the frame-driven mixer. No Python or clock in DSP logic.
use crate::sources::{Source, clamp, pack, unpack};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(default)]
#[pyclass(from_py_object, get_all)]
/// Layer gains and ramp durations; gains are linear multipliers and durations are milliseconds.
/// AmbienceMixer construction validates finite values and positive fade durations.
pub struct AmbienceConfig {
    pub room_gain: f64,
    pub duck_factor: f64,
    pub duck_ramp_ms: f64,
    pub typing_gain: f64,
    pub typing_fade_ms: f64,
    pub oneshot_gain: f64,
}

impl Default for AmbienceConfig {
    fn default() -> Self {
        Self {
            room_gain: 1.0,
            duck_factor: 0.5,
            duck_ramp_ms: 60.0,
            typing_gain: 0.15,
            typing_fade_ms: 100.0,
            oneshot_gain: 0.6,
        }
    }
}

#[pymethods]
impl AmbienceConfig {
    #[new]
    #[pyo3(signature=(room_gain=1.0, duck_factor=0.5, duck_ramp_ms=60.0, typing_gain=0.15, typing_fade_ms=100.0, oneshot_gain=0.6))]
    /// Collect layer settings; validation occurs when constructing the mixer.
    pub fn new(
        room_gain: f64,
        duck_factor: f64,
        duck_ramp_ms: f64,
        typing_gain: f64,
        typing_fade_ms: f64,
        oneshot_gain: f64,
    ) -> Self {
        Self {
            room_gain,
            duck_factor,
            duck_ramp_ms,
            typing_gain,
            typing_fade_ms,
            oneshot_gain,
        }
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Copy, PartialEq, Debug)]
#[pyclass(eq, eq_int)]
/// Typing-layer lifecycle: idle, playing, or fading toward silence.
pub enum TypingState {
    IDLE,
    PLAYING,
    STOPPING,
}

#[pyclass]
/// Owns per-call layer cursors and gain envelopes for native PCM16 mixing.
/// Rendering advances time by sample count; controls only change future rendered frames.
pub struct AmbienceMixer {
    rate: u32,
    config: AmbienceConfig,
    room: Option<Vec<i16>>,
    typing: Option<Vec<Vec<i16>>>,
    shots: Option<Vec<Vec<i16>>>,
    room_cursor: usize,
    gain: f64,
    target: f64,
    span: f64,
    typing_state: TypingState,
    typing_index: usize,
    typing_pos: usize,
    typing_gain: f64,
    typing_target: f64,
    shot_index: usize,
    shot_pos: usize,
    shot_active: bool,
}

#[pymethods]
impl AmbienceMixer {
    #[new]
    #[pyo3(signature=(sample_rate, room=None, typing=None, oneshots=None, config=None))]
    /// Create a mixer from optional sources already at the target sample rate.
    /// Reject invalid rates, mismatched source rates and invalid gain/fade settings.
    pub fn new(
        sample_rate: u32,
        room: Option<Source>,
        typing: Option<Source>,
        oneshots: Option<Source>,
        config: Option<AmbienceConfig>,
    ) -> PyResult<Self> {
        let config = config.unwrap_or_default();
        if sample_rate == 0
            || config.duck_ramp_ms <= 0.0
            || config.typing_fade_ms <= 0.0
            || ![
                config.room_gain,
                config.duck_factor,
                config.duck_ramp_ms,
                config.typing_gain,
                config.typing_fade_ms,
                config.oneshot_gain,
            ]
            .iter()
            .all(|v| v.is_finite())
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "invalid mixer rate or non-finite/zero-duration configuration",
            ));
        }
        for source in [&room, &typing, &oneshots].into_iter().flatten() {
            if source.sample_rate != sample_rate {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "source must already be resampled to mixer rate",
                ));
            }
        }
        let gain = config.room_gain;
        let span = gain * (1.0 - config.duck_factor);
        let layers = |s: Source| s.clips.iter().map(|c| unpack(&c.pcm)).collect();
        Ok(Self {
            rate: sample_rate,
            config,
            room: room.map(|s| unpack(&s.clips[0].pcm)),
            typing: typing.map(layers),
            shots: oneshots.map(layers),
            room_cursor: 0,
            gain,
            target: gain,
            span: if span == 0.0 { gain } else { span },
            typing_state: TypingState::IDLE,
            typing_index: 0,
            typing_pos: 0,
            typing_gain: 0.0,
            typing_target: 0.0,
            shot_index: 0,
            shot_pos: 0,
            shot_active: false,
        })
    }

    /// Choose the ducked or full room-gain target; rendering advances the ramp.
    pub fn set_speaking(&mut self, speaking: bool) {
        self.target = if speaking {
            self.config.room_gain * self.config.duck_factor
        } else {
            self.config.room_gain
        };
    }
    /// Restart the selected typing clip at full configured gain; indices wrap cyclically.
    pub fn start_typing(&mut self, clip_index: i64) {
        if let Some(clips) = &self.typing {
            self.typing_index = clip_index.rem_euclid(clips.len() as i64) as usize;
            self.typing_pos = 0;
            self.typing_gain = self.config.typing_gain;
            self.typing_target = self.typing_gain;
            self.typing_state = TypingState::PLAYING;
        }
    }
    /// Begin a gradual typing fade if the layer is playing; repeated stops are harmless.
    pub fn stop_typing(&mut self) {
        if self.typing_state == TypingState::PLAYING {
            self.typing_state = TypingState::STOPPING;
            self.typing_target = 0.0;
        }
    }
    /// Start a cyclically selected clip only when no one-shot is already active.
    pub fn trigger_oneshot(&mut self, clip_index: i64) {
        if !self.shot_active
            && let Some(clips) = &self.shots
        {
            self.shot_index = clip_index.rem_euclid(clips.len() as i64) as usize;
            self.shot_pos = 0;
            self.shot_active = true;
        }
    }

    #[getter]
    /// Report whether a one-shot cursor is currently consuming a clip.
    pub fn oneshot_active(&self) -> bool {
        self.shot_active
    }
    #[getter]
    /// Read the typing playback/fade state without advancing it.
    pub fn typing_state(&self) -> TypingState {
        self.typing_state
    }
    #[getter]
    /// Read the current room envelope gain rather than its target.
    pub fn room_gain_now(&self) -> f64 {
        self.gain
    }
    #[getter(_oneshot_pos)]
    fn oneshot_pos(&self) -> usize {
        self.shot_pos
    }
    #[getter(_oneshot_clip_idx)]
    fn oneshot_clip_idx(&self) -> usize {
        self.shot_index
    }

    #[pyo3(signature=(n_samples, tts_frame=None))]
    /// Expose native PCM16 mixing as Python bytes for compatibility checks.
    pub fn next_frame<'py>(
        &mut self,
        py: Python<'py>,
        n_samples: usize,
        tts_frame: Option<&[u8]>,
    ) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.mix(n_samples, tts_frame))
    }

    /// Serialize layer cursors and envelopes for deterministic diagnostics and parity.
    pub fn state_json(&self) -> String {
        serde_json::json!({"room_cursor":self.room_cursor, "gain":self.gain, "target":self.target,
            "typing_state":format!("{:?}",self.typing_state).to_lowercase(), "typing_index":self.typing_index,
            "typing_pos":self.typing_pos, "typing_gain":self.typing_gain, "typing_target":self.typing_target,
            "shot_index":self.shot_index, "shot_pos":self.shot_pos, "shot_active":self.shot_active}).to_string()
    }
}

impl AmbienceMixer {
    /// PCM bytes in and out, with no Python library calls on the mixing path.
    pub fn mix(&mut self, n: usize, tts: Option<&[u8]>) -> Vec<u8> {
        let start_gain = self.gain;
        let dt = n as f64 / self.rate as f64;
        if self.gain != self.target {
            let step = (dt / (self.config.duck_ramp_ms / 1000.0)) * self.span;
            self.gain = if self.gain < self.target {
                self.target.min(self.gain + step)
            } else {
                self.target.max(self.gain - step)
            };
        }
        if self.typing_state == TypingState::STOPPING && self.typing_gain > 0.0 {
            let step = (dt / (self.config.typing_fade_ms / 1000.0)) * self.config.typing_gain;
            self.typing_gain = (self.typing_gain - step).max(0.0);
            if self.typing_gain == 0.0 {
                self.typing_state = TypingState::IDLE;
            }
        }
        let typing_gain = self.typing_gain;
        let typing_runs = self.typing.is_some()
            && !(self.typing_state == TypingState::IDLE && typing_gain == 0.0);
        let shot_runs = self.shot_active && self.shots.is_some();
        let tts = tts.map(unpack).unwrap_or_default();
        let mut output = Vec::with_capacity(n);
        for i in 0..n {
            let mut value = 0_i64;
            if let Some(room) = &self.room
                && !room.is_empty()
            {
                let gain = if start_gain == self.gain {
                    self.gain
                } else {
                    start_gain + (self.gain - start_gain) * (i as f64 / n as f64)
                };
                value += (room[self.room_cursor] as f64 * gain) as i64;
                self.room_cursor = (self.room_cursor + 1) % room.len();
            }
            if typing_runs {
                let clip = &self.typing.as_ref().unwrap()[self.typing_index];
                if self.typing_pos < clip.len() {
                    value += (clip[self.typing_pos] as f64 * typing_gain) as i64;
                    self.typing_pos += 1;
                }
            }
            if shot_runs {
                let clip = &self.shots.as_ref().unwrap()[self.shot_index];
                if self.shot_pos < clip.len() {
                    value += (clip[self.shot_pos] as f64 * self.config.oneshot_gain) as i64;
                    self.shot_pos += 1;
                }
            }
            if let Some(s) = tts.get(i) {
                value += *s as i64;
            }
            output.push(clamp(value));
        }
        if typing_runs
            && self.typing_pos >= self.typing.as_ref().unwrap()[self.typing_index].len()
            && self.typing_state == TypingState::PLAYING
        {
            self.typing_state = TypingState::IDLE;
            self.typing_gain = 0.0;
        }
        if shot_runs && self.shot_pos >= self.shots.as_ref().unwrap()[self.shot_index].len() {
            self.shot_active = false;
        }
        pack(&output)
    }
}
