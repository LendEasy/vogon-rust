//! Serialized per-call audio ownership. Submit never blocks an async executor.
//! CPython still has a shared GIL; this is isolation, not a throughput claim.
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    sync::mpsc::{self, Receiver, SyncSender, TrySendError},
    thread::{self, JoinHandle},
};
use vogon_audio_core::{
    aec::AecProcessor,
    formats::{AudioOps, PhoneFramer, Vad},
    mixer::AmbienceMixer,
    sources,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
/// Per-call worker configuration: provider encoding, native processing and optional ambience.
/// Queue capacity counts commands, not bytes; asset paths are supplied explicitly.
pub struct Config {
    pub encoding: String,
    pub aec: bool,
    pub ambience: bool,
    pub vad_mode: u8,
    pub queue_capacity: usize,
    pub assets_dir: Option<std::path::PathBuf>,
    pub barge_gate: Option<vogon_barge_in::gate::Config>,
    pub enable_room: bool,
    pub enable_typing: bool,
    pub enable_oneshots: bool,
    pub mixer_config: Option<vogon_audio_core::mixer::AmbienceConfig>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            encoding: "mulaw".into(),
            aec: true,
            ambience: false,
            vad_mode: 3,
            queue_capacity: 64,
            assets_dir: None,
            barge_gate: None,
            enable_room: true,
            enable_typing: true,
            enable_oneshots: true,
            mixer_config: None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
/// Ordered work for one audio owner. Audio byte formats are variant-specific.
/// Generating output does not feed AEC; a successful transport send submits Reference.
pub enum Command {
    /// Enqueue dispatched mu-law bytes for AEC; never submit speculative output.
    Reference {
        data: Vec<u8>,
    },
    /// Process mu-law through optional AEC; return cleaned bytes, RMS and VAD.
    Inbound {
        data: Vec<u8>,
    },
    /// Convert provider bytes to complete phone frames, optionally mixed with ambience.
    Tts {
        data: Vec<u8>,
    },
    /// Return the short phone tail without padding/mixing or resampler reset.
    Flush,
    /// Reset framing/resampling for a new utterance, retaining outstanding AEC references.
    ResetOutput,
    /// Reset output and discard AEC references after caller-buffer clear succeeds.
    Clear,
    /// Set the room-tone ducking target; gain changes advance during rendering.
    Speaking {
        value: bool,
    },
    /// Restart a cyclically selected typing clip, if the layer exists.
    StartTyping {
        index: i64,
    },
    /// Start the typing fade-out without blocking until the fade completes.
    StopTyping,
    /// Start a selected one-shot only if no one-shot is active.
    Oneshot {
        index: i64,
    },
    /// Read AEC/mixer diagnostics and pending frame bytes without rendering audio.
    Snapshot,
    /// Apply phone overlap/AEC/gate policy at monotonic now seconds.
    /// Returns optional STT bytes and energy/gate evidence; absent STT suppresses overlap.
    PhoneInput {
        data: Vec<u8>,
        now: f64,
        tts_playing: bool,
        active_audio: bool,
    },
    /// Measure PCM16 RMS; does not apply phone AEC or forward bytes to a provider.
    PcmInput {
        data: Vec<u8>,
    },
    /// Render 160 samples of gap ambience and encode mu-law; empty if no mixer exists.
    AmbienceFrame,
    /// Begin the gate's playback grace interval at monotonic now seconds.
    GateStart {
        now: f64,
    },
    /// Reset per-episode gate state, if a gate is configured.
    GateReset,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
/// JSON-compatible validation output envelope; worker replies use Value directly.
pub enum Output {
    Value(Value),
}
/// Worker result with initialization, queue or processing failure text.
pub type Result<T> = std::result::Result<T, String>;
type Work = (Command, mpsc::Sender<Result<Value>>);

struct Session {
    aec: Option<AecProcessor>,
    ops: AudioOps,
    vad: Vad,
    framer: PhoneFramer,
    mixer: Option<AmbienceMixer>,
    gate: Option<vogon_barge_in::gate::Gate>,
}
impl Session {
    fn new(py: Python<'_>, cfg: &Config) -> PyResult<Self> {
        Ok(Self {
            aec: if cfg.aec {
                Some(AecProcessor::new(py)?)
            } else {
                None
            },
            ops: AudioOps::new(py)?,
            vad: Vad::new(py, cfg.vad_mode, 8000)?,
            framer: PhoneFramer::new(py, &cfg.encoding)?,
            gate: cfg
                .barge_gate
                .clone()
                .map(|cfg| vogon_barge_in::gate::Gate::new(cfg, true)),
            mixer: if cfg.ambience {
                Some(sources::build_mixer(
                    py,
                    8000,
                    cfg.enable_room,
                    cfg.enable_typing,
                    cfg.enable_oneshots,
                    cfg.mixer_config.clone(),
                    cfg.assets_dir.clone(),
                )?)
            } else {
                None
            },
        })
    }
    fn execute(&mut self, py: Python<'_>, command: Command) -> PyResult<Value> {
        match command {
            Command::AmbienceFrame => {
                let bytes = if let Some(mixer) = &mut self.mixer {
                    let pcm = mixer.mix(160, None);
                    self.ops.lin2ulaw(py, &pcm, 2)?.as_bytes().to_vec()
                } else {
                    vec![]
                };
                return Ok(json!(bytes));
            }
            Command::GateStart { now } => {
                if let Some(gate) = &mut self.gate {
                    gate.on_tts_start(now);
                }
            }
            Command::GateReset => {
                if let Some(gate) = &mut self.gate {
                    gate.reset();
                }
            }
            Command::PcmInput { data } => {
                return Ok(json!({"rms":self.ops.rms(py,&data,2).unwrap_or(0)}));
            }
            Command::PhoneInput {
                data,
                now,
                tts_playing,
                active_audio,
            } => {
                let raw_pcm = self.ops.ulaw2lin(py, &data, 2)?;
                let raw_rms = self.ops.rms(py, raw_pcm.as_bytes(), 2).unwrap_or(0);
                let clean = if active_audio {
                    match &mut self.aec {
                        Some(aec) => Some(aec.process(py, &data)?.as_bytes().to_vec()),
                        None => None,
                    }
                } else {
                    Some(data)
                };
                let clean_rms = if let Some(bytes) = &clean {
                    let pcm = self.ops.ulaw2lin(py, bytes, 2)?;
                    self.ops.rms(py, pcm.as_bytes(), 2).unwrap_or(0)
                } else {
                    raw_rms
                };
                let verdict = if let Some(gate) = &mut self.gate {
                    if !active_audio {
                        gate.reset();
                        None
                    } else if let Some(bytes) = &clean {
                        // Preserve the existing Twilio callback convention: the
                        // gate passes its opaque mu-law frame directly to VAD.
                        // Changing this interpretation is a separate DSP behavior change.
                        Some(gate.on_frame(now, clean_rms, tts_playing, bytes, |frame| {
                            self.vad.confirm_frame(py, frame)
                        }))
                    } else {
                        None
                    }
                } else {
                    None
                };
                return Ok(
                    json!({"stt":clean,"raw_rms":raw_rms,"clean_rms":clean_rms,"verdict":verdict,
                    "threshold":self.gate.as_ref().map(|g|g.threshold())}),
                );
            }
            Command::Reference { data } => {
                if let Some(aec) = &mut self.aec {
                    aec.feed_reference(py, &data);
                }
            }
            Command::Inbound { data } => {
                let clean = match &mut self.aec {
                    Some(aec) => aec.process(py, &data)?.as_bytes().to_vec(),
                    None => data,
                };
                let pcm = self.ops.ulaw2lin(py, &clean, 2)?;
                let rms = self.ops.rms(py, pcm.as_bytes(), 2)?;
                let speech = self.vad.confirm_frame(py, pcm.as_bytes());
                return Ok(
                    json!({"mulaw":clean, "pcm":pcm.as_bytes(), "rms":rms, "speech":speech}),
                );
            }
            Command::Tts { data } => {
                return Ok(json!(self.framer.push_native(
                    py,
                    &data,
                    self.mixer.as_mut()
                )?));
            }
            Command::Flush => return Ok(json!(self.framer.flush_tail(py).as_bytes())),
            Command::ResetOutput => {
                self.framer.reset(py);
            }
            Command::Clear => {
                self.framer.reset(py);
                if let Some(aec) = &mut self.aec {
                    aec.discard_reference();
                }
            }
            Command::Speaking { value } => {
                if let Some(mixer) = &mut self.mixer {
                    mixer.set_speaking(value);
                }
            }
            Command::StartTyping { index } => {
                if let Some(mixer) = &mut self.mixer {
                    mixer.start_typing(index);
                }
            }
            Command::StopTyping => {
                if let Some(mixer) = &mut self.mixer {
                    mixer.stop_typing();
                }
            }
            Command::Oneshot { index } => {
                if let Some(mixer) = &mut self.mixer {
                    mixer.trigger_oneshot(index);
                }
            }
            Command::Snapshot => {
                return Ok(json!({
                    "aec":self.aec.as_ref().map(|a| serde_json::from_str::<Value>(&a.state_json()).unwrap()),
                    "mixer":self.mixer.as_ref().map(|m| serde_json::from_str::<Value>(&m.state_json()).unwrap()),
                    "pending_bytes":self.framer.pending_bytes(),
                }));
            }
        }
        Ok(Value::Null)
    }
}

/// Owns a bounded command queue and one dedicated audio thread per call.
/// Create and explicitly close outside the async executor and without holding the GIL.
/// Dropping the owner also joins the worker; explicit close controls where that wait occurs.
pub struct AudioRuntime {
    sender: Option<SyncSender<Work>>,
    worker: Option<JoinHandle<()>>,
}
impl AudioRuntime {
    /// Call once per call outside the network executor (initialization may load assets).
    pub fn start(config: Config) -> Result<Self> {
        if config.queue_capacity == 0 {
            return Err("queue_capacity must be positive".into());
        }
        let (sender, receiver) = mpsc::sync_channel::<Work>(config.queue_capacity);
        let (ready_tx, ready_rx) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("vogon-audio".into())
            .spawn(move || {
                let session = Python::attach(|py| Session::new(py, &config));
                let mut session = match session {
                    Ok(session) => {
                        let _ = ready_tx.send(Ok(()));
                        session
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                // Never hold the GIL while waiting for another command.
                while let Ok((command, reply)) = receiver.recv() {
                    let result = Python::attach(|py| session.execute(py, command))
                        .map_err(|e| e.to_string());
                    let _ = reply.send(result);
                }
                Python::attach(|py| {
                    if let Some(aec) = &mut session.aec {
                        let _ = aec.close(py);
                    }
                    drop(session);
                });
            })
            .map_err(|e| e.to_string())?;
        match ready_rx.recv().map_err(|e| e.to_string())? {
            Ok(()) => Ok(Self {
                sender: Some(sender),
                worker: Some(worker),
            }),
            Err(e) => {
                let _ = worker.join();
                Err(e)
            }
        }
    }
    /// Full queues reject explicitly; frames are never silently dropped.
    /// The transport Audio bridge awaits replies outside its executor threads.
    pub fn submit(&self, command: Command) -> Result<Receiver<Result<Value>>> {
        let sender = self.sender.as_ref().ok_or("audio runtime closed")?;
        let (tx, rx) = mpsc::channel();
        sender.try_send((command, tx)).map_err(|e| match e {
            TrySendError::Full(_) => "audio queue full".to_owned(),
            TrySendError::Disconnected(_) => "audio worker unavailable".to_owned(),
        })?;
        Ok(rx)
    }
    /// Drains accepted commands, then closes native handles. Idempotent.
    pub fn close(&mut self) -> Result<()> {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| "audio worker panicked".to_owned())?;
        }
        Ok(())
    }
}
impl Drop for AudioRuntime {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Test-only Python adapter: waits outside the GIL, exercising the native owner.
#[pyclass(name = "AudioRuntime")]
struct PyRuntime {
    runtime: Option<AudioRuntime>,
}
#[pymethods]
impl PyRuntime {
    #[new]
    #[pyo3(signature=(config_json="{}"))]
    fn new(py: Python<'_>, config_json: &str) -> PyResult<Self> {
        let config: Config = serde_json::from_str(config_json)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let runtime = py
            .detach(|| AudioRuntime::start(config))
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        Ok(Self {
            runtime: Some(runtime),
        })
    }
    fn call(&self, py: Python<'_>, command_json: &str) -> PyResult<String> {
        let command: Command = serde_json::from_str(command_json)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let rx = self
            .runtime
            .as_ref()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime closed"))?
            .submit(command)
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        let result = py
            .detach(move || rx.recv())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        Ok(result.to_string())
    }
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(mut runtime) = self.runtime.take() {
            py.detach(|| runtime.close())
                .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        }
        Ok(())
    }
    /// Validation adapter for accepted-work draining during shutdown.
    fn drain_and_close(&mut self, py: Python<'_>, commands_json: &str) -> PyResult<String> {
        let commands: Vec<Command> = serde_json::from_str(commands_json)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let mut runtime = self
            .runtime
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("runtime closed"))?;
        let result: Result<Vec<Value>> = py.detach(move || {
            let mut replies = Vec::new();
            for command in commands {
                replies.push(runtime.submit(command)?);
            }
            runtime.close()?;
            replies
                .into_iter()
                .map(|rx| rx.recv().map_err(|e| e.to_string())?)
                .collect()
        });
        Ok(
            serde_json::to_string(&result.map_err(pyo3::exceptions::PyRuntimeError::new_err)?)
                .unwrap(),
        )
    }
}
impl Drop for PyRuntime {
    fn drop(&mut self) {
        if let Some(mut runtime) = self.runtime.take() {
            let _ = Python::try_attach(|py| py.detach(|| runtime.close()));
        }
    }
}

#[pymodule]
/// Register the Python validation adapter; production transport uses AudioRuntime directly.
pub fn vogon_audio_runtime(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRuntime>()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_full_queue_rejects_without_overwriting_accepted_work() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let runtime = AudioRuntime {
            sender: Some(sender),
            worker: None,
        };
        let reply = runtime
            .submit(Command::Reference { data: vec![123] })
            .unwrap();
        assert_eq!(
            runtime.submit(Command::Clear).unwrap_err(),
            "audio queue full"
        );
        let (command, response) = receiver.recv().unwrap();
        assert!(matches!(command, Command::Reference { data } if data == vec![123]));
        response.send(Ok(Value::Null)).unwrap();
        assert_eq!(reply.recv().unwrap().unwrap(), Value::Null);
    }
    #[test]
    fn closed_runtime_rejects_new_work() {
        let runtime = AudioRuntime {
            sender: None,
            worker: None,
        };
        assert_eq!(
            runtime.submit(Command::Snapshot).unwrap_err(),
            "audio runtime closed"
        );
    }
}
