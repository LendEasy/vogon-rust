//! Startup/readiness probes execute real native primitives without calling paid providers.
use crate::{Error, Result, host::Service};
use pyo3::prelude::*;
use serde_json::{Value, json};
use vogon_audio_core::{
    libraries::Libraries,
    native::{EchoCanceller, VoiceDetector},
};
/// Probe CPython/NumPy/audioop and execute one frame through SpeexDSP and native VAD.
/// Runs outside Tokio; no production call or global logical clock is touched.
fn native() -> Value {
    let python = Python::attach(|py| -> PyResult<()> {
        let libs = Libraries::new(py)?;
        let pcm = libs.decode(py, &[255; 160])?;
        let a = libs.samples(py, &pcm, "float64")?;
        let energy: f64 = a.call_method0("sum")?.extract()?;
        if energy != 0. {
            return Err(pyo3::exceptions::PyValueError::new_err("invalid silence"));
        }
        Ok(())
    })
    .is_ok();
    let aec = EchoCanceller::new()
        .and_then(|mut a| a.process(&[0; 320], &[0; 320]))
        .is_ok();
    let vad = VoiceDetector::new(2)
        .and_then(|mut v| v.process(&[0; 320], 8000))
        .is_ok();
    json!({"python_numpy_audioop":python,"speexdsp":aec,"webrtcvad":vad})
}
impl Service {
    /// Cache expensive native probes; refuse invalid native deployment unless policy is warn.
    pub async fn startup(&self) -> Result<Value> {
        let checks = self
            .native_checks
            .get_or_init(|| async {
                tokio::task::spawn_blocking(native).await.unwrap_or(
                    json!({"python_numpy_audioop":false,"speexdsp":false,"webrtcvad":false}),
                )
            })
            .await
            .clone();
        if checks.as_object().unwrap().values().any(|v| v != true)
            && self.config.gateway["capability_policy"] != "warn"
        {
            return Err(Error::new("native_capability_failed"));
        }
        if self.config.settings["recording_enabled"] == true {
            self.recordings.probe().await.map_err(|e| Error::new(e.0))?;
        }
        self.artifact_ready
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(checks)
    }
    /// Readiness includes current session-store access and optional requested caller mode.
    /// Credential presence is configuration readiness, not remote provider health.
    pub async fn readiness(&self, mode: &str) -> Value {
        let native = self.native_checks.get().cloned().unwrap_or(Value::Null);
        let native_ok = native
            .as_object()
            .is_some_and(|m| m.values().all(|v| v == true));
        let store = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            self.store.get(("health".into(), "probe".into())),
        )
        .await
        .is_ok_and(|r| r.is_ok());
        let speech = self.config.settings["deepgram_api_key"]
            .as_str()
            .is_some_and(|s| !s.trim().is_empty());
        let twilio = [
            "twilio_account_sid",
            "twilio_auth_token",
            "twilio_phone_number",
        ]
        .iter()
        .all(|k| {
            self.config.settings[k]
                .as_str()
                .is_some_and(|s| !s.trim().is_empty())
        });
        let mode_ok = match mode {
            "voice" => speech && native_ok,
            "twilio" => speech && twilio && native_ok,
            "text" => true,
            _ => false,
        };
        let artifacts = self
            .artifact_ready
            .load(std::sync::atomic::Ordering::Acquire);
        let ok = !self.is_stopping()
            && artifacts
            && store
            && mode_ok
            && (native_ok || self.config.gateway["capability_policy"] == "warn");
        json!({"ready":ok,"mode":mode,"native":native,"session_store":store,"speech_configured":speech,"twilio_configured":twilio,"stopping":self.is_stopping(),"artifact_store":artifacts})
    }
}
