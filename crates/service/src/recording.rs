//! Recording composition: incoming original mu-law, successful sends and clear receipts.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use vogon_artifacts::{recorder::Recorder, store::Stores};
use vogon_clocks::Clock;
use vogon_transport::wire::{Frame, Observer, Sink};

/// Call-local audio, output sample clock and one-shot finalization ownership.
struct State {
    recorder: Option<Recorder>,
    agent_end: f64,
    phone: bool,
}
/// Shared only by one call's Sink, observer and finish hook; no process-wide media state.
pub struct Capture {
    state: Mutex<State>,
    stores: Arc<Stores>,
    tenant: String,
    call: String,
    clock: Clock,
    metrics: Arc<vogon_observability::Metrics>,
}
impl Capture {
    /// Construct before transport startup, with the same monotonic clock as Driver.
    pub fn new(
        stores: Arc<Stores>,
        tenant: String,
        call: String,
        clock: Clock,
        phone: bool,
    ) -> Arc<Self> {
        Self::with_metrics(
            stores,
            tenant,
            call,
            clock,
            phone,
            Arc::new(vogon_observability::Metrics::default()),
        )
    }
    /// Inject process metrics without sharing call-local recording clocks or counters.
    pub fn with_metrics(
        stores: Arc<Stores>,
        tenant: String,
        call: String,
        clock: Clock,
        phone: bool,
        metrics: Arc<vogon_observability::Metrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            metrics,
            state: Mutex::new(State {
                recorder: Some(Recorder::default()),
                agent_end: 0.,
                phone,
            }),
            stores,
            tenant,
            call,
            clock,
        })
    }
    /// Apply resolved recording policy before any provider/socket media is admitted.
    pub fn configure(&self, enabled: bool) {
        if let Some(r) = &mut self.state.lock().unwrap().recorder {
            r.enabled = enabled;
        }
    }
    /// Register only a completed send. Clear trims the estimated queued agent tail.
    pub fn sent(&self, frame: &Frame) {
        let mut s = self.state.lock().unwrap();
        if !s.phone {
            return;
        }
        let now = self.clock.now();
        let start = now.max(s.agent_end);
        let Frame::Json(v) = frame else {
            return;
        };
        match v["event"].as_str() {
            Some("media") => {
                if let Some(payload) = v["media"]["payload"]
                    .as_str()
                    .and_then(|s| STANDARD.decode(s).ok())
                {
                    if let Some(r) = &mut s.recorder {
                        r.add(false, start, &payload);
                    }
                    s.agent_end = start + payload.len() as f64 / 8000.;
                }
            }
            Some("clear") => {
                self.metrics.increment(vogon_observability::Event::Clear);
                if let Some(r) = &mut s.recorder {
                    let n = r.clear(now);
                    r.event(now, "twilio_clear", json!({"discarded_samples":n}));
                }
                s.agent_end = now;
            }
            Some("mark") => {
                if let Some(r) = &mut s.recorder {
                    r.event(now, "mark_sent", json!({"name":v["mark"]["name"]}));
                }
            }
            _ => {}
        }
    }
    /// Freeze and consume capture once, encode off Tokio, then await independent stores.
    pub async fn finish(&self, reason: &str) -> Vec<&'static str> {
        let recorder = self.state.lock().unwrap().recorder.take();
        let Some(recorder) = recorder else {
            return vec![];
        };
        if !recorder.enabled {
            return vec![];
        }
        let meta =
            json!({"call_id":self.call,"reason":reason,"channels":"stereo","sample_rate":8000});
        let result = tokio::task::spawn_blocking(move || {
            let events = recorder.event_json(meta);
            let frames = recorder.frames_csv();
            recorder.wav().map(|wav| (wav, events, frames))
        })
        .await;
        let (wav, events, frames) = match result {
            Ok(Ok(v)) => v,
            _ => return vec!["recording_encode_failed"],
        };
        let mut errors = Vec::new();
        if let Some(wav) = wav {
            errors.extend(
                self.stores
                    .put(&self.tenant, &self.call, "recording.wav", wav, "audio/wav")
                    .await,
            );
        }
        if !events["events"].as_array().unwrap().is_empty() {
            errors.extend(
                self.stores
                    .put(
                        &self.tenant,
                        &self.call,
                        "audio-events.json",
                        events.to_string().into_bytes(),
                        "application/json",
                    )
                    .await,
            );
        }
        if let Some(frames) = frames {
            errors.extend(
                self.stores
                    .put(&self.tenant, &self.call, "frames.csv", frames, "text/csv")
                    .await,
            );
        }
        for _ in &errors {
            self.metrics
                .increment(vogon_observability::Event::ArtifactFailed);
        }
        errors
    }
}
impl Observer for Capture {
    fn input(&self, now: f64, v: &Value) {
        let mut s = self.state.lock().unwrap();
        if !s.phone {
            return;
        }
        let Some(r) = &mut s.recorder else {
            return;
        };
        match v["event"].as_str() {
            Some("start") => r.epoch(now),
            Some("media") => {
                if let Some(data) = v["media"]["payload"]
                    .as_str()
                    .and_then(|p| STANDARD.decode(p).ok())
                {
                    let at = r.caller_timestamp(&v["media"]["timestamp"], now);
                    r.add(true, at, &data);
                }
            }
            Some("mark") => r.event(now, "mark_echo", json!({"name":v["mark"]["name"]})),
            Some("stop") => r.event(now, "stream_stop", json!({})),
            _ => {}
        }
    }
    fn frame(&self, now: f64, detail: Value) {
        self.metrics.frame(
            detail["raw_rms"].as_i64().unwrap_or(0),
            detail["clean_rms"].as_i64().unwrap_or(0),
        );
        if let Some(r) = &mut self.state.lock().unwrap().recorder {
            r.frame(now, detail);
        }
    }
    fn event(&self, now: f64, event: &Value) {
        self.metrics.observe(event);
        let name = match event["kind"].as_str().unwrap_or("transport_event") {
            "utterance_eager" => "eager_eot",
            "utterance_final" => "stt_final",
            "silence" => "clock_silence",
            "duration" => "clock_duration",
            name => name,
        };
        if let Some(r) = &mut self.state.lock().unwrap().recorder {
            r.event(now, name, event.clone());
        }
    }
}
/// Socket decorator; failed sends never become recorded playback or successful clears.
pub struct RecordingSink {
    pub next: Box<dyn Sink>,
    pub capture: Arc<Capture>,
}
impl Sink for RecordingSink {
    fn observer(&self) -> Option<Arc<dyn Observer>> {
        Some(self.capture.clone())
    }
    fn send(
        &mut self,
        frame: Frame,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = vogon_speech_providers::Result<()>> + Send + '_>,
    > {
        Box::pin(async move {
            self.next.send(frame.clone()).await?;
            self.capture.sent(&frame);
            Ok(())
        })
    }
}
