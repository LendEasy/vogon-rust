//! Transport and future recording/escalation boundaries. No application reasoning lives here.
use crate::{Error, Future, Result, boxed};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc};
use vogon_clocks::Clock;
use vogon_contracts::{CallConfig, EscalationOrder, SpeakCommand};
use vogon_front::{
    bootstrap::{Clients, Identity},
    config::Artifact,
    source::Source,
};
use vogon_playback::delivery::DeliveryReport;
use vogon_speech_providers::{
    client::{STT_ENDPOINT, TTS_ENDPOINT},
    config::SpeechSettings,
};
use vogon_transport::{
    engine::Mode,
    session::{Config, Response, Session},
    wire::Sink,
};
/// Authenticated input from the future HTTP/WebSocket host; bounded before entering the call.
#[derive(Clone, Debug)]
pub enum Input {
    Json(Value),
    Binary(Vec<u8>),
    Disconnect,
}
/// Narrow independently callable adapter; implementations must not hold a global lock across I/O.
pub trait Port: Send + Sync + 'static {
    /// Feed media/control while requests or speech are outstanding; true means hangup.
    fn input(&self, input: Input) -> Future<'_, bool>;
    /// Emit one speech command using transport generation checks and delivery accounting.
    fn speak(
        &self,
        command: SpeakCommand,
        generation: Option<u64>,
    ) -> Future<'_, Option<DeliveryReport>>;
    /// Send a gateway/client control event.
    fn client_event(&self, value: Value) -> Future<'_, ()>;
    /// Read playback generation without waiting for network operations.
    fn generation(&self) -> Future<'_, Option<u64>>;
    /// Pause/resume reasoning silence or apply a returned ladder/grace policy.
    fn silence(&self, change: Silence) -> Future<'_, ()>;
    /// Interrupt active output before shutting down a call.
    fn interrupt(&self) -> Future<'_, ()>;
    /// Drain providers and release native worker after orchestration tasks stop.
    fn close(&self) -> Future<'_, ()>;
    /// Wait for handoff playback receipts before a transfer redirect.
    fn heard(&self, cap: Duration) -> Future<'_, bool>;
}
/// Explicit clock operations, keeping clocks out of application state.
#[derive(Clone, Debug)]
pub enum Silence {
    Pause,
    Resume,
    Grace(f64),
    Ladder(String),
}
/// Result of constructing a transport from the application-provided configuration.
pub struct Opened {
    pub port: Arc<dyn Port>,
    pub events: mpsc::UnboundedReceiver<Value>,
    pub source: Option<Arc<Source>>,
}
/// Startup seam: provider sockets are opened only after StartCall succeeds.
pub trait Factory: Send + Sync + 'static {
    /// Build one call and optionally install a per-call front source.
    fn open(&self, config: CallConfig, clock: Clock) -> Future<'_, Opened>;
}
/// Module 11/12 integration seams. Escalation yields a fact; artifact completion is awaited.
pub trait Hooks: Send + Sync + 'static {
    /// Apply recording policy before audio starts.
    fn configure(&self, config: CallConfig) -> Future<'_, ()>;
    /// Prepare transfer audio while handoff speech plays; no redirect/dial may happen here.
    fn prepare_escalation(&self, _: EscalationOrder) -> Future<'_, ()> {
        boxed(async { Ok(()) })
    }
    /// Perform an application-composed escalation order after its handoff speech.
    fn escalate(&self, order: EscalationOrder) -> Future<'_, Value>;
    /// Flush recording/observability artifacts after transport close.
    fn finish(&self, call_id: String, reason: String) -> Future<'_, ()>;
}
/// Explicit unimplemented external hooks; never reports a successful transfer/upload.
pub struct NoHooks;
impl Hooks for NoHooks {
    fn configure(&self, _: CallConfig) -> Future<'_, ()> {
        boxed(async { Ok(()) })
    }
    fn escalate(&self, order: EscalationOrder) -> Future<'_, Value> {
        boxed(async move {
            Ok(
                json!({"kind":"escalation_outcome","status":"failed","target":order.targets.first().map(|t|t.label.as_str()).unwrap_or(""),"detail":"escalation_module_not_installed","escalation_id":order.escalation_id}),
            )
        })
    }
    fn finish(&self, _: String, _: String) -> Future<'_, ()> {
        boxed(async { Ok(()) })
    }
}
/// Production adapter with only a short pointer lock. Methods clone Session before awaiting I/O.
pub struct SessionPort {
    session: Mutex<Option<Arc<Session>>>,
}
impl SessionPort {
    /// Transfer exclusive lifecycle ownership to the orchestrator.
    pub fn new(session: Session) -> Self {
        Self {
            session: Mutex::new(Some(Arc::new(session))),
        }
    }
    async fn get(&self) -> Result<Arc<Session>> {
        self.session
            .lock()
            .await
            .clone()
            .ok_or_else(|| Error::new("transport_closed"))
    }
}
fn transport_error(e: vogon_speech_providers::Error) -> Error {
    Error(e.code)
}
impl Port for SessionPort {
    fn input(&self, input: Input) -> Future<'_, bool> {
        boxed(async move {
            let s = self.get().await?;
            match input {
                Input::Json(v) => s.text(v).await.map_err(transport_error),
                Input::Binary(v) => {
                    s.binary(v).await.map_err(transport_error)?;
                    Ok(false)
                }
                Input::Disconnect => Ok(true),
            }
        })
    }
    fn speak(
        &self,
        c: SpeakCommand,
        generation: Option<u64>,
    ) -> Future<'_, Option<DeliveryReport>> {
        boxed(async move {
            let s = self.get().await?;
            let r = Response {
                text: c.text,
                audio_id: c.audio_id,
                protected: c.protected,
                absorb: c.absorb_barge,
                generation,
                ..Default::default()
            };
            let result = if c.play_after == "line_quiet" {
                s.send_held_response(r).await
            } else {
                s.send_response(r).await
            };
            match result {
                Err(e) if e.code == "playback_rejected" => Ok(None),
                other => other.map_err(transport_error),
            }
        })
    }
    fn client_event(&self, value: Value) -> Future<'_, ()> {
        boxed(async move {
            self.get()
                .await?
                .client_event(value)
                .await
                .map_err(transport_error)
        })
    }
    fn generation(&self) -> Future<'_, Option<u64>> {
        boxed(async move { Ok(self.get().await?.playback_generation().await) })
    }
    fn silence(&self, c: Silence) -> Future<'_, ()> {
        boxed(async move {
            let s = self.get().await?;
            match c {
                Silence::Pause => s.pause_silence().await,
                Silence::Resume => s.resume_silence().await,
                Silence::Grace(n) => s.grant_silence_grace(n).await,
                Silence::Ladder(n) => s.select_silence_ladder(&n).await,
            };
            Ok(())
        })
    }
    fn interrupt(&self) -> Future<'_, ()> {
        boxed(async move { self.get().await?.interrupt().await.map_err(transport_error) })
    }
    fn heard(&self, cap: Duration) -> Future<'_, bool> {
        boxed(async move { Ok(self.get().await?.wait_playout_heard(cap).await) })
    }
    fn close(&self) -> Future<'_, ()> {
        boxed(async move {
            let session = self.session.lock().await.take();
            if let Some(s) = session {
                let mut s = Arc::try_unwrap(s).map_err(|_| Error::new("transport_still_in_use"))?;
                let result = s.close().await.map_err(transport_error);
                *self.session.lock().await = Some(Arc::new(s));
                result?;
            }
            Ok(())
        })
    }
}
/// Concrete factory retaining module-1 resolution and native streaming provider settings.
/// Each factory owns one caller Sink; create one per call. It never loads dotenv itself.
pub struct SessionFactory {
    pub settings: Value,
    pub profile: Value,
    pub mode: Mode,
    pub speech: Option<SpeechSettings>,
    pub clients: Clients,
    pub artifact: Option<Artifact>,
    pub lender: String,
    pub seed: u64,
    pub sink: Mutex<Option<Box<dyn Sink>>>,
    pub initial_frame: Option<Value>,
    pub stt_endpoint: String,
    pub tts_endpoint: String,
}
impl SessionFactory {
    /// Standard endpoints; tests may override these with local WebSocket fixtures.
    pub fn endpoints() -> (String, String) {
        (STT_ENDPOINT.into(), TTS_ENDPOINT.into())
    }
}
impl Factory for SessionFactory {
    fn open(&self, call: CallConfig, clock: Clock) -> Future<'_, Opened> {
        boxed(async move {
            let value = json!(call);
            let snapshot = vogon_configuration::resolve_call_snapshot(&self.profile, &value)
                .map_err(|_| Error::new("invalid_call_snapshot"))?;
            let mut config = Config::from_snapshot(self.mode, &self.settings, &snapshot)
                .map_err(transport_error)?;
            config.judge = self.clients.judge.clone();
            config.line_client = self.clients.jev.clone();
            let sink = self
                .sink
                .lock()
                .await
                .take()
                .ok_or_else(|| Error::new("caller_sink_already_used"))?;
            let (session, events) = if self.mode == Mode::Text {
                Session::text_only(config, sink, clock.clone()).await
            } else {
                let speech = self
                    .speech
                    .as_ref()
                    .ok_or_else(|| Error::new("missing_speech_settings"))?
                    .for_call(&value);
                Session::connect(
                    config,
                    &speech,
                    sink,
                    clock.clone(),
                    &self.stt_endpoint,
                    &self.tts_endpoint,
                )
                .await
            }
            .map_err(transport_error)?;
            if let Some(frame) = &self.initial_frame {
                session.text(frame.clone()).await.map_err(transport_error)?;
            }
            let source = if self.mode == Mode::Text {
                None
            } else {
                self.clients
                    .source(
                        &self.settings,
                        self.artifact.clone(),
                        Identity {
                            agent: &call.persona_name,
                            lender: &self.lender,
                            borrower: call
                                .name_hint
                                .as_deref()
                                .unwrap_or("")
                                .split_whitespace()
                                .next()
                                .unwrap_or(""),
                        },
                        clock,
                        self.seed,
                    )
                    .map_err(|e| Error(e.code))?
                    .map(Arc::new)
            };
            Ok(Opened {
                port: Arc::new(SessionPort::new(session)),
                events,
                source,
            })
        })
    }
}
