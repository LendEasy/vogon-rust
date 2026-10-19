//! Process admission and WebSocket/Driver ownership. No lock spans call I/O.
use crate::{
    Error, Future, Result,
    bootstrap::Config,
    boxed,
    record::{Record, State},
    store::{Change, SharedStore},
};
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use uuid::Uuid;
use vogon_clocks::Clock;
use vogon_contracts::{CallConfig, EscalationOrder, StartCallRequest};
use vogon_orchestration::{
    client::{Config as ApiConfig, HttpTurnClient},
    driver::{Config as DriverConfig, Driver},
    port::{Hooks, Input, NoHooks, SessionFactory},
};
use vogon_transport::{
    engine::Mode,
    wire::{Frame, Sink},
};

/// Shared immutable dependencies and bounded process-wide ownership registry.
pub struct Service {
    pub config: Config,
    pub store: SharedStore,
    pub http: reqwest::Client,
    pub clients: vogon_front::bootstrap::Clients,
    pub artifact: Option<vogon_front::config::Artifact>,
    pub hooks: Arc<dyn Hooks>,
    pub transfers: Arc<vogon_escalation::Manager>,
    pub(crate) calls: Arc<Semaphore>,
    pub(crate) handshakes: Arc<Semaphore>,
    registry: Mutex<HashSet<String>>,
    count: watch::Sender<usize>,
    stopping: watch::Sender<bool>,
}
impl Service {
    /// Build immutable dependencies without opening provider sockets.
    pub fn new(config: Config, store: SharedStore) -> Result<Arc<Self>> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Error::new("http_client_failed"))?;
        let clients = vogon_front::bootstrap::Clients::from_settings(&config.settings, |key| {
            (key == "OPENROUTER_API_KEY").then(|| config.openrouter_fallback.clone())
        })
        .map_err(|_| Error::new("invalid_front_clients"))?;
        let path = config.settings["front_config_path"].as_str().unwrap_or("");
        let artifact = if path.is_empty() {
            None
        } else {
            vogon_front::config::Artifact::load(
                std::path::Path::new(path),
                config.settings["front_max_words"].as_u64().unwrap_or(20) as usize,
            )
            .map_err(|_| eprintln!("front_artifact_load_failed"))
            .ok()
        };
        let transfer_store = if config.gateway["session_store"] == "redis" {
            vogon_escalation::Store::redis(config.settings["redis_url"].as_str().unwrap_or(""))
                .map_err(|e| Error::new(e.0))?
        } else {
            vogon_escalation::Store::default()
        };
        let setting = |name: &str| config.settings[name].as_str().unwrap_or("").to_owned();
        let transfers = vogon_escalation::Manager::new(
            vogon_escalation::Config {
                endpoint: config.twilio_endpoint.clone(),
                account: setting("twilio_account_sid"),
                token: setting("twilio_auth_token"),
                from: setting("twilio_phone_number"),
                public_base: config.gateway["public_base_url"]
                    .as_str()
                    .unwrap_or("")
                    .into(),
                say_voice: config.gateway["escalation_say_voice"]
                    .as_str()
                    .unwrap_or(vogon_escalation::twiml::VOICE)
                    .into(),
                tts_endpoint: "https://api.deepgram.com/v1/speak".into(),
                tts_key: setting("deepgram_api_key"),
                timeout: Duration::from_secs(10),
                retry_delays: [1, 2, 4, 8, 15, 30, 60]
                    .into_iter()
                    .map(Duration::from_secs)
                    .collect(),
            },
            transfer_store,
        )
        .map_err(|e| Error::new(e.0))?;
        Ok(Arc::new(Self {
            calls: Arc::new(Semaphore::new(config.max_active)),
            handshakes: Arc::new(Semaphore::new(config.max_handshakes)),
            config,
            store,
            http,
            clients,
            artifact,
            hooks: Arc::new(NoHooks),
            transfers,
            registry: Mutex::new(HashSet::new()),
            count: watch::channel(0).0,
            stopping: watch::channel(false).0,
        }))
    }
    /// Register before HTTP upgrade so shutdown also owns pending handshakes.
    pub fn register(self: &Arc<Self>) -> Result<Lease> {
        let mut registry = self.registry.lock().unwrap();
        if *self.stopping.borrow() {
            return Err(Error::new("service_stopping"));
        }
        let permit = self
            .handshakes
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::new("handshake_capacity"))?;
        let id = Uuid::new_v4().simple().to_string();
        registry.insert(id.clone());
        self.count.send_replace(registry.len());
        Ok(Lease {
            service: self.clone(),
            id,
            handshake: Some(permit),
        })
    }
    /// Stop admission, signal every socket and wait for call finalization/lease release.
    pub async fn shutdown(&self) -> Result<()> {
        let mut count = self.count.subscribe();
        {
            let _registry = self.registry.lock().unwrap();
            self.stopping.send_replace(true);
            self.calls.close();
            self.handshakes.close();
        }
        tokio::time::timeout(
            self.config.shutdown_timeout + Duration::from_secs(15),
            async {
                while *count.borrow_and_update() > 0 {
                    count
                        .changed()
                        .await
                        .map_err(|_| Error::new("registry_closed"))?;
                }
                Ok(())
            },
        )
        .await
        .map_err(|_| Error::new("shutdown_timeout"))??;
        tokio::time::timeout(self.config.shutdown_timeout, self.transfers.drain())
            .await
            .map_err(|_| Error::new("transfer_drain_timeout"))?;
        Ok(())
    }
    /// Number of owned upgraded/pending sockets; does not expose call identifiers.
    pub fn active(&self) -> usize {
        *self.count.borrow()
    }
    /// Admission closes immediately when shutdown starts; existing records remain readable.
    pub fn is_stopping(&self) -> bool {
        *self.stopping.borrow()
    }
    /// Best-effort bounded status relay; no redirects or bearer details in errors.
    pub async fn relay(&self, r: &Record, state: State) {
        if r.app_status_webhook.is_empty() {
            return;
        }
        let mut req = self
            .http
            .post(&r.app_status_webhook)
            .timeout(Duration::from_secs(5))
            .json(
                &json!({"call_id":r.call_id,"state":state,"provider_call_id":r.provider_call_id}),
            );
        if vogon_configuration::auth_enabled(&self.config.gateway) {
            req = req.bearer_auth(self.config.gateway["service_key"].as_str().unwrap());
        }
        if req.send().await.is_err() {
            eprintln!("status_relay_failed");
        }
    }
    /// Resolve a capability/provider identity, claim it once, then run the native driver.
    pub async fn websocket(
        self: Arc<Self>,
        mut socket: WebSocket,
        mut lease: Lease,
        token: Option<String>,
    ) {
        let mut shutdown = self.stopping.subscribe();
        if *shutdown.borrow() {
            close(&mut socket, 1013).await;
            return;
        }
        let resolved = self.resolve_socket(&mut socket, &mut shutdown, token).await;
        let (record, initial) = match resolved {
            Ok(v) => v,
            Err(e) => {
                if e.0 == "invalid_token" {
                    let _=socket.send(Message::Text(json!({"type":"server.error","payload":{"code":"invalid_token","fatal":true}}).to_string().into())).await;
                }
                close(
                    &mut socket,
                    if e.0 == "unknown_provider_call" {
                        4002
                    } else {
                        4001
                    },
                )
                .await;
                return;
            }
        };
        let Ok(_capacity) = self.calls.clone().try_acquire_owned() else {
            close(&mut socket, 1013).await;
            return;
        };
        let key = (record.tenant.clone(), record.call_id.clone());
        match self.store.claim(key.clone(), lease.id.clone()).await {
            Ok(true) => {}
            _ => {
                close(&mut socket, 4003).await;
                return;
            }
        }
        lease.handshake.take();
        let result = self.run(socket, &record, initial, shutdown).await;
        let terminal = if result.is_ok() {
            State::Ended
        } else {
            State::Failed
        };
        match self
            .store
            .change(key.clone(), Change::Transition(terminal))
            .await
        {
            Ok(true) => self.relay(&record, terminal).await,
            Ok(false) => {}
            Err(_) => eprintln!("session_finalize_failed"),
        }
        if self.store.release(key, lease.id.clone()).await.is_err() {
            eprintln!("session_claim_release_failed");
        }
    }
    /// Resolve token authority or the bounded phone handshake before allocating a driver.
    async fn resolve_socket(
        &self,
        socket: &mut WebSocket,
        shutdown: &mut watch::Receiver<bool>,
        token: Option<String>,
    ) -> Result<(Record, Option<Value>)> {
        if let Some(token) = token {
            let record = self
                .store
                .lookup(false, token)
                .await?
                .ok_or_else(|| Error::new("invalid_token"))?;
            if record.mode == "twilio" {
                return Err(Error::new("wrong_socket_mode"));
            }
            return Ok((record, None));
        }
        let initial = tokio::time::timeout(self.config.handshake_timeout, async {
            loop {
                let next = tokio::select! {
                    _ = shutdown.changed() => return Err(Error::new("service_stopping")),
                    next = socket.next() => next,
                };
                match next {
                    Some(Ok(Message::Text(s))) => {
                        let v: Value =
                            serde_json::from_str(&s).map_err(|_| Error::new("invalid_start"))?;
                        if v["event"] == "start" {
                            return Ok(v);
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        socket
                            .send(Message::Pong(data))
                            .await
                            .map_err(|_| Error::new("socket_closed"))?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    _ => return Err(Error::new("invalid_start")),
                }
            }
        })
        .await
        .map_err(|_| Error::new("start_timeout"))??;
        let sid = initial["start"]["callSid"]
            .as_str()
            .or_else(|| initial["callSid"].as_str())
            .unwrap_or("");
        let record = self
            .store
            .lookup(true, sid.into())
            .await?
            .filter(|r| r.mode == "twilio")
            .ok_or_else(|| Error::new("unknown_provider_call"))?;
        Ok((record, Some(initial)))
    }
    /// Keep caller input responsive while the owned driver executes startup/response work.
    /// Closing intake starts bounded graceful finalization; dropping the task aborts its driver.
    async fn run(
        &self,
        socket: WebSocket,
        record: &Record,
        initial: Option<Value>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mode = match record.mode.as_str() {
            "text" => Mode::Text,
            "twilio" => Mode::Twilio,
            _ => Mode::Browser,
        };
        let profile = self.config.profile(&record.tenant)?;
        let speech = if mode == Mode::Text {
            None
        } else {
            Some(
                vogon_speech_providers::config::SpeechSettings::from_settings(
                    &self.config.settings,
                )
                .map_err(|_| Error::new("invalid_speech_settings"))?,
            )
        };
        let api = Arc::new(HttpTurnClient::new(ApiConfig::resolve(
            &self.config.settings,
            &json!({"turn_service":{"base_url":record.app_base_url,"auth_token":record.app_auth_token}}),
            // Driver/Session share a new per-call Clock whose origin is zero.
            // Wall time belongs only to persisted records and client wire `ts`.
            // Subtracting utc_now here corrupts Turn API events into negative epochs.
            0.0,
        )?)?);
        let (outbound, mut inbound) = socket.split();
        let factory = SessionFactory {
            settings: self.config.settings.clone(),
            profile: profile.clone(),
            mode,
            speech,
            clients: self.clients.clone(),
            artifact: self.artifact.clone(),
            lender: String::new(),
            seed: 1,
            sink: tokio::sync::Mutex::new(Some(Box::new(SocketSink(outbound)))),
            initial_frame: initial,
            stt_endpoint: self.config.stt_endpoint.clone(),
            tts_endpoint: self.config.tts_endpoint.clone(),
        };
        let mut config = DriverConfig::new(StartCallRequest {
            call_id: record.call_id.clone(),
            mode: match mode {
                Mode::Text => "text",
                Mode::Twilio => "twilio",
                Mode::Browser => "voice",
            }
            .into(),
            resume: false,
            payload: record.app_payload.clone(),
        });
        config.provider_call_id = record.provider_call_id.clone();
        config.front_timing = (&vogon_front::config::Settings::from_gateway(&self.config.settings)
            .map_err(|_| Error::new("invalid_front_settings"))?)
            .into();
        let hooks = Arc::new(SnapshotHooks {
            store: self.store.clone(),
            key: (record.tenant.clone(), record.call_id.clone()),
            profile,
            next: Arc::new(crate::extensions::CallHooks::new(
                self.transfers.clone(),
                record.clone(),
                self.hooks.clone(),
            )),
        });
        let (input, inputs) = mpsc::channel(self.config.input_capacity);
        // Own the driver even if the enclosing upgraded socket task is dropped.
        struct CallTask(tokio::task::JoinHandle<vogon_orchestration::driver::Report>);
        impl Drop for CallTask {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let mut driver = CallTask(tokio::spawn(
            Driver {
                config,
                api,
                factory: Arc::new(factory),
                hooks,
                clock: Clock::default(),
            }
            .run(inputs),
        ));
        let result = loop {
            if *shutdown.borrow() {
                break None;
            }
            tokio::select! { biased;
                _=shutdown.changed()=>break None,
                report=&mut driver.0=>break Some(report),
                message=inbound.next()=>match message {
                    Some(Ok(Message::Text(s)))=>match serde_json::from_str(&s){Ok(v)=>if input.try_send(Input::Json(v)).is_err(){break None},Err(_)=>break None},
                    Some(Ok(Message::Binary(bytes)))=>if input.try_send(Input::Binary(bytes.to_vec())).is_err(){break None},
                    Some(Ok(Message::Ping(_)|Message::Pong(_)))=>{},
                    _=>break None,
                }
            }
        };
        drop(input);
        let outcome = match result {
            Some(r) => r,
            None => match tokio::time::timeout(self.config.shutdown_timeout, &mut driver.0).await {
                Ok(r) => r,
                Err(_) => {
                    driver.0.abort();
                    let _ = (&mut driver.0).await;
                    return Err(Error::new("call_shutdown_timeout"));
                }
            },
        };
        let report = outcome.map_err(|_| Error::new("call_task_failed"))?;
        if report.reason == "start_failed" || !report.errors.is_empty() {
            Err(Error::new("call_failed"))
        } else {
            Ok(())
        }
    }
}
/// Registry guard spans upgrade, media and finalization; dropping it releases admission.
pub struct Lease {
    service: Arc<Service>,
    pub id: String,
    handshake: Option<OwnedSemaphorePermit>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut r = self.service.registry.lock().unwrap();
        r.remove(&self.id);
        self.service.count.send_replace(r.len());
    }
}
/// Best-effort protocol close with a bound even for an unresponsive peer.
async fn close(socket: &mut WebSocket, code: u16) {
    let _ = tokio::time::timeout(
        Duration::from_secs(1),
        socket.send(Message::Close(Some(CloseFrame {
            code,
            reason: "".into(),
        }))),
    )
    .await;
}
/// Sink acknowledgement means the underlying WebSocket send completed, not enqueue.
struct SocketSink(SplitSink<WebSocket, Message>);
impl Sink for SocketSink {
    fn send(
        &mut self,
        frame: Frame,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = vogon_speech_providers::Result<()>> + Send + '_>,
    > {
        Box::pin(async move {
            self.0
                .send(match frame {
                    Frame::Json(v) => Message::Text(v.to_string().into()),
                    Frame::Binary(b) => Message::Binary(b.into()),
                })
                .await
                .map_err(|_| vogon_speech_providers::Error::new("caller_send_failed"))
        })
    }
}
/// Persist the exact tenant/call snapshot before opening transport; forward later hooks.
struct SnapshotHooks {
    store: SharedStore,
    key: (String, String),
    profile: Value,
    next: Arc<dyn Hooks>,
}
impl Hooks for SnapshotHooks {
    fn configure(&self, call: CallConfig) -> Future<'_, ()> {
        boxed(async move {
            let snapshot = vogon_configuration::resolve_call_snapshot(&self.profile, &json!(call))
                .map_err(|_| Error::new("invalid_call_snapshot"))?;
            if !self
                .store
                .change(self.key.clone(), Change::Snapshot(snapshot))
                .await?
            {
                return Err(Error::new("session_expired"));
            }
            self.next.configure(call).await
        })
    }
    fn prepare_escalation(&self, order: EscalationOrder) -> Future<'_, ()> {
        self.next.prepare_escalation(order)
    }
    fn escalate(&self, order: EscalationOrder) -> Future<'_, Value> {
        self.next.escalate(order)
    }
    fn finish(&self, id: String, reason: String) -> Future<'_, ()> {
        self.next.finish(id, reason)
    }
}
