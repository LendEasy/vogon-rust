//! Async call adapter. Provider reads, caller input, pacing and timer ticks run
//! independently. All policy transitions use Engine; socket writes use Writer.
use crate::{
    audio::Audio,
    engine::{Action, Engine, Mode, is_turn_audio},
    wire::{self, Frame, Receipt, Sink, Writer, WriterTask},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    sync::{Mutex, mpsc, watch},
    task::JoinHandle,
    time::{self, Duration},
};
use vogon_audio_runtime::{Command, Config as AudioConfig};
use vogon_barge_in::floor::{self, AdmissionResult, Effect};
use vogon_clocks::{Clock, call::Config as ClockConfig};
use vogon_playback::{
    delivery::{DeliveryReport, estimate_total_ms},
    stream::{Action as StreamAction, SentenceStream},
};
use vogon_speech_providers::{
    Error, Result,
    client::{STT_ENDPOINT, SttClient, SttInput, SttOutput, TTS_ENDPOINT, TtsClient, TtsOutput},
    config::SpeechSettings,
};
use vogon_speech_providers::{
    config::ApiKey,
    live::LiveConfig,
    live_client::{LIVE_ENDPOINT, LiveClient, LiveInput},
};

mod playback;

/// Per-call configuration. Hosts transfer already-resolved call/settings values here.
#[derive(Clone)]
pub struct Config {
    /// Caller wire protocol; only Twilio owns phone AEC and floor control.
    pub mode: Mode,
    /// Per-call native audio worker settings, used by the phone adapter.
    pub audio: AudioConfig,
    /// Silence and duration ladders resolved from call configuration.
    pub clocks: ClockConfig,
    /// Acoustic episode and caller-floor timing policy.
    pub floor: floor::Config,
    /// Initial playback/semantic policy view supplied to the floor controller.
    pub policy: floor::Transport,
    /// Maximum sentence-stream buffering lead in milliseconds.
    pub lead_ms: f64,
    /// Delay before typing ambience starts during an accepted response gap.
    pub typing_delay_ms: u64,
    /// Optional alternative listener; None selects Deepgram. Testing is deferred.
    pub live: Option<(LiveConfig, ApiKey)>,
    /// Optional gateway-owned floor judge; absent preserves external classifier dispatch.
    pub judge: Option<vogon_front::judge::Client>,
    /// Optional line instrument; a configured detector without one reports unknown on failure.
    pub line_client: Option<vogon_front::jev::Client>,
    /// Enabled phone line policy resolved from CallConfig.
    pub line: Option<vogon_front::line::Config>,
    /// Caller-name hint used only in the line instrument's existing context shape.
    pub name_hint: String,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            mode: Mode::Twilio,
            audio: AudioConfig {
                barge_gate: Some(Default::default()),
                ..AudioConfig::default()
            },
            clocks: ClockConfig::default(),
            floor: floor::Config::default(),
            policy: floor::Transport::default(),
            lead_ms: 500.0,
            typing_delay_ms: 0,
            live: None,
            judge: None,
            line_client: None,
            line: None,
            name_hint: String::new(),
        }
    }
}
impl Config {
    /// Translate validated module-1 settings and call snapshot into adapter policy.
    /// Unsupported legacy modes reject explicitly instead of silently changing behavior.
    pub fn from_snapshot(mode: Mode, settings: &Value, snapshot: &Value) -> Result<Self> {
        if mode == Mode::Twilio
            && (settings["stt_feed_mode"] != "open"
                || settings["tts_pacing_mode"] != "sentence"
                || settings["tts_phone_encoding"] != "mulaw"
                || settings["barge_controller_enabled"] != true)
        {
            return Err(Error::new("unsupported_legacy_transport_mode"));
        }
        let call = &snapshot["call_config"];
        let resolved = &snapshot["resolved"];
        let mut cfg = Self {
            mode,
            ..Default::default()
        };
        cfg.name_hint = call["name_hint"].as_str().unwrap_or("").into();
        if mode == Mode::Twilio && !call["line_detection"].is_null() {
            let line: vogon_front::line::Config =
                serde_json::from_value(call["line_detection"].clone())
                    .map_err(|_| Error::new("invalid_line_config"))?;
            cfg.line = line.enabled.then_some(line);
        }
        cfg.clocks =
            serde_json::from_value(call.clone()).map_err(|_| Error::new("invalid_clock_config"))?;
        cfg.floor = serde_json::from_value(settings.clone())
            .map_err(|_| Error::new("invalid_floor_config"))?;
        cfg.lead_ms = settings["tts_buffer_lead_ms"]
            .as_f64()
            .ok_or_else(|| Error::new("invalid_transport_settings"))?;
        cfg.audio.encoding = settings["tts_phone_encoding"]
            .as_str()
            .ok_or_else(|| Error::new("invalid_transport_settings"))?
            .into();
        cfg.audio.vad_mode = settings["barge_in_vad_aggressiveness"]
            .as_u64()
            .ok_or_else(|| Error::new("invalid_transport_settings"))?
            as u8;
        let mut gate = serde_json::Map::new();
        for (key, value) in settings
            .as_object()
            .ok_or_else(|| Error::new("invalid_transport_settings"))?
        {
            if let Some(name) = key.strip_prefix("barge_in_") {
                gate.insert(name.into(), value.clone());
            }
        }
        gate.insert(
            "grace_sec".into(),
            json!(settings["barge_in_grace_ms"].as_f64().unwrap_or(100.0) / 1000.0),
        );
        gate.insert(
            "confirm_timeout_sec".into(),
            json!(
                settings["barge_in_confirm_timeout_ms"]
                    .as_f64()
                    .unwrap_or(400.0)
                    / 1000.0
            ),
        );
        cfg.audio.barge_gate = Some(
            serde_json::from_value(Value::Object(gate))
                .map_err(|_| Error::new("invalid_gate_config"))?,
        );
        let amb = |key: &str, fallback: &str| {
            call["ambience"]
                .get(key)
                .cloned()
                .unwrap_or_else(|| settings[fallback].clone())
        };
        cfg.audio.ambience = resolved["ambience"].as_bool().unwrap_or(false);
        cfg.audio.enable_room = amb("room", "ambience_room_tone").as_bool().unwrap_or(true);
        cfg.audio.enable_typing = amb("typing", "ambience_typing").as_bool().unwrap_or(true);
        cfg.audio.enable_oneshots = amb("oneshots", "ambience_oneshots")
            .as_bool()
            .unwrap_or(false);
        cfg.audio.mixer_config=Some(serde_json::from_value(json!({"room_gain":amb("room_gain","ambience_room_gain"),"duck_factor":amb("duck_factor","ambience_duck_factor"),"typing_gain":amb("typing_gain","ambience_typing_gain")})).map_err(|_|Error::new("invalid_mixer_config"))?);
        cfg.typing_delay_ms = amb("typing_delay_ms", "ambience_typing_delay_ms")
            .as_u64()
            .ok_or_else(|| Error::new("invalid_typing_delay"))?;
        cfg.policy.semantic_judge = resolved["semantic_barge_enabled"].as_bool().unwrap_or(true);
        cfg.policy.safety_phrases = serde_json::from_value(resolved["safety_phrases"].clone())
            .map_err(|_| Error::new("invalid_barge_policy"))?;
        cfg.policy.floor_phrases =
            serde_json::from_value(resolved["floor_request_phrases"].clone())
                .map_err(|_| Error::new("invalid_barge_policy"))?;
        cfg.policy.ambience = cfg.audio.ambience;
        Ok(cfg)
    }
}
/// Caller-requested delivery policy; generation comes from the triggering utterance.
#[derive(Clone, Debug, Default)]
pub struct Response {
    /// Speech or text content. Blank content returns no delivery report.
    pub text: String,
    /// Stable playback identifier used by wire frames, marks and delivery reports.
    pub audio_id: String,
    /// Apply the floor controller's protected-playback policy.
    pub protected: bool,
    /// Retain caller utterances as context under the controller's absorb policy.
    pub absorb: bool,
    /// Generation attached to the triggering utterance; stale responses may be rejected.
    pub generation: Option<u64>,
    /// Permit floor admission to wait for ownership instead of immediately rejecting.
    pub wait_for_floor: bool,
}
/// Provider choice preserves a single transcript contract for all adapters.
enum Listener {
    Flux(SttClient),
    Live(LiveClient),
}
impl Listener {
    fn input(&self) -> ListenerInput {
        match self {
            Self::Flux(c) => ListenerInput::Flux(c.input.clone()),
            Self::Live(c) => ListenerInput::Live(c.input.clone()),
        }
    }
    async fn close(&mut self) {
        match self {
            Self::Flux(c) => c.close().await,
            Self::Live(c) => c.close().await,
        }
    }
}
#[derive(Clone)]
enum ListenerInput {
    Flux(SttInput),
    Live(LiveInput),
}
impl ListenerInput {
    async fn send_audio(&self, data: Vec<u8>) -> Result<()> {
        match self {
            Self::Flux(c) => c.send_audio(data).await,
            Self::Live(c) => c.send_audio(data).await,
        }
    }
    async fn finish_utterance(&self) -> Result<()> {
        match self {
            Self::Flux(c) => c.finish_utterance().await,
            Self::Live(_) => Ok(()),
        }
    }
}
/// Shared operations; no task owns a strong reference while waiting for new input.
struct Inner {
    config: Config,
    clock: Clock,
    state: Arc<Mutex<Engine>>,
    audio: Option<Audio>,
    writer: Writer,
    stt: Option<ListenerInput>,
    tts: Mutex<Option<TtsClient>>,
    cancel: watch::Sender<u64>,
    cancel_lock: Mutex<()>,
    cancel_operation: Mutex<()>,
    cancel_pending: AtomicBool,
    response_lock: Mutex<()>,
    dispatch_lock: Mutex<()>,
    events: mpsc::UnboundedSender<Value>,
    decisions: mpsc::UnboundedSender<DecisionCompletion>,
    decision_tasks: Mutex<tokio::task::JoinSet<()>>,
}
/// Provider completion always carries its original token back to the owning controller.
enum DecisionCompletion {
    Judge(u64, floor::ClassifierResult),
    Line(u64, Option<f64>),
}
/// Owns sockets, background callbacks and heartbeat for one call. Close explicitly
/// to drain STT fallback events and release native audio off the executor.
pub struct Session {
    inner: Arc<Inner>,
    stt: Option<Listener>,
    tasks: Vec<JoinHandle<()>>,
    _writer: WriterTask,
}
impl Drop for Session {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
impl Session {
    /// Connect streaming Deepgram providers, using the caller's reusable settings.
    pub async fn open(
        config: Config,
        settings: &SpeechSettings,
        sink: Box<dyn Sink>,
        clock: Clock,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Value>)> {
        let endpoint = if config.live.is_some() {
            LIVE_ENDPOINT
        } else {
            STT_ENDPOINT
        };
        Self::connect(config, settings, sink, clock, endpoint, TTS_ENDPOINT).await
    }
    /// Endpoint injection supports local protocol/integration tests. Text mode opens no providers.
    pub async fn connect(
        config: Config,
        settings: &SpeechSettings,
        sink: Box<dyn Sink>,
        clock: Clock,
        stt_endpoint: &str,
        tts_endpoint: &str,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Value>)> {
        Self::connect_inner(
            config,
            Some(settings),
            sink,
            clock,
            stt_endpoint,
            tts_endpoint,
        )
        .await
    }
    /// Construct a text-only adapter without speech credentials or network connections.
    pub async fn text_only(
        mut config: Config,
        sink: Box<dyn Sink>,
        clock: Clock,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Value>)> {
        config.mode = Mode::Text;
        config.live = None;
        Self::connect_inner(config, None, sink, clock, "", "").await
    }
    /// Shared startup; speech configuration is required only for an audio adapter.
    async fn connect_inner(
        config: Config,
        settings: Option<&SpeechSettings>,
        sink: Box<dyn Sink>,
        clock: Clock,
        stt_endpoint: &str,
        tts_endpoint: &str,
    ) -> Result<(Self, mpsc::UnboundedReceiver<Value>)> {
        if let Some(settings) = settings {
            if config.mode == Mode::Twilio
                && (settings.phone_tts.encoding != "mulaw"
                    || settings.phone_tts.sample_rate != 8000
                    || config.audio.encoding != settings.phone_tts.encoding)
            {
                return Err(Error::new("incompatible_phone_audio_format"));
            }
            if config.mode == Mode::Browser
                && (settings.tts.encoding != "linear16" || settings.tts.sample_rate != 24000)
            {
                return Err(Error::new("incompatible_browser_audio_format"));
            }
        } else if config.mode != Mode::Text {
            return Err(Error::new("missing_speech_settings"));
        }
        let state = Arc::new(Mutex::new(Engine::new(
            config.mode,
            config.clocks.clone(),
            config.floor.clone(),
            config.policy.clone(),
            clock.now(),
        )));
        state.lock().await.typing_delay = config.typing_delay_ms as f64 / 1000.0;
        if config.mode == Mode::Twilio {
            state.lock().await.line = config
                .line
                .clone()
                .map(|cfg| vogon_front::line::Detector::new(cfg, config.name_hint.clone()));
        }
        let audio = if config.mode == Mode::Twilio {
            Some(Audio::start(config.audio.clone()).await?)
        } else {
            None
        };
        let (mut stt, mut output, mut tts) = (None, None, None);
        if config.mode != Mode::Text {
            let settings = settings.ok_or_else(|| Error::new("missing_speech_settings"))?;
            let phone = config.mode == Mode::Twilio;
            let (client, events) = if let Some((live, key)) = &config.live {
                let (client, events) =
                    LiveClient::connect(stt_endpoint, live.clone(), key.clone(), clock.clone())
                        .await?;
                (Listener::Live(client), events)
            } else {
                let (client, events) = SttClient::connect(
                    stt_endpoint,
                    if phone {
                        settings.phone.clone()
                    } else {
                        settings.browser.clone()
                    },
                    settings.key.clone(),
                    clock.clone(),
                )
                .await?;
                (Listener::Flux(client), events)
            };
            stt = Some(client);
            output = Some(events);
            tts = Some(
                TtsClient::connect(
                    tts_endpoint,
                    if phone {
                        settings.phone_tts.clone()
                    } else {
                        settings.tts.clone()
                    },
                    settings.key.clone(),
                    clock.clone(),
                )
                .await?,
            );
        }
        let (writer, writer_task) =
            Writer::start(sink, audio.clone(), state.clone(), clock.clone());
        let (events, rx) = mpsc::unbounded_channel();
        let (cancel, _) = watch::channel(0);
        let (decisions, mut completed) = mpsc::unbounded_channel();
        let inner = Arc::new(Inner {
            config,
            clock,
            state,
            audio,
            writer,
            stt: stt.as_ref().map(Listener::input),
            tts: Mutex::new(tts),
            cancel,
            cancel_lock: Mutex::new(()),
            cancel_operation: Mutex::new(()),
            cancel_pending: AtomicBool::new(false),
            response_lock: Mutex::new(()),
            dispatch_lock: Mutex::new(()),
            events,
            decisions,
            decision_tasks: Mutex::new(tokio::task::JoinSet::new()),
        });
        let mut tasks = vec![];
        if let Some(mut output) = output {
            let weak = Arc::downgrade(&inner);
            tasks.push(tokio::spawn(async move {
                while let Some(event) = output.recv().await {
                    let Some(inner) = weak.upgrade() else {
                        break;
                    };
                    match event {
                        SttOutput::Transcript { event, onset } => {
                            inner
                                .state
                                .lock()
                                .await
                                .transcript(event, onset, inner.clock.now());
                            if let Err(error) = inner.dispatch().await {
                                let _ = inner
                                    .events
                                    .send(json!({"kind":"transport_error","error":error}));
                            }
                        }
                        SttOutput::Error { error } => {
                            let _ = inner.events.send(
                                json!({"kind":"provider_error","provider":"stt","error":error}),
                            );
                        }
                        SttOutput::Closed { quality } => {
                            let _ = inner
                                .events
                                .send(json!({"kind":"stt_closed","quality":quality}));
                        }
                        _ => {}
                    }
                }
            }));
        }
        let weak = Arc::downgrade(&inner);
        tasks.push(tokio::spawn(async move {
            while let Some(completion) = completed.recv().await {
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                {
                    let mut state = inner.state.lock().await;
                    let now = inner.clock.now();
                    match completion {
                        DecisionCompletion::Judge(token, result) => {
                            state.sync(now);
                            if let Some(floor) = &mut state.floor {
                                floor.classifier_result(token, result);
                            }
                            state.tick(now);
                        }
                        DecisionCompletion::Line(token, probability) => {
                            state.line_result(token, probability, now)
                        }
                    }
                }
                if let Err(error) = inner.dispatch().await {
                    let _ = inner
                        .events
                        .send(json!({"kind":"transport_error","error":error}));
                }
            }
        }));
        let weak = Arc::downgrade(&inner);
        tasks.push(tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(50));
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                inner.state.lock().await.tick(inner.clock.now());
                if let Err(error) = inner.dispatch().await {
                    let _ = inner
                        .events
                        .send(json!({"kind":"transport_error","error":error}));
                    break;
                }
            }
        }));
        if inner.config.mode == Mode::Twilio && inner.config.audio.ambience {
            let weak = Arc::downgrade(&inner);
            tasks.push(tokio::spawn(async move {
                let mut ticker = time::interval(Duration::from_millis(20));
                ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
                loop {
                    ticker.tick().await;
                    let Some(inner) = weak.upgrade() else {
                        break;
                    };
                    if let Err(error) = inner.ambience().await {
                        let _ = inner
                            .events
                            .send(json!({"kind":"ambience_error","error":error}));
                    }
                }
            }));
        }
        Ok((
            Self {
                inner,
                stt,
                tasks,
                _writer: writer_task,
            },
            rx,
        ))
    }
    /// Caller JSON input; return true for a requested hangup/phone stop.
    pub async fn text(&self, message: Value) -> Result<bool> {
        let inner = &self.inner;
        let mode = inner.config.mode;
        if mode == Mode::Twilio {
            match message["event"].as_str() {
                Some("start") => {
                    let sid = message["start"]["streamSid"]
                        .as_str()
                        .or_else(|| message["streamSid"].as_str())
                        .ok_or_else(|| Error::new("missing_stream_sid"))?;
                    inner.state.lock().await.stream_sid = Some(sid.into());
                }
                Some("media") => {
                    let payload = message["media"]["payload"]
                        .as_str()
                        .ok_or_else(|| Error::new("invalid_media"))?;
                    let data = STANDARD
                        .decode(payload)
                        .map_err(|_| Error::new("invalid_media_base64"))?;
                    self.binary(data).await?;
                }
                Some("mark") => {
                    if let Some(name) = message["mark"]["name"].as_str() {
                        inner.state.lock().await.mark(name, inner.clock.now());
                        inner.dispatch().await?;
                    }
                }
                Some("stop") => return Ok(true),
                _ => {}
            }
        } else {
            match message["type"].as_str() {
                Some("client.hangup") => return Ok(true),
                Some("client.barge_in") if mode == Mode::Browser => inner.interrupt().await?,
                Some("client.audio_end") => {
                    if let Some(stt) = &inner.stt {
                        stt.finish_utterance().await?;
                    }
                }
                Some("client.message") if mode == Mode::Text => {
                    if let Some(text) = message["payload"]["text"]
                        .as_str()
                        .map(str::trim)
                        .filter(|t| !t.is_empty())
                    {
                        inner.state.lock().await.transcript(
                            vogon_speech_providers::stt::Transcript::Final {
                                text: text.into(),
                                confidence: 1.0,
                                elapsed_ms: 0.0,
                                low_conf_words: vec![],
                            },
                            None,
                            inner.clock.now(),
                        );
                        inner.dispatch().await?;
                    }
                }
                _ => {}
            }
        }
        Ok(false)
    }
    /// Forward raw browser PCM or clean Twilio mu-law without blocking provider reads.
    pub async fn binary(&self, data: Vec<u8>) -> Result<()> {
        let inner = &self.inner;
        if inner.config.mode == Mode::Text {
            return Ok(());
        }
        let bytes = if inner.config.mode == Mode::Twilio {
            let now = inner.clock.now();
            let (playing, active) = {
                let state = inner.state.lock().await;
                (
                    state.playing,
                    state.playing
                        || state.playback.draining(now)
                        || (state.playback.ended_at > 0.0 && now - state.playback.ended_at < 0.25),
                )
            };
            let value = inner
                .audio
                .as_ref()
                .unwrap()
                .call(Command::PhoneInput {
                    data,
                    now,
                    tts_playing: playing,
                    active_audio: active,
                })
                .await?;
            // Python's open feed reaches STT before any barge-in side effects.
            // The no-AEC overlap fallback suppresses both STT and floor frames.
            if value["stt"].is_null() {
                return Ok(());
            }
            let bytes: Vec<u8> = serde_json::from_value(value["stt"].clone())
                .map_err(|_| Error::new("invalid_audio_result"))?;
            let duration = bytes.len() as f64 / 8.0;
            inner
                .stt
                .as_ref()
                .ok_or_else(|| Error::new("stt_closed"))?
                .send_audio(bytes)
                .await?;
            inner.state.lock().await.frame(
                inner.clock.now(),
                value["raw_rms"].as_i64().unwrap_or(0),
                value["clean_rms"].as_i64().unwrap_or(0),
                value["threshold"].as_i64().unwrap_or(500),
                duration,
                value["verdict"]["confirmed_now"].as_bool().unwrap_or(false),
            );
            inner.dispatch().await?;
            return Ok(());
        } else {
            data
        };
        inner
            .stt
            .as_ref()
            .ok_or_else(|| Error::new("stt_closed"))?
            .send_audio(bytes)
            .await
    }
    /// Return remote semantic judge results with the exact dispatch token.
    pub async fn classifier_result(
        &self,
        token: u64,
        result: floor::ClassifierResult,
    ) -> Result<()> {
        let inner = &self.inner;
        {
            let mut state = inner.state.lock().await;
            state.sync(inner.clock.now());
            if let Some(f) = &mut state.floor {
                f.classifier_result(token, result);
            }
            state.collect();
        }
        inner.dispatch().await
    }
    /// Stream one response while caller audio and timer tasks continue independently.
    ///
    /// Returns None for blank text and `playback_busy` for a concurrent response.
    /// A delivery report records generation/send progress, not proof of audibility;
    /// phone marks can arrive afterward. Poll this future alongside caller input.
    /// Dropping an active response schedules cleanup rather than awaiting it.
    pub async fn send_response(&self, response: Response) -> Result<Option<DeliveryReport>> {
        self.inner.respond(response, false).await
    }
    /// Interrupt immediately even if TTS has not produced its next chunk.
    pub async fn interrupt(&self) -> Result<()> {
        self.inner.interrupt().await
    }
    /// Pause idle accumulation while a caller turn is being processed; nesting is safe.
    pub async fn pause_silence(&self) {
        self.inner.state.lock().await.clocks.pause_silence();
    }
    /// Release one outstanding reasoning pause; the clock also enforces its 30-second cap.
    pub async fn resume_silence(&self) {
        self.inner.state.lock().await.clocks.resume_silence();
    }
    /// Apply the Turn API's named silence ladder without resetting accumulated idle time.
    pub async fn select_silence_ladder(&self, name: &str) {
        self.inner.state.lock().await.clocks.select_ladder(name);
    }
    /// Apply an explicitly acknowledged caller hold; activity clears this grace.
    pub async fn grant_silence_grace(&self, seconds: f64) {
        self.inner.state.lock().await.clocks.grant_grace(seconds);
    }
    /// Current per-call playback epoch, absent for browser/text adapters.
    pub async fn playback_generation(&self) -> Option<u64> {
        self.inner
            .state
            .lock()
            .await
            .floor
            .as_ref()
            .map(|f| f.generation)
    }
    /// Send browser/text controls through the ordered socket writer. Twilio's
    /// media socket accepts only media/mark/clear, emitted by its transport path;
    /// browser server.* notifications must never be forwarded to that peer.
    pub async fn client_event(&self, event: Value) -> Result<()> {
        if self.inner.config.mode == Mode::Twilio {
            return Ok(());
        }
        self.inner
            .writer
            .send(None, Frame::Json(event), Receipt::None)
            .await
    }
    /// Play a machine-directed response after quiet, bypassing caller generation admission.
    pub async fn send_held_response(
        &self,
        mut response: Response,
    ) -> Result<Option<DeliveryReport>> {
        self.wait_line_quiet().await;
        response.protected = true;
        response.absorb = true;
        response.generation = None;
        self.inner.respond(response, true).await
    }
    /// Wait for already-sent phone playout receipts, bounded for missing marks.
    pub async fn wait_playout_heard(&self, cap: Duration) -> bool {
        let deadline = time::Instant::now() + cap;
        loop {
            if !self
                .inner
                .state
                .lock()
                .await
                .playback
                .draining(self.inner.clock.now())
            {
                return true;
            }
            if time::Instant::now() >= deadline {
                return false;
            }
            time::sleep(Duration::from_millis(50)).await;
        }
    }
    /// Wait for a machine greeting's final/quiet interval, explicit release or cap.
    pub async fn wait_line_quiet(&self) -> String {
        loop {
            let reason = self
                .inner
                .state
                .lock()
                .await
                .line
                .as_ref()
                .map_or(Some("no_detector"), |line| {
                    line.quiet_reason(self.inner.clock.now())
                });
            if let Some(reason) = reason {
                return reason.into();
            }
            time::sleep(Duration::from_millis(50)).await;
        }
    }
    /// Release a line hold after an external tone/message-end indication.
    pub async fn release_line_hold(&self) {
        if let Some(line) = &mut self.inner.state.lock().await.line {
            line.released = true;
        }
    }
    /// Inspect playback accounting without exposing keys or provider headers.
    pub async fn snapshot(&self) -> Value {
        let state = self.inner.state.lock().await;
        json!({"playing":state.playing,"playback":state.playback.snapshot(self.inner.clock.now()),"clocks":state.clocks})
    }
    /// Graceful provider close drains pending eager fallback before callbacks stop.
    ///
    /// Stop and join host response tasks before calling this method. It interrupts
    /// playback, drains STT callbacks, stops timers, closes TTS and joins the audio
    /// worker. A clear failure is returned after cleanup; buffered callback events
    /// can still be consumed from the event receiver. Drop alone is not this drain.
    pub async fn close(&mut self) -> Result<()> {
        let interrupt_result = self.inner.interrupt().await;
        if let Some(stt) = &mut self.stt {
            stt.close().await;
        }
        // The STT callback task naturally exits after its channel closes.
        if self.stt.is_some() && !self.tasks.is_empty() {
            let mut callback = self.tasks.remove(0);
            if time::timeout(Duration::from_secs(2), &mut callback)
                .await
                .is_err()
            {
                callback.abort();
            }
        }
        for task in self.tasks.drain(..) {
            task.abort();
        }
        self.inner.decision_tasks.lock().await.shutdown().await;
        if let Some(line) = &mut self.inner.state.lock().await.line {
            line.close();
        }
        if let Some(tts) = self.inner.tts.lock().await.as_mut() {
            tts.close().await;
        }
        if let Some(f) = &mut self.inner.state.lock().await.floor {
            f.close();
        }
        if let Some(audio) = &self.inner.audio {
            audio.close().await?;
        }
        interrupt_result
    }
}
/// Release cancellation admission even when an interrupt future is dropped.
struct CancelFlag<'a>(&'a AtomicBool);
impl Drop for CancelFlag<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}
impl Inner {
    /// Serialize provider epochs while letting clear bypass a slow reconnect.
    async fn provider_start_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        loop {
            let guard = self.cancel_lock.lock().await;
            if !self.cancel_pending.load(Ordering::SeqCst) {
                return guard;
            }
            drop(guard);
            time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Gap-only 20 ms ambience never refills the caller buffer after a response.
    async fn ambience(&self) -> Result<()> {
        let (sid, typing, epoch) = {
            let mut state = self.state.lock().await;
            if !state.gap_active || state.playing || state.stream_sid.is_none() {
                return Ok(());
            }
            let typing = if self.config.audio.enable_typing
                && state.typing_deadline.is_some_and(|t| self.clock.now() >= t)
            {
                state.typing_deadline = None;
                if let Some(f) = &mut state.floor {
                    f.transport.typing_arm_deadline = 0.0;
                }
                let index = state.typing_index;
                state.typing_index += 1;
                Some(index)
            } else {
                None
            };
            (
                state.stream_sid.clone().unwrap(),
                typing,
                self.writer.epoch(),
            )
        };
        let audio = self.audio.as_ref().unwrap();
        if let Some(index) = typing {
            audio.call(Command::StartTyping { index }).await?;
        }
        let bytes: Vec<u8> = serde_json::from_value(audio.call(Command::AmbienceFrame).await?)
            .map_err(|_| Error::new("invalid_audio_result"))?;
        if !bytes.is_empty() {
            self.writer
                .send(
                    Some(epoch),
                    wire::media(&sid, &bytes),
                    Receipt::Media(bytes),
                )
                .await?;
        }
        Ok(())
    }

    /// Drain pure-state effects in order, never holding state across I/O.
    async fn dispatch(&self) -> Result<()> {
        let _guard = self.dispatch_lock.lock().await;
        while self.decision_tasks.lock().await.try_join_next().is_some() {}
        loop {
            let (actions, events) = {
                let mut state = self.state.lock().await;
                (
                    std::mem::take(&mut state.actions),
                    std::mem::take(&mut state.events),
                )
            };
            if actions.is_empty() {
                for event in events {
                    let _ = self.events.send(event);
                }
                break;
            }
            let mut cut = false;
            for action in actions {
                match action {
                    Action::Cancel | Action::Floor(Effect::CutPlayback | Effect::SendClear) => {
                        if !cut {
                            self.interrupt().await?;
                            cut = true;
                        }
                    }
                    Action::Browser(value) => {
                        self.writer
                            .send(None, Frame::Json(value), Receipt::None)
                            .await?
                    }
                    Action::Classifier(request) => {
                        if let Some(client) = self.config.judge.clone() {
                            let completed = self.decisions.clone();
                            self.decision_tasks.lock().await.spawn(async move {
                                let result = client.classify(&request).await;
                                let _ = completed
                                    .send(DecisionCompletion::Judge(request.token, result));
                            });
                        } else {
                            let _ = self
                                .events
                                .send(json!({"kind":"classifier_request","request":request}));
                        }
                    }
                    Action::Line(request) => {
                        let client = self.config.line_client.clone();
                        let completed = self.decisions.clone();
                        self.decision_tasks.lock().await.spawn(async move {
                            let probability = match client {
                                Some(client) => {
                                    vogon_front::line::recording_probability(&client, request.state)
                                        .await
                                        .ok()
                                }
                                None => None,
                            };
                            let _ = completed
                                .send(DecisionCompletion::Line(request.token, probability));
                        });
                    }
                    Action::Floor(Effect::ResetGate) => {
                        if let Some(audio) = &self.audio {
                            audio.call(Command::GateReset).await?;
                        }
                    }
                    Action::Floor(Effect::StopTyping) => {
                        if let Some(audio) = &self.audio {
                            audio.call(Command::StopTyping).await?;
                        }
                    }
                    Action::Floor(effect) => {
                        let _ = self
                            .events
                            .send(json!({"kind":"floor_effect","effect":effect}));
                    }
                }
            }
            // Publish caller callbacks after their clear/browser wire effects.
            for event in events {
                let _ = self.events.send(event);
            }
        }
        Ok(())
    }
    /// Serial cancellation prevents an old provider Clear from cancelling a new stream.
    async fn interrupt(&self) -> Result<()> {
        let _operation = self.cancel_operation.lock().await;
        let (mode, sid, id, active) = {
            let state = self.state.lock().await;
            (
                state.mode,
                state.stream_sid.clone().unwrap_or_default(),
                state.playback.last_audio_id.clone(),
                state.playing || state.playback.draining(self.clock.now()),
            )
        };
        if !active {
            return Ok(());
        }
        self.cancel_pending.store(true, Ordering::SeqCst);
        let _pending = CancelFlag(&self.cancel_pending);
        self.cancel.send_modify(|epoch| *epoch += 1);
        let clear_result = self.writer.clear(wire::clear_frame(mode, &sid, &id)).await;
        if clear_result.is_err() {
            // Local generation must stop even when the caller socket is dead.
            // Retain marks: a failed clear is not proof that queued audio stopped.
            self.state.lock().await.playing = false;
        }
        let _guard = self.cancel_lock.lock().await;
        if let Some(tts) = self.tts.lock().await.as_ref() {
            let outcome = tts.cancel().await;
            let _ = self
                .events
                .send(json!({"kind":"tts_cancelled","outcome":outcome}));
        }
        if let Some(audio) = &self.audio {
            audio.call(Command::Speaking { value: false }).await?;
        }
        clear_result
    }
    /// Poll native floor admission with the same 50 ms cadence as its heartbeat.
    async fn admit(&self, response: &Response) -> Result<()> {
        let mut admission = {
            let mut state = self.state.lock().await;
            state.sync(self.clock.now());
            state.floor.as_mut().map(|f| {
                f.allow_playback(
                    response.generation,
                    response.wait_for_floor,
                    Some(response.audio_id.clone()),
                )
            })
        };
        while let Some((token, result)) = &mut admission {
            match result {
                AdmissionResult::Allowed => break,
                AdmissionResult::Rejected => return Err(Error::new("playback_rejected")),
                AdmissionResult::Waiting => {
                    time::sleep(Duration::from_millis(50)).await;
                    let mut state = self.state.lock().await;
                    state.sync(self.clock.now());
                    *result = state.floor.as_mut().unwrap().poll_admission(token);
                }
            }
        }
        Ok(())
    }
}

impl vogon_front::hold::Speaker for Session {
    /// Send filler through ordinary transport delivery accounting; application dispatch remains the host's job.
    fn speak(
        &self,
        text: String,
        audio_id: String,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = vogon_front::Result<Option<DeliveryReport>>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.send_response(Response {
                text,
                audio_id,
                ..Default::default()
            })
            .await
            .map_err(|error| vogon_front::Error::new(&error.code))
        })
    }
}
