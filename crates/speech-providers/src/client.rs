//! Async per-call provider owners. Socket I/O runs independently from consumers;
//! command order, close fallback and generation boundaries are explicit.
use crate::{
    Error, Result,
    config::ApiKey,
    stt::{SttConfig, SttState, Transcript, TurnInfo},
    tts::{AudioChunk, TtsConfig, TtsState},
    wire::{Socket, WireEvent},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{self, Duration},
};
use tokio_tungstenite::tungstenite::Message;
use vogon_clocks::Clock;

pub const STT_ENDPOINT: &str = "wss://api.deepgram.com/v2/listen";
pub const TTS_ENDPOINT: &str = "wss://api.deepgram.com/v1/speak";

/// One socket's TurnInfo high-water mark. Suppress replay before callbacks receive
/// fresh local timestamps/sequences; a later genuine turn with identical words
/// has a newer provider sequence and remains eligible. Legacy fixtures without
/// sequence metadata preserve their existing behavior. Never shared across calls.
#[derive(Default)]
struct TurnSequence(Option<u64>);
impl TurnSequence {
    fn accept(&mut self, value: &Value) -> bool {
        let Some(sequence) = value["sequence_id"].as_u64() else {
            return true;
        };
        if self.0.is_some_and(|previous| sequence <= previous) {
            return false;
        }
        self.0 = Some(sequence);
        true
    }
}

/// Ordered provider and callback events. Raw messages support captured-trace parity;
/// hosts should not log transcript-bearing events indiscriminately.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SttOutput {
    Provider {
        message: Value,
    },
    Transcript {
        event: Transcript,
        onset: Option<f64>,
    },
    Error {
        error: Error,
    },
    Closed {
        quality: Value,
    },
}
enum SttCommand {
    Audio(Vec<u8>, oneshot::Sender<Result<()>>),
    Snapshot(oneshot::Sender<Value>),
    Close(oneshot::Sender<()>),
}

/// Cloneable STT input endpoint; audio ordering follows the channel's send order.
#[derive(Clone)]
pub struct SttInput {
    commands: mpsc::Sender<SttCommand>,
}
impl SttInput {
    /// Queue encoded audio and await socket-send acknowledgement, not transcription.
    /// The bytes must match the encoding and sample rate selected at connection time.
    pub async fn send_audio(&self, data: Vec<u8>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(SttCommand::Audio(data, tx))
            .await
            .map_err(|_| Error::new("stt_closed"))?;
        rx.await.map_err(|_| Error::new("stt_closed"))?
    }
    /// Flux EOT is model-driven; preserve the Python no-op.
    pub async fn finish_utterance(&self) -> Result<()> {
        Ok(())
    }
}

/// Owns the STT state actor. Explicit close drains the callback stream; Drop aborts.
pub struct SttClient {
    pub input: SttInput,
    task: JoinHandle<()>,
}
impl Drop for SttClient {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl SttClient {
    /// Connect to the default Flux endpoint and return its independently driven event stream.
    pub async fn open(
        config: SttConfig,
        key: ApiKey,
        clock: Clock,
    ) -> Result<(Self, mpsc::UnboundedReceiver<SttOutput>)> {
        Self::connect(STT_ENDPOINT, config, key, clock).await
    }
    /// Endpoint injection is used by local protocol tests; external endpoints require TLS.
    pub async fn connect(
        endpoint: &str,
        config: SttConfig,
        key: ApiKey,
        clock: Clock,
    ) -> Result<(Self, mpsc::UnboundedReceiver<SttOutput>)> {
        let mut state = SttState::new(config.clone(), clock.now());
        let mut socket = Socket::connect(endpoint, &config.query(), &key).await?;
        let (tx, mut commands) = mpsc::channel::<SttCommand>(64);
        let (events, output) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut close_ack = None;
            let mut close_deadline = None;
            let mut turn_sequence = TurnSequence::default();
            loop {
                let now = clock.now();
                let deadline = state
                    .deadline()
                    .into_iter()
                    .chain(close_deadline)
                    .min_by(f64::total_cmp);
                let wait = deadline.map_or(Duration::from_secs(86400), |at| {
                    Duration::from_secs_f64((at - now).max(0.0))
                });
                tokio::select! {
                    command=commands.recv()=>match command {
                        Some(SttCommand::Audio(data,ack))=>{let result=if close_deadline.is_some(){Err(Error::new("stt_closed"))}else{socket.writer.send(Message::Binary(data.into())).await};let _=ack.send(result);},
                        Some(SttCommand::Snapshot(ack))=>{let _=ack.send(state.snapshot());},
                        Some(SttCommand::Close(ack))=>{
                            // Cancel legacy hold immediately, but keep reading CloseStream's final events.
                            state.cancel_hold();
                            let _=socket.writer.send(Message::Text(json!({"type":"CloseStream"}).to_string().into())).await;
                            close_ack=Some(ack);close_deadline=Some(clock.now()+1.0);
                        },
                        None=>break,
                    },
                    incoming=socket.incoming.recv()=>match incoming {
                        Some(WireEvent::Text(text))=>{
                            match serde_json::from_str::<Value>(&text){
                                Ok(value)=>{
                                    let _=events.send(SttOutput::Provider{message:value.clone()});
                                    if value["type"] == "TurnInfo" && !turn_sequence.accept(&value) { continue; }
                                    match value["type"].as_str(){
                                        Some("TurnInfo")=>match serde_json::from_value::<TurnInfo>(value){
                                            Ok(message)=>for event in state.message(message,clock.now()){let _=events.send(SttOutput::Transcript{event,onset:state.last_onset_monotonic()});},
                                            Err(_)=>{let _=events.send(SttOutput::Error{error:Error::new("invalid_turn_info")});},
                                        },
                                        Some("Error")=>{let _=events.send(SttOutput::Error{error:Error::new("stt_fatal_error")});},
                                        _=>{},
                                    }
                                },
                                Err(_)=>{let _=events.send(SttOutput::Error{error:Error::new("invalid_provider_json")});},
                            }
                        },
                        Some(WireEvent::Failed(error))=>{let _=events.send(SttOutput::Error{error});},
                        Some(WireEvent::Closed)|None=>break,
                        _=>{},
                    },
                    _=time::sleep(wait)=>{
                        for event in state.tick(clock.now()){let _=events.send(SttOutput::Transcript{event,onset:state.last_onset_monotonic()});}
                        if close_deadline.is_some_and(|at|clock.now()>=at){break;}
                    }
                }
            }
            for event in state.closed(clock.now(), close_ack.is_some()) {
                let _ = events.send(SttOutput::Transcript {
                    event,
                    onset: state.last_onset_monotonic(),
                });
            }
            // A remote close does not cancel the legacy callback already scheduled by Python.
            if let Some(deadline) = state.deadline() {
                time::sleep(Duration::from_secs_f64((deadline - clock.now()).max(0.0))).await;
                for event in state.tick(clock.now()) {
                    let _ = events.send(SttOutput::Transcript {
                        event,
                        onset: state.last_onset_monotonic(),
                    });
                }
            }
            let _ = events.send(SttOutput::Closed {
                quality: state.quality_stats(),
            });
            if let Some(ack) = close_ack {
                let _ = ack.send(());
            }
        });
        Ok((
            Self {
                input: SttInput { commands: tx },
                task,
            },
            output,
        ))
    }
    /// Read protocol state through the actor queue without racing incoming callbacks.
    pub async fn snapshot(&self) -> Result<Value> {
        let (tx, rx) = oneshot::channel();
        self.input
            .commands
            .send(SttCommand::Snapshot(tx))
            .await
            .map_err(|_| Error::new("stt_closed"))?;
        rx.await.map_err(|_| Error::new("stt_closed"))
    }
    /// Idempotent graceful close; output channel retains already delivered events.
    pub async fn close(&mut self) {
        if self.task.is_finished() {
            return;
        }
        let (tx, rx) = oneshot::channel();
        if self
            .input
            .commands
            .send(SttCommand::Close(tx))
            .await
            .is_ok()
        {
            let _ = time::timeout(Duration::from_secs(7), rx).await;
        }
        if time::timeout(Duration::from_secs(1), &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
        }
    }
}

/// A stream reports completion separately from receiver closure; a dead provider
/// must never look like successful empty TTS to the transport.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TtsOutput {
    Audio { chunk: AudioChunk },
    Finished,
    Cancelled,
    Failed { error: Error },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// Result of draining an old TTS generation. Only `Cleared` permits socket reuse.
/// Timeout or disconnection requires a fresh connection before another utterance.
pub enum CancelOutcome {
    Cleared,
    TimedOut,
    Disconnected,
}
enum TtsCommand {
    Start(
        Vec<String>,
        mpsc::UnboundedSender<TtsOutput>,
        oneshot::Sender<Result<()>>,
    ),
    Cancel(oneshot::Sender<CancelOutcome>),
    Close(oneshot::Sender<()>),
}
struct Active {
    state: TtsState,
    output: mpsc::UnboundedSender<TtsOutput>,
}

/// Persistent per-call TTS socket. One active utterance owns its audio stream;
/// cancellation consumes stale audio through Cleared before accepting a new one.
pub struct TtsClient {
    commands: mpsc::Sender<TtsCommand>,
    task: JoinHandle<()>,
    config: TtsConfig,
    key: ApiKey,
    endpoint: String,
    clock: Clock,
}
impl Drop for TtsClient {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl TtsClient {
    /// Open a persistent TTS connection to the default Deepgram Speak endpoint.
    pub async fn open(config: TtsConfig, key: ApiKey, clock: Clock) -> Result<Self> {
        Self::connect(TTS_ENDPOINT, config, key, clock).await
    }
    /// Open an injected TTS endpoint and start its independent streaming actor.
    /// External connections require TLS; loopback endpoints support offline tests.
    pub async fn connect(
        endpoint: &str,
        config: TtsConfig,
        key: ApiKey,
        clock: Clock,
    ) -> Result<Self> {
        let mut socket = Socket::connect(endpoint, &config.query(), &key).await?;
        let (tx, mut commands) = mpsc::channel::<TtsCommand>(64);
        let actor_clock = clock.clone();
        let task = tokio::spawn(async move {
            let mut active: Option<Active> = None;
            let mut cancel: Option<(f64, oneshot::Sender<CancelOutcome>)> = None;
            loop {
                let wait = cancel
                    .as_ref()
                    .map_or(Duration::from_secs(86400), |(at, _)| {
                        Duration::from_secs_f64((at - actor_clock.now()).max(0.0))
                    });
                tokio::select! {
                    command=commands.recv()=>match command{
                        Some(TtsCommand::Start(segments,output,ack))=>{
                            if active.is_some()||cancel.is_some(){let _=ack.send(Err(Error::new("tts_busy")));continue;}
                            if segments.is_empty(){let _=output.send(TtsOutput::Finished);let _=ack.send(Ok(()));continue;}
                            let state=TtsState::new(segments.len(),actor_clock.now());let mut sent=Ok(());
                            for text in segments{
                                if let Err(e)=socket.writer.send(Message::Text(json!({"type":"Speak","text":text}).to_string().into())).await{sent=Err(e);break;}
                                if let Err(e)=socket.writer.send(Message::Text(json!({"type":"Flush"}).to_string().into())).await{sent=Err(e);break;}
                            }
                            if sent.is_ok(){active=Some(Active{state,output});}let failed=sent.is_err();let _=ack.send(sent);if failed{break;}
                        },
                        Some(TtsCommand::Cancel(ack))=>{
                            if cancel.is_some(){let _=ack.send(CancelOutcome::Disconnected);continue;}
                            if let Some(old)=active.take(){let _=old.output.send(TtsOutput::Cancelled);}
                            if socket.writer.send(Message::Text(json!({"type":"Clear"}).to_string().into())).await.is_err(){let _=ack.send(CancelOutcome::Disconnected);break;}
                            cancel=Some((actor_clock.now()+0.3,ack));
                        },
                        Some(TtsCommand::Close(ack))=>{let _=socket.writer.send(Message::Close(None)).await;let _=ack.send(());break;},
                        None=>break,
                    },
                    incoming=socket.incoming.recv()=>match incoming{
                        Some(WireEvent::Binary(bytes))=>{if cancel.is_none() && let Some(current)=active.as_mut() && let Some(chunk)=current.state.audio(&bytes,actor_clock.now()){let _=current.output.send(TtsOutput::Audio{chunk});}},
                        Some(WireEvent::Text(text))=>{
                            if let Ok(value)=serde_json::from_str::<Value>(&text){match value["type"].as_str(){
                                Some("Cleared")=>{if let Some((_,ack))=cancel.take(){let _=ack.send(CancelOutcome::Cleared);}},
                                Some("Flushed")=>{if cancel.is_none() && let Some(current)=active.as_mut(){current.state.control("Flushed");if current.state.done(){let finished=active.take().unwrap();let _=finished.output.send(TtsOutput::Finished);}}},
                                Some("Error")=>{if let Some(old)=active.take(){let _=old.output.send(TtsOutput::Failed{error:Error::new("tts_provider_error")});}break;},
                                _=>{},
                            }}
                        },
                        Some(WireEvent::Failed(error))=>{if let Some(old)=active.take(){let _=old.output.send(TtsOutput::Failed{error});}break;},
                        Some(WireEvent::Closed)|None=>break,
                    },
                    _=time::sleep(wait)=>{
                        if let Some((_,ack))=cancel.take(){let _=ack.send(CancelOutcome::TimedOut);}
                        // Unlike Python's dirty-socket timeout reuse, fail closed here:
                        // reconnect is required so stale audio cannot enter a new utterance.
                        break;
                    }
                }
            }
            if let Some(old) = active.take() {
                let _ = old.output.send(TtsOutput::Failed {
                    error: Error::new("tts_disconnected"),
                });
            }
            if let Some((_, ack)) = cancel.take() {
                let _ = ack.send(CancelOutcome::Disconnected);
            }
        });
        Ok(Self {
            commands: tx,
            task,
            config,
            key,
            endpoint: endpoint.into(),
            clock,
        })
    }
    /// Send all Speak/Flush pairs upfront, matching Python stream_segments.
    pub async fn stream_segments(
        &self,
        segments: Vec<String>,
    ) -> Result<mpsc::UnboundedReceiver<TtsOutput>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let (ack, reply) = oneshot::channel();
        self.commands
            .send(TtsCommand::Start(segments, tx, ack))
            .await
            .map_err(|_| Error::new("tts_closed"))?;
        reply.await.map_err(|_| Error::new("tts_closed"))??;
        Ok(rx)
    }
    /// Start a single-segment utterance; `Finished`, not receiver closure, signals success.
    pub async fn stream(&self, text: String) -> Result<mpsc::UnboundedReceiver<TtsOutput>> {
        self.stream_segments(vec![text]).await
    }
    /// Check local actor/channel liveness; this is not a remote health probe.
    pub fn is_open(&self) -> bool {
        !self.task.is_finished() && !self.commands.is_closed()
    }
    /// Cancel the active stream and discard late audio until Cleared or a 300 ms drain timeout.
    /// A timeout terminates the socket so old audio cannot enter a later utterance.
    pub async fn cancel(&self) -> CancelOutcome {
        let (tx, rx) = oneshot::channel();
        if self.commands.send(TtsCommand::Cancel(tx)).await.is_err() {
            return CancelOutcome::Disconnected;
        }
        rx.await.unwrap_or(CancelOutcome::Disconnected)
    }
    /// Request socket closure, wait within bounded shutdown budgets, then abort if needed.
    /// Already queued output events remain available to their receiver.
    pub async fn close(&mut self) {
        if self.task.is_finished() {
            return;
        }
        let (tx, rx) = oneshot::channel();
        if self.commands.send(TtsCommand::Close(tx)).await.is_ok() {
            let _ = time::timeout(Duration::from_secs(6), rx).await;
        }
        if time::timeout(Duration::from_secs(1), &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
        }
    }
    /// Replace the complete socket epoch; failed reopen leaves the client closed.
    pub async fn reconnect(&mut self) -> bool {
        self.close().await;
        match Self::connect(
            &self.endpoint,
            self.config.clone(),
            self.key.clone(),
            self.clock.clone(),
        )
        .await
        {
            Ok(next) => {
                *self = next;
                true
            }
            Err(_) => false,
        }
    }
}
