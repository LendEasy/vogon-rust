//! Serial transport output. Clear invalidates queued audio immediately and runs
//! before the next queued media packet, after any in-flight send and AEC update.
use crate::{
    audio::Audio,
    engine::{Engine, Mode},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
    time::{Duration, timeout},
};
use vogon_audio_runtime::Command;
use vogon_clocks::Clock;
use vogon_speech_providers::{Error, Result};
/// Socket-independent outgoing frame, suitable for an HTTP/WebSocket host.
#[derive(Clone, Debug, PartialEq)]
pub enum Frame {
    Json(Value),
    Binary(Vec<u8>),
}
/// The host supplies its authenticated caller socket; completion means send completed.
pub trait Sink: Send + 'static {
    /// Complete only after the underlying frame send succeeds or fails.
    /// Do not re-enter this session synchronously: the writer awaits this future
    /// before executing receipt side effects and accepting the next frame.
    fn send(&mut self, frame: Frame) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}
/// Register side effects only after the underlying send succeeds.
pub enum Receipt {
    /// Control traffic with no local playback or AEC accounting.
    None,
    /// Successfully sent mu-law bytes to enqueue as the exact AEC reference.
    Media(Vec<u8>),
    /// Register a pending phone mark only after its wire send succeeds.
    Mark { name: String, snippet: String },
    /// Discard native output/reference state and apply the playback-clear receipt.
    Clear,
}
struct Request {
    epoch: Option<u64>,
    frame: Frame,
    receipt: Receipt,
    ack: oneshot::Sender<Result<()>>,
}
/// Cloneable handle to one writer epoch. A new clear cancels all older audio work.
#[derive(Clone)]
pub struct Writer {
    normal: mpsc::Sender<Request>,
    priority: mpsc::UnboundedSender<Request>,
    epoch: Arc<AtomicU64>,
}
/// Task owner; explicit session shutdown aborts it after stream/provider cleanup.
pub struct WriterTask(pub JoinHandle<()>);
impl Drop for WriterTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl Writer {
    /// Start one serial writer per caller connection.
    pub fn start(
        mut sink: Box<dyn Sink>,
        audio: Option<Audio>,
        state: Arc<Mutex<Engine>>,
        clock: Clock,
    ) -> (Self, WriterTask) {
        let (normal, mut rx) = mpsc::channel::<Request>(64);
        let (priority, mut urgent) = mpsc::unbounded_channel::<Request>();
        let epoch = Arc::new(AtomicU64::new(0));
        let current = epoch.clone();
        let task = tokio::spawn(async move {
            loop {
                let request = tokio::select! {biased;Some(r)=urgent.recv()=>r,Some(r)=rx.recv()=>r,else=>break};
                if request
                    .epoch
                    .is_some_and(|v| v != current.load(Ordering::SeqCst))
                {
                    let _ = request.ack.send(Err(Error::new("playback_cancelled")));
                    continue;
                }
                let mut result = timeout(Duration::from_secs(5), sink.send(request.frame))
                    .await
                    .unwrap_or_else(|_| Err(Error::new("transport_send_timeout")));
                if result.is_ok() {
                    match request.receipt {
                        Receipt::None => {}
                        Receipt::Media(data) => {
                            if let Some(audio) = &audio {
                                result = audio.call(Command::Reference { data }).await.map(|_| ());
                            }
                        }
                        Receipt::Mark { name, snippet } => {
                            state
                                .lock()
                                .await
                                .playback
                                .pending_marks
                                .insert(name, (clock.now(), snippet));
                        }
                        Receipt::Clear => {
                            if let Some(audio) = &audio {
                                result = audio.call(Command::Clear).await.map(|_| ());
                            }
                            state.lock().await.cleared(clock.now());
                        }
                    }
                }
                let failed = result.is_err()
                    && result
                        .as_ref()
                        .err()
                        .is_none_or(|e| e.code != "playback_cancelled");
                let _ = request.ack.send(result);
                if failed {
                    break;
                }
            }
        });
        (
            Self {
                normal,
                priority,
                epoch,
            },
            WriterTask(task),
        )
    }
    /// Snapshot before beginning an utterance; every packet carries this generation.
    pub fn invalidate(&self) -> u64 {
        self.epoch.fetch_add(1, Ordering::SeqCst) + 1
    }
    /// Read the current output generation for tagging cancelable media and marks.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }
    /// Enqueue output and await both the underlying send and its receipt side effects.
    /// An epoch rejects stale work; None is reserved for control traffic that survives clears.
    pub async fn send(&self, epoch: Option<u64>, frame: Frame, receipt: Receipt) -> Result<()> {
        let (ack, rx) = oneshot::channel();
        self.normal
            .send(Request {
                epoch,
                frame,
                receipt,
                ack,
            })
            .await
            .map_err(|_| Error::new("transport_closed"))?;
        rx.await.map_err(|_| Error::new("transport_closed"))?
    }
    /// Invalidates queued audio before awaiting the clear acknowledgement.
    pub async fn clear(&self, frame: Frame) -> Result<()> {
        self.epoch.fetch_add(1, Ordering::SeqCst);
        let (ack, rx) = oneshot::channel();
        self.priority
            .send(Request {
                epoch: None,
                frame,
                receipt: Receipt::Clear,
                ack,
            })
            .map_err(|_| Error::new("transport_closed"))?;
        rx.await.map_err(|_| Error::new("transport_closed"))?
    }
}
/// Phone media uses Twilio's base64 mu-law envelope.
pub fn media(sid: &str, bytes: &[u8]) -> Frame {
    Frame::Json(json!({"event":"media","streamSid":sid,"media":{"payload":STANDARD.encode(bytes)}}))
}
/// Browser packet header counts UTF-8 bytes, not Unicode scalar values.
pub fn browser_audio(id: &str, bytes: &[u8]) -> Frame {
    let mut out = Vec::with_capacity(4 + id.len() + bytes.len());
    out.extend_from_slice(&(id.len() as u32).to_le_bytes());
    out.extend_from_slice(id.as_bytes());
    out.extend_from_slice(bytes);
    Frame::Binary(out)
}
/// A clear has protocol-specific presentation but identical internal epoch semantics.
pub fn clear_frame(mode: Mode, sid: &str, id: &str) -> Frame {
    match mode {
        Mode::Twilio => Frame::Json(json!({"event":"clear","streamSid":sid})),
        _ => Frame::Json(json!({"type":"server.tts_interrupted","payload":{"audio_id":id}})),
    }
}
