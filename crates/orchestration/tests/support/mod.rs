#![allow(dead_code)]
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{mpsc, oneshot};
use vogon_clocks::Clock;
use vogon_contracts::*;
use vogon_orchestration::{
    Error, Future, Result, boxed,
    client::TurnApi,
    driver::{Config, Driver, Report},
    port::{Factory, Input, NoHooks, Opened, Port, Silence},
};
use vogon_playback::delivery::DeliveryReport;
pub struct Request {
    pub request: ProcessEventsRequest,
    pub reply: oneshot::Sender<Result<ProcessEventsResponse>>,
}
pub struct Ending {
    pub request: EndCallRequest,
    pub reply: oneshot::Sender<Result<EndCallResponse>>,
}
pub struct Speech {
    pub command: SpeakCommand,
    pub generation: Option<u64>,
    pub reply: oneshot::Sender<Result<Option<DeliveryReport>>>,
}
pub struct Api {
    pub opening: StartCallResponse,
    pub requests: mpsc::UnboundedSender<Request>,
    pub ends: mpsc::UnboundedSender<Ending>,
    pub fail_start: bool,
    pub probe: Option<Arc<Probe>>,
}
/// A bounded rendezvous proves two independent calls enter execution on different OS threads.
pub struct Probe {
    pub arrived: Mutex<Vec<std::thread::ThreadId>>,
    pub ready: std::sync::Condvar,
}
impl Probe {
    pub fn new() -> Self {
        Self {
            arrived: Mutex::new(vec![]),
            ready: std::sync::Condvar::new(),
        }
    }
    pub fn enter(&self) {
        let mut ids = self.arrived.lock().unwrap();
        ids.push(std::thread::current().id());
        self.ready.notify_all();
        let (ids, timeout) = self
            .ready
            .wait_timeout_while(ids, std::time::Duration::from_secs(2), |ids| ids.len() < 2)
            .unwrap();
        assert!(!timeout.timed_out());
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
    }
}
impl TurnApi for Api {
    fn start(&self, _: StartCallRequest) -> Future<'_, StartCallResponse> {
        boxed(async {
            if self.fail_start {
                Err(Error::new("start_failed"))
            } else {
                Ok(self.opening.clone())
            }
        })
    }
    fn process(&self, request: ProcessEventsRequest) -> Future<'_, ProcessEventsResponse> {
        boxed(async move {
            if let Some(probe) = &self.probe {
                probe.enter();
            }
            let (tx, rx) = oneshot::channel();
            self.requests.send(Request { request, reply: tx }).unwrap();
            rx.await.map_err(|_| Error::new("fixture_dropped"))?
        })
    }
    fn end(&self, request: EndCallRequest) -> Future<'_, EndCallResponse> {
        boxed(async move {
            let (tx, rx) = oneshot::channel();
            self.ends.send(Ending { request, reply: tx }).unwrap();
            rx.await.map_err(|_| Error::new("fixture_dropped"))?
        })
    }
}
pub struct FakePort {
    pub input_frames: AtomicU64,
    pub generation: AtomicU64,
    pub paused: AtomicU64,
    pub closed: AtomicU64,
    pub active_speech: AtomicU64,
    pub events: mpsc::UnboundedSender<Value>,
    pub frames: Mutex<Vec<Value>>,
    pub speak: mpsc::UnboundedSender<Speech>,
    pub heard_gate: Mutex<Option<mpsc::UnboundedSender<oneshot::Sender<bool>>>>,
}
struct Active<'a>(&'a AtomicU64);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Port for FakePort {
    fn input(&self, input: Input) -> Future<'_, bool> {
        boxed(async move {
            self.input_frames.fetch_add(1, Ordering::SeqCst);
            match input {
                Input::Json(v) if v["hangup"] == true => Ok(true),
                Input::Json(v) => {
                    self.events.send(v).unwrap();
                    Ok(false)
                }
                _ => Ok(false),
            }
        })
    }
    fn speak(
        &self,
        command: SpeakCommand,
        generation: Option<u64>,
    ) -> Future<'_, Option<DeliveryReport>> {
        boxed(async move {
            assert_eq!(
                self.active_speech.fetch_add(1, Ordering::SeqCst),
                0,
                "overlapping speech"
            );
            let _active = Active(&self.active_speech);
            let (tx, rx) = oneshot::channel();
            self.speak
                .send(Speech {
                    command,
                    generation,
                    reply: tx,
                })
                .unwrap();
            rx.await.map_err(|_| Error::new("fixture_speech_dropped"))?
        })
    }
    fn client_event(&self, v: Value) -> Future<'_, ()> {
        boxed(async move {
            self.frames.lock().unwrap().push(v);
            Ok(())
        })
    }
    fn generation(&self) -> Future<'_, Option<u64>> {
        boxed(async { Ok(Some(self.generation.load(Ordering::SeqCst))) })
    }
    fn silence(&self, c: Silence) -> Future<'_, ()> {
        boxed(async move {
            match c {
                Silence::Pause => {
                    self.paused.fetch_add(1, Ordering::SeqCst);
                }
                Silence::Resume => {
                    let _ = self
                        .paused
                        .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                            Some(n.saturating_sub(1))
                        });
                }
                _ => {}
            }
            Ok(())
        })
    }
    fn interrupt(&self) -> Future<'_, ()> {
        boxed(async { Ok(()) })
    }
    fn close(&self) -> Future<'_, ()> {
        boxed(async {
            assert_eq!(
                self.active_speech.load(Ordering::SeqCst),
                0,
                "speech task leaked"
            );
            self.closed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
    fn heard(&self, _: std::time::Duration) -> Future<'_, bool> {
        boxed(async {
            let gate = self.heard_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                let (tx, rx) = oneshot::channel();
                gate.send(tx).unwrap();
                return Ok(rx.await.unwrap());
            }
            Ok(true)
        })
    }
}
struct Make {
    port: Arc<FakePort>,
    events: Mutex<Option<mpsc::UnboundedReceiver<Value>>>,
}
impl Factory for Make {
    fn open(&self, _: CallConfig, _: Clock) -> Future<'_, Opened> {
        boxed(async {
            Ok(Opened {
                port: self.port.clone(),
                events: self.events.lock().unwrap().take().unwrap(),
                source: None,
            })
        })
    }
}
pub struct Harness {
    pub port: Arc<FakePort>,
    pub input: mpsc::Sender<Input>,
    pub events: mpsc::UnboundedSender<Value>,
    pub requests: mpsc::UnboundedReceiver<Request>,
    pub ends: mpsc::UnboundedReceiver<Ending>,
    pub speech: mpsc::UnboundedReceiver<Speech>,
    pub task: tokio::task::JoinHandle<Report>,
}
pub fn start(
    opening: Value,
    call: Value,
    probe: Option<Arc<Probe>>,
    mut adjust: impl FnMut(&mut Config),
) -> Harness {
    let (events, rx) = mpsc::unbounded_channel();
    let (speak, speech) = mpsc::unbounded_channel();
    let (requests, request_rx) = mpsc::unbounded_channel();
    let (ends, end_rx) = mpsc::unbounded_channel();
    let (input, inputs) = mpsc::channel(256);
    let port = Arc::new(FakePort {
        input_frames: AtomicU64::new(0),
        generation: AtomicU64::new(0),
        paused: AtomicU64::new(0),
        closed: AtomicU64::new(0),
        active_speech: AtomicU64::new(0),
        events: events.clone(),
        frames: Mutex::new(vec![]),
        speak,
        heard_gate: Mutex::new(None),
    });
    let api = Arc::new(Api {
        opening: serde_json::from_value(json!({"call_config":call,"speak":opening})).unwrap(),
        requests,
        ends,
        fail_start: false,
        probe,
    });
    let mut config = Config::new(StartCallRequest {
        call_id: "call".into(),
        mode: "twilio".into(),
        resume: false,
        payload: json!({}),
    });
    adjust(&mut config);
    let driver = Driver {
        config,
        api,
        factory: Arc::new(Make {
            port: port.clone(),
            events: Mutex::new(Some(rx)),
        }),
        hooks: Arc::new(NoHooks),
        clock: Clock::default(),
    };
    let task = tokio::spawn(driver.run(inputs));
    Harness {
        port,
        input,
        events,
        requests: request_rx,
        ends: end_rx,
        speech,
        task,
    }
}
pub fn basic() -> Harness {
    start(json!([]), json!({"persona_voice":"test"}), None, |_| {})
}
pub fn event(kind: &str, text: &str, seq: u64) -> Value {
    json!({"kind":kind,"text":text,"confidence":1.,"low_conf_words":[],"at":seq as f64,"onset_at":1.,"seq":seq,"playback_generation":0})
}
pub fn response(text: &str) -> ProcessEventsResponse {
    serde_json::from_value(json!({"speak":[{"audio_id":text,"text":text}]})).unwrap()
}
pub fn empty() -> ProcessEventsResponse {
    serde_json::from_value(json!({})).unwrap()
}
pub async fn recv<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap()
}
pub async fn until(mut test: impl FnMut() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
pub fn played(speech: Speech, status: &str) {
    speech.reply.send(Ok(Some(serde_json::from_value(json!({"audio_id":speech.command.audio_id,"status":status,"heard_text":speech.command.text,"sentences_total":1,"sentences_sent":1})).unwrap()))).unwrap();
}
pub async fn end(mut h: Harness) -> Report {
    h.input
        .send(Input::Json(json!({"hangup":true})))
        .await
        .unwrap();
    let end = recv(&mut h.ends).await;
    end.reply.send(Ok(EndCallResponse {})).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), h.task)
        .await
        .unwrap()
        .unwrap()
}
