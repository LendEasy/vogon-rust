//! Background-opening GPT Live socket owner. Pre-ready audio is dropped/counts;
//! callers never wait for the listener before sending the greeting.
use crate::{
    Error, Result,
    client::SttOutput,
    config::ApiKey,
    live::{LiveConfig, LiveState},
    wire::{Socket, WireEvent},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{self, Duration},
};
use tokio_tungstenite::tungstenite::Message;
use vogon_clocks::Clock;
pub const LIVE_ENDPOINT: &str = "wss://api.openai.com/v1/live/sessions";
enum Command {
    Audio(Vec<u8>, oneshot::Sender<Result<()>>),
    Said(String, oneshot::Sender<()>),
    Close(oneshot::Sender<()>),
}
/// Cloneable input gate; before readiness frames are counted rather than buffered.
#[derive(Clone)]
pub struct LiveInput {
    commands: mpsc::Sender<Command>,
    ready: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
}
impl LiveInput {
    /// Readiness is observable for health and bounded integration probes.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
    /// Send encoded input after readiness; count and discard earlier nonempty frames.
    /// A successful return before readiness means intentional dropping, not delivery.
    pub async fn send_audio(&self, data: Vec<u8>) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if !self.ready.load(Ordering::SeqCst) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }
        let (ack, rx) = oneshot::channel();
        self.commands
            .send(Command::Audio(data, ack))
            .await
            .map_err(|_| Error::new("live_closed"))?;
        rx.await.map_err(|_| Error::new("live_closed"))?
    }
    /// Best-effort private context; only the agent text actually sent is supplied.
    pub async fn note_agent_said(&self, text: String) {
        if text.is_empty() || !self.ready.load(Ordering::SeqCst) {
            return;
        }
        let (ack, rx) = oneshot::channel();
        if self.commands.send(Command::Said(text, ack)).await.is_ok() {
            let _ = rx.await;
        }
    }
}
/// Lifecycle owner for one listener. Explicit close requests final usage before aborting.
pub struct LiveClient {
    pub input: LiveInput,
    task: JoinHandle<()>,
}
impl Drop for LiveClient {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl LiveClient {
    /// Start the optional listener at its default endpoint with a bounded initial wait.
    /// Readiness may follow later; validation of this optional path is currently deferred.
    pub async fn open(
        config: LiveConfig,
        key: ApiKey,
        clock: Clock,
    ) -> Result<(Self, mpsc::UnboundedReceiver<SttOutput>)> {
        Self::connect(LIVE_ENDPOINT, config, key, clock).await
    }
    /// Start in the background and wait only the configured short startup budget.
    pub async fn connect(
        endpoint: &str,
        config: LiveConfig,
        key: ApiKey,
        clock: Clock,
    ) -> Result<(Self, mpsc::UnboundedReceiver<SttOutput>)> {
        let (endpoint, wait) = (endpoint.to_owned(), config.start_wait_ms);
        let (tx, mut commands) = mpsc::channel(64);
        let (events, output) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let ready = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicUsize::new(0));
        let (input_ready, input_dropped) = (ready.clone(), dropped.clone());
        let task = tokio::spawn(async move {
            let mut state = LiveState::new(config.clone(), clock.now());
            let mut ready_tx = Some(ready_tx);
            let opening=async{let socket=Socket::connect_with_auth(&endpoint,&[],&key,true).await?;socket.writer.send(Message::Text(json!({"type":"session.start","event_id":"start","session":config.session()}).to_string().into())).await?;Ok::<_,Error>(socket)}.await;
            let mut socket = match opening {
                Ok(s) => s,
                Err(error) => {
                    state.errors += 1;
                    let _ = ready_tx.take().unwrap().send(Err(error.clone()));
                    let _ = events.send(SttOutput::Error {
                        error: error.clone(),
                    });
                    let _ = events.send(SttOutput::Closed {
                        quality: state.quality(dropped.load(Ordering::SeqCst), Some(&error.code)),
                    });
                    return;
                }
            };
            let start_deadline = clock.now() + config.start_timeout_s;
            let mut close = None;
            let mut close_deadline = None;
            let mut open_error = None;
            loop {
                let now = clock.now();
                let deadline = state
                    .deadline
                    .into_iter()
                    .chain((!state.connected && ready_tx.is_some()).then_some(start_deadline))
                    .chain(close_deadline)
                    .min_by(f64::total_cmp)
                    .unwrap_or(now + 86400.);
                tokio::select! {
                    command=commands.recv()=>match command{
                        Some(Command::Audio(data,ack))=>{let result=socket.writer.send(Message::Text(json!({"type":"session.input_audio.append","audio":STANDARD.encode(data)}).to_string().into())).await;let _=ack.send(result);},
                        Some(Command::Said(text,ack))=>{let content=format!("The agent just said to the caller, verbatim: {}",text.chars().take(1500).collect::<String>());let _=socket.writer.send(Message::Text(json!({"type":"session.thinking.append","event_id":format!("said-{}",(clock.now()*1000.) as u64),"delegation_id":null,"content":content}).to_string().into())).await;let _=ack.send(());},
                        Some(Command::Close(ack))=>{state.deadline=None;ready.store(false,Ordering::SeqCst);let _=socket.writer.send(Message::Text(json!({"type":"session.close","event_id":"close"}).to_string().into())).await;close=Some(ack);close_deadline=Some(clock.now()+5.);},None=>break,
                    },
                    incoming=socket.incoming.recv()=>match incoming{
                        Some(WireEvent::Text(text))=>if let Ok(value)=serde_json::from_str::<Value>(&text){
                            for event in state.message(&value,clock.now()){let _=events.send(SttOutput::Transcript{event,onset:state.onset_monotonic()});}
                            match value["type"].as_str(){Some("session.started")=>{ready.store(true,Ordering::SeqCst);if let Some(tx)=ready_tx.take(){let _=tx.send(Ok(()));}},Some("session.closed")=>break,Some("error")=>{let provider_code=value["error"]["code"].as_str().filter(|s|s.len()<=64&&!s.is_empty()&&s.chars().all(|c|c.is_ascii_lowercase()||c=='_')).unwrap_or("provider_error");let code=format!("live_{}",provider_code);let _=events.send(SttOutput::Error{error:Error::new(&code)});},_=>{}}
                        },
                        Some(WireEvent::Failed(error))=>{state.errors+=1;let _=events.send(SttOutput::Error{error});break;},Some(WireEvent::Closed)|None=>break,_=>{},
                    },
                    _=time::sleep(Duration::from_secs_f64((deadline-now).max(0.0)))=>{
                        if ready_tx.is_some()&&clock.now()>=start_deadline{open_error=Some("live_start_timeout");state.errors+=1;break;}
                        if close_deadline.is_some_and(|t|clock.now()>=t){break;}
                        for event in state.tick(clock.now()){let _=events.send(SttOutput::Transcript{event,onset:state.onset_monotonic()});}
                    }
                }
            }
            ready.store(false, Ordering::SeqCst);
            if let Some(tx) = ready_tx {
                let code = open_error.unwrap_or("live_closed_before_ready");
                let _ = tx.send(Err(Error::new(code)));
                let _ = events.send(SttOutput::Error {
                    error: Error::new(code),
                });
            }
            let _ = events.send(SttOutput::Closed {
                quality: state.quality(dropped.load(Ordering::SeqCst), open_error),
            });
            if let Some(ack) = close {
                let _ = ack.send(());
            }
        });
        let client = Self {
            input: LiveInput {
                commands: tx,
                ready: input_ready,
                dropped: input_dropped,
            },
            task,
        };
        match time::timeout(Duration::from_millis(wait), ready_rx).await {
            Ok(Ok(Err(error))) => return Err(error),
            Ok(Err(_)) => return Err(Error::new("live_open_failed")),
            _ => {}
        }
        Ok((client, output))
    }
    /// Stop input and request final usage when ready; otherwise abort startup.
    /// Explicit shutdown waits within bounded budgets; Drop only aborts the actor.
    pub async fn close(&mut self) {
        if !self.input.ready.load(Ordering::SeqCst) {
            self.task.abort();
            return;
        }
        let (ack, rx) = oneshot::channel();
        if self.input.commands.send(Command::Close(ack)).await.is_ok() {
            let _ = time::timeout(Duration::from_secs(11), rx).await;
        }
        if time::timeout(Duration::from_secs(1), &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
        }
    }
}
