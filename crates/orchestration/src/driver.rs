//! Call task graph: an input pump, a responsive event collector, and one ordered
//! API/response worker. Shared locks cover only small local bookkeeping operations.
use crate::{
    Error, Result,
    client::TurnApi,
    port::{Factory, Hooks, Input, Port, Silence},
    state::{Inbox, Trigger, is_fact},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time,
};
use vogon_clocks::Clock;
use vogon_contracts::{
    CallConfig, EndCallRequest, ProcessEventsRequest, ProcessEventsResponse, SpeakCommand,
    StartCallRequest, codec,
};
use vogon_front::{
    hold::{Hold, Speaker, Timing},
    source::Source,
};
use vogon_playback::delivery::{DeliveryReport, estimate_heard_text};
/// Runtime limits bound queued work without dropping accepted physical facts.
#[derive(Clone)]
pub struct Config {
    pub start: StartCallRequest,
    pub provider_call_id: String,
    pub receive_idle: Duration,
    pub max_queued_triggers: usize,
    pub max_events: usize,
    pub end_timeout: Duration,
    pub escalation_timeout: Duration,
    pub artifact_timeout: Duration,
    /// Allow a completed farewell to drain before transport shutdown clears output.
    pub terminal_playout_timeout: Duration,
    pub front_timing: Timing,
    pub seed: u64,
}
impl Config {
    /// Standard limits; a service may tighten them for its admission budget.
    pub fn new(start: StartCallRequest) -> Self {
        Self {
            start,
            provider_call_id: String::new(),
            receive_idle: Duration::from_secs(600),
            max_queued_triggers: 256,
            max_events: 100_000,
            end_timeout: Duration::from_secs(90),
            escalation_timeout: Duration::from_secs(60),
            artifact_timeout: Duration::from_secs(30),
            terminal_playout_timeout: Duration::from_secs(8),
            front_timing: Timing::default(),
            seed: 1,
        }
    }
}
/// Final call evidence. Errors are sanitized and do not prevent best-effort finalization.
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Report {
    pub reason: String,
    pub errors: Vec<String>,
    pub end_acknowledged: bool,
    pub requests: u64,
    pub input_frames: u64,
    pub duplicate_events: u64,
    pub stale_responses: u64,
    pub speculative_responses: u64,
    pub queue_high_water: usize,
}
/// Mutable state shared only among tasks belonging to this one Driver::run.
/// A new instance is allocated for every call; there are no global counters.
struct Shared {
    facts: Mutex<Vec<Value>>,
    revision: watch::Sender<u64>,
    machine: AtomicBool,
    wire_seq: AtomicU64,
    requests: AtomicU64,
    stale: AtomicU64,
    speculative: AtomicU64,
    escalating: AtomicBool,
    clock: Clock,
}
impl Shared {
    fn fact(&self, value: Value) {
        self.facts.lock().unwrap().push(value);
    }
    fn take_facts(&self) -> Vec<Value> {
        std::mem::take(&mut *self.facts.lock().unwrap())
    }
}
struct Task<T>(JoinHandle<T>);
impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
/// Call owner: await `run` to complete teardown. Dropping it aborts owned child tasks;
/// the service should send Input::Disconnect and await the final report for graceful shutdown.
pub struct Driver {
    pub config: Config,
    pub api: Arc<dyn TurnApi>,
    pub factory: Arc<dyn Factory>,
    pub hooks: Arc<dyn Hooks>,
    pub clock: Clock,
}
impl Driver {
    /// Start the application, connect transport, and run tasks on the caller's Tokio runtime.
    /// A multi-thread runtime permits different calls and independent tasks to use multiple cores.
    pub async fn run(self, mut input: mpsc::Receiver<Input>) -> Report {
        let started_at = self.clock.now();
        let mut report = Report::default();
        let start = match self.api.start(self.config.start.clone()).await {
            Ok(v) => v,
            Err(e) => {
                report.reason = "start_failed".into();
                report.errors.push(e.0);
                return report;
            }
        };
        let (revision, _) = watch::channel(0);
        let shared = Arc::new(Shared {
            facts: Mutex::new(vec![]),
            revision,
            machine: AtomicBool::new(false),
            wire_seq: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            stale: AtomicU64::new(0),
            speculative: AtomicU64::new(0),
            escalating: AtomicBool::new(false),
            clock: self.clock.clone(),
        });
        let opened = async {
            self.hooks.configure(start.call_config.clone()).await?;
            self.factory
                .open(start.call_config.clone(), self.clock.clone())
                .await
        }
        .await;
        let mut opened = match opened {
            Ok(v) => Some(v),
            Err(e) => {
                report.errors.push(e.0);
                None
            }
        };
        if let Some(opened) = &mut opened {
            let port = opened.port.clone();
            let count = Arc::new(AtomicU64::new(0));
            let pump_count = count.clone();
            let max_duration = start
                .call_config
                .duration_thresholds
                .iter()
                .map(|t| t.after_sec)
                .fold(0., f64::max);
            let idle = if max_duration > 0. {
                Duration::from_secs_f64(max_duration)
            } else {
                self.config.receive_idle
            };
            let mut pump = Task(tokio::spawn(async move {
                loop {
                    match time::timeout(idle, input.recv()).await {
                        Err(_) => return Ok::<_, Error>("timeout".to_owned()),
                        Ok(None) | Ok(Some(Input::Disconnect)) => return Ok("abnormal_exit".into()),
                        Ok(Some(frame)) => {
                            pump_count.fetch_add(1, Ordering::Relaxed);
                            if port.input(frame).await? {
                                return Ok("hangup".into());
                            }
                        }
                    }
                }
            }));
            let (permits, mut permit_rx) =
                mpsc::unbounded_channel::<(Option<u64>, oneshot::Sender<bool>)>();
            let context = Arc::new(Context {
                permits,
                port: opened.port.clone(),
                source: opened.source.clone(),
                shared: shared.clone(),
                api: self.api.clone(),
                hooks: self.hooks.clone(),
                config: self.config.clone(),
                call: start.call_config.clone(),
                hold: tokio::sync::Mutex::new(Hold::default()),
            });
            let ambience = start
                .call_config
                .ambience
                .clone()
                .unwrap_or_else(|| json!({}));
            let ready = json!({"call_id":self.config.start.call_id,"persona_name":start.call_config.persona_name,"ambience":{"enabled":ambience.get("enabled").unwrap_or(&json!(false)),"room":ambience.get("room").unwrap_or(&json!(false)),"typing":ambience.get("typing").unwrap_or(&json!(false)),"duckFactor":ambience.get("duck_factor").unwrap_or(&json!(0.)),"typingDelayMs":ambience.get("typing_delay_ms").unwrap_or(&json!(0)),"typingGain":ambience.get("typing_gain").unwrap_or(&json!(0.))}});
            let initial = ProcessEventsResponse {
                speak: start.speak,
                client_events: start.client_events,
                ..empty_response()
            };
            let ctx = context.clone();
            let mut job = Some(Task(tokio::spawn(async move {
                ctx.client("server.session_ready", ready).await?;
                ctx.execute(initial, None, None, false).await
            })));
            let mut inbox = Inbox::default();
            let mut events_seen = 0;
            let mut pump_done = false;
            let mut reason = "abnormal_exit".to_owned();
            loop {
                tokio::select! {biased;
                    result=&mut pump.0=>{pump_done=true;match result{Ok(Ok(r))=>reason=r,Ok(Err(e))=>report.errors.push(e.0),Err(_)=>report.errors.push("input_task_failed".into())};break;},
                    event=opened.events.recv()=>{
                        let Some(event)=event else{break;};
                        // Provider diagnostics are not Turn API events. Transport failures end the call.
                        if event["kind"]=="transport_error"{report.errors.push("transport_error".into());break;}
                        let Ok(event)=codec::normalize("TransportEvent",&event) else{continue;};events_seen+=1;
                        let accepted=match inbox.accept(event.clone()){Ok(v)=>v,Err(e)=>{report.errors.push(e.0);break;}};
                        if accepted{
                            if *shared.revision.borrow()!=inbox.revision {shared.revision.send_replace(inbox.revision);}
                            shared.machine.store(inbox.machine,Ordering::Release);
                            if is_fact(&event){shared.fact(event.clone());}
                            if let Some(source)=&opened.source{match event["kind"].as_str(){Some("utterance_eager")=>source.prime(event["text"].as_str().unwrap_or("")).await,Some("utterance_final")=>{source.note_borrower(event["text"].as_str().unwrap_or("")).await;source.prime(event["text"].as_str().unwrap_or("")).await;},Some("utterance_resumed")=>source.cancel().await,_=>{}}}
                        }
                        report.queue_high_water=report.queue_high_water.max(inbox.queue.len());
                        if inbox.queue.len()>self.config.max_queued_triggers||events_seen>self.config.max_events{reason="overloaded".into();report.errors.push("orchestration_capacity_exceeded".into());break;}
                    },
                    Some((revision,reply))=permit_rx.recv()=>{let _=reply.send(revision.is_none_or(|r|r==inbox.revision));},
                    result=async{match &mut job{Some(t)=>(&mut t.0).await,None=>std::future::pending().await}}=>{
                        job=None;match result{Ok(Ok(Some(end)))=>{reason=end;break;},Ok(Ok(None))=>{},Ok(Err(e))=>{report.errors.push(e.0);break;},Err(_)=>{report.errors.push("response_task_failed".into());break;}}
                    }
                }
                if job.is_none() {
                    while let Some(trigger) = inbox.queue.pop_front() {
                        if trigger.speculative && trigger.revision != inbox.revision {
                            continue;
                        }
                        let ctx = context.clone();
                        job = Some(Task(tokio::spawn(
                            async move { ctx.process(trigger).await },
                        )));
                        break;
                    }
                }
            }
            if !pump_done {
                pump.0.abort();
                let _ = (&mut pump.0).await;
            }
            if let Some(mut task) = job {
                if shared.escalating.load(Ordering::Acquire) {
                    match time::timeout(self.config.escalation_timeout, &mut task.0).await {
                        Ok(Ok(Ok(Some(r)))) => reason = r,
                        Ok(Ok(Err(e))) => report.errors.push(e.0),
                        Err(_) => {
                            task.0.abort();
                            let _ = (&mut task.0).await;
                            report.errors.push("escalation_teardown_timeout".into());
                        }
                        _ => {}
                    }
                } else {
                    task.0.abort();
                    let _ = (&mut task.0).await;
                }
            }
            context.hold.lock().await.cancel().await;
            if let Some(source) = &opened.source {
                source.cancel().await;
            }
            if let Err(e) = opened.port.interrupt().await {
                report.errors.push(e.0);
            }
            // Aborted processing may have held a reasoning pause; balance it before close.
            let _ = opened.port.silence(Silence::Resume).await;
            drop(context);
            if let Err(e) = opened.port.close().await {
                report.errors.push(e.0);
            }
            while let Ok(event) = opened.events.try_recv() {
                if is_fact(&event) {
                    shared.fact(event);
                }
            }
            report.reason = reason;
            report.input_frames = count.load(Ordering::Relaxed);
            report.duplicate_events = inbox.duplicates;
        } else {
            report.reason = "abnormal_exit".into();
        }
        report.requests = shared.requests.load(Ordering::Relaxed);
        report.stale_responses = shared.stale.load(Ordering::Relaxed);
        report.speculative_responses = shared.speculative.load(Ordering::Relaxed);
        let events = shared
            .take_facts()
            .iter()
            .filter_map(|v| codec::from_wire("TransportEvent", v).ok())
            .collect();
        let end = EndCallRequest {
            call_id: self.config.start.call_id.clone(),
            reason: report.reason.clone(),
            events,
            provider_call_id: self.config.provider_call_id.clone(),
            transport_analytics: json!([{"event_name":"rust_orchestration","requests":report.requests,"stale_responses":report.stale_responses,"input_frames":report.input_frames,"queue_high_water":report.queue_high_water}]),
        };
        // Terminal ownership is outside the input select: hangup cannot cancel EndCall.
        match time::timeout(self.config.end_timeout, self.api.end(end)).await {
            Ok(Ok(_)) => report.end_acknowledged = true,
            Ok(Err(e)) => report.errors.push(e.0),
            Err(_) => report.errors.push("end_call_timeout".into()),
        }
        if let Some(opened) = &opened {
            let frame = json!({"type":"server.call_ended","seq":shared.wire_seq.fetch_add(1,Ordering::Relaxed)+1,"ts":wall_time(),"payload":{"call_id":self.config.start.call_id,"reason":report.reason,"duration_sec":((self.clock.now()-started_at)*100.).round()/100.}});
            let _ = opened.port.client_event(frame).await;
        }
        match time::timeout(
            self.config.artifact_timeout,
            self.hooks
                .finish(self.config.start.call_id, report.reason.clone()),
        )
        .await
        {
            Ok(Err(e)) => report.errors.push(e.0),
            Err(_) => report.errors.push("artifact_finalize_timeout".into()),
            _ => {}
        }
        report
    }
}
fn wall_time() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
fn empty_response() -> ProcessEventsResponse {
    serde_json::from_value(json!({})).unwrap()
}
struct Context {
    permits: mpsc::UnboundedSender<(Option<u64>, oneshot::Sender<bool>)>,
    port: Arc<dyn Port>,
    source: Option<Arc<Source>>,
    shared: Arc<Shared>,
    api: Arc<dyn TurnApi>,
    hooks: Arc<dyn Hooks>,
    config: Config,
    call: CallConfig,
    hold: tokio::sync::Mutex<Hold>,
}
impl Context {
    async fn authorize(&self, revision: Option<u64>) -> bool {
        let (tx, rx) = oneshot::channel();
        self.permits.send((revision, tx)).is_ok() && rx.await.unwrap_or(false)
    }
    async fn client(&self, kind: &str, payload: Value) -> Result<()> {
        self.port.client_event(json!({"type":kind,"seq":self.shared.wire_seq.fetch_add(1,Ordering::Relaxed)+1,"ts":wall_time(),"payload":payload})).await
    }
    fn outdated(&self, revision: Option<u64>) -> bool {
        revision.is_some_and(|r| r != *self.shared.revision.borrow())
    }
    async fn process(self: Arc<Self>, trigger: Trigger) -> Result<Option<String>> {
        let mut facts = self.shared.take_facts();
        facts.push(trigger.event.clone());
        let events = facts
            .iter()
            .map(|v| {
                codec::from_wire("TransportEvent", v)
                    .map_err(|_| Error::new("invalid_queued_event"))
            })
            .collect::<Result<Vec<_>>>()?;
        let id = self.shared.requests.fetch_add(1, Ordering::Relaxed) + 1;
        let request = ProcessEventsRequest {
            call_id: self.config.start.call_id.clone(),
            request_id: format!("{}-{id}", self.config.start.call_id),
            events,
        };
        let generation = match trigger.event["playback_generation"].as_u64() {
            Some(g) => Some(g),
            None => self.port.generation().await?,
        };
        if trigger.caller {
            self.client("server.thinking", json!({})).await?;
            self.port.silence(Silence::Pause).await?;
        }
        let mut hold = self.hold.lock().await;
        if trigger.event["kind"] == "utterance_final"
            && !self.shared.machine.load(Ordering::Acquire)
            && !self.outdated(Some(trigger.revision))
        {
            hold.begin(
                self.call.filler.clone(),
                self.source.clone(),
                Arc::new(Filler {
                    port: self.port.clone(),
                    source: self.source.clone(),
                    shared: self.shared.clone(),
                    permits: self.permits.clone(),
                    revision: trigger.revision,
                }),
                self.config.front_timing,
                self.config.seed,
            );
        }
        let mut revision = self.shared.revision.subscribe();
        let request = self.api.process(request);
        tokio::pin!(request);
        let mut hold_report = None;
        let result = loop {
            tokio::select! {biased;
                _=revision.changed(),if hold_report.is_none()=>{hold_report=Some(hold.end().await.map_err(|e|Error(e.code))?);},
                result=&mut request=>break result,
            }
        };
        if trigger.caller {
            self.port.silence(Silence::Resume).await?;
        }
        let held = match hold_report {
            Some(v) => v,
            None => hold.end().await.map_err(|e| Error(e.code))?,
        };
        let response = match result {
            Ok(r) => r,
            Err(e) if e.0 == "turn_service_unavailable" => {
                let farewell = if self.call.trouble_farewell.is_empty() {
                    "I'm sorry, I'm having technical difficulties on my end. Someone will follow up with you shortly. Goodbye!"
                } else {
                    &self.call.trouble_farewell
                };
                serde_json::from_value(json!({"speak":[{"audio_id":"trouble-1","text":farewell,"protected":true}],"action":"end_call","end_reason":"turn_unreachable"})).unwrap()
            }
            Err(e) => return Err(e),
        };
        if trigger.speculative {
            self.shared.speculative.fetch_add(1, Ordering::Relaxed);
            if !self.authorize(Some(trigger.revision)).await
                || self.outdated(Some(trigger.revision))
            {
                self.shared.stale.fetch_add(1, Ordering::Relaxed);
            }
            // Eager is never a commit. Even an anomalous eager payload cannot speak/end/escalate.
            return Ok(None);
        }
        self.execute(
            response,
            generation,
            trigger.caller.then_some(trigger.revision),
            held.interrupted,
        )
        .await
    }
    async fn execute(
        &self,
        response: ProcessEventsResponse,
        generation: Option<u64>,
        revision: Option<u64>,
        interrupted: bool,
    ) -> Result<Option<String>> {
        let held = response.speak.iter().any(|c| c.play_after == "line_quiet");
        if (!held
            && (!self.authorize(revision).await
                || self.outdated(revision)
                || generation.is_some() && self.port.generation().await? != generation))
            || (interrupted && response.action == "continue" && !response.speak.is_empty())
        {
            self.shared.stale.fetch_add(1, Ordering::Relaxed);
            for command in response.speak {
                self.truncated(&command);
            }
            return Ok(None);
        }
        for event in response.client_events {
            self.client(&event.r#type, event.payload).await?;
        }
        for command in response.speak {
            if !held && (!self.authorize(revision).await || self.outdated(revision)) {
                self.truncated(&command);
                continue;
            }
            self.client(
                "server.response_text",
                json!({"text":command.text,"audio_id":command.audio_id}),
            )
            .await?;
            if !held && (!self.authorize(revision).await || self.outdated(revision)) {
                self.truncated(&command);
                continue;
            }
            self.deliver(command, generation, false).await?;
        }
        if !held
            && (!self.authorize(revision).await
                || self.outdated(revision)
                || generation.is_some() && self.port.generation().await? != generation)
        {
            return Ok(None);
        }
        if let Some(order) = response.escalation {
            self.shared.escalating.store(true, Ordering::Release);
            self.port.heard(Duration::from_secs(8)).await?;
            let fallback = json!({"kind":"escalation_outcome","status":if self.config.provider_call_id.is_empty(){"no_provider_call"}else{"failed"},"target":order.targets.first().map(|t|t.label.as_str()).unwrap_or(""),"escalation_id":order.escalation_id,"detail":"escalation_failed","at":self.shared.clock.now()});
            let fact = if self.config.provider_call_id.is_empty() {
                fallback
            } else {
                self.hooks.escalate(order).await.unwrap_or(fallback)
            };
            self.shared.fact(fact);
            self.shared.escalating.store(false, Ordering::Release);
        }
        if let Some(grace) = response.silence_grace_sec.filter(|n| *n != 0.) {
            self.port.silence(Silence::Grace(grace)).await?;
        }
        if let Some(ladder) = response.silence_ladder.filter(|s| !s.is_empty()) {
            self.port.silence(Silence::Ladder(ladder)).await?;
        }
        let end = (response.action == "end_call")
            .then_some(response.end_reason)
            .flatten()
            .filter(|s| !s.is_empty());
        if end.is_some() {
            self.port
                .heard(self.config.terminal_playout_timeout)
                .await?;
            if !held
                && (!self.authorize(revision).await
                    || self.outdated(revision)
                    || generation.is_some() && self.port.generation().await? != generation)
            {
                return Ok(None);
            }
        }
        Ok(end)
    }
    fn truncated(&self, command: &SpeakCommand) {
        self.shared.fact(json!({"kind":"delivery_outcome","audio_id":command.audio_id,"status":"truncated","sentences_total":0,"sentences_sent":0,"heard_text":"","at":self.shared.clock.now()}));
    }
    async fn deliver(
        &self,
        command: SpeakCommand,
        generation: Option<u64>,
        front: bool,
    ) -> Result<Option<DeliveryReport>> {
        deliver(
            &self.port,
            &self.shared,
            &self.source,
            command,
            generation,
            front,
        )
        .await
    }
}
async fn deliver(
    port: &Arc<dyn Port>,
    shared: &Arc<Shared>,
    source: &Option<Arc<Source>>,
    command: SpeakCommand,
    generation: Option<u64>,
    front: bool,
) -> Result<Option<DeliveryReport>> {
    let mut guard = DeliveryGuard {
        shared: shared.clone(),
        command: command.clone(),
        front,
        armed: true,
    };
    let result = port.speak(command.clone(), generation).await;
    let dr = match result {
        Ok(Some(dr)) => dr,
        Ok(None) => {
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    let heard = if dr.heard_text.is_empty() {
        estimate_heard_text(&command.text, dr.audio_ms, dr.est_total_ms)
    } else {
        dr.heard_text.clone()
    };
    let mut fact = json!({"kind":"delivery_outcome","audio_id":dr.audio_id,"status":dr.status,"sentences_total":dr.sentences_total,"sentences_sent":dr.sentences_sent,"heard_text":heard,"heard_fraction":dr.heard_fraction(),"first_chunk_ms":dr.first_chunk_ms,"audio_ms":dr.audio_ms,"chunks_sent":dr.chunks_sent,"mark_pending":dr.mark_pending,"at":shared.clock.now()});
    if front {
        fact["text"] = json!(command.text);
        fact["origin"] = json!("front");
    }
    shared.fact(fact);
    guard.armed = false;
    if !front && let Some(source) = source {
        source
            .note_agent(if !dr.heard_text.is_empty() {
                &dr.heard_text
            } else if dr.status == "complete" {
                &command.text
            } else {
                ""
            })
            .await;
    }
    Ok(Some(dr))
}
struct Filler {
    port: Arc<dyn Port>,
    source: Option<Arc<Source>>,
    shared: Arc<Shared>,
    permits: mpsc::UnboundedSender<(Option<u64>, oneshot::Sender<bool>)>,
    revision: u64,
}
impl Speaker for Filler {
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
            // A selected filler may wake after resumed/final speech is already
            // queued. Ask the collector before starting new physical output.
            let (tx, rx) = oneshot::channel();
            if self.permits.send((Some(self.revision), tx)).is_err()
                || !rx.await.unwrap_or(false)
                || *self.shared.revision.borrow() != self.revision
            {
                return Ok(Some(DeliveryReport {
                    audio_id,
                    status: "truncated".into(),
                    ..serde_json::from_value(json!({"audio_id":"","status":"truncated"})).unwrap()
                }));
            }
            let command = serde_json::from_value(json!({"text":text,"audio_id":audio_id})).unwrap();
            deliver(&self.port, &self.shared, &self.source, command, None, true)
                .await
                .map_err(|e| vogon_front::Error::new(&e.0))
        })
    }
}
struct DeliveryGuard {
    shared: Arc<Shared>,
    command: SpeakCommand,
    front: bool,
    armed: bool,
}
impl Drop for DeliveryGuard {
    fn drop(&mut self) {
        if self.armed {
            self.shared.fact(json!({"kind":"delivery_outcome","audio_id":self.command.audio_id,"status":"truncated","at":self.shared.clock.now(),"text":if self.front{self.command.text.as_str()}else{""},"origin":if self.front{"front"}else{""}}));
        }
    }
}
