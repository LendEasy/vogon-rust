//! Real loopback provider fixtures exercise transitions, request retries and races.
use axum::{
    Form, Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::post,
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use vogon_contracts::EscalationOrder;
use vogon_escalation::{Binding, Config, Manager, Store};

#[derive(Default)]
struct Seen {
    rest: Mutex<Vec<(String, HashMap<String, String>)>>,
    facts: Mutex<Vec<Value>>,
    dial: AtomicUsize,
    hold_fail: AtomicUsize,
    dial_fail: AtomicUsize,
    retry: AtomicUsize,
    gate: Mutex<Option<Arc<Semaphore>>>,
}
struct Fixture {
    manager: Arc<Manager>,
    seen: Arc<Seen>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn rest(
    State(s): State<Arc<Seen>>,
    Path(path): Path<String>,
    Form(form): Form<HashMap<String, String>>,
) -> (StatusCode, Json<Value>) {
    s.rest.lock().unwrap().push((path.clone(), form.clone()));
    if path.ends_with("Calls.json") {
        let id = s.dial.fetch_add(1, Ordering::SeqCst) + 1;
        let gate = s.gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            let _ = gate.acquire().await;
        }
        if s.dial_fail
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return (StatusCode::BAD_REQUEST, Json(json!({})));
        }
        (StatusCode::OK, Json(json!({"sid":format!("CAagent{id}")})))
    } else if form.contains_key("Twiml") && s.hold_fail.load(Ordering::SeqCst) > 0 {
        (StatusCode::BAD_REQUEST, Json(json!({})))
    } else {
        (StatusCode::OK, Json(json!({"sid":"CAcaller"})))
    }
}
async fn report(State(s): State<Arc<Seen>>, Json(body): Json<Value>) -> StatusCode {
    s.facts.lock().unwrap().push(body);
    if s.retry
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok()
    {
        StatusCode::CONFLICT
    } else {
        StatusCode::OK
    }
}
impl Fixture {
    async fn new() -> Self {
        let seen = Arc::new(Seen::default());
        let app = Router::new()
            .route("/2010-04-01/{*path}", post(rest))
            .route("/turn/v1/record_transfer", post(report))
            .route("/speak", post(|| async { vec![1u8, 2, 3, 4] }))
            .with_state(seen.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let manager = Manager::new(
            Config {
                report_retry: None,
                rest_retry: vogon_resilience::Policy::once(),
                tts_retry: vogon_resilience::Policy::once(),
                endpoint: format!("{base}/2010-04-01"),
                account: "ACtest".into(),
                token: "test".into(),
                from: "+10000000000".into(),
                public_base: base.clone(),
                say_voice: "Polly.Joanna-Neural".into(),
                tts_endpoint: format!("{base}/speak"),
                tts_key: String::new(),
                timeout: Duration::from_secs(2),
                retry_delays: vec![Duration::from_millis(1); 3],
            },
            Store::default(),
        )
        .unwrap();
        Self {
            manager,
            seen,
            task,
        }
    }
    fn binding(&self, sid: &str) -> Binding {
        Binding {
            call_id: sid.into(),
            caller_sid: sid.into(),
            report_url: format!("{}/turn/v1", self.manager.config.public_base),
            report_token: "app-token".into(),
        }
    }
    async fn start(&self, sid: &str, n: usize) -> &'static str {
        self.manager
            .execute(self.binding(sid), order(n), String::new())
            .await
            .unwrap()
    }
    async fn dials(&self, n: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.seen.dial.load(Ordering::SeqCst) < n {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn facts(&self) -> Vec<Value> {
        self.manager.drain().await;
        self.seen
            .facts
            .lock()
            .unwrap()
            .iter()
            .map(|r| r["events"][0].clone())
            .collect()
    }
}
fn order(n: usize) -> EscalationOrder {
    serde_json::from_value(json!({"escalation_id":"esc-test","targets":(0..n).map(|i|json!({"number":format!("+1000000000{i}"),"label":format!("agent-{i}")})).collect::<Vec<_>>(),"whisper_text":"Summary & details","voicemail_script":"Please call back"})).unwrap()
}

#[tokio::test]
async fn duplicate_order_keypress_and_terminal_callback_resolve_once() {
    let f = Fixture::new().await;
    assert_eq!(f.start("CAcaller", 2).await, "initiated");
    assert_eq!(f.start("CAcaller", 2).await, "initiated");
    assert!(
        f.manager
            .whisper("CAcaller", "CAagent1", 0)
            .await
            .unwrap()
            .contains("Gather")
    );
    let (a, b) = tokio::join!(
        f.manager.keypress("CAcaller", "CAagent1", 0, "1"),
        f.manager.keypress("CAcaller", "CAagent1", 0, "2")
    );
    assert_eq!(a.unwrap(), b.unwrap());
    let (status, gone) = tokio::join!(
        f.manager.status("CAcaller", "CAagent1", 0, "completed"),
        f.manager.caller_gone("CAcaller")
    );
    status.unwrap();
    gone.unwrap();
    let facts = f.facts().await;
    assert_eq!(
        facts
            .iter()
            .filter(|x| x["kind"] == "escalation_resolved")
            .count(),
        1
    );
    assert_eq!(
        facts.iter().filter(|x| x["event"] == "connected").count(),
        1
    );
    assert_eq!(f.seen.dial.load(Ordering::SeqCst), 1);
    let mut seq = facts
        .iter()
        .map(|f| f["seq"].as_u64().unwrap())
        .collect::<Vec<_>>();
    seq.sort();
    assert_eq!(seq, (1..=facts.len() as u64).collect::<Vec<_>>());
}
#[tokio::test]
async fn duplicate_failed_leg_advances_once_and_stale_callbacks_cannot_bridge() {
    let f = Fixture::new().await;
    f.start("CAcaller", 2).await;
    let (a, b) = tokio::join!(
        f.manager.status("CAcaller", "CAagent1", 0, "busy"),
        f.manager.status("CAcaller", "CAagent1", 0, "busy")
    );
    assert_ne!(a.unwrap(), b.unwrap());
    assert_eq!(f.seen.dial.load(Ordering::SeqCst), 2);
    assert!(
        f.manager
            .keypress("CAcaller", "CAagent1", 0, "1")
            .await
            .unwrap()
            .contains("no longer waiting")
    );
    assert!(
        !f.manager
            .status("CAcaller", "CAagent1", 0, "completed")
            .await
            .unwrap()
    );
    assert!(
        f.manager
            .keypress("CAcaller", "CAagent2", 1, "1")
            .await
            .unwrap()
            .contains("Conference")
    );
}
#[tokio::test]
async fn caller_departure_during_dial_cancels_late_leg_without_resurrection() {
    let f = Fixture::new().await;
    let gate = Arc::new(Semaphore::new(0));
    *f.seen.gate.lock().unwrap() = Some(gate.clone());
    let manager = f.manager.clone();
    let binding = f.binding("CAcaller");
    let t = tokio::spawn(async move { manager.execute(binding, order(1), String::new()).await });
    f.dials(1).await;
    f.manager.caller_gone("CAcaller").await.unwrap();
    gate.add_permits(1);
    assert_eq!(t.await.unwrap().unwrap(), "failed");
    assert!(
        f.seen.rest.lock().unwrap().iter().any(
            |(p, f)| p.contains("CAagent1") && f.get("Status").is_some_and(|s| s == "canceled")
        )
    );
    assert!(
        f.manager
            .keypress("CAcaller", "CAagent1", 0, "1")
            .await
            .unwrap()
            .contains("no longer waiting")
    );
    assert_eq!(
        f.facts()
            .await
            .iter()
            .filter(|x| x["result"] == "caller_abandoned")
            .count(),
        1
    );
}
#[tokio::test]
async fn early_callback_advances_attempt_and_cancels_old_rest_result() {
    let f = Fixture::new().await;
    let gate = Arc::new(Semaphore::new(0));
    *f.seen.gate.lock().unwrap() = Some(gate.clone());
    let m = f.manager.clone();
    let b = f.binding("CAcaller");
    let first = tokio::spawn(async move { m.execute(b, order(2), String::new()).await });
    f.dials(1).await;
    *f.seen.gate.lock().unwrap() = None;
    f.manager
        .status("CAcaller", "CAagent1", 0, "busy")
        .await
        .unwrap();
    f.dials(2).await;
    gate.add_permits(1);
    first.await.unwrap().unwrap();
    assert!(
        f.manager
            .keypress("CAcaller", "CAagent2", 1, "1")
            .await
            .unwrap()
            .contains("Conference")
    );
}
#[tokio::test]
async fn exhausted_targets_leave_voicemail_and_callback_only_once() {
    let f = Fixture::new().await;
    f.start("CAcaller", 1).await;
    f.manager
        .status("CAcaller", "CAagent1", 0, "no-answer")
        .await
        .unwrap();
    f.manager
        .status("CAcaller", "CAagent1", 0, "no-answer")
        .await
        .unwrap();
    assert_eq!(f.seen.dial.load(Ordering::SeqCst), 2);
    assert!(f.seen.rest.lock().unwrap().iter().any(|(_, f)| {
        f.get("MachineDetection")
            .is_some_and(|s| s == "DetectMessageEnd")
    }));
    assert_eq!(
        f.facts()
            .await
            .iter()
            .filter(|f| f["result"] == "fallback_callback")
            .count(),
        1
    );
}
#[tokio::test]
async fn no_targets_and_failed_hold_do_not_dial() {
    let f = Fixture::new().await;
    assert_eq!(f.start("CAempty", 0).await, "no_targets");
    f.seen.hold_fail.store(1, Ordering::SeqCst);
    assert_eq!(f.start("CAcaller", 1).await, "failed");
    assert_eq!(f.seen.dial.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn explicit_dial_failure_tries_next_target_and_report_retry_keeps_request_id() {
    let f = Fixture::new().await;
    f.seen.dial_fail.store(1, Ordering::SeqCst);
    f.seen.retry.store(1, Ordering::SeqCst);
    assert_eq!(f.start("CAcaller", 2).await, "initiated");
    f.manager.drain().await;
    let requests = f.seen.facts.lock().unwrap();
    let first = &requests[0];
    assert!(requests.iter().skip(1).any(|r| r == first));
    assert_eq!(f.manager.report_failures.load(Ordering::Relaxed), 0);
}
#[tokio::test]
async fn answered_unbridged_leg_is_completed_on_caller_departure() {
    let f = Fixture::new().await;
    f.start("CAcaller", 1).await;
    f.manager.whisper("CAcaller", "CAagent1", 0).await.unwrap();
    f.manager.caller_gone("CAcaller").await.unwrap();
    assert!(
        f.seen
            .rest
            .lock()
            .unwrap()
            .iter()
            .any(|(p, f)| p.contains("CAagent1")
                && f.get("Status").is_some_and(|s| s == "completed"))
    );
}
#[tokio::test]
async fn no_keypress_falls_back_and_empty_keypress_does_not_connect() {
    let f = Fixture::new().await;
    f.start("CAcaller", 1).await;
    assert!(
        f.manager
            .keypress("CAcaller", "CAagent1", 0, "")
            .await
            .unwrap()
            .contains("Hangup")
    );
    f.manager
        .status("CAcaller", "CAagent1", 0, "completed")
        .await
        .unwrap();
    assert!(f.facts().await.iter().any(|f| f["event"] == "no_keypress"));
}
#[tokio::test]
async fn blocked_call_does_not_block_independent_transfer() {
    let f = Fixture::new().await;
    let gate = Arc::new(Semaphore::new(0));
    *f.seen.gate.lock().unwrap() = Some(gate.clone());
    let m = f.manager.clone();
    let b = f.binding("CAa");
    let first = tokio::spawn(async move { m.execute(b, order(1), String::new()).await });
    f.dials(1).await;
    *f.seen.gate.lock().unwrap() = None;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), f.start("CAb", 1))
            .await
            .unwrap(),
        "initiated"
    );
    gate.add_permits(1);
    first.await.unwrap().unwrap();
}
#[tokio::test(start_paused = true)]
async fn store_cas_and_expiry_do_not_recreate_deleted_state() {
    let s = Store::default();
    let ttl = Duration::from_secs(1);
    assert!(s.cas("key", None, Some(b"first"), ttl).await.unwrap());
    let (a, b) = tokio::join!(
        s.cas("key", Some(b"first"), Some(b"a"), ttl),
        s.cas("key", Some(b"first"), Some(b"b"), ttl)
    );
    assert_ne!(a.unwrap(), b.unwrap());
    tokio::time::advance(ttl).await;
    assert!(s.get("key").await.unwrap().is_none());
    assert!(
        !s.cas("key", Some(b"first"), Some(b"resurrect"), ttl)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn persona_audio_is_served_and_private_lines_expire_on_finish() {
    let mut f = Fixture::new().await;
    let mut config = f.manager.config.clone();
    config.tts_key = "local".into();
    f.manager = Manager::new(config, Store::default()).unwrap();
    assert_eq!(
        f.manager
            .execute(f.binding("CAcaller"), order(1), "aura-test".into())
            .await
            .unwrap(),
        "initiated"
    );
    f.manager.drain().await;
    let xml = f.manager.whisper("CAcaller", "CAagent1", 0).await.unwrap();
    let token = xml
        .split("/audio/")
        .nth(1)
        .unwrap()
        .split(".mp3")
        .next()
        .unwrap();
    assert_eq!(
        f.manager.audio(token).await.unwrap().unwrap(),
        vec![1, 2, 3, 4]
    );
    f.manager.caller_gone("CAcaller").await.unwrap();
    assert!(f.manager.audio(token).await.unwrap().is_none());
}

#[tokio::test]
async fn ambiguous_dial_does_not_place_a_second_agent_leg() {
    let f = Fixture::new().await;
    *f.seen.gate.lock().unwrap() = Some(Arc::new(Semaphore::new(0)));
    assert_eq!(f.start("CAcaller", 2).await, "failed");
    assert_eq!(f.seen.dial.load(Ordering::SeqCst), 1);
    assert!(
        f.facts()
            .await
            .iter()
            .any(|f| f["detail"].as_str().unwrap_or("").contains("ambiguous"))
    );
}

#[tokio::test]
async fn prepared_transfer_has_no_provider_side_effects_and_can_be_canceled() {
    let f = Fixture::new().await;
    f.manager
        .prepare(f.binding("CAcaller"), order(1), String::new())
        .await
        .unwrap();
    f.manager.drain().await;
    assert!(f.seen.rest.lock().unwrap().is_empty());
    f.manager.discard_prepared("CAcaller").await.unwrap();
    assert_eq!(f.start("CAcaller", 1).await, "failed");
    assert!(f.seen.rest.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelling_shutdown_drain_aborts_report_retry_sleep() {
    let mut f = Fixture::new().await;
    let config = &mut Arc::get_mut(&mut f.manager).unwrap().config;
    config.report_retry = Some(vogon_resilience::Policy {
        attempts: 5,
        initial_ms: 300,
        max_ms: 300,
        jitter: false,
    });
    f.seen.retry.store(100, Ordering::SeqCst);
    assert_eq!(f.start("CAshutdown", 1).await, "initiated");
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.seen.facts.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), f.manager.drain())
            .await
            .is_err()
    );
    let count = f.seen.facts.lock().unwrap().len();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(f.seen.facts.lock().unwrap().len(), count);
}
