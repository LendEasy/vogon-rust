//! Startup and bounded teardown failures must never create phantom/duplicate calls.
mod support;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use support::*;
use tokio::sync::mpsc;
use vogon_clocks::Clock;
use vogon_contracts::*;
use vogon_orchestration::{
    Error, Future, boxed,
    driver::{Config, Driver},
    port::{Factory, Hooks, Opened},
};
struct FailingFactory(AtomicU64);
impl Factory for FailingFactory {
    fn open(&self, _: CallConfig, _: Clock) -> Future<'_, Opened> {
        boxed(async {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(Error::new("provider_connect_failed"))
        })
    }
}
struct Hook {
    configured: AtomicU64,
    finished: AtomicU64,
    fail_config: bool,
    block_finish: bool,
}
impl Hooks for Hook {
    fn configure(&self, _: CallConfig) -> Future<'_, ()> {
        boxed(async {
            self.configured.fetch_add(1, Ordering::SeqCst);
            if self.fail_config {
                Err(Error::new("recording_setup_failed"))
            } else {
                Ok(())
            }
        })
    }
    fn escalate(&self, _: EscalationOrder) -> Future<'_, Value> {
        boxed(async { panic!("no escalation requested") })
    }
    fn finish(&self, _: String, _: String) -> Future<'_, ()> {
        boxed(async {
            self.finished.fetch_add(1, Ordering::SeqCst);
            if self.block_finish {
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }
}
#[tokio::test(start_paused = true)]
async fn failed_start_never_opens_transport_or_ends_an_unstarted_call() {
    let (requests, _) = mpsc::unbounded_channel();
    let (ends, mut end_rx) = mpsc::unbounded_channel();
    let (_input, rx) = mpsc::channel(1);
    let factory = Arc::new(FailingFactory(AtomicU64::new(0)));
    let hooks = Arc::new(Hook {
        configured: AtomicU64::new(0),
        finished: AtomicU64::new(0),
        fail_config: false,
        block_finish: false,
    });
    let api = Arc::new(Api {
        opening: serde_json::from_value(json!({"call_config":{"persona_voice":"x"}})).unwrap(),
        requests,
        ends,
        fail_start: true,
        probe: None,
    });
    let config = Config::new(StartCallRequest {
        call_id: "start-fails".into(),
        mode: "text".into(),
        resume: false,
        payload: json!({}),
    });
    let report = Driver {
        config,
        api,
        factory: factory.clone(),
        hooks: hooks.clone(),
        clock: Clock::default(),
    }
    .run(rx)
    .await;
    assert_eq!(report.reason, "start_failed");
    assert_eq!(factory.0.load(Ordering::SeqCst), 0);
    assert_eq!(hooks.configured.load(Ordering::SeqCst), 0);
    assert!(end_rx.try_recv().is_err());
}
#[tokio::test(start_paused = true)]
async fn initialization_failures_finalize_once_and_bound_terminal_and_artifact_waits() {
    for fail_config in [false, true] {
        for timeout_end in [false, true] {
            let (requests, _) = mpsc::unbounded_channel();
            let (ends, mut end_rx) = mpsc::unbounded_channel();
            let (_input, rx) = mpsc::channel(1);
            let factory = Arc::new(FailingFactory(AtomicU64::new(0)));
            let hooks = Arc::new(Hook {
                configured: AtomicU64::new(0),
                finished: AtomicU64::new(0),
                fail_config,
                block_finish: true,
            });
            let api = Arc::new(Api {
                opening: serde_json::from_value(json!({"call_config":{"persona_voice":"x"}}))
                    .unwrap(),
                requests,
                ends,
                fail_start: false,
                probe: None,
            });
            let mut config = Config::new(StartCallRequest {
                call_id: "initialization".into(),
                mode: "text".into(),
                resume: false,
                payload: json!({}),
            });
            config.end_timeout = Duration::from_secs(3);
            config.artifact_timeout = Duration::from_secs(2);
            let call = tokio::spawn(
                Driver {
                    config,
                    api,
                    factory: factory.clone(),
                    hooks: hooks.clone(),
                    clock: Clock::default(),
                }
                .run(rx),
            );
            let ending = end_rx.recv().await.unwrap();
            assert_eq!(ending.request.reason, "abnormal_exit");
            assert_eq!(hooks.finished.load(Ordering::SeqCst), 0);
            if !timeout_end {
                ending.reply.send(Ok(EndCallResponse {})).unwrap();
            } else {
                tokio::time::advance(Duration::from_secs(3)).await;
            }
            let report = call.await.unwrap();
            assert_eq!(report.end_acknowledged, !timeout_end);
            assert!(
                report
                    .errors
                    .contains(&"artifact_finalize_timeout".to_owned())
            );
            assert_eq!(hooks.finished.load(Ordering::SeqCst), 1);
            assert_eq!(factory.0.load(Ordering::SeqCst), u64::from(!fail_config));
            assert!(end_rx.try_recv().is_err());
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnect_drops_pending_request_and_never_plays_its_late_response() {
    let mut h = basic();
    h.events
        .send(event("utterance_final", "pending", 1))
        .unwrap();
    let pending = recv(&mut h.requests).await;
    let port = h.port.clone();
    let report = end(h).await;
    assert_eq!(report.requests, 1);
    assert!(pending.reply.send(Ok(response("too late"))).is_err());
    assert_eq!(port.active_speech.load(Ordering::SeqCst), 0);
    assert_eq!(port.paused.load(Ordering::SeqCst), 0);
    assert_eq!(port.closed.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_response_cannot_publish_client_events_escalate_or_end_the_call() {
    let mut h = basic();
    h.events.send(event("utterance_final", "old", 1)).unwrap();
    let pending = recv(&mut h.requests).await;
    h.events.send(event("utterance_resumed", "", 2)).unwrap();
    pending.reply.send(Ok(serde_json::from_value(json!({"action":"end_call","end_reason":"wrong","client_events":[{"type":"server.parameter_collected","payload":{"name":"wrong"}}],"escalation":{"escalation_id":"wrong"}})).unwrap())).unwrap();
    let resumed = recv(&mut h.requests).await;
    assert!(
        !json!(resumed.request.events)
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["kind"] == "escalation_outcome")
    );
    resumed.reply.send(Ok(empty())).unwrap();
    assert!(
        !h.port
            .frames
            .lock()
            .unwrap()
            .iter()
            .any(|v| v["type"] == "server.parameter_collected")
    );
    assert!(h.ends.try_recv().is_err());
    assert_eq!(end(h).await.stale_responses, 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn machine_quiet_command_can_take_floor_after_revision_changes() {
    let mut h = basic();
    h.events
        .send(event("utterance_final", "machine greeting", 1))
        .unwrap();
    let pending = recv(&mut h.requests).await;
    h.events.send(event("utterance_resumed", "", 2)).unwrap();
    pending
        .reply
        .send(Ok(serde_json::from_value(
            json!({"speak":[{"audio_id":"held","text":"Hello","play_after":"line_quiet"}]}),
        )
        .unwrap()))
        .unwrap();
    let speech = recv(&mut h.speech).await;
    assert_eq!(speech.command.play_after, "line_quiet");
    played(speech, "complete");
    let resumed = recv(&mut h.requests).await;
    resumed.reply.send(Ok(empty())).unwrap();
    assert_eq!(end(h).await.stale_responses, 0);
}
