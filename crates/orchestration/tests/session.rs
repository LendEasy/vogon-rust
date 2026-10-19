//! Real Rust Session + orchestrator without speech/network credentials, using text mode.
mod support;
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use support::*;
use tokio::sync::mpsc;
use vogon_clocks::Clock;
use vogon_contracts::*;
use vogon_orchestration::{
    driver::{Config, Driver},
    port::{Input, NoHooks, SessionFactory},
};
use vogon_transport::{
    engine::Mode,
    wire::{Frame, Sink},
};
struct Capture(Arc<Mutex<Vec<Frame>>>);
impl Sink for Capture {
    fn send(
        &mut self,
        frame: Frame,
    ) -> Pin<Box<dyn Future<Output = vogon_speech_providers::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(frame);
            Ok(())
        })
    }
}

/// Real Session interval -> Engine -> transport channel -> Inbox -> API worker.
/// The second threshold must be timestamped during the blocked first request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_heartbeat_thresholds_queue_during_api_wait_and_finalize_once() {
    let (requests, mut rx) = mpsc::unbounded_channel();
    let (ends, mut end_rx) = mpsc::unbounded_channel();
    let api = Arc::new(Api {
        opening: serde_json::from_value(json!({"call_config":{
            "persona_voice":"x",
            "duration_thresholds":[{"id":"first","after_sec":0.1},{"id":"second","after_sec":1.1},{"id":"idle_budget","after_sec":60.0}]
        }})).unwrap(),
        requests, ends, fail_start: false, probe: None,
    });
    let settings =
        vogon_configuration::settings("settings", &json!({}), &json!({}), &json!({})).unwrap();
    let profile = vogon_configuration::built_in(&settings).unwrap();
    let factory = SessionFactory {
        settings,
        profile,
        mode: Mode::Text,
        speech: None,
        clients: Default::default(),
        artifact: None,
        lender: String::new(),
        seed: 1,
        sink: tokio::sync::Mutex::new(Some(Box::new(Capture(Arc::new(Mutex::new(vec![])))))),
        initial_frame: None,
        stt_endpoint: String::new(),
        tts_endpoint: String::new(),
    };
    let clock = Clock::default();
    let config = Config::new(StartCallRequest {
        call_id: "timer".into(),
        mode: "text".into(),
        resume: false,
        payload: json!({}),
    });
    let (_input, inputs) = mpsc::channel(10);
    let task = tokio::spawn(
        Driver {
            config,
            api,
            factory: Arc::new(factory),
            hooks: Arc::new(NoHooks),
            clock: clock.clone(),
        }
        .run(inputs),
    );
    let first = recv(&mut rx).await;
    assert_eq!(
        json!(first.request.events.last().unwrap())["threshold_id"],
        "first"
    );
    tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
    assert!(
        rx.try_recv().is_err(),
        "a second same-call transaction must not overlap"
    );
    let released_at = clock.now();
    first.reply.send(Ok(empty())).unwrap();
    let second = recv(&mut rx).await;
    let event = json!(second.request.events.last().unwrap());
    assert_eq!(event["threshold_id"], "second");
    assert!(
        event["at"].as_f64().unwrap() < released_at,
        "heartbeat must fire while API worker is blocked"
    );
    second
        .reply
        .send(Ok(serde_json::from_value(
            json!({"action":"end_call","end_reason":"duration_limit"}),
        )
        .unwrap()))
        .unwrap();
    let end = recv(&mut end_rx).await;
    assert_eq!(end.request.reason, "duration_limit");
    end.reply.send(Ok(EndCallResponse {})).unwrap();
    let report = tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.requests, 2);
    assert_eq!(report.stale_responses, 0);
    assert!(report.end_acknowledged);
    assert!(rx.try_recv().is_err());
    assert!(end_rx.try_recv().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_text_session_start_input_response_terminal_frame_and_close() {
    let (requests, mut rx) = mpsc::unbounded_channel();
    let (ends, mut end_rx) = mpsc::unbounded_channel();
    let api = Arc::new(Api {
        opening: serde_json::from_value(json!({"call_config":{"persona_voice":"x"}})).unwrap(),
        requests,
        ends,
        fail_start: false,
        probe: None,
    });
    let settings =
        vogon_configuration::settings("settings", &json!({}), &json!({}), &json!({})).unwrap();
    let profile = vogon_configuration::built_in(&settings).unwrap();
    let frames = Arc::new(Mutex::new(vec![]));
    let factory = SessionFactory {
        settings,
        profile,
        mode: Mode::Text,
        speech: None,
        clients: Default::default(),
        artifact: None,
        lender: String::new(),
        seed: 1,
        sink: tokio::sync::Mutex::new(Some(Box::new(Capture(frames.clone())))),
        initial_frame: None,
        stt_endpoint: String::new(),
        tts_endpoint: String::new(),
    };
    let config = Config::new(StartCallRequest {
        call_id: "text".into(),
        mode: "text".into(),
        resume: false,
        payload: json!({}),
    });
    let (input, inputs) = mpsc::channel(10);
    let call = tokio::spawn(
        Driver {
            config,
            api,
            factory: Arc::new(factory),
            hooks: Arc::new(NoHooks),
            clock: Clock::default(),
        }
        .run(inputs),
    );
    input
        .send(Input::Json(
            json!({"type":"client.message","payload":{"text":"hello"}}),
        ))
        .await
        .unwrap();
    let request = tokio::select! {
        request = recv(&mut rx) => request,
        end = recv(&mut end_rx) => {
            end.reply.send(Ok(EndCallResponse {})).unwrap();
            panic!("call ended before input: {:?}", call.await.unwrap());
        }
    };
    assert_eq!(
        json!(request.request.events.last().unwrap())["text"],
        "hello"
    );
    let mut response = response("answer");
    response.action = "end_call".into();
    response.end_reason = Some("complete".into());
    request.reply.send(Ok(response)).unwrap();
    let end = recv(&mut end_rx).await;
    assert!(
        json!(end.request.events)
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["kind"] == "delivery_outcome")
    );
    end.reply.send(Ok(EndCallResponse {})).unwrap();
    let report = call.await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(report.end_acknowledged);
    let values = frames
        .lock()
        .unwrap()
        .iter()
        .filter_map(|f| {
            if let Frame::Json(v) = f {
                Some(v.clone())
            } else {
                None
            }
        })
        .collect::<Vec<Value>>();
    assert!(
        values
            .iter()
            .any(|v| v["type"] == "server.message" && v["payload"]["text"] == "answer")
    );
    assert_eq!(values.last().unwrap()["type"], "server.call_ended");
}
