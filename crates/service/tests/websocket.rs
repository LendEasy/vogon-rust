mod support;
use futures_util::SinkExt;
use serde_json::json;
use support::*;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use vogon_service::record::State;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_text_socket_preserves_payload_snapshot_and_end_call() {
    let mut turn = Turn::start().await;
    let gateway = Gateway::start(config()).await;
    let made = gateway.create("text", "text", &turn.url).await;
    let (mut ws, _) = connect_async(made["ws_url"].as_str().unwrap())
        .await
        .unwrap();
    let start = recv(&mut turn.requests).await;
    assert_eq!(start.operation, "start_call");
    assert_eq!(start.body["payload"], json!({"opaque":[1,2]}));
    start.reply.send(json!({"call_config":{"persona_voice":"x","language":"hi-IN"},"speak":[{"audio_id":"opening","text":"Hello."}]})).unwrap();
    assert_eq!(
        message(&mut ws, "server.message").await["payload"]["text"],
        "Hello."
    );
    ws.send(Message::Text(
        json!({"type":"client.message","payload":{"text":"question"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let request = recv(&mut turn.requests).await;
    assert_eq!(request.operation, "process_events");
    let at = request.body["events"].as_array().unwrap().last().unwrap()["at"]
        .as_f64()
        .unwrap();
    assert!(
        (0.0..30.0).contains(&at),
        "Turn API time must be call-relative: {at}"
    );
    assert_eq!(
        request.body["events"].as_array().unwrap().last().unwrap()["text"],
        "question"
    );
    request
        .reply
        .send(json!({"speak":[{"audio_id":"reply","text":"Answer."}]}))
        .unwrap();
    assert_eq!(
        message(&mut ws, "server.message").await["payload"]["text"],
        "Answer."
    );
    let stored = gateway
        .service
        .store
        .get(("acme".into(), "text".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.call_config.unwrap()["resolved"]["language"], "hi-IN");
    ws.send(Message::Text(
        json!({"type":"client.hangup"}).to_string().into(),
    ))
    .await
    .unwrap();
    let end = recv(&mut turn.requests).await;
    assert_eq!(end.operation, "end_call");
    end.reply.send(json!({})).unwrap();
    message(&mut ws, "server.call_ended").await;
    gateway.service.shutdown().await.unwrap();
    let record = gateway
        .service
        .store
        .get(("acme".into(), "text".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, State::Ended);
    assert!(record.ended_at_utc > 0.);
    assert_eq!(gateway.service.active(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_socket_capacity_and_parallel_shutdown_keep_calls_isolated() {
    let mut turn = Turn::start().await;
    let mut cfg = config();
    cfg.max_active = 2;
    let gateway = Gateway::start(cfg).await;
    let a = gateway.create("a", "text", &turn.url).await;
    let b = gateway.create("b", "text", &turn.url).await;
    let c = gateway.create("c", "text", &turn.url).await;
    let (mut aw, _) = connect_async(a["ws_url"].as_str().unwrap()).await.unwrap();
    let ar = recv(&mut turn.requests).await;
    assert_eq!(ar.body["call_id"], "a"); // deliberately block StartCall
    let (mut duplicate, _) = connect_async(a["ws_url"].as_str().unwrap()).await.unwrap();
    assert_eq!(closed(&mut duplicate).await, 4003);
    let (mut bw, _) = connect_async(b["ws_url"].as_str().unwrap()).await.unwrap();
    let br = recv(&mut turn.requests).await;
    assert_eq!(br.body["call_id"], "b");
    br.reply
        .send(json!({"call_config":{"persona_voice":"b"}}))
        .unwrap();
    message(&mut bw, "server.session_ready").await;
    let (mut cw, _) = connect_async(c["ws_url"].as_str().unwrap()).await.unwrap();
    assert_eq!(closed(&mut cw).await, 1013);
    assert_eq!(
        gateway
            .service
            .store
            .get(("acme".into(), "c".into()))
            .await
            .unwrap()
            .unwrap()
            .state,
        State::Created
    );
    ar.reply
        .send(json!({"call_config":{"persona_voice":"a"}}))
        .unwrap();
    message(&mut aw, "server.session_ready").await;
    let service = gateway.service.clone();
    let stop = tokio::spawn(async move { service.shutdown().await });
    let mut ended = vec![];
    for _ in 0..2 {
        let req = recv(&mut turn.requests).await;
        assert_eq!(req.operation, "end_call");
        ended.push(req.body["call_id"].as_str().unwrap().to_owned());
        req.reply.send(json!({})).unwrap();
    }
    ended.sort();
    assert_eq!(ended, ["a", "b"]);
    stop.await.unwrap().unwrap();
    assert_eq!(gateway.service.active(), 0);
    assert!(connect_async(c["ws_url"].as_str().unwrap()).await.is_err());
}

#[tokio::test]
async fn invalid_tokens_wrong_modes_and_twilio_handshake_expiry_close_without_starting_calls() {
    let mut cfg = config();
    cfg.handshake_timeout = std::time::Duration::from_millis(30);
    let gateway = Gateway::start(cfg).await;
    let base = gateway.url.replacen("http://", "ws://", 1);
    let (mut ws, _) = connect_async(format!("{base}/ws/call?token=nope"))
        .await
        .unwrap();
    assert_eq!(
        message(&mut ws, "server.error").await["payload"]["code"],
        "invalid_token"
    );
    assert_eq!(closed(&mut ws).await, 4001);
    let (mut ws, _) = connect_async(format!("{base}/ws/twilio")).await.unwrap();
    assert_eq!(closed(&mut ws).await, 4001);
    let (mut ws, _) = connect_async(format!("{base}/ws/twilio")).await.unwrap();
    ws.send(Message::Text(
        json!({"event":"start","start":{"callSid":"unknown"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    assert_eq!(closed(&mut ws).await, 4002);
    gateway.service.shutdown().await.unwrap();
    assert_eq!(gateway.service.active(), 0);
}

#[tokio::test]
async fn disconnect_during_request_finishes_once_and_rejects_reusing_ended_token() {
    let mut turn = Turn::start().await;
    let gateway = Gateway::start(config()).await;
    let made = gateway.create("disconnect", "text", &turn.url).await;
    let (mut ws, _) = connect_async(made["ws_url"].as_str().unwrap())
        .await
        .unwrap();
    recv(&mut turn.requests)
        .await
        .reply
        .send(json!({"call_config":{"persona_voice":"x"}}))
        .unwrap();
    message(&mut ws, "server.session_ready").await;
    ws.send(Message::Text(
        json!({"type":"client.message","payload":{"text":"pending"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let pending = recv(&mut turn.requests).await;
    ws.close(None).await.unwrap();
    drop(ws);
    let end = recv(&mut turn.requests).await;
    assert_eq!(end.operation, "end_call");
    end.reply.send(json!({})).unwrap();
    drop(pending);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while gateway.service.active() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (mut again, _) = connect_async(made["ws_url"].as_str().unwrap())
        .await
        .unwrap();
    assert_eq!(closed(&mut again).await, 4003);
    gateway.service.shutdown().await.unwrap();
    assert!(turn.requests.try_recv().is_err());
}

#[tokio::test]
async fn overload_during_start_drops_input_and_does_not_leave_worker_or_claim() {
    let mut turn = Turn::start().await;
    let mut cfg = config();
    cfg.input_capacity = 1;
    let gateway = Gateway::start(cfg).await;
    let made = gateway.create("overflow", "text", &turn.url).await;
    let (mut ws, _) = connect_async(made["ws_url"].as_str().unwrap())
        .await
        .unwrap();
    let start = recv(&mut turn.requests).await;
    for _ in 0..4 {
        let _ = ws
            .send(Message::Text(
                json!({"type":"client.message","payload":{"text":"burst"}})
                    .to_string()
                    .into(),
            ))
            .await;
    }
    // Release startup only after the input task has had a chance to fill its bounded queue.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    start
        .reply
        .send(json!({"call_config":{"persona_voice":"x"}}))
        .unwrap();
    loop {
        let request = recv(&mut turn.requests).await;
        let end = request.operation == "end_call";
        request.reply.send(json!({})).unwrap();
        if end {
            break;
        }
    }
    gateway.service.shutdown().await.unwrap();
    assert_eq!(gateway.service.active(), 0);
}

#[tokio::test]
async fn shutdown_cancels_pending_handshakes_and_rejects_new_management_work() {
    let mut cfg = config();
    cfg.max_handshakes = 1;
    let gateway = Gateway::start(cfg).await;
    let url = format!("{}/ws/twilio", gateway.url.replacen("http://", "ws://", 1));
    let (mut pending, _) = connect_async(&url).await.unwrap();
    assert!(connect_async(&url).await.is_err());
    gateway.service.shutdown().await.unwrap();
    assert_eq!(closed(&mut pending).await, 4001);
    assert_eq!(gateway.service.active(), 0);
    let response = reqwest::Client::new()
        .post(format!("{}/v1/tenants/acme/calls", gateway.url))
        .json(&json!({"mode":"text","app":{"base_url":"http://app"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
}

#[tokio::test]
async fn failed_start_releases_admission_without_spurious_end_call() {
    let gateway = Gateway::start(config()).await;
    let mut turn = Turn::start().await;
    let made = gateway.create("bad-start", "text", &turn.url).await;
    let (_ws, _) = connect_async(made["ws_url"].as_str().unwrap())
        .await
        .unwrap();
    // A successful HTTP response with an invalid typed contract is a failed StartCall.
    recv(&mut turn.requests)
        .await
        .reply
        .send(json!({"call_config":"invalid"}))
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while gateway.service.active() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(turn.requests.try_recv().is_err());
    assert_eq!(
        gateway
            .service
            .store
            .get(("acme".into(), "bad-start".into()))
            .await
            .unwrap()
            .unwrap()
            .state,
        State::Failed
    );
    gateway.service.shutdown().await.unwrap();
}
