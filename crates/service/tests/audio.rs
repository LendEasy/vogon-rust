//! Real caller sockets through the host and native driver to gated local providers.
#[allow(dead_code)]
#[path = "../../orchestration/tests/support/network.rs"]
mod network;
mod support;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use network::{Stt, Tts};
use serde_json::json;
use support::*;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use vogon_service::{
    record::{Record, State},
    store::Change,
};

async fn flush(tts: &mut Tts) {
    while recv(&mut tts.commands).await != "Flush" {}
}
async fn finish(gateway: &Gateway, turn: &mut Turn, ws: &mut Ws, phone: bool) {
    ws.send(Message::Text(
        if phone {
            json!({"event":"stop"})
        } else {
            json!({"type":"client.hangup"})
        }
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    let end = recv(&mut turn.requests).await;
    assert_eq!(end.operation, "end_call");
    end.reply.send(json!({})).unwrap();
    gateway.service.shutdown().await.unwrap();
    assert_eq!(gateway.service.active(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn browser_host_streams_while_eager_http_and_tts_are_blocked_and_discards_stale_speech() {
    let mut stt = Stt::start().await;
    let mut tts = Tts::start().await;
    let mut turn = Turn::start().await;
    let mut cfg = config();
    cfg.stt_endpoint = stt.url.clone();
    cfg.tts_endpoint = tts.url.clone();
    cfg.settings["ambience_enabled"] = json!(false);
    let gateway = Gateway::start(cfg).await;
    let made = gateway.create("browser", "widget", &turn.url).await;
    let (mut ws, _) = connect_async(made["ws_url"].as_str().unwrap())
        .await
        .unwrap();
    let start = recv(&mut turn.requests).await;
    assert_eq!(start.body["mode"], "voice");
    start.reply.send(json!({"call_config":{"persona_voice":"aura-2-thalia-en","ambience":{"enabled":false},"barge_policy":{"semantic_judge":false}}})).unwrap();
    message(&mut ws, "server.session_ready").await;
    stt.turn("EagerEndOfTurn", "Can I pay");
    let eager = recv(&mut turn.requests).await;
    assert_eq!(
        eager.body["events"].as_array().unwrap().last().unwrap()["kind"],
        "utterance_eager"
    );
    // A held HTTP response must not hold the caller/STT pump.
    ws.send(Message::Binary(vec![11; 640].into()))
        .await
        .unwrap();
    assert_eq!(recv(&mut stt.audio).await, vec![11; 640]);
    stt.turn("EndOfTurn", "Can I pay tomorrow instead");
    loop {
        if message(&mut ws, "server.transcript_final").await["payload"]["text"]
            == "Can I pay tomorrow instead"
        {
            break;
        }
    }
    eager
        .reply
        .send(json!({"speak":[{"audio_id":"stale","text":"STALE"}]}))
        .unwrap();
    let final_turn = recv(&mut turn.requests).await;
    assert_eq!(
        final_turn.body["events"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()["text"],
        "Can I pay tomorrow instead"
    );
    assert_eq!(final_turn.body["request_id"], "browser-2");
    final_turn
        .reply
        .send(json!({"speak":[{"audio_id":"answer","text":"Yes tomorrow."}]}))
        .unwrap();
    assert_eq!(recv(&mut tts.spoken).await, "Yes tomorrow.");
    flush(&mut tts).await;
    // TTS has received Flush but intentionally has no audio yet; STT still progresses.
    ws.send(Message::Binary(vec![22; 640].into()))
        .await
        .unwrap();
    assert_eq!(recv(&mut stt.audio).await, vec![22; 640]);
    tts.finish(42);
    let audio = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(Ok(Message::Binary(b))) = ws.next().await {
                break b;
            }
        }
    })
    .await
    .unwrap();
    let vogon_transport::wire::Frame::Binary(expected) =
        vogon_transport::wire::browser_audio("answer", &[42; 640])
    else {
        panic!()
    };
    assert_eq!(audio.as_ref(), expected);
    assert!(tts.spoken.try_recv().is_err());
    finish(&gateway, &mut turn, &mut ws, false).await;
    stt.task.await.unwrap();
    tts.task.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phone_host_resolves_sid_feeds_initial_start_and_streams_media_both_directions() {
    let mut stt = Stt::start().await;
    let mut tts = Tts::start().await;
    let mut turn = Turn::start().await;
    let mut cfg = config();
    cfg.stt_endpoint = stt.url.clone();
    cfg.tts_endpoint = tts.url.clone();
    cfg.settings["ambience_enabled"] = json!(false);
    let gateway = Gateway::start(cfg).await;
    // Seed the post-dial record: the HTTP dial contract is independently tested locally.
    let record: Record = serde_json::from_value(json!({"tenant":"acme","call_id":"phone","session_token":"phone-token","mode":"twilio","app_base_url":turn.url})).unwrap();
    gateway.service.store.create(record).await.unwrap();
    gateway
        .service
        .store
        .change(
            ("acme".into(), "phone".into()),
            Change::Provider("CA-local".into()),
        )
        .await
        .unwrap();
    let (mut wrong, _) = connect_async(format!(
        "{}/ws/call?token=phone-token",
        gateway.url.replacen("http://", "ws://", 1)
    ))
    .await
    .unwrap();
    assert_eq!(closed(&mut wrong).await, 4001);
    let (mut ws, _) = connect_async(format!(
        "{}/ws/twilio",
        gateway.url.replacen("http://", "ws://", 1)
    ))
    .await
    .unwrap();
    for v in [
        json!({"event":"connected"}),
        json!({"event":"start","start":{"callSid":"CA-local","streamSid":"MZ-local"}}),
    ] {
        ws.send(Message::Text(v.to_string().into())).await.unwrap();
    }
    let start = recv(&mut turn.requests).await;
    assert_eq!(start.body["call_id"], "phone");
    assert_eq!(start.body["mode"], "twilio");
    start.reply.send(json!({"call_config":{"persona_voice":"aura-2-thalia-en","ambience":{"enabled":false},"barge_policy":{"semantic_judge":false}}})).unwrap();
    stt.turn("EndOfTurn", "Hello");
    let req = recv(&mut turn.requests).await;
    assert_eq!(req.operation, "process_events");
    req.reply
        .send(json!({"speak":[{"audio_id":"phone-answer","text":"Hello back."}]}))
        .unwrap();
    assert_eq!(recv(&mut tts.spoken).await, "Hello back.");
    flush(&mut tts).await;
    for _ in 0..5 {
        ws.send(Message::Text(json!({"event":"media","streamSid":"MZ-local","media":{"payload":STANDARD.encode([255;160])}}).to_string().into())).await.unwrap();
    }
    for _ in 0..5 {
        assert_eq!(recv(&mut stt.audio).await.len(), 160);
    }
    tts.finish(255);
    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(Ok(Message::Text(t))) = ws.next().await {
                let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                assert!(
                    matches!(v["event"].as_str(), Some("media" | "mark" | "clear")),
                    "browser controls are invalid on a Twilio media socket: {v}"
                );
                if v["event"] == "media" {
                    break v;
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(frame["streamSid"], "MZ-local");
    assert!(
        !STANDARD
            .decode(frame["media"]["payload"].as_str().unwrap())
            .unwrap()
            .is_empty()
    );
    finish(&gateway, &mut turn, &mut ws, true).await;
    assert_eq!(
        gateway
            .service
            .store
            .get(("acme".into(), "phone".into()))
            .await
            .unwrap()
            .unwrap()
            .state,
        State::Ended
    );
    stt.task.await.unwrap();
    tts.task.await.unwrap();
}
