//! Real local WebSocket tests for ordered provider streaming and lifecycle.
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use vogon_clocks::Clock;
use vogon_speech_providers::{
    client::{CancelOutcome, SttClient, SttOutput, TtsClient, TtsOutput},
    config::ApiKey,
    stt::SttConfig,
    tts::TtsConfig,
};
fn key() -> ApiKey {
    ApiKey::new("local-test-key".into()).unwrap()
}
async fn listen() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    (listener, url)
}
fn turn(event: &str, text: &str) -> Message {
    Message::Text(json!({"type":"TurnInfo","event":event,"transcript":text,"words":[{"word":text,"confidence":0.9,"start":0.0}]}).to_string().into())
}

#[tokio::test]
async fn eager_resume_stitch_and_close_fallback() {
    let (listener, url) = listen().await;
    let server = tokio::spawn(async move {
        let (mut socket, _) = (
            accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap(),
            (),
        );
        assert!(matches!(
            socket.next().await.unwrap().unwrap(),
            Message::Binary(_)
        ));
        for event in [
            turn("EagerEndOfTurn", "Can I pay it"),
            turn("TurnResumed", ""),
            turn("EagerEndOfTurn", "on the phone?"),
        ] {
            socket.send(event).await.unwrap();
        }
        // Remote close must emit exactly one stitched final fallback.
        socket.close(None).await.unwrap();
    });
    let cfg = SttConfig {
        eager_eot_threshold: Some(0.7),
        ..SttConfig::default()
    };
    let (mut client, mut events) = SttClient::connect(&url, cfg, key(), Clock::default())
        .await
        .unwrap();
    client.input.send_audio(vec![0; 320]).await.unwrap();
    let mut transcript = vec![];
    while let Some(event) = timeout(Duration::from_secs(3), events.recv())
        .await
        .unwrap()
    {
        if let SttOutput::Transcript { event, .. } = event {
            transcript.push(serde_json::to_value(event).unwrap());
        }
    }
    assert_eq!(
        transcript
            .iter()
            .map(|e| e["kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["eager", "resumed", "eager", "final"]
    );
    assert_eq!(transcript[3]["text"], "Can I pay it on the phone?");
    client.close().await;
    client.close().await;
    server.await.unwrap();
}

#[tokio::test]
async fn legacy_hold_runs_without_another_socket_message() {
    let (listener, url) = listen().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        socket.send(turn("EndOfTurn", "hello")).await.unwrap();
        while let Some(Ok(message)) = socket.next().await {
            if matches!(message, Message::Text(_)) {
                socket.close(None).await.unwrap();
                break;
            }
        }
    });
    let (mut client, mut events) = SttClient::connect(
        &url,
        SttConfig {
            hold_ms: 30,
            ..SttConfig::default()
        },
        key(),
        Clock::default(),
    )
    .await
    .unwrap();
    loop {
        if let SttOutput::Transcript { event, .. } = timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap()
        {
            assert_eq!(serde_json::to_value(event).unwrap()["kind"], "final");
            break;
        }
    }
    client.close().await;
    server.await.unwrap();
}

#[tokio::test]
async fn tts_segments_stream_and_cancel_drains_old_audio() {
    let (listener, url) = listen().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let mut texts = 0;
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let value: Value = serde_json::from_str(&text).unwrap();
            match value["type"].as_str().unwrap() {
                "Speak" => texts += 1,
                "Flush" => {
                    socket
                        .send(Message::Binary(vec![texts as u8; 320].into()))
                        .await
                        .unwrap();
                    socket
                        .send(Message::Text(json!({"type":"Flushed"}).to_string().into()))
                        .await
                        .unwrap();
                }
                "Clear" => {
                    socket
                        .send(Message::Binary(vec![99; 320].into()))
                        .await
                        .unwrap();
                    socket
                        .send(Message::Text(json!({"type":"Cleared"}).to_string().into()))
                        .await
                        .unwrap();
                }
                _ => {}
            }
        }
    });
    let mut client = TtsClient::connect(&url, TtsConfig::default(), key(), Clock::default())
        .await
        .unwrap();
    let mut stream = client
        .stream_segments(vec!["a".into(), "b".into()])
        .await
        .unwrap();
    let mut chunks = vec![];
    while let Some(event) = timeout(Duration::from_secs(2), stream.recv())
        .await
        .unwrap()
    {
        match event {
            TtsOutput::Audio { chunk } => chunks.push(chunk),
            TtsOutput::Finished => break,
            _ => panic!("unexpected stream event"),
        }
    }
    assert_eq!(chunks.iter().map(|c| c.segment).collect::<Vec<_>>(), [0, 1]);
    assert!(chunks[0].first_chunk_ms.is_some());
    assert!(chunks[1].first_chunk_ms.is_none());
    assert_eq!(client.cancel().await, CancelOutcome::Cleared);
    let mut next = client.stream("next".into()).await.unwrap();
    if let TtsOutput::Audio { chunk } = timeout(Duration::from_secs(2), next.recv())
        .await
        .unwrap()
        .unwrap()
    {
        assert_eq!(chunk.data, vec![3; 320]);
    } else {
        panic!("missing next audio")
    }
    client.close().await;
    server.await.unwrap();
}

#[tokio::test]
async fn cancel_timeout_requires_reconnect_and_never_reuses_dirty_socket() {
    let (listener, url) = listen().await;
    let server = tokio::spawn(async move {
        for attempt in 0..2 {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            while let Some(Ok(message)) = socket.next().await {
                if let Message::Text(text) = message {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if attempt == 1 && v["type"] == "Flush" {
                        socket
                            .send(Message::Binary(vec![2; 320].into()))
                            .await
                            .unwrap();
                        socket
                            .send(Message::Text(json!({"type":"Flushed"}).to_string().into()))
                            .await
                            .unwrap();
                    }
                }
            }
        }
    });
    let mut client = TtsClient::connect(&url, TtsConfig::default(), key(), Clock::default())
        .await
        .unwrap();
    let mut old = client.stream("old".into()).await.unwrap();
    assert_eq!(client.cancel().await, CancelOutcome::TimedOut);
    assert!(matches!(old.recv().await, Some(TtsOutput::Cancelled)));
    assert!(client.stream("unsafe".into()).await.is_err());
    assert!(client.reconnect().await);
    let mut fresh = client.stream("fresh".into()).await.unwrap();
    assert!(matches!(fresh.recv().await, Some(TtsOutput::Audio { .. })));
    client.close().await;
    server.await.unwrap();
}

#[tokio::test]
async fn slow_tts_consumer_does_not_block_ping_or_completion() {
    let (listener, url) = listen().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        for _ in 0..2 {
            socket.next().await.unwrap().unwrap();
        }
        for _ in 0..100 {
            socket
                .send(Message::Binary(vec![1; 320].into()))
                .await
                .unwrap();
        }
        socket.send(Message::Ping(vec![7].into())).await.unwrap();
        let pong = timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(pong, Message::Pong(_)));
        socket
            .send(Message::Text(json!({"type":"Flushed"}).to_string().into()))
            .await
            .unwrap();
        while let Some(Ok(message)) = socket.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });
    let mut client = TtsClient::connect(&url, TtsConfig::default(), key(), Clock::default())
        .await
        .unwrap();
    let mut stream = client.stream("long".into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut count = 0;
    while let Some(event) = timeout(Duration::from_secs(3), stream.recv())
        .await
        .unwrap()
    {
        match event {
            TtsOutput::Audio { .. } => count += 1,
            TtsOutput::Finished => break,
            _ => panic!("unexpected"),
        }
    }
    assert_eq!(count, 100);
    client.close().await;
    server.await.unwrap();
}

#[tokio::test]
async fn disconnect_is_failure_not_successful_empty_audio() {
    let (listener, url) = listen().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        for _ in 0..2 {
            socket.next().await.unwrap().unwrap();
        }
        socket.close(None).await.unwrap();
    });
    let mut client = TtsClient::connect(&url, TtsConfig::default(), key(), Clock::default())
        .await
        .unwrap();
    let mut stream = client.stream("x".into()).await.unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(2), stream.recv())
            .await
            .unwrap(),
        Some(TtsOutput::Failed { .. })
    ));
    client.close().await;
    server.await.unwrap();
}

#[tokio::test]
async fn live_listener_background_open_quiet_final_and_late_delegation() {
    use vogon_speech_providers::{live::LiveConfig, live_client::LiveClient};
    let (listener, url) = listen().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let start = socket.next().await.unwrap().unwrap();
        assert!(
            matches!(start,Message::Text(t)if t.contains("session.start")&&t.contains("gpt-live-1"))
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        socket
            .send(Message::Text(
                json!({"type":"session.started","session":{"id":"local-live"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                json!({"type":"session.output_audio.delta","audio":"not-caller-audio"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        socket.send(Message::Text(json!({"type":"session.input_transcript.delta","delta":"hello there","start_ms":125}).to_string().into())).await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        socket
            .send(Message::Text(
                json!({"type":"session.delegation.created"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let v: Value = serde_json::from_str(&text).unwrap();
            if v["type"] == "session.close" {
                socket
                    .send(Message::Text(
                        json!({"type":"session.closed","usage":{"seconds":1.25}})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .unwrap();
                break;
            }
        }
    });
    let (mut client, mut events) = LiveClient::connect(
        &url,
        LiveConfig {
            start_wait_ms: 1,
            final_quiet_ms: 20,
            ..Default::default()
        },
        key(),
        Clock::default(),
    )
    .await
    .unwrap();
    client.input.send_audio(vec![1; 160]).await.unwrap();
    let mut transcript = vec![];
    while transcript.len() < 2 {
        if let SttOutput::Transcript { event, onset } =
            timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap()
        {
            assert!(onset.is_some());
            transcript.push(event);
        }
    }
    assert!(matches!(
        &transcript[0],
        vogon_speech_providers::stt::Transcript::Interim { .. }
    ));
    assert!(
        matches!(&transcript[1],vogon_speech_providers::stt::Transcript::Final{text,..}if text=="hello there")
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    client.close().await;
    let mut quality = None;
    while let Some(event) = events.recv().await {
        match event {
            SttOutput::Transcript { .. } => panic!("duplicate final or leaked model audio"),
            SttOutput::Closed { quality: q } => quality = Some(q),
            _ => {}
        }
    }
    let q = quality.unwrap();
    assert_eq!(q["live_fallback_finals"], 1);
    assert_eq!(q["live_frames_dropped_before_open"], 1);
    assert_eq!(q["live_usage_seconds"], 1.25);
    server.await.unwrap();
}

#[tokio::test]
async fn live_start_timeout_is_reported_after_background_return() {
    use vogon_speech_providers::{live::LiveConfig, live_client::LiveClient};
    let (listener, url) = listen().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        while socket.next().await.is_some() {}
    });
    let (mut client, mut output) = LiveClient::connect(
        &url,
        LiveConfig {
            start_wait_ms: 1,
            start_timeout_s: 0.03,
            ..Default::default()
        },
        key(),
        Clock::default(),
    )
    .await
    .unwrap();
    let event = timeout(Duration::from_secs(2), output.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(event,SttOutput::Error{error}if error.code=="live_start_timeout"));
    client.close().await;
    server.await.unwrap();
}

// Tungstenite fixes this callback error type to its HTTP response.
#[allow(clippy::result_large_err)]
#[tokio::test]
async fn flux_connection_preserves_eager_clamp_repeated_keyterms_and_auth() {
    let (listener, url) = listen().await;
    let server = tokio::spawn(async move {
        let mut socket = tokio_tungstenite::accept_hdr_async(
            listener.accept().await.unwrap().0,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                assert_eq!(request.headers()["authorization"], "Token local-test-key");
                let query: Vec<_> =
                    url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes())
                        .into_owned()
                        .collect();
                assert!(query.contains(&("eager_eot_threshold".into(), "0.85".into())));
                assert_eq!(
                    query
                        .iter()
                        .filter(|(k, _)| k == "keyterm")
                        .map(|(_, v)| v.as_str())
                        .collect::<Vec<_>>(),
                    ["Joe", "A+B & café"]
                );
                assert!(!query.iter().any(|(k, _)| k == "language"));
                Ok(response)
            },
        )
        .await
        .unwrap();
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            if text.contains("CloseStream") {
                let _ = socket.close(None).await;
                break;
            }
        }
    });
    let (mut client, _) = SttClient::connect(
        &url,
        SttConfig {
            eager_eot_threshold: Some(0.99),
            eot_threshold: 0.85,
            keyterms: vec!["Joe".into(), "A+B & café".into()],
            language: "es".into(),
            ..Default::default()
        },
        key(),
        Clock::default(),
    )
    .await
    .unwrap();
    client.close().await;
    server.await.unwrap();
    assert_eq!(format!("{:?}", key()), "ApiKey([redacted])");
}

/// Provider replay must be filtered before transport assigns fresh callback identities.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_sequence_replay_is_call_local_and_later_identical_turn_is_preserved() {
    async fn call() {
        let (listener, url) = listen().await;
        let server = tokio::spawn(async move {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            for (seq, event) in [
                (1, "EagerEndOfTurn"),
                (1, "EagerEndOfTurn"),
                (2, "EndOfTurn"),
                (2, "EndOfTurn"),
                (1, "EagerEndOfTurn"),
                (3, "StartOfTurn"),
                (4, "EndOfTurn"),
            ] {
                socket.send(Message::Text(json!({"type":"TurnInfo","sequence_id":seq,"event":event,"transcript":"same words","words":[{"word":"same","confidence":0.9,"start":0.0}]}).to_string().into())).await.unwrap();
            }
            socket.close(None).await.unwrap();
        });
        let config = SttConfig {
            eager_eot_threshold: Some(0.3),
            ..SttConfig::default()
        };
        let (mut client, mut events) = SttClient::connect(&url, config, key(), Clock::default())
            .await
            .unwrap();
        let mut kinds = vec![];
        while let Some(event) = timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
        {
            if let SttOutput::Transcript { event, .. } = event {
                kinds.push(
                    serde_json::to_value(event).unwrap()["kind"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                );
            }
        }
        assert_eq!(kinds, ["eager", "final", "interim", "final"]);
        client.close().await;
        server.await.unwrap();
    }
    tokio::join!(call(), call());
}

#[tokio::test]
async fn configured_handshake_retries_transient_status_without_sending_audio_twice() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (listener, url) = listen().await;
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert!(tcp.read(&mut request).await.unwrap() > 0);
            tcp.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
        }
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(
            ws.next().await.unwrap().unwrap(),
            Message::Binary(vec![1, 2, 3].into())
        );
        ws.close(None).await.unwrap();
    });
    let p = vogon_resilience::Policy {
        attempts: 3,
        initial_ms: 1,
        max_ms: 2,
        jitter: false,
    };
    let (mut client, _) =
        SttClient::connect_with_retry(&url, SttConfig::default(), key(), Clock::default(), p)
            .await
            .unwrap();
    client.input.send_audio(vec![1, 2, 3]).await.unwrap();
    server.await.unwrap();
    client.close().await;
}
#[tokio::test]
async fn configured_handshake_does_not_retry_auth_failure() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (listener, url) = listen().await;
    let server = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        assert!(tcp.read(&mut request).await.unwrap() > 0);
        tcp.write_all(
            b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let result = SttClient::connect_with_retry(
        &url,
        SttConfig::default(),
        key(),
        Clock::default(),
        vogon_resilience::Policy::default(),
    )
    .await;
    assert!(matches!(result,Err(e) if e.code=="provider_http_401"));
    server.await.unwrap();
}
