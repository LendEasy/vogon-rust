//! Integration tests exercise real local provider sockets and native audio ownership.
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Semaphore},
    time::{Duration, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use vogon_audio_runtime::Command;
use vogon_clocks::Clock;
use vogon_speech_providers::{
    Error, Result,
    config::{ApiKey, SpeechSettings},
    stt::{SttConfig, Transcript},
    tts::TtsConfig,
};
use vogon_transport::{
    audio::Audio,
    engine::{Engine, Mode},
    session::{Config, Response, Session},
    wire::{self, Frame, Receipt, Sink, Writer},
};

struct Capture(Arc<Mutex<Vec<Frame>>>);
impl Sink for Capture {
    fn send(&mut self, frame: Frame) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.0.lock().await.push(frame);
            Ok(())
        })
    }
}
fn settings() -> SpeechSettings {
    SpeechSettings {
        key: ApiKey::new("local-test-key".into()).unwrap(),
        browser: SttConfig {
            eager_eot_threshold: Some(0.7),
            ..Default::default()
        },
        phone: SttConfig {
            encoding: "mulaw".into(),
            sample_rate: 8000,
            eager_eot_threshold: Some(0.7),
            ..Default::default()
        },
        tts: TtsConfig::default(),
        phone_tts: TtsConfig {
            encoding: "mulaw".into(),
            sample_rate: 8000,
            ..Default::default()
        },
    }
}

#[tokio::test]
async fn machine_quiet_delivery_explicitly_takes_floor_despite_old_generation() {
    let (sl, su) = listen().await;
    let (tl, tu) = listen().await;
    let st = tokio::spawn(stt_server(sl, false));
    let tt = tokio::spawn(tts_server(tl, false));
    let frames = Arc::new(Mutex::new(vec![]));
    let (mut call, _) = Session::connect(
        Config::default(),
        &settings(),
        Box::new(Capture(frames)),
        Clock::default(),
        &su,
        &tu,
    )
    .await
    .unwrap();
    call.text(json!({"event":"start","streamSid":"MZheld"}))
        .await
        .unwrap();
    let rejected = call
        .send_response(Response {
            text: "Old normal response".into(),
            audio_id: "old".into(),
            generation: Some(999),
            ..Default::default()
        })
        .await;
    assert!(rejected.is_err());
    let result = call
        .send_held_response(Response {
            text: "Protected machine breakthrough".into(),
            audio_id: "held".into(),
            generation: Some(999),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, "complete");
    assert_eq!(result.chunks_sent, 3);
    call.close().await.unwrap();
    st.await.unwrap();
    tt.await.unwrap();
}
async fn listen() -> (TcpListener, String) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", l.local_addr().unwrap());
    (l, url)
}
async fn stt_server(listener: TcpListener, transcripts: bool) {
    let mut socket = accept_async(listener.accept().await.unwrap().0)
        .await
        .unwrap();
    while let Some(Ok(message)) = socket.next().await {
        match message {
            Message::Text(text) if text.contains("CloseStream") => {
                let _ = socket.close(None).await;
                break;
            }
            Message::Binary(_) if transcripts => {
                for (event, text) in [
                    ("EagerEndOfTurn", "Can I pay"),
                    ("TurnResumed", ""),
                    ("EndOfTurn", "tomorrow instead"),
                ] {
                    socket.send(Message::Text(json!({"type":"TurnInfo","event":event,"transcript":text,"words":[{"word":text,"confidence":0.9,"start":0.0}]}).to_string().into())).await.unwrap();
                }
            }
            _ => {}
        }
    }
}
async fn tts_server(listener: TcpListener, stalled: bool) {
    let mut socket = accept_async(listener.accept().await.unwrap().0)
        .await
        .unwrap();
    while let Some(Ok(message)) = socket.next().await {
        if let Message::Text(text) = message {
            let v: Value = serde_json::from_str(&text).unwrap();
            match v["type"].as_str() {
                Some("Flush") if !stalled => {
                    for _ in 0..3 {
                        socket
                            .send(Message::Binary(vec![255; 160].into()))
                            .await
                            .unwrap();
                    }
                    socket
                        .send(Message::Text(json!({"type":"Flushed"}).to_string().into()))
                        .await
                        .unwrap();
                }
                Some("Clear") => {
                    socket
                        .send(Message::Binary(vec![77; 160].into()))
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
    }
}

#[tokio::test]
async fn phone_streams_to_aec_and_commits_only_final_after_resume() {
    let (sl, su) = listen().await;
    let (tl, tu) = listen().await;
    let st = tokio::spawn(stt_server(sl, true));
    let tt = tokio::spawn(tts_server(tl, false));
    let frames = Arc::new(Mutex::new(vec![]));
    let (mut call, mut events) = Session::connect(
        Config::default(),
        &settings(),
        Box::new(Capture(frames.clone())),
        Clock::default(),
        &su,
        &tu,
    )
    .await
    .unwrap();
    call.text(json!({"event":"start","streamSid":"MZtest"}))
        .await
        .unwrap();
    let report = call
        .send_response(Response {
            text: "Welcome to the test.".into(),
            audio_id: "a".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.status, "complete");
    assert_eq!(report.audio_ms, Some(60.0));
    assert!(report.mark_pending);
    assert_eq!(report.chunks_sent, 3);
    let snapshot = call.snapshot().await;
    assert_eq!(snapshot["clocks"]["playing_depth"], 1);
    call.text(json!({"event":"mark","mark":{"name":"tts_s0_a"}}))
        .await
        .unwrap();
    call.text(json!({"event":"mark","mark":{"name":"tts_end_a"}}))
        .await
        .unwrap();
    assert_eq!(call.snapshot().await["clocks"]["playing_depth"], 0);
    call.binary(vec![255; 160]).await.unwrap();
    let mut utterances = vec![];
    while utterances.len() < 3 {
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        if event["kind"]
            .as_str()
            .unwrap_or("")
            .starts_with("utterance_")
        {
            utterances.push(event);
        }
    }
    assert_eq!(
        utterances
            .iter()
            .map(|v| v["kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["utterance_eager", "utterance_resumed", "utterance_final"]
    );
    assert_eq!(utterances[2]["text"], "Can I pay tomorrow instead");
    assert!(utterances[0]["seq"].as_u64() < utterances[2]["seq"].as_u64());
    call.close().await.unwrap();
    st.await.unwrap();
    tt.await.unwrap();
}

/// Real phone receipts arm silence; the real heartbeat respects nested pauses.
#[tokio::test]
async fn heartbeat_phone_receipt_arms_silence_but_nested_reasoning_pause_blocks_it() {
    use vogon_clocks::call::{Config as ClockConfig, Threshold};
    let (sl, su) = listen().await;
    let (tl, tu) = listen().await;
    let st = tokio::spawn(stt_server(sl, false));
    let tt = tokio::spawn(tts_server(tl, false));
    let (mut call, mut events) = Session::connect(
        Config {
            clocks: ClockConfig {
                silence_thresholds: vec![Threshold {
                    id: "probe".into(),
                    after_sec: 0.1,
                }],
                duration_thresholds: vec![
                    Threshold {
                        id: "first".into(),
                        after_sec: 0.1,
                    },
                    Threshold {
                        id: "second".into(),
                        after_sec: 1.1,
                    },
                ],
                ..Default::default()
            },
            ..Default::default()
        },
        &settings(),
        Box::new(Capture(Arc::new(Mutex::new(vec![])))),
        Clock::default(),
        &su,
        &tu,
    )
    .await
    .unwrap();
    call.text(json!({"event":"start","streamSid":"MZtimer"}))
        .await
        .unwrap();
    call.pause_silence().await;
    call.pause_silence().await;
    call.send_response(Response {
        text: "Welcome.".into(),
        audio_id: "opening".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    call.text(json!({"event":"mark","mark":{"name":"tts_end_opening"}}))
        .await
        .unwrap();
    let snapshot = call.snapshot().await;
    assert_eq!(snapshot["clocks"]["sil_armed"], true);
    assert_eq!(snapshot["clocks"]["playing_depth"], 0);
    assert_eq!(
        receive_kind(&mut events, "duration_threshold").await["threshold_id"],
        "first"
    );
    assert_eq!(call.snapshot().await["clocks"]["sil_idle"], 0.);
    call.resume_silence().await;
    assert_eq!(
        receive_kind(&mut events, "duration_threshold").await["threshold_id"],
        "second"
    );
    let snapshot = call.snapshot().await;
    assert_eq!(snapshot["clocks"]["pause_depth"], 1);
    assert_eq!(snapshot["clocks"]["sil_idle"], 0.);
    assert_eq!(snapshot["clocks"]["sil_fired"], json!([]));
    call.resume_silence().await;
    let silence = receive_kind(&mut events, "silence_threshold").await;
    assert_eq!(silence["threshold_id"], "probe");
    assert!(silence["silent_for_sec"].as_f64().unwrap() >= 0.1);
    call.close().await.unwrap();
    st.await.unwrap();
    tt.await.unwrap();
}

#[tokio::test]
async fn browser_merges_audio_and_uses_utf8_header() {
    let (sl, su) = listen().await;
    let (tl, tu) = listen().await;
    let st = tokio::spawn(stt_server(sl, false));
    let tt = tokio::spawn(tts_server(tl, false));
    let frames = Arc::new(Mutex::new(vec![]));
    let (mut call, _) = Session::connect(
        Config {
            mode: Mode::Browser,
            ..Default::default()
        },
        &settings(),
        Box::new(Capture(frames.clone())),
        Clock::default(),
        &su,
        &tu,
    )
    .await
    .unwrap();
    let report = call
        .send_response(Response {
            text: "Hello".into(),
            audio_id: "你好".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.chunks_sent, 1);
    let out = frames.lock().await;
    assert!(matches!(&out[0],Frame::Json(v)if v["type"]=="server.tts_stream_start"));
    assert_eq!(out[1], wire::browser_audio("你好", &vec![255; 480]));
    assert!(matches!(&out[2],Frame::Json(v)if v["type"]=="server.tts_stream_end"));
    drop(out);
    call.close().await.unwrap();
    st.await.unwrap();
    tt.await.unwrap();
}

#[tokio::test]
async fn cancellation_does_not_wait_for_next_tts_chunk() {
    let (sl, su) = listen().await;
    let (tl, tu) = listen().await;
    let st = tokio::spawn(stt_server(sl, false));
    let tt = tokio::spawn(tts_server(tl, true));
    let frames = Arc::new(Mutex::new(vec![]));
    let (mut call, _) = Session::connect(
        Config {
            mode: Mode::Browser,
            ..Default::default()
        },
        &settings(),
        Box::new(Capture(frames.clone())),
        Clock::default(),
        &su,
        &tu,
    )
    .await
    .unwrap();
    let response = call.send_response(Response {
        text: "Stalled provider".into(),
        audio_id: "old".into(),
        ..Default::default()
    });
    let cut = async {
        loop {
            if call.snapshot().await["playing"] == true {
                break;
            }
            tokio::task::yield_now().await;
        }
        call.interrupt().await.unwrap();
    };
    let (report, _) = timeout(Duration::from_secs(2), async {
        tokio::join!(response, cut)
    })
    .await
    .unwrap();
    assert_eq!(report.unwrap().unwrap().status, "truncated");
    assert_eq!(call.snapshot().await["clocks"]["playing_depth"], 0);
    assert!(
        !frames
            .lock()
            .await
            .iter()
            .any(|f| matches!(f, Frame::Binary(_)))
    );
    call.close().await.unwrap();
    st.await.unwrap();
    tt.await.unwrap();
}

#[tokio::test]
async fn text_adapter_does_not_open_speech_connections() {
    let frames = Arc::new(Mutex::new(vec![]));
    let (mut call, mut events) = Session::text_only(
        Config::default(),
        Box::new(Capture(frames.clone())),
        Clock::default(),
    )
    .await
    .unwrap();
    call.text(json!({"type":"client.message","payload":{"text":"  hello  "}}))
        .await
        .unwrap();
    let event = events.recv().await.unwrap();
    assert_eq!(event["text"], "hello");
    assert_eq!(event["confidence"], 1.0);
    assert!(
        call.send_response(Response::default())
            .await
            .unwrap()
            .is_none()
    );
    let report = call
        .send_response(Response {
            text: "answer".into(),
            audio_id: "x".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.chunks_sent, 1);
    assert_eq!(frames.lock().await.len(), 1);
    assert!(call.text(json!({"type":"client.hangup"})).await.unwrap());
    call.close().await.unwrap();
}

struct BlockFirst {
    frames: Arc<Mutex<Vec<Frame>>>,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    first: bool,
}
impl Sink for BlockFirst {
    fn send(&mut self, frame: Frame) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            if self.first {
                self.first = false;
                self.entered.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            self.frames.lock().await.push(frame);
            Ok(())
        })
    }
}
#[tokio::test]
async fn clear_waits_for_inflight_reference_and_discards_queued_media() {
    let audio = Audio::start(Default::default()).await.unwrap();
    let clock = Clock::default();
    let state = Arc::new(Mutex::new(Engine::new(
        Mode::Twilio,
        Default::default(),
        Default::default(),
        Default::default(),
        0.0,
    )));
    let frames = Arc::new(Mutex::new(vec![]));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let (writer, _owner) = Writer::start(
        Box::new(BlockFirst {
            frames: frames.clone(),
            entered: entered.clone(),
            release: release.clone(),
            first: true,
        }),
        Some(audio.clone()),
        state,
        clock,
    );
    let w = writer.clone();
    let first = tokio::spawn(async move {
        w.send(
            Some(0),
            wire::media("MZ", &[5; 160]),
            Receipt::Media(vec![5; 160]),
        )
        .await
    });
    entered.acquire().await.unwrap().forget();
    let w = writer.clone();
    let queued = tokio::spawn(async move {
        w.send(
            Some(0),
            wire::media("MZ", &[6; 160]),
            Receipt::Media(vec![6; 160]),
        )
        .await
    });
    let w = writer.clone();
    let clear =
        tokio::spawn(async move { w.clear(wire::clear_frame(Mode::Twilio, "MZ", "a")).await });
    while writer.epoch() == 0 {
        tokio::task::yield_now().await;
    }
    release.add_permits(1);
    first.await.unwrap().unwrap();
    clear.await.unwrap().unwrap();
    assert_eq!(
        queued.await.unwrap().unwrap_err().code,
        "playback_cancelled"
    );
    let frames = frames.lock().await;
    assert_eq!(frames.len(), 2);
    assert!(matches!(&frames[1],Frame::Json(v)if v["event"]=="clear"));
    drop(frames);
    let snapshot = audio.call(Command::Snapshot).await.unwrap();
    assert_eq!(snapshot["aec"]["fifo"], 0);
    audio.close().await.unwrap();
}
struct Fail;
impl Sink for Fail {
    fn send(&mut self, _: Frame) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async { Err(Error::new("test_send_failure")) })
    }
}
#[tokio::test]
async fn failed_send_does_not_register_mark() {
    let state = Arc::new(Mutex::new(Engine::new(
        Mode::Twilio,
        Default::default(),
        Default::default(),
        Default::default(),
        0.0,
    )));
    let (writer, _owner) = Writer::start(Box::new(Fail), None, state.clone(), Clock::default());
    assert!(
        writer
            .send(
                Some(0),
                Frame::Json(json!({})),
                Receipt::Mark {
                    name: "end".into(),
                    snippet: "x".into()
                }
            )
            .await
            .is_err()
    );
    assert!(state.lock().await.playback.pending_marks.is_empty());
}

#[test]
fn eager_resume_final_routes_without_early_commit() {
    let mut engine = Engine::new(
        Mode::Twilio,
        Default::default(),
        Default::default(),
        Default::default(),
        10.0,
    );
    engine.transcript(
        Transcript::Eager {
            text: "partial".into(),
            confidence: 0.9,
            elapsed_ms: 100.,
            low_conf_words: vec![],
        },
        Some(10.1),
        10.5,
    );
    assert_eq!(engine.events.len(), 1);
    assert_eq!(engine.events[0]["kind"], "utterance_eager");
    engine.transcript(Transcript::Resumed, None, 10.6);
    assert_eq!(engine.events[1]["kind"], "utterance_resumed");
    engine.transcript(
        Transcript::Final {
            text: "partial continued".into(),
            confidence: 0.9,
            elapsed_ms: 500.,
            low_conf_words: vec![],
        },
        Some(10.1),
        11.,
    );
    assert_eq!(engine.events[2]["kind"], "utterance_final");
    assert_eq!(engine.events[2]["onset_at"], 10.1);
    assert_eq!(engine.events[2]["at"], 10.5);
}

#[test]
fn ambiguous_final_waits_bounded_time_and_late_judge_cannot_cut() {
    let mut engine = Engine::new(
        Mode::Twilio,
        Default::default(),
        Default::default(),
        vogon_barge_in::floor::Transport {
            semantic_judge: true,
            ..Default::default()
        },
        10.,
    );
    engine.begin(
        "a".into(),
        "Please tell me something about your account today.".into(),
        false,
        false,
        10.,
    );
    engine.transcript(
        Transcript::Final {
            text: "next Tuesday".into(),
            confidence: 1.,
            elapsed_ms: 100.,
            low_conf_words: vec![],
        },
        Some(10.1),
        10.2,
    );
    assert!(engine.events.is_empty());
    let token = engine
        .actions
        .iter()
        .find_map(|a| {
            if let vogon_transport::engine::Action::Classifier(r) = a {
                Some(r.token)
            } else {
                None
            }
        })
        .unwrap();
    engine.tick(10.5);
    assert!(engine.events.is_empty());
    engine.tick(11.0);
    assert_eq!(
        engine
            .events
            .iter()
            .filter(|e| e["kind"] == "utterance_absorbed")
            .count(),
        1
    );
    let f = engine.floor.as_mut().unwrap();
    let generation = f.generation;
    f.classifier_result(
        token,
        vogon_barge_in::floor::ClassifierResult::Verdict("substantive_interrupt".into()),
    );
    assert_eq!(f.generation, generation);
}

#[tokio::test]
async fn dropping_response_future_clears_stalled_provider() {
    let (sl, su) = listen().await;
    let (tl, tu) = listen().await;
    let st = tokio::spawn(stt_server(sl, false));
    let tt = tokio::spawn(tts_server(tl, true));
    let frames = Arc::new(Mutex::new(vec![]));
    let (mut call, _) = Session::connect(
        Config {
            mode: Mode::Browser,
            ..Default::default()
        },
        &settings(),
        Box::new(Capture(frames.clone())),
        Clock::default(),
        &su,
        &tu,
    )
    .await
    .unwrap();
    let outcome = timeout(
        Duration::from_millis(50),
        call.send_response(Response {
            text: "stalled".into(),
            audio_id: "aborted".into(),
            ..Default::default()
        }),
    )
    .await;
    assert!(outcome.is_err());
    timeout(Duration::from_secs(2), async {
        loop {
            if call.snapshot().await["playing"] == false {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(call.snapshot().await["clocks"]["playing_depth"], 0);
    call.close().await.unwrap();
    st.await.unwrap();
    tt.await.unwrap();
}

#[tokio::test]
async fn output_reset_retains_outstanding_aec_reference() {
    let audio = Audio::start(vogon_audio_runtime::Config {
        encoding: "linear16".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let input = vec![33; 962];
    let a = audio
        .call(Command::Tts {
            data: input.clone(),
        })
        .await
        .unwrap();
    audio
        .call(Command::Reference {
            data: vec![255; 160],
        })
        .await
        .unwrap();
    let before = audio.call(Command::Snapshot).await.unwrap();
    audio.call(Command::ResetOutput).await.unwrap();
    let after = audio.call(Command::Snapshot).await.unwrap();
    assert_eq!(before["aec"], after["aec"]);
    assert_eq!(after["pending_bytes"], 0);
    assert_eq!(audio.call(Command::Tts { data: input }).await.unwrap(), a);
    audio.close().await.unwrap();
}

#[tokio::test]
async fn phone_retry_reconnects_and_keeps_both_attempts_in_watchdog() {
    let (sl, su) = listen().await;
    let (tl, tu) = listen().await;
    let st = tokio::spawn(stt_server(sl, false));
    let retry_text = Arc::new(Mutex::new(String::new()));
    let captured = retry_text.clone();
    let tt = tokio::spawn(async move {
        let mut first = accept_async(tl.accept().await.unwrap().0).await.unwrap();
        for _ in 0..4 {
            first.next().await.unwrap().unwrap();
        }
        first
            .send(Message::Binary(vec![255; 160].into()))
            .await
            .unwrap();
        first
            .send(Message::Text(json!({"type":"Flushed"}).to_string().into()))
            .await
            .unwrap();
        first
            .send(Message::Binary(vec![255; 160].into()))
            .await
            .unwrap();
        first.close(None).await.unwrap();
        drop(first);
        let mut next = accept_async(tl.accept().await.unwrap().0).await.unwrap();
        while let Some(Ok(Message::Text(text))) = next.next().await {
            let value: Value = serde_json::from_str(&text).unwrap();
            match value["type"].as_str() {
                Some("Speak") => *captured.lock().await = value["text"].as_str().unwrap().into(),
                Some("Flush") => {
                    next.send(Message::Binary(vec![255; 480].into()))
                        .await
                        .unwrap();
                    next.send(Message::Text(json!({"type":"Flushed"}).to_string().into()))
                        .await
                        .unwrap();
                }
                Some("Clear") => {
                    next.send(Message::Text(json!({"type":"Cleared"}).to_string().into()))
                        .await
                        .unwrap();
                }
                _ => {}
            }
        }
    });
    let frames = Arc::new(Mutex::new(vec![]));
    let (mut call, _) = Session::connect(
        Config::default(),
        &settings(),
        Box::new(Capture(frames.clone())),
        Clock::default(),
        &su,
        &tu,
    )
    .await
    .unwrap();
    call.text(json!({"event":"start","streamSid":"MZ"}))
        .await
        .unwrap();
    let report=timeout(Duration::from_secs(3),call.send_response(Response{text:"This is the fully completed first sentence. This is the second sentence that needs another attempt.".into(),audio_id:"retry".into(),..Default::default()})).await.unwrap().unwrap().unwrap();
    assert_eq!(report.status, "complete");
    assert_eq!(report.chunks_sent, 3);
    assert_eq!(report.audio_ms, Some(60.));
    assert_eq!(call.snapshot().await["clocks"]["audio_ms"], 100.);
    let text = retry_text.lock().await;
    assert!(text.starts_with("Sorry about that."));
    assert!(!text.contains("fully completed first sentence"));
    assert!(text.contains("second sentence"));
    drop(text);
    assert!(
        !frames
            .lock()
            .await
            .iter()
            .any(|f| matches!(f,Frame::Json(v)if v["event"]=="clear"))
    );
    call.close().await.unwrap();
    st.await.unwrap();
    tt.await.unwrap();
}

#[test]
fn validated_snapshot_preserves_provider_and_transport_call_options() {
    let values = vogon_configuration::settings(
        "settings",
        &json!({"deepgram_api_key":"local-test-key"}),
        &json!({}),
        &json!({}),
    )
    .unwrap();
    let snapshot=vogon_configuration::resolve_call_snapshot(&json!({}),&json!({"persona_voice":"aura-2-luna-en","name_hint":"Joe Biden","silence_thresholds":[{"id":"warn","after_sec":8}],"barge_policy":{"semantic_judge":false,"safety_phrases":["stop"],"floor_request_phrases":["my turn"]},"ambience":{"enabled":false}})).unwrap();
    let cfg = Config::from_snapshot(Mode::Twilio, &values, &snapshot).unwrap();
    assert_eq!(cfg.lead_ms, values["tts_buffer_lead_ms"].as_f64().unwrap());
    assert!(!cfg.policy.semantic_judge);
    assert_eq!(cfg.policy.safety_phrases, ["stop"]);
    assert_eq!(cfg.clocks.silence_thresholds[0].id, "warn");
    let speech = SpeechSettings::from_settings(&values)
        .unwrap()
        .for_call(&snapshot["call_config"]);
    assert_eq!(speech.phone.keyterms, ["Joe", "Biden"]);
    assert_eq!(speech.phone_tts.model, "aura-2-luna-en");
    assert_eq!(
        speech.phone.eager_eot_threshold,
        values["deepgram_phone_eager_eot_threshold"].as_f64()
    );
    let mut legacy = values;
    legacy["stt_feed_mode"] = json!("gated");
    assert!(Config::from_snapshot(Mode::Twilio, &legacy, &snapshot).is_err());
}

// Shared loopback-only HTTP fixture; no production provider credentials are loaded.
#[path = "../../front/tests/support/mod.rs"]
mod decision_mock;

async fn scripted_stt(
    listener: TcpListener,
    mut send: tokio::sync::mpsc::UnboundedReceiver<Value>,
) {
    let mut socket = accept_async(listener.accept().await.unwrap().0)
        .await
        .unwrap();
    loop {
        tokio::select! {
            message=socket.next()=>match message {Some(Ok(Message::Text(text))) if text.contains("CloseStream")=>{let _=socket.close(None).await;break;},None|Some(Err(_))=>break,_=>{}},
            Some(value)=send.recv()=>{if socket.send(Message::Text(value.to_string().into())).await.is_err(){break;}}
        }
    }
}
async fn receive_kind(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Value>,
    kind: &str,
) -> Value {
    timeout(Duration::from_secs(3), async {
        loop {
            let event = events.recv().await.unwrap();
            if event["kind"] == kind {
                return event;
            }
        }
    })
    .await
    .unwrap()
}
fn final_message(text: &str) -> Value {
    json!({"type":"TurnInfo","event":"EndOfTurn","transcript":text,"words":[{"word":text,"confidence":1.,"start":0.0}]})
}

/// No new transcript or judge completion wakes the engine: heartbeat must expire it.
#[tokio::test]
async fn heartbeat_expires_pending_final_and_late_classifier_cannot_clear_audio() {
    let (sl, su) = listen().await;
    let (tl, tu) = listen().await;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let st = tokio::spawn(scripted_stt(sl, rx));
    let tt = tokio::spawn(tts_server(tl, false));
    let frames = Arc::new(Mutex::new(vec![]));
    let (mut call, mut events) = Session::connect(
        Config {
            policy: vogon_barge_in::floor::Transport {
                semantic_judge: true,
                ..Default::default()
            },
            ..Default::default()
        },
        &settings(),
        Box::new(Capture(frames.clone())),
        Clock::default(),
        &su,
        &tu,
    )
    .await
    .unwrap();
    call.text(json!({"event":"start","streamSid":"MZdeadline"}))
        .await
        .unwrap();
    call.send_response(Response {
        text: "Please tell me something about your account.".into(),
        audio_id: "opening".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    tx.send(final_message("next Tuesday")).unwrap();
    let request = receive_kind(&mut events, "classifier_request").await;
    let token = request["request"]["token"].as_u64().unwrap();
    let absorbed = receive_kind(&mut events, "utterance_absorbed").await;
    assert_eq!(absorbed["text"], "next Tuesday");
    let generation = call.playback_generation().await;
    call.classifier_result(
        token,
        vogon_barge_in::floor::ClassifierResult::Verdict("substantive_interrupt".into()),
    )
    .await
    .unwrap();
    assert_eq!(call.playback_generation().await, generation);
    assert!(
        !frames
            .lock()
            .await
            .iter()
            .any(|f| matches!(f, Frame::Json(v) if v["event"] == "clear"))
    );
    while let Ok(event) = events.try_recv() {
        assert_ne!(event["kind"], "utterance_final");
        assert_ne!(
            event["kind"], "utterance_absorbed",
            "deadline published twice"
        );
    }
    call.close().await.unwrap();
    st.await.unwrap();
    tt.await.unwrap();
}

#[tokio::test]
async fn configured_judge_completes_floor_dispatch_and_clears_before_final() {
    let http = decision_mock::Server::start(vec![decision_mock::Reply::json(
        json!({"choices":[{"message":{"content":"{\"decision\":\"YIELD\"}"}}]}),
    )])
    .await;
    let (sl, su) = listen().await;
    let (tl, tu) = listen().await;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let st = tokio::spawn(scripted_stt(sl, rx));
    let tt = tokio::spawn(tts_server(tl, false));
    let frames = Arc::new(Mutex::new(vec![]));
    let cfg = Config {
        judge: Some(http.judge()),
        policy: vogon_barge_in::floor::Transport {
            semantic_judge: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let (mut call, mut events) = Session::connect(
        cfg,
        &settings(),
        Box::new(Capture(frames.clone())),
        Clock::default(),
        &su,
        &tu,
    )
    .await
    .unwrap();
    call.text(json!({"event":"start","streamSid":"MZjudge"}))
        .await
        .unwrap();
    call.send_response(Response {
        text: "Please tell me something about your account.".into(),
        audio_id: "opening".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    tx.send(final_message("next Tuesday")).unwrap();
    let final_event = receive_kind(&mut events, "utterance_final").await;
    assert_eq!(final_event["text"], "next Tuesday");
    assert!(
        frames
            .lock()
            .await
            .iter()
            .any(|f| matches!(f,Frame::Json(v) if v["event"]=="clear"))
    );
    assert_eq!(http.requests.lock().await.len(), 1);
    call.close().await.unwrap();
    st.await.unwrap();
    tt.await.unwrap();
}
#[tokio::test]
async fn machine_line_breaks_protected_opening_and_replays_final_with_new_generation() {
    let http =
        decision_mock::Server::start(vec![decision_mock::Reply::json(decision_mock::answer())])
            .await;
    let (sl, su) = listen().await;
    let (tl, tu) = listen().await;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let st = tokio::spawn(scripted_stt(sl, rx));
    let tt = tokio::spawn(tts_server(tl, false));
    let frames = Arc::new(Mutex::new(vec![]));
    let cfg = Config {
        line: Some(vogon_front::line::Config {
            enabled: true,
            quiet_ms: 10.,
            ..Default::default()
        }),
        line_client: Some(http.jev()),
        ..Default::default()
    };
    let (mut call, mut events) = Session::connect(
        cfg,
        &settings(),
        Box::new(Capture(frames.clone())),
        Clock::default(),
        &su,
        &tu,
    )
    .await
    .unwrap();
    call.text(json!({"event":"start","streamSid":"MZline"}))
        .await
        .unwrap();
    call.send_response(Response {
        text: "Hello this is the account desk speaking.".into(),
        audio_id: "opening".into(),
        protected: true,
        ..Default::default()
    })
    .await
    .unwrap();
    tx.send(final_message("please leave your message after the tone"))
        .unwrap();
    let verdict = receive_kind(&mut events, "line_verdict").await;
    assert_eq!(verdict["answered_by"], "machine");
    assert!(
        frames
            .lock()
            .await
            .iter()
            .any(|f| matches!(f,Frame::Json(v) if v["event"]=="clear"))
    );
    let replay = receive_kind(&mut events, "utterance_final").await;
    assert_eq!(replay["text"], "please leave your message after the tone");
    assert_eq!(replay["playback_generation"], 1);
    assert_eq!(
        replay["heard"],
        json!({"completed":[],"in_flight":null,"in_flight_heard_text":"","last_heard_text":"","unheard_text":""})
    );
    assert!(replay.get("carry_forward").unwrap().is_null());
    assert!(replay["seq"].as_u64() > verdict["seq"].as_u64());
    assert_eq!(call.wait_line_quiet().await, "quiet");
    call.release_line_hold().await;
    assert_eq!(call.wait_line_quiet().await, "released");
    call.close().await.unwrap();
    st.await.unwrap();
    tt.await.unwrap();
}
#[tokio::test]
async fn line_without_instrument_reports_unknown_and_close_cancels_pending_http() {
    for instrument in [false, true] {
        let http = decision_mock::Server::start(vec![decision_mock::Reply {
            delay: 10_000,
            ..decision_mock::Reply::json(decision_mock::answer())
        }])
        .await;
        let (sl, su) = listen().await;
        let (tl, tu) = listen().await;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let st = tokio::spawn(scripted_stt(sl, rx));
        let tt = tokio::spawn(tts_server(tl, false));
        let traces = Arc::new(std::sync::Mutex::new(vec![]));
        let sink = traces.clone();
        let mut client = http.jev();
        client.tracer = Some(Arc::new(move |t| sink.lock().unwrap().push(t)));
        let cfg = Config {
            line: Some(vogon_front::line::Config {
                enabled: true,
                ..Default::default()
            }),
            line_client: instrument.then_some(client),
            ..Default::default()
        };
        let (mut call, mut events) = Session::connect(
            cfg,
            &settings(),
            Box::new(Capture(Arc::new(Mutex::new(vec![])))),
            Clock::default(),
            &su,
            &tu,
        )
        .await
        .unwrap();
        tx.send(final_message("please leave your message after the tone"))
            .unwrap();
        if instrument {
            http.received(1).await;
        } else {
            let verdict = receive_kind(&mut events, "line_verdict").await;
            assert_eq!(verdict["answered_by"], "unknown");
            assert_eq!(verdict["source"], "none");
        }
        call.close().await.unwrap();
        st.await.unwrap();
        tt.await.unwrap();
        if instrument {
            assert_eq!(traces.lock().unwrap().len(), 1);
            assert_eq!(
                traces.lock().unwrap()[0].error.as_ref().unwrap().code,
                "decision_cancelled"
            );
            while let Ok(event) = events.try_recv() {
                assert_ne!(event["kind"], "line_verdict");
            }
        }
    }
}
#[test]
fn line_machine_retains_original_episode_after_history_eviction() {
    let mut engine = Engine::new(
        Mode::Twilio,
        Default::default(),
        Default::default(),
        Default::default(),
        10.,
    );
    engine.line = Some(vogon_front::line::Detector::new(
        vogon_front::line::Config {
            enabled: true,
            ..Default::default()
        },
        String::new(),
    ));
    engine.begin(
        "a".into(),
        "Hello this is the account desk speaking.".into(),
        true,
        false,
        10.,
    );
    engine.transcript(
        Transcript::Final {
            text: "please leave your message after the tone".into(),
            confidence: 1.,
            elapsed_ms: 0.,
            low_conf_words: vec![],
        },
        Some(10.1),
        10.2,
    );
    let token = engine
        .actions
        .iter()
        .find_map(|a| match a {
            vogon_transport::engine::Action::Line(r) => Some(r.token),
            _ => None,
        })
        .unwrap();
    let original = engine.line.as_ref().unwrap().episode.as_ref().unwrap().id;
    // A caller turn can span more acoustic episodes than the visible history budget.
    for i in 0..60 {
        engine.frame(11. + i as f64, 1000, 1000, 100, 20., false);
        engine.frame(11.6 + i as f64, 0, 0, 100, 20., false);
    }
    assert!(
        engine
            .floor
            .as_ref()
            .unwrap()
            .get_episode(original)
            .is_none()
    );
    engine.line_result(token, Some(0.9), 80.);
    assert!(engine.floor.as_ref().unwrap().caller_floor);
    assert_eq!(
        engine.line.as_ref().unwrap().episode.as_ref().unwrap().id,
        original
    );
    assert_eq!(
        engine
            .events
            .iter()
            .filter(|v| v["kind"] == "line_verdict")
            .count(),
        1
    );
    assert!(
        engine
            .events
            .iter()
            .any(|v| v["kind"] == "utterance_final" && v["playback_generation"] == 1)
    );
}

#[test]
fn delayed_final_line_observation_keeps_the_original_episode() {
    let mut e = Engine::new(
        Mode::Twilio,
        Default::default(),
        Default::default(),
        vogon_barge_in::floor::Transport {
            semantic_judge: true,
            ..Default::default()
        },
        10.,
    );
    e.line = Some(vogon_front::line::Detector::new(
        vogon_front::line::Config {
            enabled: true,
            min_words: 2,
            ..Default::default()
        },
        String::new(),
    ));
    e.begin(
        "opening".into(),
        "Please tell me something about your account.".into(),
        false,
        false,
        10.,
    );
    e.transcript(
        Transcript::Final {
            text: "next Tuesday".into(),
            confidence: 1.,
            elapsed_ms: 100.,
            low_conf_words: vec![],
        },
        Some(10.1),
        10.2,
    );
    let original = e.floor.as_ref().unwrap().current.unwrap();
    assert!(!e.line.as_ref().unwrap().bound);
    e.frame(10.8, 0, 0, 100, 20., false);
    e.frame(10.85, 1000, 1000, 100, 20., false);
    assert_ne!(e.floor.as_ref().unwrap().current, Some(original));
    e.tick(11.);
    let bound = e.line.as_ref().unwrap().episode.as_ref().unwrap();
    assert_eq!(bound.id, original);
    assert_eq!(bound.text, "next Tuesday");
}
#[tokio::test]
async fn zero_audio_completion_starts_line_retirement_window() {
    let (sl, su) = listen().await;
    let (tl, tu) = listen().await;
    let st = tokio::spawn(stt_server(sl, false));
    let tt = tokio::spawn(async move {
        let mut socket = accept_async(tl.accept().await.unwrap().0).await.unwrap();
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let value: Value = serde_json::from_str(&text).unwrap();
            if value["type"] == "Flush" {
                socket
                    .send(Message::Text(json!({"type":"Flushed"}).to_string().into()))
                    .await
                    .unwrap();
            } else if value["type"] == "Close" {
                break;
            }
        }
    });
    let cfg = Config {
        line: Some(vogon_front::line::Config {
            enabled: true,
            ask_window_s: 0.05,
            ..Default::default()
        }),
        ..Default::default()
    };
    let (mut call, mut events) = Session::connect(
        cfg,
        &settings(),
        Box::new(Capture(Arc::new(Mutex::new(vec![])))),
        Clock::default(),
        &su,
        &tu,
    )
    .await
    .unwrap();
    call.text(json!({"event":"start","streamSid":"MZempty"}))
        .await
        .unwrap();
    let report = call
        .send_response(Response {
            text: "Opening without audio.".into(),
            audio_id: "opening".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.chunks_sent, 0);
    let verdict = receive_kind(&mut events, "line_verdict").await;
    assert_eq!(verdict["answered_by"], "unknown");
    assert_eq!(verdict["source"], "none");
    call.close().await.unwrap();
    st.await.unwrap();
    tt.await.unwrap();
}

/// Neither handshake may complete until both TCP peers have connected. A sequential
/// implementation deadlocks here, independently of machine speed or network latency.
#[tokio::test]
async fn speech_handshakes_overlap_and_failure_drops_the_other_socket() {
    for fail in [None, Some(0), Some(1)] {
        let (sl, su) = listen().await;
        let (tl, tu) = listen().await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let mut peers = Vec::new();
        for (index, listener) in [sl, tl].into_iter().enumerate() {
            let barrier = barrier.clone();
            peers.push(tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                barrier.wait().await;
                if fail == Some(index) {
                    // Let the other provider's actor start before rejecting this handshake.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    return;
                }
                let mut socket = accept_async(socket).await.unwrap();
                while let Some(message) = socket.next().await {
                    match message {
                        Ok(Message::Close(_)) | Err(_) => break,
                        Ok(Message::Text(text)) if text.contains("CloseStream") => {
                            let _ = socket.close(None).await;
                            break;
                        }
                        _ => {}
                    }
                }
            }));
        }
        let result = timeout(
            Duration::from_secs(3),
            Session::connect(
                Config {
                    mode: Mode::Browser,
                    ..Default::default()
                },
                &settings(),
                Box::new(Capture(Arc::new(Mutex::new(vec![])))),
                Clock::default(),
                &su,
                &tu,
            ),
        )
        .await
        .expect("both handshakes must progress concurrently");
        match result {
            Ok((mut session, _)) => {
                assert!(fail.is_none());
                session.close().await.unwrap();
            }
            Err(_) => assert!(fail.is_some()),
        }
        for peer in peers {
            timeout(Duration::from_secs(3), peer)
                .await
                .expect("peer socket leaked")
                .unwrap();
        }
    }
}
