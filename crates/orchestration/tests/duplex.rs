//! Whole Rust stack: HTTP Turn API + transport + streaming STT/TTS over local sockets.
//! Provider/model answers are controlled fixtures; no external model or microphone claim.
#[path = "support/network.rs"]
mod network;
mod support;
use network::*;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use vogon_clocks::Clock;
use vogon_contracts::StartCallRequest;
use vogon_orchestration::{
    client::{Config as HttpConfig, HttpTurnClient},
    driver::{Config, Driver, Report},
    port::{Input, NoHooks, SessionFactory},
};
use vogon_speech_providers::config::SpeechSettings;
use vogon_transport::{engine::Mode, wire::Frame};

struct Call {
    input: mpsc::Sender<Input>,
    stt: Stt,
    tts: Tts,
    frames: Arc<Mutex<Vec<Frame>>>,
    task: tokio::task::JoinHandle<Report>,
}
async fn launch(url: &str, id: &str, mode: Mode) -> Call {
    let stt = Stt::start().await;
    let tts = Tts::start().await;
    let settings = vogon_configuration::settings(
        "settings",
        &json!({"deepgram_api_key":"local-fixture","ambience_enabled":false}),
        &json!({}),
        &json!({}),
    )
    .unwrap();
    let profile = vogon_configuration::built_in(&settings).unwrap();
    let speech = SpeechSettings::from_settings(&settings).unwrap();
    let frames = Arc::new(Mutex::new(vec![]));
    let factory = SessionFactory {
        settings,
        profile,
        mode,
        speech: Some(speech),
        clients: Default::default(),
        artifact: None,
        lender: String::new(),
        seed: 1,
        sink: tokio::sync::Mutex::new(Some(Box::new(Capture(frames.clone())))),
        initial_frame: (mode == Mode::Twilio)
            .then(|| json!({"event":"start","streamSid":format!("MZ{id}")})),
        stt_endpoint: stt.url.clone(),
        tts_endpoint: tts.url.clone(),
    };
    let api = HttpTurnClient::new(HttpConfig {
        base_url: url.into(),
        api_key: String::new(),
        timeout: Duration::from_secs(10),
        retry_delays: vec![],
        epoch: 0.,
    })
    .unwrap();
    let config = Config::new(StartCallRequest {
        call_id: id.into(),
        mode: if mode == Mode::Twilio {
            "twilio"
        } else {
            "widget"
        }
        .into(),
        resume: false,
        payload: json!({}),
    });
    let (input, rx) = mpsc::channel(256);
    let task = tokio::spawn(
        Driver {
            config,
            api: Arc::new(api),
            factory: Arc::new(factory),
            hooks: Arc::new(NoHooks),
            clock: Clock::default(),
        }
        .run(rx),
    );
    Call {
        input,
        stt,
        tts,
        frames,
        task,
    }
}
async fn request(http: &mut Http, operation: &str) -> Request {
    let r = support::recv(&mut http.requests).await;
    assert_eq!(r.operation, operation);
    r
}
fn start(r: Request) {
    r.reply.send(json!({"call_config":{"persona_voice":"aura-2-thalia-en","ambience":{"enabled":false},"barge_policy":{"semantic_judge":false}}})).unwrap();
}
fn trigger(r: &Request) -> &Value {
    r.body["events"].as_array().unwrap().last().unwrap()
}
fn answer(r: Request, text: &str) {
    r.reply
        .send(json!({"speak":[{"audio_id":text,"text":text}]}))
        .unwrap();
}
async fn flush(tts: &mut Tts) {
    loop {
        if support::recv(&mut tts.commands).await == "Flush" {
            break;
        }
    }
}
async fn close(http: &mut Http, call: Call) -> Report {
    call.input.send(Input::Disconnect).await.unwrap();
    let end = request(http, "end_call").await;
    end.reply.send(json!({})).unwrap();
    let report = tokio::time::timeout(Duration::from_secs(5), call.task)
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{report:?}");
    assert!(report.end_acknowledged);
    tokio::time::timeout(Duration::from_secs(3), call.stt.task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), call.tts.task)
        .await
        .unwrap()
        .unwrap();
    report
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn eight_calls_speculate_in_parallel_and_one_stalled_call_cannot_block_others() {
    let mut http = Http::start().await;
    let mut calls = vec![];
    for n in 0..8 {
        calls.push(launch(&http.url, &format!("c{n}"), Mode::Browser).await);
    }
    for _ in 0..8 {
        start(request(&mut http, "start_call").await);
    }
    for (n, c) in calls.iter_mut().enumerate() {
        c.input
            .send(Input::Binary(vec![n as u8; 640]))
            .await
            .unwrap();
        assert_eq!(support::recv(&mut c.stt.audio).await, vec![n as u8; 640]);
        c.stt.turn("EagerEndOfTurn", &format!("question {n}"));
    }
    // All eight requests must arrive BEFORE any replies: overlap is structural, not a stopwatch.
    let mut eager = BTreeMap::new();
    for _ in 0..8 {
        let r = request(&mut http, "process_events").await;
        assert_eq!(trigger(&r)["kind"], "utterance_eager");
        assert_eq!(
            r.body["request_id"],
            format!("{}-1", r.body["call_id"].as_str().unwrap())
        );
        assert_eq!(trigger(&r)["seq"], 1);
        eager.insert(r.body["call_id"].as_str().unwrap().to_owned(), r);
    }
    assert_eq!(eager.len(), 8);
    let blocked = eager.remove("c0").unwrap();
    for (_, r) in eager {
        r.reply.send(json!({})).unwrap();
    }
    for (n, c) in calls.iter().enumerate().skip(1) {
        c.stt.turn("EndOfTurn", &format!("question {n}"));
    }
    let mut committed = BTreeSet::new();
    for _ in 1..8 {
        let r = request(&mut http, "process_events").await;
        assert_eq!(trigger(&r)["kind"], "utterance_final");
        let id = r.body["call_id"].as_str().unwrap().to_owned();
        assert_eq!(r.body["request_id"], format!("{id}-2"));
        assert_eq!(trigger(&r)["seq"], 2);
        assert!(committed.insert(id.clone()));
        answer(r, &format!("answer {id}"));
    }
    for (n, c) in calls.iter_mut().enumerate().skip(1) {
        assert_eq!(
            support::recv(&mut c.tts.spoken).await,
            format!("answer c{n}")
        );
        flush(&mut c.tts).await;
        // Continue receiving caller audio while TTS has not produced its first chunk.
        c.input
            .send(Input::Binary(vec![100 + n as u8; 640]))
            .await
            .unwrap();
        assert_eq!(
            support::recv(&mut c.stt.audio).await,
            vec![100 + n as u8; 640]
        );
        c.tts.finish(n as u8);
    }
    // Changed final queued behind blocked eager invalidates an anomalous late eager answer.
    calls[0].stt.turn("EndOfTurn", "question 0 with more words");
    answer(blocked, "STALE speculative speech");
    let final0 = request(&mut http, "process_events").await;
    assert_eq!(final0.body["request_id"], "c0-2");
    assert_eq!(trigger(&final0)["text"], "question 0 with more words");
    answer(final0, "answer c0");
    assert_eq!(support::recv(&mut calls[0].tts.spoken).await, "answer c0");
    flush(&mut calls[0].tts).await;
    calls[0].tts.finish(0);
    for c in &calls {
        support::until(|| {
            c.frames
                .lock()
                .unwrap()
                .iter()
                .any(|f| matches!(f,Frame::Json(v) if v["type"]=="server.tts_stream_end"))
        })
        .await;
    }
    for (n, c) in calls.into_iter().enumerate() {
        let sequences = c
            .frames
            .lock()
            .unwrap()
            .iter()
            .filter_map(|f| match f {
                Frame::Json(v) => v["seq"].as_u64(),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(sequences, (1..=sequences.len() as u64).collect::<Vec<_>>());
        let text = c
            .frames
            .lock()
            .unwrap()
            .iter()
            .filter_map(|f| {
                if let Frame::Json(v) = f {
                    (v["type"] == "server.response_text")
                        .then(|| v["payload"]["text"].as_str().unwrap().to_owned())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(text, [format!("answer c{n}")]);
        let report = close(&mut http, c).await;
        assert_eq!(report.requests, 2);
        assert_eq!(report.speculative_responses, 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn browser_barge_in_clears_blocked_tts_and_late_audio_cannot_leak_into_new_turn() {
    let mut http = Http::start().await;
    let mut c = launch(&http.url, "duplex", Mode::Browser).await;
    start(request(&mut http, "start_call").await);
    c.stt.turn("EndOfTurn", "first question");
    answer(request(&mut http, "process_events").await, "first answer");
    assert_eq!(support::recv(&mut c.tts.spoken).await, "first answer");
    flush(&mut c.tts).await;
    c.input.send(Input::Binary(vec![11; 640])).await.unwrap();
    assert_eq!(support::recv(&mut c.stt.audio).await, vec![11; 640]);
    c.stt.turn("Update", "please stop");
    c.stt.turn("TurnResumed", "");
    c.stt.turn("EndOfTurn", "different question");
    let resumed = request(&mut http, "process_events").await;
    assert_eq!(trigger(&resumed)["kind"], "utterance_resumed");
    assert!(
        resumed.body["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["kind"] == "delivery_outcome" && v["status"] == "truncated")
    );
    resumed.reply.send(json!({})).unwrap();
    let final_turn = request(&mut http, "process_events").await;
    assert_eq!(trigger(&final_turn)["text"], "different question");
    answer(final_turn, "second answer");
    assert_eq!(support::recv(&mut c.tts.spoken).await, "second answer");
    flush(&mut c.tts).await;
    c.tts.finish(42);
    support::until(|| {
        c.frames
            .lock()
            .unwrap()
            .iter()
            .any(|f| matches!(f, Frame::Binary(_)))
    })
    .await;
    let audio = c
        .frames
        .lock()
        .unwrap()
        .iter()
        .filter(|f| matches!(f, Frame::Binary(_)))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        audio,
        [vogon_transport::wire::browser_audio(
            "second answer",
            &[42; 640]
        )]
    );
    close(&mut http, c).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phone_aec_input_continues_during_tts_and_disconnect_flushes_delivery() {
    let mut http = Http::start().await;
    let mut c = launch(&http.url, "phone", Mode::Twilio).await;
    start(request(&mut http, "start_call").await);
    c.stt.turn("EagerEndOfTurn", "Can I pay");
    let eager = support::recv(&mut http.requests).await;
    if eager.operation == "end_call" {
        eager.reply.send(json!({})).unwrap();
        panic!("phone startup ended: {:?}", c.task.await.unwrap());
    }
    assert_eq!(eager.operation, "process_events");
    eager.reply.send(json!({})).unwrap();
    c.stt.turn("EndOfTurn", "Can I pay");
    answer(request(&mut http, "process_events").await, "Yes you can");
    assert_eq!(support::recv(&mut c.tts.spoken).await, "Yes you can");
    flush(&mut c.tts).await;
    for _ in 0..5 {
        c.input.send(Input::Binary(vec![255; 160])).await.unwrap();
    }
    for _ in 0..5 {
        assert_eq!(support::recv(&mut c.stt.audio).await.len(), 160);
    }
    // Disconnect with speech outstanding; no fake completion is reported.
    c.input.send(Input::Disconnect).await.unwrap();
    let end = request(&mut http, "end_call").await;
    assert!(
        end.body["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["kind"] == "delivery_outcome" && v["status"] == "truncated")
    );
    end.reply.send(json!({})).unwrap();
    let r = tokio::time::timeout(Duration::from_secs(5), c.task)
        .await
        .unwrap()
        .unwrap();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(r.input_frames, 5);
    assert_eq!(r.requests, 2);
    c.stt.task.await.unwrap();
    c.tts.task.await.unwrap();
}
