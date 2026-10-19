//! HTTP protocol, retry, parsing, trace and cancellation boundaries; no external APIs.
mod support;
use serde_json::json;
use std::sync::{Arc, Mutex};
use support::*;
use tokio::time::{Duration, timeout};
use vogon_front::{
    bootstrap::Clients,
    http::{Http, Key},
    jev,
};

#[tokio::test]
async fn jev_wire_and_typed_result() {
    let s = Server::start(vec![Reply::json(answer())]).await;
    let traces = Arc::new(Mutex::new(vec![]));
    let sink = traces.clone();
    let mut client = s.jev();
    client.tracer = Some(Arc::new(move |t| sink.lock().unwrap().push(t)));
    let result = client
        .decide(
            json!({"caller":"hello"}),
            questions(),
            Some(900),
            "line_detector",
        )
        .await
        .unwrap();
    assert_eq!(result.noul("recording").unwrap(), 0.8);
    assert_eq!(
        (result.input_tokens, result.output_tokens, result.cost_usd),
        (10, 2, 0.01)
    );
    assert_eq!(result.request_id, "id");
    let req = s.requests.lock().await;
    assert!(req[0].0.contains("Bearer fixture-secret"));
    assert_eq!(
        req[0].1,
        json!({"model":"fixture-jev","state":{"caller":"hello"},"questions":questions()})
    );
    let t = traces.lock().unwrap();
    assert_eq!(t.len(), 1);
    assert_eq!(t[0].attempts, 1);
    assert!(t[0].error.is_none());
    assert_eq!(t[0].tag, "line_detector");
}
#[test]
fn typed_answers_missing_wrong_kind_nonfinite_and_stable_ties() {
    let raw = json!({"answers":{"c":{"type":"choice","choice":"b","confidence":"0.7","probabilities":{"b":0.5,"a":0.5,"c":0.1}},"s":{"type":"score","score":true},"n":{"type":"noul","noul":"0.2"}}});
    let r = jev::DecisionResult::parse(&raw, 1.).unwrap();
    assert_eq!(r.choice("c").unwrap(), ("b", 0.7));
    assert_eq!(r.ranked("c").unwrap()[0].0, "b");
    assert_eq!(r.score("s").unwrap(), (1., 0.));
    assert!(r.noul("c").is_err());
    assert!(r.choice("missing").is_err());
    assert!(r.score("n").is_err());
    assert!(r.ranked("n").is_err());
    for val in [
        json!({"answers":[]}),
        json!({"answers":{"n":{"type":"noul","noul":"NaN"}}}),
        json!({"answers":{"n":{"type":"unknown"}}}),
    ] {
        assert!(jev::DecisionResult::parse(&val, 0.).is_err());
    }
    assert!(jev::choice("", &json!({"only":"one"})).is_err());
    assert!(jev::score("", &[]).is_err());
    assert!(jev::check(&json!({})).is_err());
    assert!(jev::check(&json!({"q":{"type":"text"}})).is_err());
}
#[tokio::test]
async fn every_transient_status_retries_and_every_other_status_stops() {
    for status in [
        408, 409, 425, 429, 500, 502, 503, 504, 400, 401, 403, 404, 422, 501, 302,
    ] {
        let mut reply = Reply::json(json!({"secret":"never surface"}));
        reply.status = status;
        let s = Server::start(vec![reply, Reply::json(answer())]).await;
        let result = s.jev().decide(json!({}), questions(), None, "").await;
        if [408, 409, 425, 429, 500, 502, 503, 504].contains(&status) {
            assert!(result.is_ok(), "{status}");
            assert_eq!(s.requests.lock().await.len(), 2);
        } else {
            let e = result.unwrap_err();
            assert_eq!(e.status, Some(status));
            assert!(!e.to_string().contains("secret"));
            assert_eq!(s.requests.lock().await.len(), 1);
        }
    }
}
#[tokio::test]
async fn retry_budget_is_five_attempts() {
    let mut reply = Reply::json(json!({}));
    reply.status = 503;
    let s = Server::start(vec![reply; 6]).await;
    let start = tokio::time::Instant::now();
    let e = s
        .jev()
        .decide(json!({}), questions(), None, "")
        .await
        .unwrap_err();
    assert_eq!(e.status, Some(503));
    assert_eq!(s.requests.lock().await.len(), 5);
    assert!(start.elapsed() >= Duration::from_millis(1875));
}
#[tokio::test]
async fn malformed_json_and_schema_are_not_retried() {
    for reply in [
        Reply {
            body: "broken secret".into(),
            ..Reply::json(json!({}))
        },
        Reply::json(json!({"answers":{"x":{"type":"bad"}}})),
    ] {
        let s = Server::start(vec![reply, Reply::json(answer())]).await;
        assert!(
            s.jev()
                .decide(json!({}), questions(), None, "")
                .await
                .is_err()
        );
        assert_eq!(s.requests.lock().await.len(), 1);
    }
}
#[tokio::test]
async fn timeout_covers_headers_and_body_and_is_retryable() {
    for body in [false, true] {
        let mut reply = Reply::json(answer());
        if body {
            reply.body_delay = 200;
        } else {
            reply.delay = 200;
        }
        let s = Server::start(vec![reply, Reply::json(answer())]).await;
        let r = s.jev().decide(json!({}), questions(), Some(40), "").await;
        assert!(r.is_ok(), "{r:?}");
        assert_eq!(s.requests.lock().await.len(), 2);
    }
}
#[tokio::test]
async fn response_size_is_bounded() {
    let s = Server::start(vec![Reply {
        body: "x".repeat(4 * 1024 * 1024 + 1),
        ..Reply::json(json!({}))
    }])
    .await;
    assert_eq!(
        s.http()
            .post(&json!({}), Duration::from_secs(2))
            .await
            .unwrap_err()
            .code,
        "provider_response_too_large"
    );
}
#[tokio::test]
async fn cancellation_during_request_and_backoff_emits_one_trace() {
    for retry in [false, true] {
        let mut reply = Reply::json(answer());
        if retry {
            reply.status = 429;
        } else {
            reply.delay = 10_000;
        }
        let s = Server::start(vec![reply, Reply::json(answer())]).await;
        let traces = Arc::new(Mutex::new(vec![]));
        let sink = traces.clone();
        let mut client = s.jev();
        client.tracer = Some(Arc::new(move |t| sink.lock().unwrap().push(t)));
        let task =
            tokio::spawn(
                async move { client.decide(json!({}), questions(), None, "cancel").await },
            );
        s.received(1).await;
        tokio::time::sleep(Duration::from_millis(15)).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::sleep(Duration::from_millis(140)).await;
        assert_eq!(s.requests.lock().await.len(), 1);
        let t = traces.lock().unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].error.as_ref().unwrap().code, "decision_cancelled");
    }
}
#[tokio::test]
async fn trace_failure_does_not_break_success() {
    let s = Server::start(vec![Reply::json(answer())]).await;
    let mut c = s.jev();
    c.tracer = Some(Arc::new(|_| panic!("fixture tracer")));
    assert!(c.decide(json!({}), questions(), None, "").await.is_ok());
}
#[tokio::test]
async fn cerebras_wire_effort_json_errors_and_no_retry() {
    let s = Server::start(vec![Reply::json(
        json!({"choices":[{"message":{"content":"{\"say\":\"Got it.\"}"}}]}),
    )])
    .await;
    let c = s.judge();
    assert_eq!(
        c.complete("system", "user", 1024, 0.3, Some("none"))
            .await
            .unwrap(),
        json!({"say":"Got it."})
    );
    let b = &s.requests.lock().await[0].1;
    assert_eq!(b["reasoning_effort"], "none");
    assert_eq!(b["model"], "fixture-judge");
    assert_eq!(b["temperature"], 0.3);
    assert_eq!(b["response_format"], json!({"type":"json_object"}));
    assert_eq!(
        b["messages"],
        json!([{"role":"system","content":"system"},{"role":"user","content":"user"}])
    );
    for effort in [None, Some(""), Some("default")] {
        assert!(
            c.request("", "", 10, 0., effort)
                .get("reasoning_effort")
                .is_none()
        );
    }
    for val in [
        json!({}),
        json!({"choices":[{"message":{"content":"not json"}}]}),
    ] {
        let s = Server::start(vec![Reply::json(val)]).await;
        assert!(s.judge().complete("", "", 1, 0., None).await.is_err());
    }
    let mut reply = Reply::json(json!({}));
    reply.status = 503;
    let s = Server::start(vec![reply, Reply::json(json!({}))]).await;
    assert!(s.judge().complete("", "", 1, 0., None).await.is_err());
    assert_eq!(s.requests.lock().await.len(), 1);
}
#[tokio::test]
async fn judge_decisions_and_invalid_content() {
    use vogon_barge_in::floor::{ClassifierRequest, ClassifierResult, Key};
    let request = ClassifierRequest {
        token: 7,
        key: Key(1, 2, 3, Some("a".into())),
        text: "stop".into(),
        heard: "heard".into(),
        unheard: "unheard".into(),
        agent_line: "line".into(),
        attempts: vec![json!({"attempt":1})],
        deadline: 2.,
    };
    for decision in ["CONTINUE", "YIELD", "UNCERTAIN", "invalid"] {
        let s = Server::start(vec![Reply::json(
            json!({"choices":[{"message":{"content":json!({"decision":decision}).to_string()}}]}),
        )])
        .await;
        match s.judge().classify(&request).await {
            ClassifierResult::Verdict(v) => assert_eq!(v, decision),
            ClassifierResult::Error => assert_eq!(decision, "invalid"),
            _ => panic!(),
        };
        let b = &s.requests.lock().await[0].1;
        assert_eq!(b["temperature"], 0.);
        let user: serde_json::Value =
            serde_json::from_str(b["messages"][1]["content"].as_str().unwrap()).unwrap();
        assert_eq!(
            user,
            json!({"text":"stop","heard":"heard","unheard":"unheard","agent_line":"line","attempts":[{"attempt":1}]})
        );
    }
}
#[test]
fn endpoint_security_and_settings_defaults() {
    let key = Key::new("secret".into()).unwrap();
    assert!(!format!("{key:?}").contains("secret"));
    assert!(Key::new(" ".into()).is_err());
    assert!(Http::new("http://example.com", key.clone()).is_err());
    assert!(Http::new("https://user:secret@example.com", key).is_err());
    let empty = Clients::from_settings(&json!({}), |_| None).unwrap();
    assert!(empty.jev.is_none() && empty.front.is_none() && empty.judge.is_none());
    // Invalid fallback must not override a configured key.
    let c=Clients::from_settings(&json!({"open_router_api_key":"configured","cerebras_api_key":"fixture","front_beat_after_ms":1,"semantic_barge_llm_timeout_ms":1}),|_|Some(" ".into())).unwrap();
    assert_eq!(c.jev.unwrap().model, "typesafe/jev-1.13");
    assert_eq!(c.front.unwrap().timeout_ms, 200);
    assert_eq!(c.judge.unwrap().timeout_ms, 50);
    assert!(
        Clients::from_settings(&json!({}), |key| (key == "OPENROUTER_API_KEY")
            .then(|| "fallback".into()))
        .unwrap()
        .jev
        .is_some()
    );
}
#[tokio::test]
async fn invalid_questions_never_reach_wire() {
    let s = Server::start(vec![]).await;
    assert!(
        timeout(
            Duration::from_millis(100),
            s.jev().decide(json!({}), json!({}), None, "")
        )
        .await
        .unwrap()
        .is_err()
    );
    assert!(s.requests.lock().await.is_empty());
}

#[tokio::test]
async fn source_http_generation_supersession_and_cancel_do_not_deliver_stale_speech() {
    use vogon_front::{
        config::{Artifact, Settings},
        source::{Source, State},
    };
    let artifact = Artifact::from_value(
        &json!({"prompt":"You are {agent_name}.","beats":{"hold":["One moment."]}}),
        12,
    )
    .unwrap();
    let first = Reply {
        delay: 300,
        ..Reply::json(json!({"choices":[{"message":{"content":"{\"say\":\"Old response.\"}"}}]}))
    };
    let second =
        Reply::json(json!({"choices":[{"message":{"content":"{\"say\":\"I am listening.\"}"}}]}));
    let server = Server::start(vec![first.clone(), second, first]).await;
    let state = State::new(
        artifact,
        Settings {
            engine: "llm".into(),
            min_gap_sec: 0.,
            ..Default::default()
        },
        "Agent",
        "",
        "Alex",
        1,
    );
    let src = Arc::new(Source::new(
        state,
        vogon_clocks::Clock::default(),
        None,
        Some(server.judge()),
    ));
    src.prime("old turn").await;
    server.received(1).await;
    let copy = src.clone();
    let waiting = tokio::spawn(async move { copy.hold_line(0.9, Some(1.5)).await });
    tokio::task::yield_now().await;
    src.prime("new turn").await;
    assert!(waiting.await.unwrap().0.is_empty());
    assert_eq!(src.hold_line(0.9, Some(1.5)).await.0, "I am listening.");
    src.prime("new turn").await;
    assert_eq!(server.requests.lock().await.len(), 2);
    src.prime("cancel turn").await;
    server.received(3).await;
    src.cancel().await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(src.snapshot().await["metrics"]["front_generated"], 1);
    assert!(
        src.snapshot().await["lines_played"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}
#[tokio::test]
async fn source_drop_cancels_http_and_emits_jev_trace() {
    use vogon_front::{
        config::{Artifact, Settings},
        source::{Source, State},
    };
    let server = Server::start(vec![Reply {
        delay: 10_000,
        ..Reply::json(answer())
    }])
    .await;
    let traces = Arc::new(Mutex::new(vec![]));
    let sink = traces.clone();
    let mut client = server.jev();
    client.tracer = Some(Arc::new(move |t| sink.lock().unwrap().push(t)));
    let artifact=Artifact::from_value(&json!({"prompt":"Agent","tiers":{"beat":["One moment."]},"move_criteria":{"ASSENT":"agree","REFUSAL":"decline"}}),12).unwrap();
    let src = Source::new(
        State::new(artifact, Settings::default(), "", "", "", 1),
        vogon_clocks::Clock::default(),
        Some(client),
        None,
    );
    src.prime("hello").await;
    server.received(1).await;
    drop(src);
    timeout(Duration::from_secs(1), async {
        while traces.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        traces.lock().unwrap()[0].error.as_ref().unwrap().code,
        "decision_cancelled"
    );
}
#[tokio::test]
async fn null_completion_content_is_empty_json() {
    let server = Server::start(vec![Reply::json(
        json!({"choices":[{"message":{"content":null}}]}),
    )])
    .await;
    assert_eq!(
        server.judge().complete("", "", 1, 0., None).await.unwrap(),
        json!({})
    );
}

#[tokio::test]
async fn connection_failure_has_bounded_retries_and_sanitized_trace() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let traces = Arc::new(Mutex::new(vec![]));
    let sink = traces.clone();
    let client = jev::Client {
        http: Http::new(&endpoint, Key::new("secret".into()).unwrap()).unwrap(),
        model: "fixture".into(),
        timeout_ms: 50,
        tracer: Some(Arc::new(move |t| sink.lock().unwrap().push(t))),
    };
    let err = client
        .decide(json!({}), questions(), None, "")
        .await
        .unwrap_err();
    assert_eq!(err.code, "provider_connect_failed");
    assert_eq!(traces.lock().unwrap()[0].attempts, 5);
    assert!(!err.to_string().contains(&endpoint));
}
