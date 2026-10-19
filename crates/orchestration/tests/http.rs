//! Real loopback HTTP: replay identity, request ordering, auth and timeout/failure boundaries.
#[path = "../../front/tests/support/mod.rs"]
mod server;
use serde_json::json;
use server::{Reply, Server};
use std::{sync::Arc, time::Duration};
use vogon_contracts::*;
use vogon_orchestration::client::{Config, HttpTurnClient, TurnApi};
fn client(s: &Server) -> HttpTurnClient {
    HttpTurnClient::new(Config {
        base_url: s.url.clone(),
        api_key: "fixture-secret".into(),
        timeout: Duration::from_secs(1),
        retry_delays: vec![Duration::from_millis(1); 2],
        epoch: 100.,
    })
    .unwrap()
}
fn req() -> ProcessEventsRequest {
    codec::from_wire("ProcessEventsRequest",&json!({"call_id":"call","request_id":"call-1","events":[{"kind":"utterance_final","text":"hello","confidence":1.,"low_conf_words":[],"at":102.,"onset_at":101.}]})).unwrap()
}
#[tokio::test]
async fn triad_urls_auth_epoch_and_started_state() {
    let s = Server::start(vec![
        Reply::json(json!({"call_config":{"persona_voice":"test"}})),
        Reply::json(json!({})),
        Reply::json(json!({})),
    ])
    .await;
    let c = client(&s);
    assert!(!c.started());
    c.start(StartCallRequest {
        call_id: "call".into(),
        mode: "twilio".into(),
        resume: false,
        payload: json!({"opaque":"unchanged"}),
    })
    .await
    .unwrap();
    assert!(c.started());
    c.process(req()).await.unwrap();
    c.end(EndCallRequest {
        call_id: "call".into(),
        reason: "hangup".into(),
        events: req().events,
        transport_analytics: json!([]),
        provider_call_id: "CA1".into(),
    })
    .await
    .unwrap();
    let requests = s.requests.lock().await;
    for (i, path) in ["start_call", "process_events", "end_call"]
        .iter()
        .enumerate()
    {
        assert!(requests[i].0.starts_with(&format!("POST /test/{path} ")));
        assert!(requests[i].0.contains("Bearer fixture-secret"));
    }
    assert_eq!(requests[1].1["events"][0]["at"], 2.);
    assert_eq!(requests[2].1["events"][0]["onset_at"], 1.);
}
#[tokio::test]
async fn all_5xx_retry_identical_body_but_4xx_do_not() {
    for status in [500, 501, 502, 503, 504, 599, 400, 401, 403, 408, 429] {
        let mut error = Reply::json(json!({"secret":"not surfaced"}));
        error.status = status;
        let s = Server::start(vec![error, Reply::json(json!({}))]).await;
        let r = client(&s).process(req()).await;
        let requests = s.requests.lock().await;
        if status >= 500 {
            assert!(r.is_ok());
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0].1, requests[1].1);
        } else {
            assert_eq!(r.unwrap_err().0, "turn_contract_http_error");
            assert_eq!(requests.len(), 1);
        }
    }
}
#[tokio::test]
async fn timeout_retry_budget_and_short_end_ladder() {
    for ending in [false, true] {
        let reply = Reply {
            delay: 100,
            ..Reply::json(json!({}))
        };
        let s = Server::start(vec![reply; 4]).await;
        let mut cfg = Config {
            base_url: s.url.clone(),
            api_key: String::new(),
            timeout: Duration::from_millis(15),
            retry_delays: vec![Duration::ZERO; 2],
            epoch: 0.,
        };
        if ending {
            cfg.retry_delays.clear();
        }
        let c = HttpTurnClient::new(cfg).unwrap();
        let e = if ending {
            c.end(EndCallRequest {
                call_id: "c".into(),
                reason: "end".into(),
                events: vec![],
                transport_analytics: json!([]),
                provider_call_id: String::new(),
            })
            .await
            .err()
            .unwrap()
        } else {
            c.process(req()).await.err().unwrap()
        };
        assert_eq!(e.0, "turn_service_unavailable");
        assert_eq!(s.requests.lock().await.len(), if ending { 2 } else { 3 });
    }
}
#[tokio::test]
async fn malformed_success_does_not_retry_and_failed_start_never_sets_started() {
    for reply in [
        Reply {
            body: "bad-json".into(),
            ..Reply::json(json!({}))
        },
        Reply::json(json!({})),
    ] {
        let s = Server::start(vec![
            reply,
            Reply::json(json!({"call_config":{"persona_voice":"x"}})),
        ])
        .await;
        let c = client(&s);
        assert!(
            c.start(StartCallRequest {
                call_id: "c".into(),
                mode: "text".into(),
                resume: false,
                payload: json!({})
            })
            .await
            .is_err()
        );
        assert!(!c.started());
        assert_eq!(s.requests.lock().await.len(), 1);
    }
}
#[tokio::test]
async fn same_call_http_is_serialized_while_independent_clients_can_overlap() {
    let s = Server::start(vec![
        Reply {
            delay: 100,
            ..Reply::json(json!({}))
        },
        Reply::json(json!({})),
    ])
    .await;
    let c = Arc::new(client(&s));
    let a = c.clone();
    let first = tokio::spawn(async move { a.process(req()).await });
    s.received(1).await;
    let b = c.clone();
    let second = tokio::spawn(async move { b.process(req()).await });
    tokio::task::yield_now().await;
    assert_eq!(s.requests.lock().await.len(), 1);
    assert!(!first.is_finished());
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert_eq!(s.requests.lock().await.len(), 2);
    let a = Server::start(vec![Reply {
        delay: 100,
        ..Reply::json(json!({}))
    }])
    .await;
    let b = Server::start(vec![Reply {
        delay: 100,
        ..Reply::json(json!({}))
    }])
    .await;
    let ac = client(&a);
    let bc = client(&b);
    let at = tokio::spawn(async move { ac.process(req()).await });
    let bt = tokio::spawn(async move { bc.process(req()).await });
    tokio::join!(a.received(1), b.received(1));
    assert!(!at.is_finished() && !bt.is_finished());
    at.await.unwrap().unwrap();
    bt.await.unwrap().unwrap();
}
#[test]
fn binding_precedence_full_root_and_empty_retry_schedule() {
    assert!(Config::resolve(&json!({}), &json!({}), 0.).is_err());
    let c=Config::resolve(&json!({"turn_service_url":"http://deployment/turn/v1","turn_service_api_key":"deployment","turn_retry_delays":""}),&json!({"turn_service":{"base_url":"http://override/turn/v1","auth_token":"call"}}),0.).unwrap();
    assert_eq!(c.base_url, "http://override/turn/v1");
    assert_eq!(c.api_key, "call");
    assert!(c.retry_delays.is_empty());
    assert_eq!(
        Config::resolve(
            &json!({"turn_service_url":"http://test","turn_retry_delays":"broken"}),
            &json!({}),
            0.
        )
        .unwrap()
        .retry_delays
        .len(),
        5
    );
}
