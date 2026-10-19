//! Offline HTTP contract checks; no gateway, speech provider or credentials needed.
use super::*;
use axum::{body::Body, http::Request};
use tower::ServiceExt;
use vogon_contracts::{ProcessEventsResponse, StartCallResponse, codec};

/// Exercise the real router and JSON extractors without opening a network socket.
async fn post(app: &Router, operation: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/turn/v1/{operation}"))
                .header("authorization", "Bearer echo-demo")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 65536)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Open one call and validate policy through the same codec used by the gateway.
async fn start(app: &Router, call_id: &str) {
    let (status, body) = post(app, "start_call", json!({"call_id":call_id,"mode":"voice"})).await;
    assert_eq!(status, StatusCode::OK);
    codec::from_wire::<StartCallResponse>("StartCallResponse", &body).unwrap();
}

/// Make an actual contract-shaped provider transcript event.
fn speech(kind: &str, text: &str) -> Value {
    json!({"kind":kind,"text":text,"confidence":1.0,"low_conf_words":[]})
}

/// Submit a batch and validate the returned command contract.
async fn process(app: &Router, cid: &str, rid: &str, events: Vec<Value>) -> Value {
    let (status, body) = post(
        app,
        "process_events",
        json!({"call_id":cid,"request_id":rid,"events":events}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    codec::from_wire::<ProcessEventsResponse>("ProcessEventsResponse", &body).unwrap();
    body
}

#[tokio::test]
async fn speculation_waits_for_final_and_resumption_or_changed_text_invalidates_it() {
    let app = router("echo-demo".into());
    start(&app, "call").await;
    let eager = process(&app, "call", "1", vec![speech("utterance_eager", "Friday")]).await;
    assert_eq!(eager["speak"], json!([]));
    let matching = process(&app, "call", "2", vec![speech("utterance_final", "Friday")]).await;
    assert_eq!(matching["speak"][0]["text"], "You said: Friday");
    process(&app, "call", "3", vec![speech("utterance_eager", "Friday")]).await;
    let resumed = process(&app, "call", "4", vec![json!({"kind":"utterance_resumed"})]).await;
    assert_eq!(resumed["speak"], json!([]));
    let changed = process(
        &app,
        "call",
        "5",
        vec![speech("utterance_final", "Monday instead")],
    )
    .await;
    assert_eq!(changed["speak"][0]["text"], "You said: Monday instead");
    // Changed final text is sufficient even when no resumed notification arrives.
    process(
        &app,
        "call",
        "6",
        vec![speech("utterance_eager", "Tuesday")],
    )
    .await;
    let changed = process(
        &app,
        "call",
        "7",
        vec![speech("utterance_final", "Wednesday")],
    )
    .await;
    assert_eq!(changed["speak"][0]["text"], "You said: Wednesday");
}

#[tokio::test]
async fn retries_keep_audio_ids_but_later_identical_words_are_a_new_turn() {
    let app = router("echo-demo".into());
    start(&app, "call").await;
    let events = vec![speech("utterance_final", "hello")];
    let original = process(&app, "call", "1", events.clone()).await;
    start(&app, "call").await;
    assert_eq!(process(&app, "call", "1", events.clone()).await, original);
    let later = process(&app, "call", "2", events).await;
    assert_ne!(
        original["speak"][0]["audio_id"],
        later["speak"][0]["audio_id"]
    );
    let (status, _) = post(
        &app,
        "process_events",
        json!({"call_id":"call","request_id":"1","events":[]}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn concurrent_calls_have_independent_state_and_end_releases_it() {
    let app = router("echo-demo".into());
    tokio::join!(start(&app, "a"), start(&app, "b"));
    let (a, b) = tokio::join!(
        process(&app, "a", "1", vec![speech("utterance_final", "apple")]),
        process(&app, "b", "1", vec![speech("utterance_final", "banana")]),
    );
    assert_eq!(
        a["speak"][0],
        json!({"audio_id":"a-echo-1","text":"You said: apple"})
    );
    assert_eq!(
        b["speak"][0],
        json!({"audio_id":"b-echo-1","text":"You said: banana"})
    );
    for _ in 0..2 {
        assert_eq!(
            post(&app, "end_call", json!({"call_id":"a","reason":"hangup"}))
                .await
                .0,
            StatusCode::OK
        );
    }
    assert_eq!(
        post(
            &app,
            "process_events",
            json!({"call_id":"a","request_id":"2","events":[]})
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let b = process(
        &app,
        "b",
        "2",
        vec![speech("utterance_final", "still here")],
    )
    .await;
    assert_eq!(b["speak"][0]["audio_id"], "b-echo-2");
}

#[tokio::test]
async fn empty_batches_and_blank_transcripts_are_silent_and_auth_is_required() {
    let app = router("echo-demo".into());
    start(&app, "call").await;
    assert_eq!(process(&app, "call", "1", vec![]).await["speak"], json!([]));
    assert_eq!(
        process(&app, "call", "2", vec![speech("utterance_final", "  ")]).await["speak"],
        json!([])
    );
    for (operation, body) in [
        ("start_call", json!({"call_id":"other","mode":"voice"})),
        (
            "process_events",
            json!({"call_id":"call","request_id":"3","events":[]}),
        ),
        ("end_call", json!({"call_id":"call","reason":"hangup"})),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::post(format!("/turn/v1/{operation}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
