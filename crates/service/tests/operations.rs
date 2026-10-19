//! Actual native capability checks, shutdown readiness and bounded concurrent admission.
mod support;
use serde_json::json;
use std::sync::Arc;
use vogon_service::{bootstrap::Config, host::Service, store::Memory};
#[test]
fn retry_configuration_is_validated_and_s3_receives_budget() {
    let c=Config::resolve(&json!({"GATEWAY_RETRY_ATTEMPTS":"4","GATEWAY_RETRY_INITIAL_MS":"20","GATEWAY_RETRY_MAX_MS":"80","GATEWAY_RETRY_STT_ATTEMPTS":"2","S3_ENABLED":"true","S3_BUCKET":"test"}),&json!({})).unwrap();
    assert_eq!(c.retries.stt.attempts, 2);
    assert_eq!(c.retries.turn.attempts, 4);
    let s3 = c.s3.unwrap();
    assert_eq!(s3.max_attempts, 4);
    assert_eq!(s3.initial_backoff.as_millis(), 20);
    for env in [
        json!({"GATEWAY_RETRY_ATTEMPTS":"0"}),
        json!({"GATEWAY_RETRY_INITIAL_MS":"100","GATEWAY_RETRY_MAX_MS":"1"}),
        json!({"GATEWAY_RETRY_JITTER":"maybe"}),
    ] {
        assert!(Config::resolve(&env, &json!({})).is_err());
    }
}
#[tokio::test]
async fn native_startup_and_mode_readiness_are_explicit_and_stop_on_shutdown() {
    let gateway = support::Gateway::start(support::config()).await;
    let checks = gateway.service.startup().await.unwrap();
    assert!(checks.as_object().unwrap().values().all(|v| v == true));
    let client = reqwest::Client::new();
    for (mode, status) in [
        ("text", 200),
        ("voice", 200),
        ("twilio", 503),
        ("unknown", 503),
    ] {
        assert_eq!(
            client
                .get(format!("{}/ready?mode={mode}", gateway.url))
                .send()
                .await
                .unwrap()
                .status(),
            status
        );
    }
    let metrics = client
        .get(format!("{}/metrics", gateway.url))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("vogon_active_sockets 0"));
    assert!(!metrics.contains("local-test"));
    gateway.service.shutdown().await.unwrap();
    assert_eq!(
        client
            .get(format!("{}/ready", gateway.url))
            .send()
            .await
            .unwrap()
            .status(),
        503
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_parallel_admission_releases_every_owner_and_readiness_never_changes_call_counters()
 {
    let mut cfg = support::config();
    cfg.max_handshakes = 32;
    let service = Service::new(cfg, Arc::new(Memory::default())).unwrap();
    service.startup().await.unwrap();
    for _ in 0..20 {
        let barrier = Arc::new(tokio::sync::Barrier::new(33));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..32 {
            let s = service.clone();
            let b = barrier.clone();
            tasks.spawn(async move {
                let lease = s.register().unwrap();
                b.wait().await;
                tokio::task::yield_now().await;
                drop(lease);
            });
        }
        barrier.wait().await;
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        assert_eq!(service.active(), 0);
        assert_eq!(service.readiness("text").await["ready"], true);
    }
    assert_eq!(
        service
            .metrics
            .count(vogon_observability::Event::CallStarted),
        0
    );
    service.shutdown().await.unwrap();
    assert!(service.register().is_err());
}

#[tokio::test]
async fn twilio_retries_only_explicit_throttling_and_preserves_dial_form() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    use axum::{
        Form, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post,
    };
    use std::collections::HashMap;
    use vogon_service::twilio::{self, Dial};
    type Seen = Arc<tokio::sync::Mutex<Vec<HashMap<String, String>>>>;
    async fn reply(
        State((status, seen)): State<(u16, Seen)>,
        Form(form): Form<HashMap<String, String>>,
    ) -> axum::response::Response {
        let mut seen = seen.lock().await;
        seen.push(form);
        if seen.len() < 3 {
            (StatusCode::from_u16(status).unwrap(), "{}").into_response()
        } else {
            axum::Json(json!({"sid":"CAnew"})).into_response()
        }
    }
    for (status, count) in [(429, 3), (503, 1), (401, 1)] {
        let seen: Seen = Arc::new(tokio::sync::Mutex::new(vec![]));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/{*path}", post(reply))
            .with_state((status, seen.clone()));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let retry = vogon_resilience::Policy {
            attempts: 3,
            initial_ms: 1,
            max_ms: 2,
            jitter: false,
        };
        let result = twilio::dial_with_retry(
            &reqwest::Client::new(),
            Dial {
                endpoint: &url,
                account: "ACtest",
                token: "test",
                to: "+10000000001",
                from: "+10000000002",
                base: "https://example.test",
                session_token: "cap",
            },
            &retry,
        )
        .await;
        assert_eq!(result.is_ok(), status == 429);
        let rows = seen.lock().await;
        assert_eq!(rows.len(), count);
        assert!(rows.iter().all(|r| r == &rows[0]));
        task.abort();
    }
}
#[tokio::test]
async fn failed_artifact_startup_never_reports_ready() {
    let path = std::env::temp_dir().join(format!("vogon-probe-file-{}", uuid::Uuid::new_v4()));
    tokio::fs::write(&path, b"not a directory").await.unwrap();
    let mut config = support::config();
    config.settings["recording_enabled"] = json!(true);
    config.settings["recordings_path"] = json!(path);
    let s = Service::new(config, Arc::new(Memory::default())).unwrap();
    assert!(s.startup().await.is_err());
    assert_eq!(s.readiness("text").await["ready"], false);
    tokio::fs::remove_file(path).await.unwrap();
}

#[tokio::test]
async fn front_warm_failure_is_nonfatal_single_attempt_and_disabled_mode_is_silent() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let app = axum::Router::new().route(
        "/v1/models",
        axum::routing::get(move || {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for enabled in [false, true] {
        let mut config = support::config();
        config.settings["front_model_enabled"] = json!(enabled);
        let mut service = Service::new(config, Arc::new(Memory::default())).unwrap();
        Arc::get_mut(&mut service).unwrap().clients.front = Some(vogon_front::judge::Client {
            http: vogon_front::http::Http::new(
                &endpoint,
                vogon_front::http::Key::new("fixture".into()).unwrap(),
            )
            .unwrap(),
            model: "fixture".into(),
            timeout_ms: 100,
        });
        let (a, b) = tokio::join!(service.startup(), service.startup());
        a.unwrap();
        b.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), usize::from(enabled));
        assert_eq!(service.readiness("text").await["ready"], true);
        service.shutdown().await.unwrap();
    }
    server.abort();
}
