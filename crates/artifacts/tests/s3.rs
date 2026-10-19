//! Official AWS SDK requests against a local S3 protocol fixture; no AWS credentials/network.
use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::put,
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use vogon_artifacts::store::{S3Config, Stores};
#[derive(Default)]
struct Fake {
    attempts: AtomicUsize,
    failures: AtomicUsize,
    permanent: bool,
    objects: Mutex<HashMap<String, Vec<u8>>>,
}
async fn upload(
    State(s): State<Arc<Fake>>,
    Path(key): Path<String>,
    h: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    assert!(
        h["authorization"]
            .to_str()
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256")
    );
    s.attempts.fetch_add(1, Ordering::SeqCst);
    if s.permanent {
        return (
            StatusCode::FORBIDDEN,
            "<Error><Code>AccessDenied</Code></Error>",
        );
    }
    if s.failures
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "<Error><Code>SlowDown</Code></Error>",
        );
    }
    s.objects.lock().unwrap().insert(key, body.to_vec());
    (StatusCode::OK, "")
}
async fn download(State(s): State<Arc<Fake>>, Path(key): Path<String>) -> impl IntoResponse {
    match s.objects.lock().unwrap().get(&key).cloned() {
        Some(b) => (StatusCode::OK, b),
        None => (
            StatusCode::NOT_FOUND,
            b"<Error><Code>NoSuchKey</Code></Error>".to_vec(),
        ),
    }
}
async fn fixture(fake: Arc<Fake>) -> (Arc<Stores>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = S3Config::new("recordings".into(), "us-east-1".into());
    config.credentials = Some(("test-key".into(), "test-secret".into(), None));
    config.endpoint = Some(format!("http://{}", listener.local_addr().unwrap()));
    config.initial_backoff = Duration::from_millis(1);
    config.max_backoff = Duration::from_millis(4);
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/{*key}", put(upload).get(download))
                .with_state(fake),
        )
        .await
        .unwrap();
    });
    (
        Stores::new(
            std::env::temp_dir().join(format!("vogon-s3-{}", uuid::Uuid::new_v4())),
            Some(config),
        ),
        task,
    )
}
#[tokio::test]
async fn sdk_retries_transient_put_then_gets_cloud_after_local_removed() {
    let fake = Arc::new(Fake::default());
    fake.failures.store(2, Ordering::SeqCst);
    let (stores, task) = fixture(fake.clone()).await;
    let errors = stores
        .put(
            "a",
            "call",
            "recording.wav",
            b"wave bytes".to_vec(),
            "audio/wav",
        )
        .await;
    assert!(errors.is_empty(), "{errors:?}");
    assert_eq!(fake.attempts.load(Ordering::SeqCst), 3);
    tokio::fs::remove_dir_all(&stores.directory).await.unwrap();
    assert_eq!(
        stores.get("a", "call", "recording.wav").await.unwrap(),
        Some(b"wave bytes".to_vec())
    );
    assert!(
        stores
            .get("b", "call", "recording.wav")
            .await
            .unwrap()
            .is_none()
    );
    task.abort();
}
#[tokio::test]
async fn sdk_does_not_retry_auth_and_preserves_local_success() {
    let fake = Arc::new(Fake {
        permanent: true,
        ..Default::default()
    });
    let (stores, task) = fixture(fake.clone()).await;
    assert_eq!(
        stores
            .put("a", "call", "recording.wav", vec![1, 2], "audio/wav")
            .await,
        vec!["artifact_s3_put_failed"]
    );
    assert_eq!(fake.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        stores.get("a", "call", "recording.wav").await.unwrap(),
        Some(vec![1, 2])
    );
    tokio::fs::remove_dir_all(&stores.directory).await.unwrap();
    task.abort();
}
