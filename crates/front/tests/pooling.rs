#[path = "support/pool.rs"]
mod pool;
use std::time::Duration;
use vogon_front::http::{Http, Key};

#[tokio::test]
async fn warm_once_across_clones_and_reuse_tcp_for_completion() {
    let mut server = pool::Server::start().await;
    let http = Http::new(
        &format!("{}/v1/chat/completions", server.url),
        Key::new("warm-secret".into()).unwrap(),
    )
    .unwrap();
    let a = http.clone();
    let b = http.clone();
    let task = tokio::spawn(async move {
        tokio::join!(
            a.warm(Duration::from_secs(2)),
            b.warm(Duration::from_secs(2))
        )
    });
    let warm = server.next().await;
    assert!(warm.head.starts_with("GET /v1/models "));
    assert!(warm.head.contains("Bearer warm-secret"));
    assert!(warm.body.is_null());
    let connection = warm.connection;
    warm.reply.send(()).unwrap();
    let (a, b) = task.await.unwrap();
    a.unwrap();
    b.unwrap();
    let task = tokio::spawn(async move {
        http.post(&serde_json::json!({"model":"test"}), Duration::from_secs(2))
            .await
    });
    let completion = server.next().await;
    assert_eq!(completion.connection, connection);
    assert_eq!(completion.body["model"], "test");
    completion.reply.send(()).unwrap();
    task.await.unwrap().unwrap();
    assert!(server.requests.try_recv().is_err());
}

#[tokio::test]
async fn warm_timeout_does_not_poison_normal_requests() {
    let mut server = pool::Server::start().await;
    let http = Http::new(
        &format!("{}/v1/chat/completions", server.url),
        Key::new("test".into()).unwrap(),
    )
    .unwrap();
    let clone = http.clone();
    let task = tokio::spawn(async move { clone.warm(Duration::from_millis(50)).await });
    let _held = server.next().await;
    assert_eq!(task.await.unwrap().unwrap_err().code, "provider_timeout");
    assert!(http.warm(Duration::from_secs(1)).await.is_err());
    let task = tokio::spawn(async move {
        http.post(&serde_json::json!({}), Duration::from_secs(2))
            .await
    });
    server.next().await.reply.send(()).unwrap();
    task.await.unwrap().unwrap();
}
