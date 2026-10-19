//! Real Redis behind a local TLS terminator; run through validation/redis_tls/run.py.
use std::time::Duration;
use vogon_service::{
    record::Record,
    store::{Redis, Store, Ttl},
};

#[test]
fn rediss_urls_preserve_tls_and_credentials() {
    let client = redis::Client::open("rediss://default:p%3Ass@localhost:6379/0").unwrap();
    assert!(matches!(
        client.get_connection_info().addr,
        redis::ConnectionAddr::TcpTls {
            insecure: false,
            ..
        }
    ));
    assert_eq!(
        client.get_connection_info().redis.password.as_deref(),
        Some("p:ss")
    );
    vogon_escalation::Store::redis("rediss://localhost:6379/0").unwrap();
}

#[tokio::test]
#[ignore = "run validation/redis_tls/run.py for real Redis, certificates and TLS endpoints"]
async fn tls_stores_roundtrip_and_reject_invalid_peers() {
    let good = std::env::var("VOGON_TEST_REDIS_TLS_URL").unwrap();
    let store = Redis::connect_with_retry(&good, Ttl::default(), vogon_resilience::Policy::once())
        .await
        .expect("trusted TLS and password authentication must succeed before host construction");
    let record = Record::decode(
        r#"{"tenant":"tls","call_id":"test","session_token":"tls-token","mode":"text"}"#,
    )
    .unwrap();
    store.create(record).await.unwrap();
    assert_eq!(
        store
            .lookup(false, "tls-token".into())
            .await
            .unwrap()
            .unwrap()
            .call_id,
        "test"
    );
    let key = ("tls".into(), "test".into());
    assert!(store.claim(key.clone(), "owner".into()).await.unwrap());
    assert!(!store.claim(key.clone(), "other".into()).await.unwrap());
    store.release(key, "owner".into()).await.unwrap();

    let transfer = vogon_escalation::Store::redis(&good).unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let s = transfer.clone();
        tasks.spawn(async move {
            s.cas(
                "tls-transfer",
                None,
                Some(b"winner"),
                Duration::from_secs(60),
            )
            .await
            .unwrap()
        });
    }
    let mut winners = 0;
    while let Some(result) = tasks.join_next().await {
        winners += usize::from(result.unwrap());
    }
    assert_eq!(winners, 1);
    assert_eq!(
        transfer.get("tls-transfer").await.unwrap(),
        Some(b"winner".to_vec())
    );

    for name in ["BAD_HOST", "UNTRUSTED", "BAD_AUTH"] {
        let url = std::env::var(format!("VOGON_TEST_REDIS_TLS_{name}")).unwrap();
        let result =
            Redis::connect_with_retry(&url, Ttl::default(), vogon_resilience::Policy::once()).await;
        assert!(result.is_err(), "session store accepted {name}");
        let error = result.err().unwrap();
        assert_eq!(
            error.0, "redis_unavailable",
            "valid URLs must reach connection validation"
        );
        assert!(!error.0.contains("password"));
        let transfer = vogon_escalation::Store::redis(&url).unwrap();
        assert!(
            transfer.get("tls-transfer").await.is_err(),
            "transfer store accepted {name}"
        );
    }
}
