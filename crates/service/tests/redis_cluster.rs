//! Three-primary cluster tests driven by validation/redis_cluster/run.py.
use serde_json::json;
use std::{sync::Arc, time::Duration};
use vogon_service::{
    bootstrap::Config,
    host::Service,
    record::Record,
    store::{Redis, Store, Ttl},
};

#[tokio::test]
#[ignore = "requires the isolated three-primary cluster runner"]
async fn cluster_routes_slots_preserves_atomicity_and_serves_http_create() {
    let seeds = std::env::var("VOGON_TEST_REDIS_CLUSTER_SEEDS").unwrap();
    let urls: Vec<_> = seeds.split(',').collect();
    let once = vogon_resilience::Policy::once();
    let client = vogon_redis::Client::new(urls[0], true).unwrap();
    let old: redis::RedisResult<i64> = client
        .query(
            redis::cmd("EVAL")
                .arg("return 1")
                .arg(2)
                .arg("gw:a:call:c")
                .arg("gw:idx:token:t"),
            &once,
        )
        .await;
    assert_eq!(vogon_redis::category(&old.unwrap_err()), "crossslot");
    let mut slots = Vec::new();
    for key in [
        "gw:{vogon-sessions-v1}:a:call:c",
        "gw:{vogon-sessions-v1}:idx:token:t",
        "gw:{vogon-sessions-v1}:idx:sid:CA",
        "gw:{vogon-sessions-v1}:a:claim:c",
    ] {
        let slot: i64 = client
            .query(redis::cmd("CLUSTER").arg("KEYSLOT").arg(key), &once)
            .await
            .unwrap();
        slots.push(slot);
    }
    assert!(slots.iter().all(|slot| *slot == slots[0]));

    // Each seed must route to the owner, not simply send every request to its seed.
    let mut stores = Vec::new();
    for url in &urls {
        stores.push(Arc::new(
            Redis::connect_mode(url, Ttl::default(), once.clone(), true)
                .await
                .unwrap(),
        ));
    }
    let mut tasks = tokio::task::JoinSet::new();
    for n in 0..24 {
        let store = stores[n % 3].clone();
        tasks.spawn(async move {
            let r = Record::decode(&json!({"tenant":"cluster-race","call_id":"same","session_token":format!("cluster-token-{n}"),"mode":"text"}).to_string()).unwrap();
            store.create(r).await.is_ok()
        });
    }
    let mut wins = 0;
    while let Some(result) = tasks.join_next().await {
        wins += usize::from(result.unwrap());
    }
    assert_eq!(wins, 1);

    // Single-key transfer values remain spread across the cluster's slots.
    let a = vogon_escalation::Store::redis_mode(urls[0], true).unwrap();
    let b = vogon_escalation::Store::redis_mode(urls[2], true).unwrap();
    let mut transfer_slots = std::collections::HashSet::new();
    for n in 0..30 {
        let key = format!("gw:rust:esc:cluster-{n}");
        let slot: u16 = client
            .query(redis::cmd("CLUSTER").arg("KEYSLOT").arg(&key), &once)
            .await
            .unwrap();
        transfer_slots.insert(slot / 5462);
        assert!(
            a.cas(&key, None, Some(b"value"), Duration::from_secs(60))
                .await
                .unwrap()
        );
        assert_eq!(b.get(&key).await.unwrap(), Some(b"value".to_vec()));
    }
    assert_eq!(transfer_slots.len(), 3);

    // The proxy loses a reply AFTER Redis executes INCR. A default retry budget
    // must not repeat the command and turn one side effect into two.
    let lost: redis::RedisResult<i64> = client
        .query(
            redis::cmd("EVAL")
                .arg("return redis.call('INCR',KEYS[1])")
                .arg(1)
                .arg("vogon-test-lost-reply"),
            &vogon_resilience::Policy::default(),
        )
        .await;
    assert!(
        lost.is_err(),
        "lost mutation reply must remain an explicit failure"
    );
    let observer = vogon_redis::Client::new(urls[2], true).unwrap();
    let count: i64 = observer
        .query(redis::cmd("GET").arg("vogon-test-lost-reply"), &once)
        .await
        .unwrap();
    assert_eq!(
        count, 1,
        "an ambiguously completed mutation must never be replayed"
    );

    // Readiness can pass with an ACL that denies EVAL; the script diagnostic must
    // preserve the safe permission category rather than hiding a network failure.
    let restricted = urls[0].replacen("://:", "://noeval:", 1);
    let restricted_client = vogon_redis::Client::new(&restricted, true).unwrap();
    let denied: redis::RedisResult<i64> = restricted_client
        .query(redis::cmd("EVAL").arg("return 1").arg(0), &once)
        .await;
    assert_eq!(
        vogon_redis::category(&denied.unwrap_err()),
        "permission_denied"
    );

    let config = Config::resolve(
        &json!({"GATEWAY_SESSION_STORE":"redis", "REDIS_CLUSTER_MODE":"true",
        "REDIS_URL":urls[1], "FRONT_MODEL_ENABLED":"false", "RECORDING_ENABLED":"false",
        "GATEWAY_SERVICE_KEY":"cluster-test-key"}),
        &json!({}),
    )
    .unwrap();
    let service = Service::new(config, stores[1].clone()).unwrap();
    service.startup().await.unwrap();
    assert_eq!(service.readiness("text").await["ready"], true);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, vogon_service::routes::router(service))
            .await
            .unwrap()
    });
    let response = reqwest::Client::new()
        .post(format!("{base}/v1/tenants/cluster-http/calls"))
        .bearer_auth("cluster-test-key")
        .json(
            &json!({"mode":"text","app":{"base_url":"http://127.0.0.1:9/turn/v1","auth_token":""}}),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(
        stores[2]
            .lookup(false, body["session_token"].as_str().unwrap().into())
            .await
            .unwrap()
            .is_some()
    );
    task.abort();
}
