//! Run identical lifecycle/race/expiry assertions against memory and a real isolated Redis.
use serde_json::json;
use std::{
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use vogon_service::{
    record::{Record, State},
    store::{Change, Memory, Redis, SharedStore, Ttl},
};
fn record(tenant: &str, id: &str, token: &str) -> Record {
    Record::decode(
        &json!({"tenant":tenant,"call_id":id,"session_token":token,"mode":"text"}).to_string(),
    )
    .unwrap()
}
struct Server(Option<Child>, String);
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
async fn backend(redis: bool, ttl: Ttl) -> (SharedStore, Option<Server>) {
    if !redis {
        return (Arc::new(Memory::new(ttl)), None);
    }
    if let Ok(url) = std::env::var("VOGON_TEST_REDIS_CLUSTER_URL") {
        return (
            Arc::new(
                Redis::connect_mode(&url, ttl, vogon_resilience::Policy::default(), true)
                    .await
                    .unwrap(),
            ),
            Some(Server(None, url)),
        );
    }
    let path = std::env::var_os("REDIS_SERVER_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../../tools/redis-stable/src/redis-server")
        });
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let child = Command::new(path)
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--save",
            "",
            "--appendonly",
            "no",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("build tools/redis-stable or provide Redis test binary");
    let url = format!("redis://127.0.0.1:{port}");
    let server = Server(Some(child), url.clone());
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return (
                Arc::new(Redis::connect(&url, ttl).await.unwrap()),
                Some(server),
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("isolated Redis did not start")
}
async fn lifecycle(redis: bool) {
    let (s, _server) = backend(redis, Ttl::default()).await;
    s.create(record("a", "same", "a-token")).await.unwrap();
    s.create(record("b", "same", "b-token")).await.unwrap();
    assert!(s.get(("c".into(), "same".into())).await.unwrap().is_none());
    assert_eq!(
        s.lookup(false, "a-token".into())
            .await
            .unwrap()
            .unwrap()
            .tenant,
        "a"
    );
    let key = ("a".into(), "same".into());
    s.change(key.clone(), Change::Provider("CA1".into()))
        .await
        .unwrap();
    assert_eq!(
        s.lookup(true, "CA1".into()).await.unwrap().unwrap().tenant,
        "a"
    );
    assert!(s.claim(key.clone(), "owner-1".into()).await.unwrap());
    assert!(s.claim(key.clone(), "owner-1".into()).await.unwrap());
    assert!(!s.claim(key.clone(), "owner-2".into()).await.unwrap());
    let connected = s.get(key.clone()).await.unwrap().unwrap().connected_at_utc;
    assert!(connected > 0.);
    assert!(
        !s.change(key.clone(), Change::Transition(State::Ringing))
            .await
            .unwrap()
    );
    assert!(
        s.change(key.clone(), Change::Transition(State::Ended))
            .await
            .unwrap()
    );
    let ended = s.get(key.clone()).await.unwrap().unwrap().ended_at_utc;
    for state in [
        State::Dialing,
        State::Connected,
        State::Ended,
        State::Failed,
    ] {
        assert!(
            !s.change(key.clone(), Change::Transition(state))
                .await
                .unwrap()
        );
    }
    let snapshot = json!({"array":[],"obj":{},"nested":[{"x":[]}],"language":"en-US"});
    s.change(key.clone(), Change::Snapshot(snapshot.clone()))
        .await
        .unwrap();
    let r = s.get(key.clone()).await.unwrap().unwrap();
    assert_eq!(r.call_config, Some(snapshot));
    assert_eq!(r.ended_at_utc, ended);
    assert_eq!(r.connected_at_utc, connected);
    s.release(key.clone(), "owner-1".into()).await.unwrap();
    assert!(!s.claim(key, "new-owner".into()).await.unwrap());
}
async fn races(redis: bool) {
    let (s, _server) = backend(redis, Ttl::default()).await;
    let mut jobs = tokio::task::JoinSet::new();
    for n in 0..16 {
        let s = s.clone();
        jobs.spawn(async move {
            s.create(record("race", "call", &format!("t{n}")))
                .await
                .is_ok()
        });
    }
    let mut wins = 0;
    while let Some(r) = jobs.join_next().await {
        wins += usize::from(r.unwrap());
    }
    assert_eq!(wins, 1);
    for n in 0..16 {
        let s = s.clone();
        jobs.spawn(async move {
            s.claim(("race".into(), "call".into()), format!("owner-{n}"))
                .await
                .unwrap()
        });
    }
    wins = 0;
    while let Some(r) = jobs.join_next().await {
        wins += usize::from(r.unwrap());
    }
    assert_eq!(wins, 1);
    let key = ("race".into(), "call".into());
    let a = s.change(key.clone(), Change::Provider("CA-race".into()));
    let b = s.change(key.clone(), Change::Snapshot(json!({"kept":[]})));
    let (a, b) = tokio::join!(a, b);
    assert!(a.unwrap());
    assert!(b.unwrap());
    let r = s.get(key.clone()).await.unwrap().unwrap();
    assert_eq!(r.provider_call_id, "CA-race");
    assert_eq!(r.call_config, Some(json!({"kept":[]})));
    s.release(key.clone(), "wrong-owner".into()).await.unwrap();
    assert!(!s.claim(key.clone(), "intruder".into()).await.unwrap());
    s.release(key.clone(), r.gateway_instance).await.unwrap();
    assert!(s.claim(key, "replacement".into()).await.unwrap());
}
async fn expiry(redis: bool) {
    let (s, _server) = backend(
        redis,
        Ttl {
            live: Duration::from_millis(300),
            ended: Duration::from_millis(50),
        },
    )
    .await;
    let key = ("expire".into(), "c".into());
    s.create(record("expire", "c", "tok")).await.unwrap();
    s.change(key.clone(), Change::Provider("CA-expire".into()))
        .await
        .unwrap();
    s.change(key.clone(), Change::Transition(State::Ended))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(s.get(key.clone()).await.unwrap().is_none());
    assert!(s.lookup(false, "tok".into()).await.unwrap().is_none());
    assert!(s.lookup(true, "CA-expire".into()).await.unwrap().is_none());
    assert!(!s.claim(key, "late".into()).await.unwrap());
    s.create(record("expire", "live", "live-token"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        s.lookup(false, "live-token".into())
            .await
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn memory_lifecycle() {
    lifecycle(false).await
}
#[tokio::test]
async fn redis_lifecycle() {
    lifecycle(true).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_atomic_races() {
    races(false).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redis_atomic_races() {
    races(true).await
}
#[tokio::test]
async fn memory_expiry() {
    expiry(false).await
}
#[tokio::test]
async fn redis_expiry() {
    expiry(true).await
}
#[test]
fn migrate_future_record_and_reject_missing_required_identity() {
    let r=Record::decode(r#"{"tenant":"a","call_id":"c","session_token":"t","mode":"text","schema_version":99,"future":true}"#).unwrap();
    assert_eq!(r.schema_version, 1);
    assert_eq!(r.state, State::Created);
    assert!(Record::decode("{}").is_err());
}

async fn refreshed_indexes(redis: bool) {
    let (s, _server) = backend(
        redis,
        Ttl {
            live: Duration::from_millis(600),
            ended: Duration::from_millis(100),
        },
    )
    .await;
    let key = ("refresh".into(), "c".into());
    s.create(record("refresh", "c", "refresh-token"))
        .await
        .unwrap();
    s.change(key.clone(), Change::Provider("refresh-sid".into()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(s.claim(key.clone(), "owner".into()).await.unwrap());
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        s.lookup(false, "refresh-token".into())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        s.lookup(true, "refresh-sid".into())
            .await
            .unwrap()
            .is_some()
    );
    s.change(key.clone(), Change::Snapshot(json!({"items":[]})))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        s.lookup(false, "refresh-token".into())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        s.lookup(true, "refresh-sid".into())
            .await
            .unwrap()
            .is_some()
    );
    s.change(key, Change::Transition(State::Ended))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        s.lookup(false, "refresh-token".into())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        s.lookup(true, "refresh-sid".into())
            .await
            .unwrap()
            .is_none()
    );
}
#[tokio::test]
async fn memory_claim_and_snapshot_refresh_lookup_expiry() {
    refreshed_indexes(false).await;
}
#[tokio::test]
async fn redis_claim_and_snapshot_refresh_lookup_expiry() {
    refreshed_indexes(true).await;
}

#[test]
fn all_49_lifecycle_transitions_have_explicit_acceptance_and_stable_timestamps() {
    use State::*;
    let states = [
        Created, Dialing, Ringing, Connected, Ended, Failed, Cancelled,
    ];
    let allowed = [
        vec![Dialing, Ringing, Connected, Ended, Failed, Cancelled],
        vec![Ringing, Connected, Ended, Failed, Cancelled],
        vec![Connected, Ended, Failed, Cancelled],
        vec![Ended, Failed, Cancelled],
        vec![],
        vec![],
        vec![],
    ];
    for (i, from) in states.into_iter().enumerate() {
        for to in states {
            let mut r = record("a", "c", "token");
            r.state = from;
            r.connected_at_utc = 7.;
            r.ended_at_utc = 8.;
            let expected = allowed[i].contains(&to);
            assert_eq!(r.transition(to, 100.), expected, "{from:?} -> {to:?}");
            assert_eq!(r.state, if expected { to } else { from });
            assert_eq!((r.connected_at_utc, r.ended_at_utc), (7., 8.));
        }
    }
}

/// Two independent clients contend on an opaque transfer value in a real Redis.
#[tokio::test]
async fn transfer_redis_cas_is_atomic_across_clients() {
    let (_sessions, server) = backend(true, Ttl::default()).await;
    let url = &server.as_ref().unwrap().1;
    let a = vogon_escalation::Store::redis_mode(
        url,
        std::env::var_os("VOGON_TEST_REDIS_CLUSTER_URL").is_some(),
    )
    .unwrap();
    let b = vogon_escalation::Store::redis_mode(
        url,
        std::env::var_os("VOGON_TEST_REDIS_CLUSTER_URL").is_some(),
    )
    .unwrap();
    let ttl = Duration::from_millis(200);
    assert!(
        a.cas("gw:rust:esc:test", None, Some(b"old"), ttl)
            .await
            .unwrap()
    );
    let (x, y) = tokio::join!(
        a.cas("gw:rust:esc:test", Some(b"old"), Some(b"a"), ttl),
        b.cas("gw:rust:esc:test", Some(b"old"), Some(b"b"), ttl)
    );
    assert_ne!(x.unwrap(), y.unwrap());
    assert_eq!(
        a.get("gw:rust:esc:test").await.unwrap(),
        b.get("gw:rust:esc:test").await.unwrap()
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(a.get("gw:rust:esc:test").await.unwrap().is_none());
    assert!(
        !b.cas("gw:rust:esc:test", Some(b"a"), Some(b"resurrect"), ttl)
            .await
            .unwrap()
    );
}

/// Forwards the first connection at once and holds every later one for `hold` before forwarding:
/// a Redis that keeps serving its established connection while new connections cannot come up in time.
async fn first_connection_only_proxy(upstream: String, hold: Duration) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut accepted = 0usize;
        while let Ok((mut inbound, _)) = listener.accept().await {
            let wait = if accepted == 0 { Duration::ZERO } else { hold };
            accepted += 1;
            let upstream = upstream.clone();
            tokio::spawn(async move {
                tokio::time::sleep(wait).await;
                if let Ok(mut outbound) = tokio::net::TcpStream::connect(upstream).await {
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                }
            });
        }
    });
    format!("redis://{address}")
}

/// Production 2026-11-26 (call e5b7b2eb): the session store answered the hang-up callback while every
/// transfer-store read logged `deadline_exceeded connected=false waited_ms=5000`, and the farewell call
/// was finalized `failed` with `transfer_store_timeout`. The transfer store opened its own second
/// connection inside each 5 s command deadline; the session store's established one was never used.
#[tokio::test]
async fn transfer_store_reads_over_the_session_stores_established_connection() {
    if std::env::var_os("VOGON_TEST_REDIS_CLUSTER_URL").is_some() {
        return; // the proxy below speaks to one standalone node
    }
    let (_unused, server) = backend(true, Ttl::default()).await;
    let upstream = server
        .as_ref()
        .unwrap()
        .1
        .trim_start_matches("redis://")
        .to_owned();
    let url = first_connection_only_proxy(upstream, Duration::from_secs(6)).await;
    let sessions: SharedStore = Arc::new(
        Redis::connect_mode(
            &url,
            Ttl::default(),
            vogon_resilience::Policy::once(),
            false,
        )
        .await
        .unwrap(),
    );
    let config = vogon_service::bootstrap::Config::resolve(
        &json!({"GATEWAY_SESSION_STORE":"redis", "REDIS_URL":url, "FRONT_MODEL_ENABLED":"false",
        "RECORDING_ENABLED":"false", "GATEWAY_SERVICE_KEY":"transfer-test-key"}),
        &json!({}),
    )
    .unwrap();
    let service = vogon_service::host::Service::new(config, sessions.clone()).unwrap();
    let started = std::time::Instant::now();
    let read = service.transfers.audio(&"0".repeat(32)).await;
    assert!(
        matches!(read, Ok(None)),
        "a transfer read failed with {:?} after {:?} while the session store was connected",
        read.err().map(|e| e.0),
        started.elapsed()
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(
        sessions
            .lookup(false, "no-such-token".into())
            .await
            .unwrap()
            .is_none()
    );
}
