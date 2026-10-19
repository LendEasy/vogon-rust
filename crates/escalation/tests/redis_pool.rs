//! Explicit real-Redis test: no emulation, connection counts include actual handshakes.
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use vogon_escalation::Store;
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
async fn connections(c: &mut redis::aio::MultiplexedConnection) -> u64 {
    let info: String = redis::cmd("INFO")
        .arg("stats")
        .query_async(c)
        .await
        .unwrap();
    info.lines()
        .find_map(|l| l.strip_prefix("total_connections_received:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}
#[tokio::test]
#[ignore = "requires REDIS_TEST_SERVER pointing to a real redis-server executable"]
async fn concurrent_initialization_reuses_connection_cas_is_atomic_and_disconnect_recovers() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let _server = Server(
        Command::new(std::env::var("REDIS_TEST_SERVER").expect("set REDIS_TEST_SERVER"))
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
            .unwrap(),
    );
    let url = format!("redis://127.0.0.1:{port}/");
    let admin = redis::Client::open(url.as_str()).unwrap();
    let mut conn = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(c) = admin.get_multiplexed_async_connection().await {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let before = connections(&mut conn).await;
    let store = Store::redis(&url).unwrap();
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let s = store.clone();
        jobs.spawn(async move {
            s.cas("key", None, Some(b"winner"), Duration::from_secs(60))
                .await
                .unwrap()
        });
    }
    let mut winners = 0;
    while let Some(r) = jobs.join_next().await {
        winners += usize::from(r.unwrap());
    }
    assert_eq!(winners, 1);
    for _ in 0..32 {
        assert_eq!(store.get("key").await.unwrap(), Some(b"winner".to_vec()));
    }
    assert_eq!(
        connections(&mut conn).await - before,
        1,
        "all clones/operations must share one connection"
    );
    let killed: i64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("normal")
        .arg("SKIPME")
        .arg("yes")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(killed, 1);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(Some(value)) = store.get("key").await {
                assert_eq!(value, b"winner");
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        connections(&mut conn).await - before,
        2,
        "one reconnect after disconnect"
    );
    assert!(
        !store
            .cas("key", None, Some(b"wrong"), Duration::from_secs(60))
            .await
            .unwrap()
    );
}
