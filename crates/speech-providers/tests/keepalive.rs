//! Actual socket interval tests; Tokio virtual time applies to wire timers only.
//! No Clock/std::Instant policy deadlines are advanced by these tests.
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpListener,
    sync::oneshot,
    time::{self, Duration},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use vogon_speech_providers::{
    config::ApiKey,
    wire::{Socket, WireEvent},
};

async fn exercise(reply_with_pong: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (ping_tx, ping_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert!(matches!(
            socket.next().await.unwrap().unwrap(),
            Message::Text(_)
        ));
        assert!(matches!(
            socket.next().await.unwrap().unwrap(),
            Message::Ping(_)
        ));
        if reply_with_pong {
            // flush sends tungstenite's automatically queued Pong. The following
            // text is an ordered barrier proving the client's reader consumed it.
            socket.flush().await.unwrap();
            socket
                .send(Message::Text("pong sent".into()))
                .await
                .unwrap();
        }
        ping_tx.send(()).unwrap();
        if reply_with_pong {
            assert!(matches!(
                socket.next().await.unwrap().unwrap(),
                Message::Ping(_)
            ));
            socket.flush().await.unwrap();
            socket
                .send(Message::Text("second ping".into()))
                .await
                .unwrap();
        }
        // In the failure branch, do not poll/flush again: that would send a Pong.
        let _ = release_rx.await;
    });
    let key = ApiKey::new("local-test".into()).unwrap();
    let mut socket = Socket::connect(&endpoint, &[], &key).await.unwrap();
    socket
        .writer
        .send(Message::Text("ready".into()))
        .await
        .unwrap();
    time::pause();
    // Prevent paused-time auto-advance while loopback I/O is being scheduled.
    let runnable = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    // Tokio rounds timer deadlines up to millisecond resolution.
    time::advance(Duration::from_millis(20_001)).await;
    // A real-time watchdog bounds I/O waits even though Tokio time is paused.
    async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::pin!(future);
        let limit = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            tokio::select! { biased; result = &mut future => return result, _ = tokio::task::yield_now() => {} }
            assert!(
                std::time::Instant::now() < limit,
                "keepalive fixture stalled"
            );
        }
    }
    bounded(ping_rx).await.unwrap();
    if reply_with_pong {
        assert!(
            matches!(bounded(socket.incoming.recv()).await, Some(WireEvent::Text(v)) if v == "pong sent")
        );
    }
    time::advance(Duration::from_secs(19)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(
        socket.incoming.try_recv().is_err(),
        "keepalive fired before next interval"
    );
    time::advance(Duration::from_secs(1)).await;
    let event = bounded(socket.incoming.recv()).await.unwrap();
    if reply_with_pong {
        assert!(matches!(event, WireEvent::Text(v) if v == "second ping"));
    } else {
        assert!(matches!(event, WireEvent::Failed(e) if e.code == "provider_keepalive_timeout"));
        assert!(matches!(
            bounded(socket.incoming.recv()).await,
            Some(WireEvent::Closed)
        ));
    }
    release_tx.send(()).unwrap();
    bounded(server).await.unwrap();
    runnable.abort();
    time::resume();
}

#[tokio::test]
async fn missing_pong_fails_on_next_twenty_second_tick() {
    exercise(false).await;
}

#[tokio::test]
async fn pong_allows_next_scheduled_ping() {
    exercise(true).await;
}
