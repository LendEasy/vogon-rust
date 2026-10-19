//! Exercise Session's real Tokio interval and event channel, without manual ticks.
//! Use real elapsed time: pausing Tokio does not advance the std::Instant Clock.
use serde_json::{Value, json};
use std::{future::Future, pin::Pin, time::Duration};
use tokio::{sync::mpsc, time::timeout};
use vogon_clocks::{
    Clock,
    call::{Config as ClockConfig, Threshold},
};
use vogon_speech_providers::Result;
use vogon_transport::{
    session::{Config, Session},
    wire::{Frame, Sink},
};

struct Discard;
impl Sink for Discard {
    fn send(&mut self, _: Frame) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}
async fn session(clock: Clock) -> (Session, mpsc::UnboundedReceiver<Value>) {
    Session::text_only(
        Config {
            clocks: ClockConfig {
                duration_thresholds: vec![
                    Threshold {
                        id: "first".into(),
                        after_sec: 0.1,
                    },
                    Threshold {
                        id: "second".into(),
                        after_sec: 2.,
                    },
                ],
                silence_thresholds: vec![Threshold {
                    id: "unarmed".into(),
                    after_sec: 0.1,
                }],
                ..Default::default()
            },
            ..Default::default()
        },
        Box::new(Discard),
        clock,
    )
    .await
    .unwrap()
}
async fn event(rx: &mut mpsc::UnboundedReceiver<Value>) -> Value {
    timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("heartbeat did not publish")
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_publishes_without_input_gates_accounting_and_never_arms_silence_itself() {
    let (mut call, mut events) = session(Clock::default()).await;
    // Many 50 ms heartbeats run, but the one-second accounting gate is not due.
    assert!(
        timeout(Duration::from_millis(250), events.recv())
            .await
            .is_err()
    );
    let first = event(&mut events).await;
    assert_eq!(first["kind"], "duration_threshold");
    assert_eq!(first["threshold_id"], "first");
    assert!(first["elapsed_sec"].as_f64().unwrap() >= 1.);
    assert_eq!(first["seq"], 1);
    let second = event(&mut events).await;
    assert_eq!(second["threshold_id"], "second");
    assert_eq!(second["seq"], 2);
    assert_eq!(call.snapshot().await["clocks"]["sil_idle"], 0.);
    assert!(
        timeout(Duration::from_millis(150), events.recv())
            .await
            .is_err()
    );
    call.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_clock_origin_does_not_share_pause_state_and_close_stops_only_its_heartbeat() {
    let clock = Clock::default();
    let ((mut a, mut ae), (mut b, mut be)) = tokio::join!(session(clock.clone()), session(clock));
    a.pause_silence().await;
    a.pause_silence().await;
    let (av, bv) = tokio::join!(event(&mut ae), event(&mut be));
    assert_eq!(av["seq"], 1);
    assert_eq!(bv["seq"], 1);
    assert_eq!(a.snapshot().await["clocks"]["pause_depth"], 2);
    assert_eq!(b.snapshot().await["clocks"]["pause_depth"], 0);
    a.close().await.unwrap();
    // close aborts timer tasks; give cancellation a scheduler turn before snapshot.
    tokio::task::yield_now().await;
    let stopped = a.snapshot().await["clocks"].clone();
    let next = event(&mut be).await;
    assert_eq!(next["threshold_id"], "second");
    assert_eq!(a.snapshot().await["clocks"], stopped);
    assert!(
        ae.try_recv().is_err(),
        "closed call emitted a later threshold"
    );
    assert_eq!(
        b.snapshot().await["clocks"]["dur_fired"],
        json!(["first", "second"])
    );
    b.close().await.unwrap();
}
