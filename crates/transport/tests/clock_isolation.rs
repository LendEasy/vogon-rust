//! Multiple producers may tick an engine; only one elapsed-time interval is charged.
use serde_json::json;
use std::sync::{Arc, Barrier, Mutex};
use vogon_clocks::call::{Config, Threshold};
use vogon_transport::engine::{Engine, Mode};
fn engine() -> Engine {
    let mut e = Engine::new(
        Mode::Text,
        Config {
            silence_thresholds: vec![Threshold {
                id: "probe".into(),
                after_sec: 1.,
            }],
            duration_thresholds: vec![Threshold {
                id: "limit".into(),
                after_sec: 2.,
            }],
            ..Default::default()
        },
        Default::default(),
        Default::default(),
        1.,
    );
    e.clocks.playout_end(true);
    e
}
#[test]
fn parallel_tickers_on_one_engine_do_not_advance_other_calls_or_duplicate_thresholds() {
    let a = Arc::new(Mutex::new(engine()));
    let b = engine();
    let before = json!(&b.clocks);
    let barrier = Arc::new(Barrier::new(16));
    let jobs = (0..16)
        .map(|_| {
            let a = a.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..1000 {
                    let mut a = a.lock().unwrap();
                    a.tick(2.);
                    a.tick(3.);
                    a.tick(2.5);
                }
            })
        })
        .collect::<Vec<_>>();
    for job in jobs {
        job.join().unwrap();
    }
    let a = a.lock().unwrap();
    assert_eq!(a.clocks.sil_idle, 2.);
    assert_eq!(a.events.len(), 2);
    assert_eq!(a.events[0]["seq"], 1);
    assert_eq!(a.events[1]["seq"], 2);
    assert_eq!(json!(b.clocks), before);
}
#[test]
fn identical_threshold_names_event_sequences_and_playback_counters_are_call_local() {
    let mut a = engine();
    let mut b = engine();
    a.clocks.pause_silence();
    a.clocks.playout_start(true);
    a.tick(3.);
    assert_eq!(b.clocks.pause_depth, 0);
    assert_eq!(b.clocks.playing_depth, 0);
    assert!(b.events.is_empty());
    b.tick(3.);
    assert_eq!(b.events.len(), 2);
    assert_eq!(b.events[0]["seq"], 1);
    assert_eq!(a.events.len(), 1);
    assert_eq!(a.events[0]["seq"], 1);
}
