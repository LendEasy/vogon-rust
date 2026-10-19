//! Clocks are values, not process-wide counters. No timing/sleep assertions are needed.
use serde_json::json;
use std::sync::{Arc, Barrier};
use vogon_clocks::{
    Clock, OffsetEstimator,
    call::{CallClocks, Config, Threshold},
};

fn config() -> Config {
    Config {
        silence_thresholds: vec![Threshold {
            id: "silence".into(),
            after_sec: 5.,
        }],
        duration_thresholds: vec![Threshold {
            id: "duration".into(),
            after_sec: 20.,
        }],
        ..Default::default()
    }
}

#[test]
fn parallel_clock_reads_do_not_mutate_call_clocks_or_offset_counters() {
    let host = Clock::default();
    let clocks = CallClocks::new(config(), host.now());
    let offset = OffsetEstimator::default();
    let before = json!(&clocks);
    let before_offset = json!(&offset);
    std::thread::scope(|scope| {
        for _ in 0..16 {
            let host = host.clone();
            let clocks = &clocks;
            let offset = &offset;
            let before = &before;
            let before_offset = &before_offset;
            scope.spawn(move || {
                for _ in 0..1000 {
                    let _ = host.now();
                    let _ = offset.project_back(host.now());
                    assert_eq!(json!(clocks), *before);
                    assert_eq!(json!(offset), *before_offset);
                }
            });
        }
    });
    assert_eq!(json!(clocks), before);
}

fn trace(id: usize) -> serde_json::Value {
    let mut c = CallClocks::new(config(), 1.);
    let mut offset = OffsetEstimator::default();
    c.playout_end(true);
    for tick in 1..=100 {
        let now = 1. + tick as f64 * 0.25;
        if tick == 3 {
            for _ in 0..id {
                c.pause_silence();
            }
        }
        if tick == 12 {
            for _ in 0..id {
                c.resume_silence();
            }
        }
        if tick == 20 {
            for _ in 0..id {
                c.playout_start(id.is_multiple_of(2));
            }
            c.playout_sent(100. * id as f64);
        }
        if tick == 25 {
            for _ in 0..id {
                c.playout_end(true);
            }
        }
        if tick == 30 {
            c.grant_grace(id as f64);
        }
        c.tick(now);
        offset.observe(id as f64);
    }
    json!({"clock":c,"offset":offset})
}

#[test]
fn sixteen_parallel_call_traces_equal_their_isolated_runs() {
    let expected = (0..16).map(trace).collect::<Vec<_>>();
    let barrier = Arc::new(Barrier::new(16));
    let jobs = (0..16)
        .map(|id| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                trace(id)
            })
        })
        .collect::<Vec<_>>();
    for (id, job) in jobs.into_iter().enumerate() {
        assert_eq!(job.join().unwrap(), expected[id]);
        assert_eq!(expected[id]["offset"]["samples"], 100);
    }
}

#[test]
fn another_calls_ticks_pauses_playout_and_watchdogs_cannot_touch_idle_call() {
    let idle = CallClocks::new(config(), 1.);
    let before = json!(&idle);
    let mut busy = CallClocks::new(config(), 1.);
    busy.pause_silence();
    busy.playout_start(true);
    busy.playout_sent(100.);
    busy.tick(2.);
    busy.tick(200.);
    assert_eq!(busy.pause_depth, 0);
    assert_eq!(busy.playing_depth, 0);
    assert!(!busy.dur_fired.is_empty());
    assert_eq!(json!(idle), before);
}

#[test]
fn duplicate_or_late_tick_cannot_double_count_elapsed_time_or_refire_thresholds() {
    let mut c = CallClocks::new(config(), 1.);
    c.playout_end(true);
    assert_eq!(c.tick(6.).len(), 1);
    let before = json!(&c);
    assert!(c.tick(6.).is_empty());
    assert_eq!(json!(&c), before);
    assert!(c.tick(2.).is_empty());
    assert_eq!(json!(&c), before);
    assert!(c.tick(7.).is_empty());
    assert_eq!(c.sil_idle, 6.);
    assert_eq!(c.tick(21.).len(), 1);
    assert!(c.tick(21.).is_empty());
    assert_eq!(c.dur_fired.len(), 1);
}
