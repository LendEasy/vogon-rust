//! Boundary tests for the orthogonal duration, silence, pause and playout states.
//! Explicit timestamps test policy; Session integration tests test actual wakeups.
use serde_json::Value;
use vogon_clocks::call::{CallClocks, Config, Threshold};

fn rung(id: &str, after_sec: f64) -> Threshold {
    Threshold {
        id: id.into(),
        after_sec,
    }
}
fn clock() -> CallClocks {
    CallClocks::new(
        Config {
            silence_thresholds: vec![rung("probe", 2.), rung("exit", 5.)],
            opening_silence_thresholds: vec![rung("opening", 1.)],
            duration_thresholds: vec![rung("duration", 4.)],
            ..Default::default()
        },
        100.,
    )
}
fn ids(events: Vec<Value>) -> Vec<String> {
    events
        .into_iter()
        .map(|e| e["threshold_id"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn unarmed_silence_never_fires_but_duration_does_and_arming_starts_idle() {
    let mut c = clock();
    assert_eq!(ids(c.tick(104.)), ["duration"]);
    assert_eq!(c.sil_idle, 0.);
    c.playout_end(true);
    assert!(c.tick(105.).is_empty());
    assert_eq!(ids(c.tick(106.)), ["probe"]);
    assert!(c.tick(106.).is_empty());
}

#[test]
fn nested_pause_and_overlapping_playout_independently_block_silence() {
    let mut c = clock();
    c.playout_end(true);
    c.pause_silence();
    c.pause_silence();
    c.playout_start(true);
    c.playout_start(false);
    assert_eq!(ids(c.tick(104.)), ["duration"]);
    c.resume_silence();
    c.playout_end(false);
    assert!(c.tick(105.).is_empty());
    c.resume_silence();
    assert!(
        c.tick(106.).is_empty(),
        "remaining playout must still block silence"
    );
    c.playout_end(false);
    assert!(c.tick(107.).is_empty());
    assert_eq!(ids(c.tick(108.)), ["probe"]);
    c.resume_silence();
    assert_eq!(c.pause_depth, 0, "extra resume cannot underflow");
}

#[test]
fn activity_rearms_silence_and_clears_grace_without_refiring_duration() {
    let mut c = clock();
    c.playout_end(true);
    assert_eq!(ids(c.tick(105.)), ["duration", "probe", "exit"]);
    c.grant_grace(60.);
    c.caller_activity();
    assert_eq!(c.grace, 0.);
    assert!(c.tick(106.).is_empty());
    assert_eq!(ids(c.tick(107.)), ["probe"]);
    assert_eq!(ids(c.tick(110.)), ["exit"]);
}

#[test]
fn ladder_switch_preserves_idle_and_grace_exit_is_fifteen_seconds_later() {
    let mut c = clock();
    c.playout_end(true);
    assert!(c.tick(101.).is_empty());
    c.select_ladder("opening");
    assert_eq!(ids(c.tick(101.)), ["opening"]);
    c.select_ladder("missing");
    assert_eq!(c.ladder_name, "opening");
    c.select_ladder("default");
    c.grant_grace(10.);
    assert_eq!(ids(c.tick(109.)), ["duration"]);
    assert_eq!(ids(c.tick(110.)), ["probe"]);
    assert!(c.tick(124.).is_empty());
    assert_eq!(ids(c.tick(125.)), ["exit"]);
}

#[test]
fn pause_watchdog_expires_at_thirty_seconds_from_first_observation() {
    let mut c = clock();
    c.playout_end(true);
    c.pause_silence();
    c.pause_silence();
    c.tick(101.);
    c.tick(130.);
    assert_eq!(c.pause_depth, 2);
    assert_eq!(c.sil_idle, 0.);
    c.tick(131.);
    assert_eq!(c.pause_depth, 0);
    assert_eq!(c.paused_since, None);
    assert_eq!(
        c.sil_idle, 1.,
        "accounting uses the current tick interval on release"
    );
}

#[test]
fn playout_watchdogs_use_known_duration_or_ninety_seconds_and_keep_longest_overlap() {
    for (audio, budget) in [(Some(2000.), 12.), (None, 90.)] {
        let mut c = clock();
        c.playout_start(true);
        if let Some(ms) = audio {
            c.playout_sent(ms);
        }
        c.tick(101.);
        c.tick(101. + budget - 0.001);
        assert_eq!(c.playing_depth, 1);
        c.tick(101. + budget);
        assert_eq!(c.playing_depth, 0);
        assert!(c.sil_armed);
    }
    let mut c = clock();
    c.playout_start(true);
    c.playout_sent(100_000.);
    c.tick(101.);
    c.playout_start(true);
    c.playout_sent(1000.);
    c.tick(102.);
    c.tick(113.);
    assert_eq!(
        c.playing_depth, 2,
        "shorter overlapping audio cannot expire the older playout"
    );
    c.tick(211.);
    assert_eq!(c.playing_depth, 0);
}

#[test]
fn delayed_tick_emits_all_crossed_rungs_once_in_duration_then_silence_order() {
    let mut c = clock();
    c.playout_end(true);
    assert_eq!(ids(c.tick(110.)), ["duration", "probe", "exit"]);
    assert_eq!(c.sil_idle, 10.);
    assert!(c.tick(110.).is_empty());
    assert!(c.tick(109.).is_empty());
    assert!(c.tick(111.).is_empty());
    assert_eq!(c.sil_idle, 11.);
}
