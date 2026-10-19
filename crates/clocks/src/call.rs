//! Duration/silence ladders and playout watchdog ported from CallClocks.
//! No sleeping or I/O: the host owns one ticker and supplies monotonic seconds.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// One named timer rung, sorted by elapsed threshold within a ladder.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Threshold {
    pub id: String,
    pub after_sec: f64,
}
/// Timer policy supplied from CallConfig. Filler text selection belongs to the host.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub silence_thresholds: Vec<Threshold>,
    pub opening_silence_thresholds: Vec<Threshold>,
    pub duration_thresholds: Vec<Threshold>,
    pub silence_ladder: String,
}
/// Explicit hold grace replaces ordinary silence thresholds; exit remains 15s later.
pub fn silence_deadline(id: &str, ordinary: f64, hold: f64) -> f64 {
    if hold > 0.0 {
        hold + if id == "exit" { 15.0 } else { 0.0 }
    } else {
        ordinary
    }
}

/// One call's accumulated idle time, nested pauses and outstanding playout deadlines.
#[derive(Debug, Serialize)]
pub struct CallClocks {
    pub ladders: BTreeMap<String, Vec<Threshold>>,
    pub ladder_name: String,
    pub duration: Vec<Threshold>,
    pub start_t: f64,
    pub last_tick_t: f64,
    pub dur_fired: BTreeSet<String>,
    pub sil_armed: bool,
    pub sil_idle: f64,
    pub sil_fired: BTreeSet<String>,
    pub pause_depth: usize,
    pub paused_since: Option<f64>,
    pub grace: f64,
    pub playing_depth: usize,
    pub playing_since: Option<f64>,
    pub audio_ms: Option<f64>,
    pub watch_committed: Option<f64>,
    pub playing_is_turn: bool,
}
impl CallClocks {
    /// Sort thresholds and initialize an unarmed silence window at monotonic `now` seconds.
    pub fn new(mut config: Config, now: f64) -> Self {
        config
            .silence_thresholds
            .sort_by(|a, b| a.after_sec.total_cmp(&b.after_sec));
        config
            .opening_silence_thresholds
            .sort_by(|a, b| a.after_sec.total_cmp(&b.after_sec));
        config
            .duration_thresholds
            .sort_by(|a, b| a.after_sec.total_cmp(&b.after_sec));
        let ladders = BTreeMap::from([
            ("default".into(), config.silence_thresholds),
            ("opening".into(), config.opening_silence_thresholds),
        ]);
        let ladder_name = if ladders
            .get(&config.silence_ladder)
            .is_some_and(|v| !v.is_empty())
        {
            config.silence_ladder
        } else {
            "default".into()
        };
        Self {
            ladders,
            ladder_name,
            duration: config.duration_thresholds,
            start_t: now,
            last_tick_t: now,
            dur_fired: BTreeSet::new(),
            sil_armed: false,
            sil_idle: 0.0,
            sil_fired: BTreeSet::new(),
            pause_depth: 0,
            paused_since: None,
            grace: 0.0,
            playing_depth: 0,
            playing_since: None,
            audio_ms: None,
            watch_committed: None,
            playing_is_turn: false,
        }
    }
    /// Reset accumulated silence, fired silence rungs and explicit hold grace.
    /// Duration thresholds and the selected ladder remain unchanged.
    pub fn caller_activity(&mut self) {
        self.sil_idle = 0.0;
        self.sil_fired.clear();
        self.grace = 0.0;
    }
    /// Switch to a nonempty named ladder and clear fired rungs, preserving idle time.
    /// Unknown or empty ladders leave the current selection unchanged.
    pub fn select_ladder(&mut self, name: &str) {
        if name != self.ladder_name && self.ladders.get(name).is_some_and(|v| !v.is_empty()) {
            self.ladder_name = name.into();
            self.sil_fired.clear();
        }
    }
    /// Replace the silence deadline with nonnegative hold seconds; exit is 15 seconds later.
    pub fn grant_grace(&mut self, seconds: f64) {
        self.grace = seconds.max(0.0);
    }
    /// Add one nested silence pause. Duration events and the playout watchdog still run.
    pub fn pause_silence(&mut self) {
        self.pause_depth += 1;
    }
    /// Release one pause without underflow; the last release clears its watchdog anchor.
    pub fn resume_silence(&mut self) {
        self.pause_depth = self.pause_depth.saturating_sub(1);
        if self.pause_depth == 0 {
            self.paused_since = None;
        }
    }
    fn newest_deadline(&self) -> Option<f64> {
        let anchor = self.playing_since.unwrap_or(self.last_tick_t);
        if anchor == 0.0 {
            return None;
        }
        Some(anchor + self.audio_ms.map_or(90.0, |ms| ms / 1000.0 + 10.0))
    }
    /// Begin another outstanding playout and preserve any older watchdog deadline.
    /// `is_turn` determines whether watchdog completion resets the silence window.
    pub fn playout_start(&mut self, is_turn: bool) {
        if self.playing_depth > 0
            && let Some(earlier) = self.newest_deadline()
        {
            self.watch_committed = Some(self.watch_committed.unwrap_or(earlier).max(earlier));
        }
        self.playing_depth += 1;
        self.playing_is_turn = is_turn;
        self.playing_since = None;
        self.audio_ms = None;
    }
    /// Record the current playout duration in milliseconds for watchdog expiry.
    /// This is a duration update, not confirmation that the caller heard the audio.
    pub fn playout_sent(&mut self, audio_ms: f64) {
        self.audio_ms = Some(audio_ms);
    }
    /// Release one playout, arm silence detection and optionally reset its window.
    /// Overlapping playout keeps the watchdog active until the final release.
    pub fn playout_end(&mut self, reset_window: bool) {
        self.playing_depth = self.playing_depth.saturating_sub(1);
        if self.playing_depth == 0 {
            self.playing_since = None;
            self.audio_ms = None;
            self.watch_committed = None;
        }
        self.sil_armed = true;
        if reset_window {
            self.sil_idle = 0.0;
            self.sil_fired.clear();
        }
    }
    /// Emit each named threshold once per window, matching duration-before-silence order.
    /// Ignore a late tick sampled before the last applied tick. Rewinding this
    /// anchor would count that interval twice on the next current tick.
    pub fn tick(&mut self, now: f64) -> Vec<Value> {
        if now < self.last_tick_t {
            return vec![];
        }
        let dt = now - self.last_tick_t;
        self.last_tick_t = now;
        let elapsed = now - self.start_t;
        let mut events = vec![];
        for threshold in &self.duration {
            if elapsed >= threshold.after_sec && self.dur_fired.insert(threshold.id.clone()) {
                events.push(json!({"kind":"duration_threshold","threshold_id":threshold.id,"elapsed_sec":elapsed,"at":now,"seq":0}));
            }
        }
        if self.playing_depth > 0 {
            if self.playing_since.is_none() {
                self.playing_since = Some(now);
            }
            let deadline = self
                .newest_deadline()
                .unwrap_or(f64::INFINITY)
                .max(self.watch_committed.unwrap_or(f64::NEG_INFINITY));
            if now >= deadline {
                self.playing_depth = 0;
                self.playing_since = None;
                self.audio_ms = None;
                self.watch_committed = None;
                self.sil_armed = true;
                if self.playing_is_turn {
                    self.sil_idle = 0.0;
                    self.sil_fired.clear();
                }
                self.playing_is_turn = false;
            }
        }
        if self.pause_depth > 0 {
            match self.paused_since {
                None => self.paused_since = Some(now),
                Some(start) if now - start >= 30.0 => {
                    self.pause_depth = 0;
                    self.paused_since = None;
                }
                _ => {}
            }
        }
        if self.sil_armed && self.playing_depth == 0 && self.pause_depth == 0 {
            self.sil_idle += dt;
            for threshold in &self.ladders[&self.ladder_name] {
                if self.sil_idle >= silence_deadline(&threshold.id, threshold.after_sec, self.grace)
                    && self.sil_fired.insert(threshold.id.clone())
                {
                    events.push(json!({"kind":"silence_threshold","threshold_id":threshold.id,"silent_for_sec":self.sil_idle,"at":now,"seq":0}));
                }
            }
        }
        events
    }
}
