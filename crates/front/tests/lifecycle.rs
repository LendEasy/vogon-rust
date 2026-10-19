//! Per-call ownership, stale generations and filler timing. Tokio time is injected.
use serde_json::json;
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::{
    sync::{Mutex, Semaphore},
    time::{self, Duration},
};
use vogon_clocks::Clock;
use vogon_front::{
    Error, Result,
    bootstrap::{Clients, Identity},
    config::{Artifact, Settings},
    hold::{Hold, Policy, Speaker, Timing},
    moves::Read,
    source::{Source, State},
};
use vogon_playback::delivery::DeliveryReport;

fn artifact() -> Artifact {
    Artifact::from_value(&json!({"prompt":"You are {agent_name} for {lender_name}.","beats":{"hold":["One moment."],"wait":["Still here."]},"tiers":{"beat":["I'm here."],"repeat":["Please bear with me."]},"move_criteria":{"ASSENT":"agree","REFUSAL":"decline"}}),12).unwrap()
}
fn state() -> State {
    State::new(
        artifact(),
        Settings {
            min_gap_sec: 0.,
            ..Default::default()
        },
        "Agent",
        "Bank",
        "Alex",
        4,
    )
}
fn source(s: State) -> Arc<Source> {
    Arc::new(Source::new(s, Clock::default(), None, None))
}
async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}
async fn advance(ms: u64) {
    time::advance(Duration::from_millis(ms)).await;
    settle().await;
}
fn read() -> Read {
    Read {
        movement: "ASSENT".into(),
        confidence: 0.9,
        ..Default::default()
    }
}
fn policy() -> Policy {
    Policy {
        first_after_ms: 100.,
        interval_ms: 200.,
        max: 3,
        phrases: vec!["Primary".into()],
        continuations: vec!["Next".into(), "Later".into()],
    }
}
struct Speech {
    sent: Mutex<Vec<(String, String)>>,
    gate: Option<Semaphore>,
    status: &'static str,
}
impl Speech {
    fn new(status: &'static str, blocked: bool) -> Arc<Self> {
        Arc::new(Self {
            sent: Mutex::new(vec![]),
            gate: blocked.then(|| Semaphore::new(0)),
            status,
        })
    }
}
impl Speaker for Speech {
    fn speak(
        &self,
        text: String,
        id: String,
    ) -> Pin<Box<dyn Future<Output = Result<Option<DeliveryReport>>> + Send + '_>> {
        Box::pin(async move {
            self.sent.lock().await.push((text, id.clone()));
            if let Some(gate) = &self.gate {
                gate.acquire().await.unwrap().forget();
            }
            if self.status == "error"
                || (self.status == "error_after_first" && self.sent.lock().await.len() > 1)
            {
                return Err(Error::new("fixture_speaker_failed"));
            }
            Ok(Some(
                serde_json::from_value(json!({"audio_id":id,"status":self.status})).unwrap(),
            ))
        })
    }
}

#[test]
fn generation_idempotence_cancel_and_stale_completion() {
    let mut s = state();
    assert!(s.prime("").is_none());
    let first = s.prime("hello").unwrap();
    assert!(s.prime("hello").is_none());
    let second = s.prime("new words").unwrap();
    assert!(!s.complete_read(first, read()));
    assert!(s.complete_read(second, read()));
    assert!(!s.complete_read(second, read()));
    let third = s.prime("more").unwrap();
    s.cancel();
    assert!(!s.complete_read(third, read()));
    assert!(s.result.is_none());
    s.settings.engine = "off".into();
    let previous = s.generation;
    assert!(s.prime("off text").is_none());
    assert!(s.generation > previous);
}
#[test]
fn deliberate_silence_distress_and_repeat_disabled() {
    let mut s = state();
    for rider in [
        "identity_data",
        "unintelligible",
        "closing",
        "plain_answer",
        "distress",
    ] {
        let token = s.prime(rider).unwrap();
        let mut r = read();
        r.riders.insert(rider.into(), 1.);
        s.complete_read(token, r);
        assert!(s.deliberate_silence, "{rider}");
        assert_eq!(s.result.as_deref(), Some(""));
    }
    let token = s.prime("still distressed").unwrap();
    s.complete_read(token, read());
    assert!(s.deliberate_silence);
    s.note_agent("Desk answer", false);
    assert!(!s.distress);
    s.settings.repeat_enabled = false;
    let token = s.prime("repeat").unwrap();
    let mut r = read();
    r.riders.insert("repeat_request".into(), 1.);
    s.complete_read(token, r);
    assert!(s.deliberate_silence);
}
#[test]
fn admission_gap_boundary_guards_and_played_ledger() {
    let mut s = state();
    s.settings.min_gap_sec = 6.;
    assert_eq!(
        s.admit("I'm here.", 10.),
        ("I'm here.".into(), "front-1".into())
    );
    assert!(s.lines_played.is_empty());
    assert!(s.admit("All right.", 15.999).0.is_empty());
    assert!(!s.admit("All right.", 16.).0.is_empty());
    s.note_played("I am here and listening closely.");
    assert!(
        s.admit("I am here and listening closely.", 22.)
            .0
            .is_empty()
    );
    assert!(s.admit("Thanks Alex.", 22.).0.is_empty());
    assert_eq!(s.lines_played, vec!["I am here and listening closely."]);
    assert_eq!(s.snapshot()["metrics"]["front_guard_gap"], 1);
}
#[test]
fn pool_rotation_has_no_repeats_until_exhausted_or_across_cycles() {
    let mut s = state();
    s.artifact
        .beats
        .insert("hold".into(), vec!["A".into(), "B".into(), "C".into()]);
    let mut last = String::new();
    for _ in 0..30 {
        let cycle = (0..3).map(|_| s.beat("hold")).collect::<Vec<_>>();
        assert_ne!(last, cycle[0]);
        assert_eq!(
            cycle
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            3
        );
        last = cycle[2].clone();
    }
    assert!(s.beat("missing").is_empty());
}
#[test]
fn model_json_empty_and_error_states() {
    for (value, metric) in [
        (Ok(json!({"say":"  I'm here.  "})), "front_generated"),
        (Ok(json!({"say":""})), "front_empty"),
        (Ok(json!({})), "front_empty"),
        (Ok(json!({"say":null})), "front_empty"),
        (Ok(json!({"say":3})), "front_error"),
        (Err(Error::new("provider_timeout")), "front_error"),
    ] {
        let mut s = state();
        let token = s.prime("test").unwrap();
        assert!(s.complete_text(token, value));
        assert_eq!(s.metrics[metric], 1);
    }
}
#[test]
fn artifact_rejects_whole_unsafe_pool_and_invalid_structure() {
    let a=Artifact::from_value(&json!({"prompt":"  hello  ","beats":{"hold":["One moment.","Pay $10."],"wait":["  Still here.  "," "]},"tiers":{"lookup":["Let me take a look."],"beat":["This is far too many words."]}}),12).unwrap();
    assert_eq!(a.prompt, "hello");
    assert!(!a.beats.contains_key("hold"));
    assert_eq!(a.beats["wait"], vec!["Still here."]);
    assert!(!a.tiers.contains_key("beat"));
    assert_eq!(a.rejected.len(), 2);
    assert!(Artifact::from_value(&json!({"beats":{"hold":[123]}}), 12).is_err());
    assert!(
        Artifact::load(
            std::path::Path::new("/nonexistent-vogon-front-artifact"),
            12
        )
        .is_err()
    );
}
#[test]
fn bootstrap_enablement_and_settings_validation() {
    let gateway = json!({"front_model_enabled":true,"front_engine":"jev","front_model_effort":"none","front_max_words":12,"front_min_gap_sec":6.,"front_move_floor":0.45,"front_repeat_enabled":true,"front_beat_after_ms":900,"front_beat_after_empty_ms":1500,"front_wait_again_after_ms":2500,"cerebras_api_key":"fixture"});
    let clients = Clients::from_settings(&gateway, |_| None).unwrap();
    assert!(
        clients
            .source(
                &gateway,
                Some(artifact()),
                Identity::default(),
                Clock::default(),
                1
            )
            .unwrap()
            .is_some()
    );
    assert!(
        clients
            .source(&gateway, None, Identity::default(), Clock::default(), 1)
            .unwrap()
            .is_none()
    );
    let mut disabled = gateway.clone();
    disabled["front_model_enabled"] = json!(false);
    assert!(
        clients
            .source(
                &disabled,
                Some(artifact()),
                Identity::default(),
                Clock::default(),
                1
            )
            .unwrap()
            .is_none()
    );
    assert!(
        Clients::default()
            .source(
                &gateway,
                Some(artifact()),
                Identity::default(),
                Clock::default(),
                1
            )
            .unwrap()
            .is_none()
    );
    assert!(Settings::from_gateway(&json!({})).is_err());
}
#[tokio::test(start_paused = true)]
async fn pending_deadline_and_empty_deadline_have_distinct_fallbacks() {
    let mut pending = state();
    pending.prime("pending");
    let src = source(pending);
    let copy = src.clone();
    let task = tokio::spawn(async move { copy.hold_line(0.9, Some(1.5)).await });
    settle().await;
    advance(899).await;
    assert!(!task.is_finished());
    advance(1).await;
    assert_eq!(task.await.unwrap().0, "One moment.");
    assert_eq!(src.snapshot().await["metrics"]["front_late"], 1);
    let mut empty = state();
    let t = empty.prime("empty").unwrap();
    empty.complete_text(t, Ok(json!({"say":""})));
    let src = source(empty);
    let copy = src.clone();
    let task = tokio::spawn(async move { copy.hold_line(0.9, Some(1.5)).await });
    settle().await;
    advance(900).await;
    assert!(!task.is_finished());
    advance(600).await;
    assert_eq!(task.await.unwrap().0, "One moment.");
}
#[tokio::test(start_paused = true)]
async fn supersession_and_cancel_withhold_old_waiters() {
    for empty in [false, true] {
        let mut s = state();
        let t = s.prime("old").unwrap();
        if empty {
            s.complete_text(t, Ok(json!({"say":""})));
        }
        let src = source(s);
        let copy = src.clone();
        let task = tokio::spawn(async move { copy.hold_line(1., Some(2.)).await });
        settle().await;
        src.cancel().await;
        assert!(task.await.unwrap().0.is_empty());
        assert_eq!(src.snapshot().await["metrics"]["front_beat_withheld"], 1);
    }
    let mut s = state();
    s.prime("old");
    let src = source(s);
    let copy = src.clone();
    let task = tokio::spawn(async move { copy.hold_line(1., Some(2.)).await });
    settle().await;
    src.prime("new").await;
    assert!(task.await.unwrap().0.is_empty());
    src.cancel().await;
}
#[tokio::test(start_paused = true)]
async fn reasoned_silence_never_falls_back_and_no_job_uses_beat() {
    let mut s = state();
    let t = s.prime("identity").unwrap();
    let mut r = read();
    r.riders.insert("identity_data".into(), 1.);
    s.complete_read(t, r);
    let src = source(s);
    assert!(src.hold_line(1., Some(2.)).await.0.is_empty());
    assert_eq!(
        src.snapshot().await["metrics"]["front_silent_on_purpose"],
        1
    );
    assert_eq!(
        source(state()).hold_line(1., Some(2.)).await.0,
        "One moment."
    );
}
#[tokio::test(start_paused = true)]
async fn missing_provider_fails_to_empty_then_fallback() {
    let src = source(state());
    src.prime("hello").await;
    settle().await;
    assert_eq!(src.snapshot().await["metrics"]["front_error"], 1);
    assert_eq!(src.hold_line(0., Some(0.)).await.0, "One moment.");
    src.cancel().await;
}
#[tokio::test(start_paused = true)]
async fn hold_disabled_duplicate_begin_early_end_and_canned_ids() {
    let speech = Speech::new("complete", false);
    let mut hold = Hold::default();
    assert!(!hold.begin(
        None,
        None,
        speech.clone(),
        Timing {
            beat_ms: 0,
            empty_ms: None,
            again_ms: 0
        },
        1
    ));
    assert!(hold.begin(
        Some(policy()),
        None,
        speech.clone(),
        Timing {
            beat_ms: 0,
            empty_ms: None,
            again_ms: 0
        },
        1
    ));
    assert!(hold.begin(
        Some(policy()),
        None,
        speech.clone(),
        Timing {
            beat_ms: 0,
            empty_ms: None,
            again_ms: 0
        },
        1
    ));
    assert_eq!(hold.end().await.unwrap().fillers_played, 0);
    assert!(speech.sent.lock().await.is_empty());
    hold.begin(
        Some(policy()),
        None,
        speech.clone(),
        Timing {
            beat_ms: 0,
            empty_ms: None,
            again_ms: 0,
        },
        1,
    );
    settle().await;
    advance(100).await;
    assert_eq!(
        speech.sent.lock().await[0],
        ("Primary".into(), "filler-1-1".into())
    );
    let report = hold.end().await.unwrap();
    assert_eq!(report.fillers_played, 1);
    assert_eq!(hold.end().await.unwrap(), report);
}
#[tokio::test(start_paused = true)]
async fn canned_primary_continuations_limit_empty_and_zero_max() {
    let speech = Speech::new("complete", false);
    let mut hold = Hold::default();
    hold.begin(
        Some(policy()),
        None,
        speech.clone(),
        Timing {
            beat_ms: 0,
            empty_ms: None,
            again_ms: 0,
        },
        1,
    );
    settle().await;
    advance(100).await;
    advance(200).await;
    advance(200).await;
    advance(200).await;
    assert_eq!(hold.end().await.unwrap().fillers_played, 3);
    let sent = speech.sent.lock().await;
    assert_eq!(sent[0].0, "Primary");
    assert_ne!(sent[1].0, sent[2].0);
    assert_eq!(sent[2].1, "filler-1-3");
    drop(sent);
    for p in [
        Policy {
            phrases: vec![],
            continuations: vec![],
            ..policy()
        },
        Policy { max: 0, ..policy() },
        Policy {
            max: -1,
            ..policy()
        },
    ] {
        let speech = Speech::new("complete", false);
        hold.begin(
            Some(p),
            None,
            speech.clone(),
            Timing {
                beat_ms: 0,
                empty_ms: None,
                again_ms: 0,
            },
            1,
        );
        settle().await;
        advance(500).await;
        assert_eq!(hold.end().await.unwrap().fillers_played, 0);
        assert!(speech.sent.lock().await.is_empty());
    }
}
#[tokio::test(start_paused = true)]
async fn source_hold_at_most_two_lines_and_records_actual_playback() {
    let src = source(state());
    let speech = Speech::new("complete", false);
    let mut hold = Hold::default();
    hold.begin(
        Some(policy()),
        Some(src.clone()),
        speech.clone(),
        Timing {
            beat_ms: 900,
            empty_ms: Some(1500),
            again_ms: 2500,
        },
        1,
    );
    settle().await;
    assert_eq!(speech.sent.lock().await.len(), 1);
    advance(2500).await;
    advance(10_000).await;
    assert_eq!(hold.end().await.unwrap().fillers_played, 2);
    assert_eq!(
        src.snapshot().await["lines_played"],
        json!(["One moment.", "Still here."])
    );
}
#[tokio::test(start_paused = true)]
async fn end_waits_for_inflight_speech_and_prevents_followup() {
    let speech = Speech::new("complete", true);
    let mut hold = Hold::default();
    hold.begin(
        Some(policy()),
        None,
        speech.clone(),
        Timing {
            beat_ms: 0,
            empty_ms: None,
            again_ms: 0,
        },
        1,
    );
    settle().await;
    advance(100).await;
    assert_eq!(speech.sent.lock().await.len(), 1);
    let task = tokio::spawn(async move { hold.end().await });
    settle().await;
    assert!(!task.is_finished());
    speech.gate.as_ref().unwrap().add_permits(1);
    let report = task.await.unwrap().unwrap();
    assert_eq!(report.fillers_played, 1);
    assert_eq!(speech.sent.lock().await.len(), 1);
}
#[tokio::test(start_paused = true)]
async fn truncated_speech_stops_canned_and_source_holds() {
    for src in [None, Some(source(state()))] {
        let speech = Speech::new("truncated", false);
        let mut hold = Hold::default();
        hold.begin(
            Some(policy()),
            src.clone(),
            speech.clone(),
            Timing {
                beat_ms: 0,
                empty_ms: None,
                again_ms: 200,
            },
            1,
        );
        settle().await;
        advance(100).await;
        advance(2000).await;
        let report = hold.end().await.unwrap();
        assert_eq!(report.fillers_played, 1);
        assert!(report.interrupted);
        if let Some(src) = src {
            assert_eq!(src.snapshot().await["metrics"]["front_played"], 1);
        }
    }
}
#[tokio::test(start_paused = true)]
async fn speaker_error_propagates_and_owner_drop_cancels_waiting() {
    let speech = Speech::new("error", false);
    let mut hold = Hold::default();
    hold.begin(
        Some(policy()),
        None,
        speech.clone(),
        Timing {
            beat_ms: 0,
            empty_ms: None,
            again_ms: 0,
        },
        1,
    );
    settle().await;
    advance(100).await;
    assert_eq!(hold.end().await.unwrap_err().code, "fixture_speaker_failed");
    let speech = Speech::new("complete", false);
    let mut hold = Hold::default();
    hold.begin(
        Some(policy()),
        None,
        speech.clone(),
        Timing {
            beat_ms: 0,
            empty_ms: None,
            again_ms: 0,
        },
        1,
    );
    drop(hold);
    advance(1000).await;
    assert!(speech.sent.lock().await.is_empty());
}
#[tokio::test(start_paused = true)]
async fn ending_during_model_wait_never_speaks() {
    let mut s = state();
    s.prime("waiting");
    let src = source(s);
    let speech = Speech::new("complete", false);
    let mut hold = Hold::default();
    hold.begin(
        None,
        Some(src),
        speech.clone(),
        Timing {
            beat_ms: 900,
            empty_ms: Some(1500),
            again_ms: 2500,
        },
        1,
    );
    settle().await;
    assert_eq!(hold.end().await.unwrap().fillers_played, 0);
    advance(2000).await;
    assert!(speech.sent.lock().await.is_empty());
}

#[test]
fn falsey_generated_json_matches_python_empty_selection() {
    for v in [
        json!(false),
        json!(0),
        json!(0.0),
        json!([]),
        json!({}),
        json!(null),
    ] {
        let mut s = state();
        let token = s.prime("test").unwrap();
        s.complete_text(token, Ok(json!({"say":v})));
        assert_eq!(s.metrics["front_empty"], 1);
        assert_eq!(s.result.as_deref(), Some(""));
    }
}
#[tokio::test(start_paused = true)]
async fn cancelled_end_future_keeps_ownership_of_inflight_task() {
    let speech = Speech::new("complete", true);
    let mut hold = Hold::default();
    hold.begin(
        Some(policy()),
        None,
        speech.clone(),
        Timing {
            beat_ms: 0,
            empty_ms: None,
            again_ms: 0,
        },
        1,
    );
    settle().await;
    advance(100).await;
    assert!(
        time::timeout(Duration::from_millis(10), hold.end())
            .await
            .is_err()
    );
    speech.gate.as_ref().unwrap().add_permits(1);
    assert_eq!(hold.end().await.unwrap().fillers_played, 1);
}

#[tokio::test(start_paused = true)]
async fn failed_followup_keeps_prior_delivery_count_on_repeated_end() {
    let speech = Speech::new("error_after_first", false);
    let mut hold = Hold::default();
    hold.begin(Some(policy()), None, speech, Timing::default(), 1);
    settle().await;
    advance(100).await;
    advance(200).await;
    assert_eq!(hold.end().await.unwrap_err().code, "fixture_speaker_failed");
    assert_eq!(hold.end().await.unwrap().fillers_played, 1);
}
