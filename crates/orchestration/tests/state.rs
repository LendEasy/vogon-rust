//! Pure event identity/revision policy, separate from socket and scheduler tests.
use serde_json::json;
use vogon_orchestration::state::Inbox;
fn event(kind: &str, text: &str, seq: u64) -> serde_json::Value {
    json!({"kind":kind,"text":text,"seq":seq,"at":seq as f64,"onset_at":1.})
}
#[test]
fn matching_final_retains_revision_but_changed_final_and_resume_invalidate_it() {
    for final_text in ["pay", "pay tomorrow", ""] {
        let mut s = Inbox::default();
        s.accept(event("utterance_eager", "pay", 1)).unwrap();
        let eager = s.revision;
        s.accept(event("utterance_final", final_text, 2)).unwrap();
        assert_eq!(s.revision == eager, final_text == "pay");
        let final_revision = s.revision;
        s.accept(event("utterance_resumed", "", 3)).unwrap();
        assert!(s.revision > final_revision);
    }
}
#[test]
fn repeated_phrase_is_a_new_turn_when_its_timestamps_change() {
    let mut s = Inbox::default();
    let first = event("utterance_final", "yes", 1);
    s.accept(first.clone()).unwrap();
    assert!(!s.accept(first).unwrap());
    let second = event("utterance_final", "yes", 2);
    assert!(s.accept(second).unwrap());
    assert_eq!(s.queue.len(), 2);
    assert_eq!(s.revision, 2);
    assert_eq!(s.duplicates, 1);
}
#[test]
fn duplicate_callbacks_without_sequence_ids_and_repeated_eager_are_not_new_turns() {
    let mut s = Inbox::default();
    let e = event("utterance_eager", "yes", 0);
    s.accept(e.clone()).unwrap();
    assert!(!s.accept(e).unwrap());
    let f = event("utterance_final", "yes", 0);
    s.accept(f.clone()).unwrap();
    assert!(!s.accept(f).unwrap());
    assert_eq!(s.queue.len(), 2);
    assert_eq!(s.duplicates, 2);
}
#[test]
fn facts_do_not_create_turns_and_machine_verdict_is_sticky_and_call_local() {
    let mut a = Inbox::default();
    let b = Inbox::default();
    for kind in [
        "delivery_outcome",
        "playout_finished",
        "drain_cleared",
        "escalation_outcome",
    ] {
        assert!(a.accept(event(kind, "", 0)).unwrap());
    }
    a.accept(json!({"kind":"line_verdict","answered_by":"machine"}))
        .unwrap();
    a.accept(json!({"kind":"line_verdict","answered_by":"human"}))
        .unwrap();
    assert!(a.machine);
    assert!(!b.machine);
    assert!(a.queue.is_empty());
    assert_eq!(a.revision, 0);
}
#[test]
fn replayed_sequences_never_enqueue_twice_across_exhaustive_eager_resume_final_traces() {
    // 3^7 ordered traces, each callback delivered twice; no random scheduling dependency.
    for mut choice in 0..2187 {
        let mut s = Inbox::default();
        for seq in 1..=7 {
            let kind = ["utterance_eager", "utterance_resumed", "utterance_final"][choice % 3];
            choice /= 3;
            let e = event(kind, &format!("words {seq}"), seq);
            assert!(s.accept(e.clone()).unwrap());
            assert!(!s.accept(e).unwrap());
        }
        assert_eq!(s.queue.len(), 7);
        assert_eq!(s.duplicates, 7);
    }
}
#[test]
fn nullable_phone_history_round_trips_through_typed_turn_contracts() {
    for kind in ["utterance_eager", "utterance_final"] {
        let raw =
            json!({"kind":kind,"text":"hello","confidence":1.,"low_conf_words":[],"heard":null});
        let event: vogon_contracts::TransportEvent =
            vogon_contracts::codec::from_wire("TransportEvent", &raw).unwrap();
        assert!(json!(event)["heard"].is_null());
    }
}
