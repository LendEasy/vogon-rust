//! Resource/lifecycle invariants specific to the native host interface. Policy
//! correctness is covered by the Python differential corpus, not copied here.
use vogon_barge_in::floor::{ClassifierResult, Config, Controller, Transport};

/// Build an isolated active-playback controller with a judge enabled and a fixed clock.
fn controller() -> Controller {
    Controller::new(
        Config::default(),
        Transport {
            tts_playing: true,
            semantic_judge: true,
            last_audio_id: Some("reply".into()),
            ..Transport::default()
        },
        100.0,
    )
}

/// Prove a timed-out physical call reserves its slot, only the latest revision dispatches
/// after draining, and wrong/duplicate tokens cannot take the floor.
#[test]
fn deadline_keeps_one_physical_request_and_duplicate_results_are_ignored() {
    let mut c = controller();
    c.frame(3000, 1200, 20.0, true);
    c.transcript("short fragment", false);
    let first = c.begin_classification().unwrap();
    c.now = first.deadline;
    c.classifier_deadline();
    c.transcript("second fragment", false);
    c.transcript("latest fragment", false);
    assert!(c.begin_classification().is_none());
    assert!(!c.classifier_result(first.token + 1, ClassifierResult::Error));
    assert!(c.classifier_result(first.token, ClassifierResult::Verdict("YIELD".into())));
    assert!(!c.caller_floor);
    let second = c.begin_classification().unwrap();
    assert_eq!(second.text, "latest fragment");
    assert_ne!(first.token, second.token);
    assert!(!c.classifier_result(first.token, ClassifierResult::Verdict("YIELD".into())));
    assert!(c.classifier_result(second.token, ClassifierResult::Error));
    assert!(!c.worker_active());
}

/// Prove bounded visible history does not free an episode still referenced by a
/// provider, and that draining the provider releases the evicted episode afterward.
#[test]
fn outstanding_requests_survive_history_eviction() {
    let mut c = controller();
    c.frame(3000, 1200, 20.0, true);
    c.transcript("short fragment", false);
    let request = c.begin_classification().unwrap();
    // While the remote call runs, acoustic episodes continue independently.
    for _ in 0..40 {
        c.now += 0.5;
        c.frame(0, 1200, 20.0, false);
        c.now += 0.5;
        c.frame(3000, 1200, 20.0, true);
    }
    assert_eq!(c.episodes().count(), 16);
    assert!(c.get_episode(request.key.0).is_some());
    c.classifier_deadline();
    assert!(c.begin_classification().is_none());
    c.classifier_result(request.token, ClassifierResult::Error);
    assert!(c.get_episode(request.key.0).is_none());
}

/// Prove overlapping final callbacks have independent deadlines and re-polling a
/// completed token cannot increment the revision twice or consume the other waiter.
#[test]
fn final_wait_tokens_are_independent_and_repolling_is_idempotent() {
    let mut c = controller();
    c.frame(3000, 1200, 20.0, true);
    let first = c.transcript("short fragment", true).unwrap();
    let request = c.begin_classification().unwrap();
    c.now += 0.1;
    let second = c.transcript("short fragment", true).unwrap();
    c.now = first.deadline;
    assert!(c.poll_final(&first));
    let revision = c.get_episode(first.episode_id).unwrap().revision;
    assert!(c.poll_final(&first));
    assert_eq!(c.get_episode(first.episode_id).unwrap().revision, revision);
    assert!(!c.poll_final(&second));
    c.classifier_result(request.token, ClassifierResult::Verdict("YIELD".into()));
    assert!(c.poll_final(&second));
    assert!(!c.caller_floor);
}

/// Prove shutdown removes queued dispatch and makes a late provider yield harmless.
#[test]
fn close_stops_dispatch_and_ignores_late_provider_completion() {
    let mut c = controller();
    c.frame(3000, 1200, 20.0, true);
    c.transcript("short fragment", false);
    let request = c.begin_classification().unwrap();
    c.transcript("latest fragment", false);
    c.close();
    assert!(!c.worker_active());
    assert!(c.begin_classification().is_none());
    assert!(!c.classifier_result(request.token, ClassifierResult::Verdict("YIELD".into())));
    assert!(!c.caller_floor);
}

/// Prove a later partial's new worker cannot extend the earlier final callback
/// once its original worker has completed.
#[test]
fn final_wait_belongs_to_original_worker_not_a_later_request() {
    let mut c = controller();
    c.frame(3000, 1200, 20.0, true);
    let wait = c.transcript("short fragment", true).unwrap();
    let first = c.begin_classification().unwrap();
    c.classifier_result(first.token, ClassifierResult::Verdict("CONTINUE".into()));
    c.transcript("different fragment", false);
    let second = c.begin_classification().unwrap();
    assert_ne!(first.token, second.token);
    assert!(c.worker_active());
    // The old worker finished; its EOT continuation must not wait again just
    // because a later partial started an unrelated worker before it ran.
    assert!(c.poll_final(&wait));
}
