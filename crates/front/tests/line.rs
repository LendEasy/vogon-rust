//! Boundary and adversarial line-detector traces complement Python differential cases.
use vogon_front::line::{Config, Detector, Effect, Episode};
fn detector() -> Detector {
    Detector::new(
        Config {
            enabled: true,
            ..Default::default()
        },
        "Alex".into(),
    )
}
fn episode(id: u64, text: &str) -> Option<Episode> {
    Some(Episode {
        id,
        onset: 1.,
        text: text.into(),
        echo: false,
        possible_echo: false,
    })
}
const WORDS: &str = "please leave your message";
#[test]
fn thresholds_are_inclusive_and_only_machine_can_finish_on_interim() {
    for (p, expected) in [
        (0.6, "machine"),
        (0.3, "human"),
        (0.300001, "unknown"),
        (0.599999, "unknown"),
        (0., "human"),
        (1., "machine"),
    ] {
        for final_event in [false, true] {
            let mut d = detector();
            d.observe(episode(1, WORDS), final_event, "", 1.);
            let r = d.begin_request(1.).unwrap();
            assert!(d.complete(r.token, Some(p), 1.2));
            if final_event || expected == "machine" {
                assert_eq!(d.verdict.as_ref().unwrap()["answered_by"], expected);
            } else {
                assert!(d.verdict.is_none());
            }
        }
    }
}
#[test]
fn disabled_short_echo_and_missing_episode_do_not_bind() {
    let mut d = detector();
    d.observe(None, true, "", 1.);
    d.observe(episode(1, "too short"), true, "", 1.);
    let mut ep = episode(1, WORDS).unwrap();
    ep.echo = true;
    d.observe(Some(ep.clone()), true, "", 1.);
    ep.echo = false;
    ep.possible_echo = true;
    d.observe(Some(ep), true, "", 1.);
    d.observe(episode(1, WORDS), true, WORDS, 1.);
    assert!(!d.bound);
    assert!(d.begin_request(1.).is_none());
    d.config.enabled = false;
    d.observe(episode(1, WORDS), true, "", 1.);
    assert!(!d.bound);
}
#[test]
fn final_during_flight_uses_original_ask_and_original_episode() {
    let mut d = detector();
    d.playout_start(2.);
    d.observe(episode(7, WORDS), false, "", 2.);
    let r = d.begin_request(2.).unwrap();
    d.observe(
        episode(8, "please leave your message after the tone"),
        true,
        "",
        2.1,
    );
    assert!(d.begin_request(2.1).is_none());
    d.complete(r.token, Some(0.9), 2.2);
    let effects = d.drain_effects();
    assert!(matches!(effects[0], Effect::Breakthrough { episode: 7 }));
    let Effect::Event(ref verdict) = effects[1] else {
        panic!()
    };
    assert_eq!(verdict["transcript"], WORDS);
    let Effect::Event(ref final_event) = effects[2] else {
        panic!()
    };
    assert_eq!(final_event["onset_at"], 1.);
    assert_eq!(final_event["text"], WORDS);
    assert_eq!(d.onset_before_audio, Some(true));
    assert!(d.drain_effects().is_empty());
    assert!(!d.complete(r.token, Some(0.), 3.));
}
#[test]
fn pending_revisions_coalesce_and_identical_final_reuses_probability() {
    let mut d = detector();
    d.observe(episode(1, WORDS), false, "", 1.);
    let r = d.begin_request(1.).unwrap();
    for text in ["now this is newer", "now this is newest"] {
        d.observe(episode(2, text), false, "", 1.1);
    }
    d.complete(r.token, Some(0.1), 1.2);
    let second = d.begin_request(1.2).unwrap();
    assert_eq!(second.text, "now this is newest");
    d.complete(second.token, Some(0.1), 1.3);
    d.observe(episode(3, "now this is newest"), true, "", 1.4);
    assert!(d.begin_request(1.4).is_none());
    assert_eq!(d.asks, 2);
    assert_eq!(d.episode.as_ref().unwrap().id, 1);
    assert_eq!(d.verdict.as_ref().unwrap()["answered_by"], "human");
}
#[test]
fn failure_is_unknown_and_late_tokens_and_close_are_inert() {
    let mut d = detector();
    d.observe(episode(1, WORDS), true, "", 1.);
    let r = d.begin_request(1.).unwrap();
    assert!(!d.complete(r.token + 1, Some(1.), 2.));
    assert!(d.complete(r.token, None, 2.));
    assert_eq!(d.failures, 1);
    assert_eq!(d.verdict.as_ref().unwrap()["source"], "none");
    assert_eq!(d.verdict.as_ref().unwrap()["answered_by"], "unknown");
    let mut d = detector();
    d.observe(episode(1, WORDS), true, "", 1.);
    let r = d.begin_request(1.).unwrap();
    d.close();
    assert!(!d.complete(r.token, Some(1.), 2.));
    assert!(d.verdict.is_none());
    assert!(d.drain_effects().is_empty());
    assert!(d.begin_request(2.).is_none());
}
#[test]
fn opening_window_and_twenty_second_extension_do_not_retire_inflight() {
    let mut d = detector();
    d.playout_end(0.);
    assert!(d.retire_at.is_none());
    d.playout_start(1.);
    d.playout_end(2.);
    d.playout_end(3.);
    assert_eq!(d.retire_at, Some(4.));
    d.tick(3.999);
    assert!(d.verdict.is_none());
    d.tick(4.);
    assert_eq!(d.verdict.as_ref().unwrap()["source"], "none");
    let mut d = detector();
    d.playout_start(1.);
    d.playout_end(2.);
    d.observe(episode(1, WORDS), false, "", 2.);
    let r = d.begin_request(2.).unwrap();
    d.tick(100.);
    assert!(d.verdict.is_none());
    d.complete(r.token, Some(0.2), 3.);
    d.tick(23.999);
    assert!(d.verdict.is_none());
    d.tick(24.);
    assert_eq!(d.verdict.as_ref().unwrap()["answered_by"], "unknown");
}
#[test]
fn quiet_wait_final_then_quiet_cap_and_explicit_release() {
    let mut d = detector();
    assert_eq!(d.quiet_reason(1.), Some("no_verdict"));
    d.observe(episode(1, WORDS), false, "", 1.);
    let r = d.begin_request(1.).unwrap();
    d.complete(r.token, Some(0.9), 1.5);
    assert_eq!(d.quiet_reason(2.5), None);
    d.observe(episode(2, WORDS), true, "", 2.);
    assert_eq!(d.quiet_reason(2.699), None);
    assert_eq!(d.quiet_reason(2.701), Some("quiet"));
    assert_eq!(d.quiet_reason(9.5), Some("cap"));
    d.released = true;
    assert_eq!(d.quiet_reason(9.5), Some("released"));
}

#[test]
fn misheard_brand_partial_preserves_documented_python_echo_limitation() {
    let opening = "Hi, this is Maya with LendEasy Finance, calling on a recorded line. May I speak with Alex Morgan?";
    let partial = "Hi. This is Maya with lend easy finance calling";
    assert_eq!(
        vogon_front::line::strip_echo(partial, opening, 3),
        "lend easy finance calling"
    );
    let mut d = detector();
    d.observe(episode(1, partial), false, opening, 1.);
    assert_eq!(
        d.begin_request(1.).unwrap().text,
        "lend easy finance calling"
    );
}
