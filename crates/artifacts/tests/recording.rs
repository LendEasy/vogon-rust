//! Recorder edge cases independent of Python; differential gate covers sample arithmetic.
use serde_json::json;
use vogon_artifacts::{recorder::Recorder, store::Stores, timeline};
#[test]
fn disabled_policy_captures_nothing() {
    let mut r = Recorder::default();
    r.enabled = false;
    r.add(true, 0., &[0; 160]);
    r.event(0., "secret", json!({"text":"private"}));
    assert!(r.wav().unwrap().is_none());
    assert_eq!(r.event_json(json!({}))["events"], json!([]));
}
#[test]
fn bounds_and_nonfinite_clocks_cannot_allocate_unbounded_wav() {
    let mut r = Recorder::default();
    r.max_bytes = 160;
    r.max_seconds = 1.;
    r.add(true, 0., &[255; 160]);
    r.add(false, 100000., &[0]);
    r.add(false, f64::NAN, &[0]);
    r.add(false, 0., &[0]);
    assert!(r.truncated);
    assert_eq!(r.wav().unwrap().unwrap().len(), 44 + 160 * 4);
}
#[test]
fn clear_preserves_caller_and_trims_partial_agent() {
    let mut r = Recorder::default();
    r.add(true, 0., &[255; 160]);
    r.add(false, 0., &[0; 160]);
    assert_eq!(r.clear(0.01), 80);
    let wav = r.wav().unwrap().unwrap();
    assert!(wav[44 + 80 * 4..].iter().all(|b| *b == 0));
    assert_eq!(r.clear(0.01), 0);
}
#[test]
fn malformed_timestamp_falls_back_and_drift_reanchors() {
    let mut r = Recorder::default();
    assert_eq!(r.caller_timestamp(&json!("NaN"), 1.), 1.);
    assert_eq!(r.caller_timestamp(&json!(0), 1.), 1.);
    assert_eq!(r.caller_timestamp(&json!(20), 1.025), 1.02);
    assert_eq!(r.caller_timestamp(&json!(40), 2.), 2.);
    assert_eq!(r.reanchors, 1);
    assert_eq!(r.stream_missing, 1);
}
#[test]
fn timeline_escapes_untrusted_text() {
    let html = timeline::render(
        "</title><script>",
        b"RIFF",
        &json!({"meta":{},"events":[{"text":"</script><script>evil"}]}),
        None,
    );
    assert!(!html.contains("</script><script>evil"));
    assert!(html.contains("\\u003c/script\\u003e"));
    assert!(html.contains("&lt;/title&gt;"));
}
#[tokio::test]
async fn local_atomic_roundtrip_tenant_isolation_and_missing() {
    let root = std::env::temp_dir().join(format!("vogon-artifacts-{}", uuid::Uuid::new_v4()));
    let s = Stores::new(root.clone(), None);
    assert!(
        s.put("a", "call", "recording.wav", vec![1, 2], "audio/wav")
            .await
            .is_empty()
    );
    assert_eq!(
        s.get("a", "call", "recording.wav").await.unwrap(),
        Some(vec![1, 2])
    );
    assert_eq!(s.get("b", "call", "recording.wav").await.unwrap(), None);
    assert!(s.get("..", "call", "recording.wav").await.is_err());
    assert!(s.get("a", "call", "../secret").await.is_err());
    s.probe().await.unwrap();
    tokio::fs::remove_dir_all(root).await.unwrap();
}
#[tokio::test]
async fn local_failure_is_explicit() {
    let root = std::env::temp_dir().join(format!("vogon-artifact-file-{}", uuid::Uuid::new_v4()));
    tokio::fs::write(&root, b"file").await.unwrap();
    let s = Stores::new(root.clone(), None);
    assert_eq!(
        s.put("a", "call", "recording.wav", vec![1], "audio/wav")
            .await,
        vec!["artifact_directory_failed"]
    );
    tokio::fs::remove_file(root).await.unwrap();
}
