//! Actual-send recording, independent capture owners and tenant-authorized artifact routes.
mod support;
use serde_json::json;
use vogon_artifacts::store::Stores;
use vogon_clocks::Clock;
use vogon_service::recording::{Capture, RecordingSink};
use vogon_transport::wire::{self, Frame, Observer, Sink};
struct Socket(bool);
impl Sink for Socket {
    fn send(
        &mut self,
        _: Frame,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = vogon_speech_providers::Result<()>> + Send + '_>,
    > {
        Box::pin(async move {
            if self.0 {
                Ok(())
            } else {
                Err(vogon_speech_providers::Error::new("fixture_send_failed"))
            }
        })
    }
}
#[tokio::test]
async fn failed_sends_are_not_recorded_and_finish_is_once() {
    let path = std::env::temp_dir().join(format!("vogon-capture-{}", uuid::Uuid::new_v4()));
    let stores = Stores::new(path.clone(), None);
    let c = Capture::new(
        stores.clone(),
        "a".into(),
        "call".into(),
        Clock::default(),
        true,
    );
    let mut sink = RecordingSink {
        next: Box::new(Socket(false)),
        capture: c.clone(),
    };
    assert!(sink.send(wire::media("MS", &[0; 160])).await.is_err());
    assert!(c.finish("done").await.is_empty());
    assert!(
        stores
            .get("a", "call", "recording.wav")
            .await
            .unwrap()
            .is_none()
    );
    c.input(
        0.,
        &json!({"event":"media","media":{"payload":"AAAA","timestamp":"0"}}),
    );
    assert!(c.finish("duplicate").await.is_empty());
}
#[tokio::test]
async fn successful_capture_isolated_and_disabled_policy_has_no_artifacts() {
    let path = std::env::temp_dir().join(format!("vogon-capture-{}", uuid::Uuid::new_v4()));
    let stores = Stores::new(path.clone(), None);
    let a = Capture::new(
        stores.clone(),
        "a".into(),
        "call".into(),
        Clock::default(),
        true,
    );
    let b = Capture::new(
        stores.clone(),
        "b".into(),
        "call".into(),
        Clock::default(),
        true,
    );
    b.configure(false);
    for c in [&a, &b] {
        c.input(0., &json!({"event":"start"}));
        c.input(
            0.,
            &json!({"event":"media","media":{"payload":"AAAA","timestamp":"0"}}),
        );
    }
    let mut sink = RecordingSink {
        next: Box::new(Socket(true)),
        capture: a.clone(),
    };
    sink.send(wire::media("MS", &[255; 160])).await.unwrap();
    let (ea, eb) = tokio::join!(a.finish("done"), b.finish("done"));
    assert!(ea.is_empty() && eb.is_empty());
    assert!(
        stores
            .get("a", "call", "recording.wav")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        stores
            .get("b", "call", "recording.wav")
            .await
            .unwrap()
            .is_none()
    );
    tokio::fs::remove_dir_all(path).await.unwrap();
}
#[tokio::test]
async fn artifact_routes_require_auth_and_tenant_record() {
    let mut config = support::config();
    let path = std::env::temp_dir().join(format!("vogon-route-{}", uuid::Uuid::new_v4()));
    config.settings["recordings_path"] = json!(path);
    let gateway = support::Gateway::start(config).await;
    gateway
        .create("call", "text", "http://127.0.0.1:1/turn/v1")
        .await;
    gateway
        .service
        .recordings
        .put(
            "acme",
            "call",
            "recording.wav",
            b"RIFF".to_vec(),
            "audio/wav",
        )
        .await;
    let client = reqwest::Client::new();
    let url = format!("{}/v1/tenants/acme/calls/call", gateway.url);
    assert_eq!(
        client
            .get(format!("{url}/recording.wav"))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        &b"RIFF"[..]
    );
    assert_eq!(
        client
            .get(format!("{url}/audio-timeline"))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .get(format!(
                "{}/v1/tenants/other/calls/call/recording.wav",
                gateway.url
            ))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let mut cfg = support::config();
    cfg.gateway["service_key"] = json!("secret");
    let protected = support::Gateway::start(cfg).await;
    assert_eq!(
        client
            .get(format!(
                "{}/v1/tenants/acme/calls/call/recording.wav",
                protected.url
            ))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    tokio::fs::remove_dir_all(path).await.unwrap();
}
