//! Real HTTP signature, callback and per-call extension integration checks.
mod support;
use serde_json::json;
use support::{Gateway, config};
use vogon_service::twilio;
#[tokio::test]
async fn callbacks_verify_signature_and_return_native_twiml() {
    let mut config = config();
    config.settings["twilio_auth_token"] = json!("secret");
    config.gateway["public_base_url"] = json!("https://gateway.example/gw");
    let gateway = Gateway::start(config).await;
    let client = reqwest::Client::new();
    let path = "/v1/escalation/whisper/CAcaller?attempt=0&CallSid=CAagent";
    assert_eq!(
        client
            .get(format!("{}{path}", gateway.url))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let signature = twilio::signature("secret", &format!("https://gateway.example/gw{path}"), &[]);
    let response = client
        .get(format!("{}{path}", gateway.url))
        .header("x-twilio-signature", signature)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.text().await.unwrap().contains("no longer waiting"));
    let path = "/v1/escalation/agent-status/CAcaller?attempt=0";
    let form = vec![
        ("CallSid".into(), "CAagent".into()),
        ("CallStatus".into(), "completed".into()),
    ];
    let signature = twilio::signature(
        "secret",
        &format!("https://gateway.example/gw{path}"),
        &form,
    );
    let response = client
        .post(format!("{}{path}", gateway.url))
        .header("x-twilio-signature", signature)
        .form(&form)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap(),
        json!({"status":"ignored"})
    );
    assert_eq!(
        client
            .get(format!(
                "{}/v1/escalation/audio/not-a-token.mp3",
                gateway.url
            ))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
}
