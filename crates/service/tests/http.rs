mod support;
use serde_json::{Value, json};
use support::*;
use vogon_service::{record::Record, store::Change, twilio};

#[tokio::test]
async fn authentication_validation_tenant_scope_duplicates_and_secret_redaction() {
    let mut cfg = config();
    cfg.gateway["service_key"] = json!("pair-secret");
    cfg.gateway["public_base_url"] = json!("https://public.example/gw");
    let gateway = Gateway::start(cfg).await;
    let client = reqwest::Client::new();
    let path = format!("{}/v1/tenants/acme/calls", gateway.url);
    let good = json!({"mode":"text","call_id":"app-call","app":{"base_url":"http://127.0.0.1:9/turn/v1","auth_token":"never-expose"}});
    for key in [None, Some("bad")] {
        let mut r = client.post(&path).json(&good);
        if let Some(k) = key {
            r = r.bearer_auth(k)
        }
        assert_eq!(r.send().await.unwrap().status(), 401);
    }
    for bad in [
        json!({}),
        json!({"mode":"wrong","app":{"base_url":"http://app"}}),
        json!({"mode":"twilio","app":{"base_url":"http://app"}}),
        json!({"mode":"text","call_id":"../bad","app":{"base_url":"http://app"}}),
        json!({"mode":"text","app":{"base_url":" "}}),
        json!({"mode":"text","app":{"base_url":"file:///tmp/x"}}),
        json!({"mode":"text","app":{"base_url":"http://app"},"app_payload":[]}),
    ] {
        assert_eq!(
            client
                .post(&path)
                .bearer_auth("pair-secret")
                .json(&bad)
                .send()
                .await
                .unwrap()
                .status(),
            422
        );
    }
    let made: Value = client
        .post(&path)
        .bearer_auth("pair-secret")
        .json(&good)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        made["ws_url"]
            .as_str()
            .unwrap()
            .starts_with("wss://public.example/gw/ws/call?token=")
    );
    assert_eq!(
        client
            .post(&path)
            .bearer_auth("pair-secret")
            .json(&good)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    let body = client
        .get(format!("{path}/app-call"))
        .bearer_auth("pair-secret")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!body.contains("never-expose"));
    assert!(!body.contains(made["session_token"].as_str().unwrap()));
    assert_eq!(
        client
            .get(format!("{}/v1/tenants/other/calls/app-call", gateway.url))
            .bearer_auth("pair-secret")
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        client
            .get(format!("{path}/app-call/recording.wav"))
            .bearer_auth("pair-secret")
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    gateway.service.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_http_creates_have_one_winner() {
    let gateway = Gateway::start(config()).await;
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let url = gateway.url.clone();
        jobs.spawn(async move{reqwest::Client::new().post(format!("{url}/v1/tenants/acme/calls")).json(&json!({"mode":"text","call_id":"race","app":{"base_url":"http://app/turn/v1"}})).send().await.unwrap().status().as_u16()});
    }
    let mut codes = vec![];
    while let Some(v) = jobs.join_next().await {
        codes.push(v.unwrap())
    }
    assert_eq!(codes.iter().filter(|&&n| n == 200).count(), 1);
    assert_eq!(codes.iter().filter(|&&n| n == 409).count(), 15);
}

#[tokio::test]
async fn signed_callbacks_preserve_proxy_prefix_and_do_not_reopen_ended_calls() {
    let mut cfg = config();
    cfg.gateway["public_base_url"] = json!("https://public.example/gw");
    cfg.settings["twilio_auth_token"] = json!("secret");
    let gateway = Gateway::start(cfg).await;
    let r = Record::decode(
        r#"{"tenant":"acme","call_id":"c","session_token":"token","mode":"twilio"}"#,
    )
    .unwrap();
    gateway.service.store.create(r).await.unwrap();
    gateway
        .service
        .store
        .change(("acme".into(), "c".into()), Change::Provider("CA1".into()))
        .await
        .unwrap();
    let client = reqwest::Client::new();
    let twiml = "/v1/twilio/twiml?token=token";
    assert_eq!(
        client
            .get(format!("{}{twiml}", gateway.url))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let sig = twilio::signature("secret", &format!("https://public.example/gw{twiml}"), &[]);
    let xml = client
        .get(format!("{}{twiml}", gateway.url))
        .header("x-twilio-signature", sig)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(xml.contains("wss://public.example/gw/ws/twilio"));
    for status in ["ringing", "in-progress", "completed", "ringing", "answered"] {
        let form = vec![
            ("CallSid".into(), "CA1".into()),
            ("CallStatus".into(), status.into()),
        ];
        let sig = twilio::signature(
            "secret",
            "https://public.example/gw/v1/twilio/status",
            &form,
        );
        let r = client
            .post(format!("{}/v1/twilio/status", gateway.url))
            .header("x-twilio-signature", sig)
            .form(&form)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        assert_eq!(r.json::<Value>().await.unwrap()["ok"], true);
    }
    let r = gateway
        .service
        .store
        .get(("acme".into(), "c".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.state, vogon_service::record::State::Ended);
    assert!(r.ended_at_utc > 0.);
}

#[tokio::test]
async fn dial_uses_explicit_get_callback_forms_and_failure_marks_record_failed() {
    use axum::{Form, Router, extract::State, routing::post};
    use tokio::sync::mpsc;
    async fn dial(
        State(tx): State<mpsc::UnboundedSender<Vec<(String, String)>>>,
        Form(form): Form<Vec<(String, String)>>,
    ) -> axum::Json<Value> {
        tx.send(form).unwrap();
        axum::Json(json!({"sid":"CA-native"}))
    }
    let (tx, mut rx) = mpsc::unbounded_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/Accounts/account/Calls.json", post(dial))
                .with_state(tx),
        )
        .await
        .unwrap()
    });
    let mut cfg = config();
    cfg.twilio_endpoint = base;
    cfg.settings["twilio_account_sid"] = json!("account");
    cfg.settings["twilio_auth_token"] = json!("secret");
    cfg.settings["twilio_phone_number"] = json!("+1000");
    let gateway = Gateway::start(cfg).await;
    let r=reqwest::Client::new().post(format!("{}/v1/tenants/acme/calls",gateway.url)).json(&json!({"mode":"twilio","app":{"base_url":"http://app/turn/v1"},"twilio":{"to":"+2000"}})).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(
        r.json::<Value>().await.unwrap()["provider_call_id"],
        "CA-native"
    );
    let form = recv(&mut rx).await;
    assert!(form.contains(&("Method".into(), "GET".into())));
    assert!(form.contains(&("From".into(), "+1000".into())));
    assert_eq!(
        form.iter()
            .filter(|(k, _)| k == "StatusCallbackEvent")
            .count(),
        4
    );
    task.abort();
    let r=reqwest::Client::new().post(format!("{}/v1/tenants/acme/calls",gateway.url)).json(&json!({"mode":"twilio","call_id":"failed-dial","app":{"base_url":"http://app/turn/v1"},"twilio":{"to":"+2000"}})).send().await.unwrap();
    assert_eq!(r.status(), 502);
    assert_eq!(
        gateway
            .service
            .store
            .get(("acme".into(), "failed-dial".into()))
            .await
            .unwrap()
            .unwrap()
            .state,
        vogon_service::record::State::Failed
    );
}

#[test]
fn bootstrap_precedence_limits_and_invalid_store_fail_before_listening() {
    use vogon_service::bootstrap::Config;
    let c=Config::resolve(&json!({"GATEWAY_SERVICE_KEY":"env","DEEPGRAM_API_KEY":"env-provider","GATEWAY_MAX_ACTIVE_CALLS":"3"}),&json!({"GATEWAY_SERVICE_KEY":"file","DEEPGRAM_API_KEY":"file-provider","UNRELATED_SERVICE_KEY":"ignored"})).unwrap();
    assert_eq!(c.gateway["service_key"], "env");
    assert_eq!(c.settings["deepgram_api_key"], "env-provider");
    assert_eq!(c.max_active, 3);
    let alias =
        Config::resolve(&json!({}), &json!({"OPENROUTER_API_KEY":"local-alternate"})).unwrap();
    assert_eq!(alias.openrouter_fallback, "local-alternate");
    let host = vogon_service::host::Service::new(
        alias,
        std::sync::Arc::new(vogon_service::store::Memory::default()),
    )
    .unwrap();
    assert!(host.clients.jev.is_some());
    for env in [
        json!({"GATEWAY_MAX_ACTIVE_CALLS":"0"}),
        json!({"GATEWAY_SESSION_STORE":"unknown"}),
        json!({"GATEWAY_SESSION_STORE":"redis","REDIS_CLUSTER_MODE":"true"}),
    ] {
        assert!(Config::resolve(&env, &json!({})).is_err());
    }
}

#[tokio::test]
async fn body_limit_unknown_callbacks_and_status_relay_are_explicit() {
    use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
    use tokio::sync::mpsc;
    async fn relay(
        State(tx): State<mpsc::UnboundedSender<(String, Value)>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        tx.send((headers["authorization"].to_str().unwrap().to_owned(), body))
            .unwrap();
        Json(json!({}))
    }
    let (tx, mut rx) = mpsc::unbounded_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let webhook = format!("http://{}/status", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/status", post(relay)).with_state(tx),
        )
        .await
        .unwrap()
    });
    let mut cfg = config();
    cfg.max_message_bytes = 512;
    cfg.gateway["service_key"] = json!("pair-secret");
    let gateway = Gateway::start(cfg).await;
    let client = reqwest::Client::new();
    let r=client.post(format!("{}/v1/tenants/acme/calls",gateway.url)).bearer_auth("pair-secret").json(&json!({"mode":"text","app":{"base_url":"http://app"},"app_payload":{"big":"x".repeat(1024)}})).send().await.unwrap();
    assert_eq!(r.status(), 413);
    let record:Record=serde_json::from_value(json!({"tenant":"acme","call_id":"relay","session_token":"t-relay","mode":"twilio","app_status_webhook":webhook})).unwrap();
    gateway.service.store.create(record).await.unwrap();
    gateway
        .service
        .store
        .change(
            ("acme".into(), "relay".into()),
            Change::Provider("CA-relay".into()),
        )
        .await
        .unwrap();
    for (sid, status, ok) in [
        ("unknown", "completed", false),
        ("CA-relay", "unknown", true),
        ("CA-relay", "completed", true),
        ("CA-relay", "completed", true),
        ("CA-relay", "ringing", true),
    ] {
        let r = client
            .post(format!("{}/v1/twilio/status", gateway.url))
            .form(&[("CallSid", sid), ("CallStatus", status)])
            .send()
            .await
            .unwrap();
        assert_eq!(r.json::<Value>().await.unwrap()["ok"], ok);
    }
    let (auth, body) = recv(&mut rx).await;
    assert_eq!(auth, "Bearer pair-secret");
    assert_eq!(
        body,
        json!({"call_id":"relay","state":"ended","provider_call_id":"CA-relay"})
    );
    assert!(rx.try_recv().is_err());
    task.abort();
}
