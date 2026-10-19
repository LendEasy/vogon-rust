//! Native Twilio REST, signed callback validation, and escaped stream TwiML.
use crate::{Error, Result, record::State};
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha1::Sha1;
use std::collections::{BTreeMap, BTreeSet};
use subtle::ConstantTimeEq;

/// Constant-time comparison for service credentials and provider signatures.
pub fn equal(a: &str, b: &str) -> bool {
    bool::from(a.as_bytes().ct_eq(b.as_bytes()))
}
/// Twilio signs URL followed by sorted parameter names and unique sorted values.
pub fn signature(token: &str, url: &str, params: &[(String, String)]) -> String {
    let mut mac =
        Hmac::<Sha1>::new_from_slice(token.as_bytes()).expect("HMAC accepts any key length");
    mac.update(url.as_bytes());
    let mut sorted: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for (k, v) in params {
        sorted.entry(k).or_default().insert(v);
    }
    for (k, values) in sorted {
        for v in values {
            mac.update(k.as_bytes());
            mac.update(v.as_bytes());
        }
    }
    STANDARD.encode(mac.finalize().into_bytes())
}
/// Accept signatures computed with or without the URL port, as the Python SDK does.
pub fn signed(token: &str, url: &str, params: &[(String, String)], header: &str) -> bool {
    if token.is_empty() {
        return true;
    }
    if equal(&signature(token, url, params), header) {
        return true;
    }
    let Ok(mut parsed) = url::Url::parse(url) else {
        return false;
    };
    let mut without = parsed.clone();
    let _ = without.set_port(None);
    if equal(&signature(token, without.as_str(), params), header) {
        return true;
    }
    let port = parsed.port_or_known_default().unwrap_or(443);
    // Url normalizes explicit default ports away, so insert the SDK's alternate spelling.
    let authority = parsed.host_str().unwrap_or("").to_owned();
    let _ = parsed.set_port(None);
    let alternative = parsed.as_str().replacen(
        &format!("://{authority}"),
        &format!("://{authority}:{port}"),
        1,
    );
    equal(&signature(token, &alternative, params), header)
}
/// Map provider callback vocabulary to the persisted transport lifecycle.
pub fn state(status: &str) -> Option<State> {
    Some(match status {
        "initiated" | "queued" => State::Dialing,
        "ringing" => State::Ringing,
        "in-progress" | "answered" => State::Connected,
        "completed" => State::Ended,
        "busy" | "no-answer" | "failed" => State::Failed,
        "canceled" => State::Cancelled,
        _ => return None,
    })
}
/// Escape an XML attribute so configured public URLs cannot break generated TwiML.
pub fn xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\'', "&apos;")
}
/// Existing stream/hangup response contract. Recording/transfers are separate modules.
pub fn twiml(base: &str, valid: bool) -> String {
    let body = if valid {
        format!(
            "<Connect><Stream url=\"{}/ws/twilio\"/></Connect>",
            xml(&ws_base(base))
        )
    } else {
        "<Say>This call cannot be completed.</Say><Hangup/>".into()
    };
    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Response>{body}</Response>")
}
/// Preserve public proxy prefixes when deriving WebSocket URLs.
pub fn ws_base(base: &str) -> String {
    base.trim_end_matches('/')
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1)
}
/// One outbound dial binding. Borrows credentials and deliberately omits Debug.
#[derive(Clone)]
pub struct Dial<'a> {
    pub endpoint: &'a str,
    pub account: &'a str,
    pub token: &'a str,
    pub to: &'a str,
    pub from: &'a str,
    pub base: &'a str,
    pub session_token: &'a str,
}
/// Retry explicit throttling only. Ambiguous failures may have already created a physical call.
pub async fn dial_with_retry(
    client: &reqwest::Client,
    request: Dial<'_>,
    retry: &vogon_resilience::Policy,
) -> Result<String> {
    retry
        .run(
            || dial(client, request.clone()),
            |e| e.0 == "twilio_throttled",
        )
        .await
}
/// Place a call using form-encoded REST and explicit webhook verbs; no SDK/Python.
pub async fn dial(client: &reqwest::Client, request: Dial<'_>) -> Result<String> {
    let Dial {
        endpoint,
        account,
        token,
        to,
        from,
        base,
        session_token,
    } = request;
    if account.is_empty() || token.is_empty() || from.is_empty() {
        return Err(Error::new("twilio_not_configured"));
    }
    let mut form = vec![
        ("To", to.to_owned()),
        ("From", from.to_owned()),
        (
            "Url",
            format!("{base}/v1/twilio/twiml?token={session_token}"),
        ),
        ("Method", "GET".into()),
        ("StatusCallback", format!("{base}/v1/twilio/status")),
        ("StatusCallbackMethod", "POST".into()),
    ];
    for event in ["initiated", "ringing", "answered", "completed"] {
        form.push(("StatusCallbackEvent", event.into()));
    }
    let response = client
        .post(format!(
            "{}/Accounts/{account}/Calls.json",
            endpoint.trim_end_matches('/')
        ))
        .basic_auth(account, Some(token))
        .form(&form)
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await
        .map_err(|_| Error::new("twilio_dial_failed"))?;
    if response.status().as_u16() == 429 {
        return Err(Error::new("twilio_throttled"));
    }
    if !response.status().is_success() {
        return Err(Error::new("twilio_dial_failed"));
    }
    let value: serde_json::Value = response
        .json()
        .await
        .map_err(|_| Error::new("twilio_dial_failed"))?;
    value["sid"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| Error::new("twilio_dial_failed"))
}
