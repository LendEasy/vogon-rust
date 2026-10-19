//! Management routes, signed Twilio callbacks and bounded media upgrades.
use crate::{
    host::Service,
    record::{Record, State, valid_id},
    store::Change,
    twilio,
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State as AppState, WebSocketUpgrade},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

/// Stable public errors never contain provider bodies, credentials or internal URLs.
pub struct Failure(pub StatusCode, pub &'static str);
impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"detail":self.1}))).into_response()
    }
}
type HttpResult<T> = std::result::Result<T, Failure>;
/// Map backend failures to a stable public category without exposing connection URLs.
fn storage(_: crate::Error) -> Failure {
    Failure(StatusCode::SERVICE_UNAVAILABLE, "session store unavailable")
}
/// Authenticate the trusted application/gateway pair; tenant identity remains request data.
fn authorize(s: &Service, h: &HeaderMap) -> HttpResult<()> {
    let key = s.config.gateway["service_key"].as_str().unwrap_or("");
    if !key.trim().is_empty() {
        let header = h
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");
        let candidate = header.strip_prefix("Bearer ").unwrap_or(header).trim();
        if !twilio::equal(candidate, key) {
            return Err(Failure(StatusCode::UNAUTHORIZED, "bad service key"));
        }
    }
    Ok(())
}
/// Preserve the configured external proxy prefix; otherwise use the direct Host header.
fn request_base(s: &Service, h: &HeaderMap) -> String {
    let base = s.config.gateway["public_base_url"].as_str().unwrap_or("");
    if base.is_empty() {
        format!(
            "http://{}",
            h.get("host")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("localhost")
        )
    } else {
        base.trim_end_matches('/').into()
    }
}
/// Rebuild the externally registered callback URL before HMAC validation.
fn verify(s: &Service, h: &HeaderMap, uri: &Uri, form: &[(String, String)]) -> HttpResult<()> {
    let url = format!(
        "{}{}",
        request_base(s, h),
        uri.path_and_query().map(|p| p.as_str()).unwrap_or("/")
    );
    if twilio::signed(
        s.config.settings["twilio_auth_token"]
            .as_str()
            .unwrap_or(""),
        &url,
        form,
        h.get("x-twilio-signature")
            .and_then(|v| v.to_str().ok())
            .unwrap_or(""),
    ) {
        Ok(())
    } else {
        Err(Failure(StatusCode::FORBIDDEN, "bad Twilio signature"))
    }
}
/// Construct the embeddable route tree; host startup owns the injected dependencies.
pub fn router(service: Arc<Service>) -> Router {
    let limit = service.config.max_message_bytes;
    Router::new()
        .route("/health", get(health))
        .route("/v1/tenants/{tenant}/calls", post(create))
        .route("/v1/tenants/{tenant}/calls/{call_id}", get(inspect))
        .route("/v1/twilio/twiml", get(stream_twiml).post(stream_twiml))
        .route("/v1/twilio/status", post(status))
        .route("/ws/call", get(ws_call))
        .route("/ws/twilio", get(ws_twilio))
        .route(
            "/v1/tenants/{tenant}/calls/{call_id}/{artifact}",
            get(artifact),
        )
        .route("/v1/escalation/{*rest}", get(escalation).post(escalation))
        .layer(DefaultBodyLimit::max(limit))
        .with_state(service)
}
/// Report basic liveness and socket ownership, not native/provider readiness.
async fn health(AppState(s): AppState<Arc<Service>>) -> Json<Value> {
    Json(
        json!({"ok":!s.is_stopping(),"service":"voice-gateway","capabilities":{},"active_sockets":s.active()}),
    )
}
/// Application HTTP endpoint, authorization and optional physical-status webhook.
#[derive(Deserialize)]
struct App {
    base_url: String,
    #[serde(default)]
    auth_token: String,
    #[serde(default)]
    status_webhook: String,
}
/// Public dial request; accept both Python field spellings for the source number.
#[derive(Deserialize)]
struct Dial {
    to: String,
    #[serde(default, rename = "from", alias = "from_")]
    from: String,
}
/// Typed creation request; the application payload remains opaque JSON object data.
#[derive(Deserialize)]
struct Create {
    mode: String,
    app: App,
    #[serde(default = "empty_object")]
    app_payload: Value,
    #[serde(default)]
    route: String,
    #[serde(default)]
    twilio: Option<Dial>,
    #[serde(default)]
    call_id: String,
}
/// Default opaque payload matches the Python request model.
fn empty_object() -> Value {
    json!({})
}
/// Accept absolute HTTP application bindings without credential or fragment ambiguity.
fn valid_http(s: &str) -> bool {
    url::Url::parse(s).is_ok_and(|u| {
        matches!(u.scheme(), "http" | "https")
            && u.host_str().is_some()
            && u.username().is_empty()
            && u.password().is_none()
            && u.fragment().is_none()
    })
}
/// Persist the call before optional provider dialing and return the attachment capability.
async fn create(
    AppState(s): AppState<Arc<Service>>,
    Path(tenant): Path<String>,
    h: HeaderMap,
    body: std::result::Result<Json<Create>, axum::extract::rejection::JsonRejection>,
) -> HttpResult<Json<Value>> {
    authorize(&s, &h)?;
    if s.is_stopping() {
        return Err(Failure(StatusCode::SERVICE_UNAVAILABLE, "service stopping"));
    }
    let Json(body) = body.map_err(|e| {
        if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Failure(StatusCode::PAYLOAD_TOO_LARGE, "call request too large")
        } else {
            Failure(StatusCode::UNPROCESSABLE_ENTITY, "invalid call request")
        }
    })?;
    if !matches!(body.mode.as_str(), "text" | "widget" | "twilio") {
        return Err(Failure(
            StatusCode::UNPROCESSABLE_ENTITY,
            "mode must be twilio|widget|text",
        ));
    }
    if body.mode == "twilio" && body.twilio.is_none() {
        return Err(Failure(
            StatusCode::UNPROCESSABLE_ENTITY,
            "twilio block required",
        ));
    }
    if !valid_id(&tenant) || (!body.call_id.is_empty() && !valid_id(&body.call_id)) {
        return Err(Failure(StatusCode::UNPROCESSABLE_ENTITY, "invalid call_id"));
    }
    if !body.app_payload.is_object()
        || !valid_http(&body.app.base_url)
        || (!body.app.status_webhook.is_empty() && !valid_http(&body.app.status_webhook))
    {
        return Err(Failure(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid application binding",
        ));
    }
    let record: Record = serde_json::from_value(json!({
        "tenant": tenant,
        "call_id": if body.call_id.is_empty() {
            format!("gwc-{}", Uuid::new_v4().simple())
        } else { body.call_id },
        "session_token": Uuid::new_v4().simple().to_string(),
        "mode": body.mode,
        "route": body.route,
        "app_base_url": body.app.base_url.trim_end_matches('/'),
        "app_auth_token": body.app.auth_token,
        "app_status_webhook": body.app.status_webhook,
        "app_payload": body.app_payload
    }))
    .unwrap();
    s.store.create(record.clone()).await.map_err(|e| {
        if e.0 == "session_exists" {
            Failure(StatusCode::CONFLICT, "call_id already exists")
        } else {
            storage(e)
        }
    })?;
    let base = request_base(&s, &h);
    let key = (record.tenant.clone(), record.call_id.clone());
    let mut provider = String::new();
    if record.mode == "twilio" {
        s.store
            .change(key.clone(), Change::Transition(State::Dialing))
            .await
            .map_err(storage)?;
        let dial = body.twilio.unwrap();
        let value = |key: &str| s.config.settings[key].as_str().unwrap_or("");
        match twilio::dial(
            &s.http,
            twilio::Dial {
                endpoint: &s.config.twilio_endpoint,
                account: value("twilio_account_sid"),
                token: value("twilio_auth_token"),
                to: &dial.to,
                from: if dial.from.is_empty() {
                    value("twilio_phone_number")
                } else {
                    &dial.from
                },
                base: &base,
                session_token: &record.session_token,
            },
        )
        .await
        {
            Ok(sid) => {
                s.store
                    .change(key.clone(), Change::Provider(sid.clone()))
                    .await
                    .map_err(storage)?;
                provider = sid;
            }
            Err(_) => {
                s.store
                    .change(key, Change::Transition(State::Failed))
                    .await
                    .map_err(storage)?;
                s.relay(&record, State::Failed).await;
                return Err(Failure(StatusCode::BAD_GATEWAY, "twilio dial failed"));
            }
        }
    }
    Ok(Json(
        json!({"call_id":record.call_id,"session_token":record.session_token,"ws_url":format!("{}/ws/call?token={}",twilio::ws_base(&base),record.session_token),"provider_call_id":provider}),
    ))
}
/// Return only the public physical state and timestamps for the requested tenant.
async fn inspect(
    AppState(s): AppState<Arc<Service>>,
    Path((tenant, id)): Path<(String, String)>,
    h: HeaderMap,
) -> HttpResult<Json<Value>> {
    authorize(&s, &h)?;
    let r = s
        .store
        .get((tenant, id))
        .await
        .map_err(storage)?
        .ok_or(Failure(StatusCode::NOT_FOUND, "not found"))?;
    Ok(Json(
        json!({"call_id":r.call_id,"state":r.state,"mode":r.mode,"route":r.route,"provider_call_id":r.provider_call_id,"created_at_utc":r.created_at_utc,"connected_at_utc":r.connected_at_utc,"ended_at_utc":r.ended_at_utc}),
    ))
}
/// Authorize the callback and resolve a live phone token to stream or hangup XML.
async fn stream_twiml(
    AppState(s): AppState<Arc<Service>>,
    Query(q): Query<HashMap<String, String>>,
    h: HeaderMap,
    uri: Uri,
    body: String,
) -> HttpResult<Response> {
    let token = q
        .get("token")
        .ok_or(Failure(StatusCode::UNPROCESSABLE_ENTITY, "token required"))?;
    let form = url::form_urlencoded::parse(body.as_bytes())
        .into_owned()
        .collect::<Vec<_>>();
    verify(&s, &h, &uri, &form)?;
    let valid = s
        .store
        .lookup(false, token.clone())
        .await
        .map_err(storage)?
        .is_some_and(|r| r.mode == "twilio" && !r.state.terminal());
    let xml = twilio::twiml(
        s.config.gateway["public_base_url"].as_str().unwrap_or(""),
        valid,
    );
    Ok(([("content-type", "application/xml")], xml).into_response())
}
/// Apply a monotonic provider status and relay only effective transitions.
async fn status(
    AppState(s): AppState<Arc<Service>>,
    h: HeaderMap,
    uri: Uri,
    body: String,
) -> HttpResult<Json<Value>> {
    let form = url::form_urlencoded::parse(body.as_bytes())
        .into_owned()
        .collect::<Vec<_>>();
    verify(&s, &h, &uri, &form)?;
    let get = |name: &str| {
        form.iter()
            .rev()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
            .unwrap_or("")
    };
    let Some(r) = s
        .store
        .lookup(true, get("CallSid").into())
        .await
        .map_err(storage)?
    else {
        return Ok(Json(json!({"ok":false})));
    };
    if twilio::state(get("CallStatus")).is_some_and(|state| state.terminal()) {
        s.transfers
            .caller_gone(get("CallSid"))
            .await
            .map_err(|_| Failure(StatusCode::SERVICE_UNAVAILABLE, "transfer callback failed"))?;
    }
    if let Some(state) = twilio::state(get("CallStatus"))
        && s.store
            .change(
                (r.tenant.clone(), r.call_id.clone()),
                Change::Transition(state),
            )
            .await
            .map_err(storage)?
    {
        s.relay(&r, state).await;
    }
    Ok(Json(json!({"ok":true})))
}
/// Register browser/text ownership before upgrading so shutdown includes the handshake.
async fn ws_call(
    AppState(s): AppState<Arc<Service>>,
    Query(q): Query<HashMap<String, String>>,
    upgrade: WebSocketUpgrade,
) -> HttpResult<Response> {
    let token = q
        .get("token")
        .ok_or(Failure(StatusCode::UNPROCESSABLE_ENTITY, "token required"))?
        .clone();
    let lease = s.register().map_err(|_| {
        Failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "session capacity unavailable",
        )
    })?;
    Ok(upgrade
        .max_message_size(s.config.max_message_bytes)
        .max_frame_size(s.config.max_message_bytes)
        .on_upgrade(move |socket| s.websocket(socket, lease, Some(token))))
}
/// Register a phone handshake whose start frame supplies the provider identity.
async fn ws_twilio(
    AppState(s): AppState<Arc<Service>>,
    upgrade: WebSocketUpgrade,
) -> HttpResult<Response> {
    let lease = s.register().map_err(|_| {
        Failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "session capacity unavailable",
        )
    })?;
    Ok(upgrade
        .max_message_size(s.config.max_message_bytes)
        .max_frame_size(s.config.max_message_bytes)
        .on_upgrade(move |socket| s.websocket(socket, lease, None)))
}
/// Authenticated tenant-scoped artifact retrieval, with S3 fallback and native timeline rendering.
async fn artifact(
    AppState(s): AppState<Arc<Service>>,
    Path((tenant, id, name)): Path<(String, String, String)>,
    h: HeaderMap,
) -> HttpResult<Response> {
    authorize(&s, &h)?;
    if s.store
        .get((tenant.clone(), id.clone()))
        .await
        .map_err(storage)?
        .is_none()
    {
        return Err(Failure(StatusCode::NOT_FOUND, "not found"));
    }
    if !matches!(
        name.as_str(),
        "recording.wav" | "audio-events.json" | "frames.csv" | "audio-timeline"
    ) {
        return Err(Failure(StatusCode::NOT_FOUND, "not found"));
    }
    let failure = |_| {
        Failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "artifact store unavailable",
        )
    };
    let (bytes, content_type) = if name == "audio-timeline" {
        let wav = s
            .recordings
            .get(&tenant, &id, "recording.wav")
            .await
            .map_err(failure)?
            .ok_or(Failure(StatusCode::NOT_FOUND, "not found"))?;
        let events = s
            .recordings
            .get(&tenant, &id, "audio-events.json")
            .await
            .map_err(failure)?
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
            .unwrap_or(json!({"meta":{},"events":[]}));
        let frames = s
            .recordings
            .get(&tenant, &id, "frames.csv")
            .await
            .map_err(failure)?;
        let html = tokio::task::spawn_blocking(move || {
            vogon_artifacts::timeline::render(&id, &wav, &events, frames.as_deref())
        })
        .await
        .map_err(|_| Failure(StatusCode::INTERNAL_SERVER_ERROR, "timeline failed"))?;
        (html.into_bytes(), "text/html; charset=utf-8")
    } else {
        let bytes = s
            .recordings
            .get(&tenant, &id, &name)
            .await
            .map_err(failure)?
            .ok_or(Failure(StatusCode::NOT_FOUND, "not found"))?;
        let kind = match name.as_str() {
            "recording.wav" => "audio/wav",
            "frames.csv" => "text/csv",
            _ => "application/json",
        };
        (bytes, kind)
    };
    Ok((
        [
            ("content-type", content_type),
            ("cache-control", "private, no-store"),
            ("x-content-type-options", "nosniff"),
        ],
        bytes,
    )
        .into_response())
}
/// Signed transfer callbacks; audio uses a short-lived unguessable capability token.
async fn escalation(
    AppState(s): AppState<Arc<Service>>,
    Path(rest): Path<String>,
    h: HeaderMap,
    uri: Uri,
    body: String,
) -> HttpResult<Response> {
    let Some((kind, id)) = rest.split_once('/') else {
        return Err(Failure(StatusCode::NOT_FOUND, "not found"));
    };
    if kind == "audio" {
        let token = id.strip_suffix(".mp3").unwrap_or("");
        let data = s
            .transfers
            .audio(token)
            .await
            .map_err(|_| {
                Failure(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "transfer store unavailable",
                )
            })?
            .ok_or(Failure(StatusCode::NOT_FOUND, "not found"))?;
        return Ok((
            [
                ("content-type", "audio/mpeg"),
                ("cache-control", "private, no-store"),
            ],
            data,
        )
            .into_response());
    }
    let form = url::form_urlencoded::parse(body.as_bytes())
        .into_owned()
        .collect::<Vec<_>>();
    verify(&s, &h, &uri, &form)?;
    let query = url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .into_owned()
        .collect::<Vec<_>>();
    let get = |name: &str| {
        form.iter()
            .chain(query.iter())
            .rev()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
            .unwrap_or("")
    };
    let attempt = get("attempt")
        .parse::<usize>()
        .map_err(|_| Failure(StatusCode::UNPROCESSABLE_ENTITY, "attempt required"))?;
    let leg = get("CallSid");
    let failure = |_: vogon_escalation::Error| {
        Failure(StatusCode::SERVICE_UNAVAILABLE, "transfer callback failed")
    };
    let xml=match kind {
        "whisper"=>s.transfers.whisper(id,leg,attempt).await.map_err(failure)?,
        "agent-keypress"=>s.transfers.keypress(id,leg,attempt,get("Digits")).await.map_err(failure)?,
        "agent-status"=>return Ok(Json(json!({"status":if s.transfers.status(id,leg,attempt,get("CallStatus")).await.map_err(failure)?{"ok"}else{"ignored"}})).into_response()),
        _=>return Err(Failure(StatusCode::NOT_FOUND,"not found")),
    };
    Ok(([("content-type", "application/xml")], xml).into_response())
}
