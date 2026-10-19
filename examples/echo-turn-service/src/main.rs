//! Standalone echo Turn API. Speech recognition and synthesis belong to the gateway.
use std::{collections::HashMap, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use vogon_contracts::{EndCallRequest, ProcessEventsRequest, StartCallRequest, TransportEvent};

/// One call's speculative text, audio numbering and request replay cache.
/// Counters and cached replies are never reused by another call.
#[derive(Default)]
struct Call {
    prepared: Option<(String, String)>,
    next_audio: u64,
    responses: HashMap<String, (ProcessEventsRequest, Value)>,
}

impl Call {
    /// Apply a batch in order. Eager speech prepares text; only final speech emits audio.
    /// An identical request retry returns the original audio IDs without a new turn.
    fn process(&mut self, request: ProcessEventsRequest) -> Result<Value, StatusCode> {
        if request.request_id.trim().is_empty() {
            return Err(StatusCode::UNPROCESSABLE_ENTITY);
        }
        if let Some((previous, response)) = self.responses.get(&request.request_id) {
            return if previous == &request {
                Ok(response.clone())
            } else {
                Err(StatusCode::CONFLICT)
            };
        }
        let mut speak = Vec::new();
        for event in &request.events {
            match event {
                TransportEvent::UtteranceEager(event) => {
                    self.prepared = Some((event.text.clone(), echo(&event.text)));
                }
                TransportEvent::UtteranceResumed(_) => self.prepared = None,
                TransportEvent::UtteranceFinal(event) => {
                    let prepared = self.prepared.take();
                    if event.text.trim().is_empty() {
                        continue;
                    }
                    let answer = prepared
                        .filter(|(text, _)| text == &event.text)
                        .map(|(_, answer)| answer)
                        .unwrap_or_else(|| echo(&event.text));
                    self.next_audio += 1;
                    speak.push(json!({
                        "audio_id": format!("{}-echo-{}", request.call_id, self.next_audio),
                        "text": answer,
                    }));
                }
                // Delivery, timer and other facts require no action in this demo.
                _ => {}
            }
        }
        let response = json!({"speak": speak});
        self.responses
            .insert(request.request_id.clone(), (request, response.clone()));
        Ok(response)
    }
}

/// Build the application reply without calling a model or speech provider.
fn echo(text: &str) -> String {
    format!("You said: {text}")
}

/// Demo process state. The short lock protects in-memory mutations only: handlers
/// perform no provider/network work while holding it. EndCall removes call state.
struct App {
    authorization: String,
    calls: Mutex<HashMap<String, Call>>,
}

impl App {
    /// Require the configured demo bearer token on the Turn API endpoints.
    fn authorize(&self, headers: &HeaderMap) -> Result<(), StatusCode> {
        if headers.get("authorization").and_then(|h| h.to_str().ok())
            == Some(self.authorization.as_str())
        {
            Ok(())
        } else {
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}

/// Construct a router usable by the binary and the offline HTTP contract tests.
fn router(token: String) -> Router {
    Router::new()
        .route("/health", get(|| async { Json(json!({"ok": true})) }))
        .route("/turn/v1/start_call", post(start_call))
        .route("/turn/v1/process_events", post(process_events))
        .route("/turn/v1/end_call", post(end_call))
        .with_state(Arc::new(App {
            authorization: format!("Bearer {token}"),
            calls: Mutex::new(HashMap::new()),
        }))
}

/// Initialize a call idempotently, preserving active state on a retried StartCall.
/// Disable optional app-model features and timers to keep the echo flow explicit.
async fn start_call(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<StartCallRequest>,
) -> Result<Json<Value>, StatusCode> {
    app.authorize(&headers)?;
    if request.call_id.trim().is_empty() {
        return Err(StatusCode::UNPROCESSABLE_ENTITY);
    }
    app.calls.lock().await.entry(request.call_id).or_default();
    Ok(Json(json!({"call_config": {
        "persona_voice": "aura-2-thalia-en",
        "language": "en",
        "ambience": {"enabled": false},
        "barge_policy": {"semantic_judge": false},
        "silence_thresholds": [],
        "opening_silence_thresholds": [],
        "duration_thresholds": [],
    }})))
}

/// Return echo commands for an existing call, with request-ID replay protection.
async fn process_events(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<ProcessEventsRequest>,
) -> Result<Json<Value>, StatusCode> {
    app.authorize(&headers)?;
    let mut calls = app.calls.lock().await;
    let call = calls
        .get_mut(&request.call_id)
        .ok_or(StatusCode::NOT_FOUND)?;
    call.process(request).map(Json)
}

/// Release speculative text and cached responses; repeated EndCall is harmless.
async fn end_call(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(request): Json<EndCallRequest>,
) -> Result<Json<Value>, StatusCode> {
    app.authorize(&headers)?;
    app.calls.lock().await.remove(&request.call_id);
    Ok(Json(json!({})))
}

/// Bind the demo listener and stop on Ctrl-C. No dotenv or provider keys are needed.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind = std::env::var("ECHO_BIND").unwrap_or_else(|_| "127.0.0.1:8090".into());
    let token = std::env::var("ECHO_AUTH_TOKEN").unwrap_or_else(|_| "echo-demo".into());
    if token.trim().is_empty() {
        return Err("ECHO_AUTH_TOKEN must not be empty".into());
    }
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    println!(
        "Echo Turn service: http://{}/turn/v1",
        listener.local_addr()?
    );
    axum::serve(listener, router(token))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
