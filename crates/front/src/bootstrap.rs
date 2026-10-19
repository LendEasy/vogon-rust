//! Construct clients from resolved settings without reading process-global secrets.
//! The service owns environment loading and artifact path resolution.
use crate::{
    Result,
    config::{Artifact, Settings},
    http::{Http, Key},
    jev, judge,
    source::{Source, State},
};
use serde_json::Value;
use vogon_clocks::Clock;

/// Reusable connection pools; front sources and tracing remain call-local.
#[derive(Clone, Default)]
pub struct Clients {
    pub jev: Option<jev::Client>,
    pub front: Option<judge::Client>,
    pub judge: Option<judge::Client>,
}
impl Clients {
    /// Apply Python's configured-key, OPEN_ROUTER_API_KEY, OPENROUTER_API_KEY precedence.
    /// An injected lookup avoids global environment mutation in tests and concurrent calls.
    pub fn from_settings(settings: &Value, env: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let text = |key: &str, default: &str| settings[key].as_str().unwrap_or(default).to_owned();
        let router = Some(text("open_router_api_key", ""))
            .filter(|s| !s.is_empty())
            .or_else(|| env("OPEN_ROUTER_API_KEY").filter(|s| !s.is_empty()))
            .or_else(|| env("OPENROUTER_API_KEY").filter(|s| !s.is_empty()));
        let jev = router
            .map(|key| {
                Ok(jev::Client {
                    http: Http::new(jev::ENDPOINT, Key::new(key)?)?,
                    model: text("jev_model", "typesafe/jev-1.13"),
                    timeout_ms: settings["jev_timeout_ms"].as_u64().unwrap_or(4000),
                    tracer: None,
                })
            })
            .transpose()?;
        let cerebras = text("cerebras_api_key", "");
        let (front, judge) = if cerebras.is_empty() {
            (None, None)
        } else {
            let http = Http::new(judge::ENDPOINT, Key::new(cerebras)?)?;
            (
                Some(judge::Client {
                    http: http.clone(),
                    model: text("front_model", "qwen-3.8-27b"),
                    timeout_ms: settings["front_beat_after_ms"]
                        .as_u64()
                        .unwrap_or(900)
                        .max(200),
                }),
                Some(judge::Client {
                    http,
                    model: text("cerebras_model", "gpt-oss-120b"),
                    timeout_ms: settings["semantic_barge_llm_timeout_ms"]
                        .as_u64()
                        .unwrap_or(700)
                        .max(50),
                }),
            )
        };
        Ok(Self { jev, front, judge })
    }
    /// Match existing source enablement, including its Cerebras-key prerequisite for Jev.
    /// Missing/invalid artifacts should be represented as None by the host after reporting the load error.
    pub fn source(
        &self,
        gateway: &Value,
        artifact: Option<Artifact>,
        identity: Identity<'_>,
        clock: Clock,
        seed: u64,
    ) -> Result<Option<Source>> {
        if gateway["front_model_enabled"] == false || self.front.is_none() {
            return Ok(None);
        }
        let Some(artifact) = artifact.filter(|a| !a.prompt.is_empty()) else {
            return Ok(None);
        };
        let state = State::new(
            artifact,
            Settings::from_gateway(gateway)?,
            identity.agent,
            identity.lender,
            identity.borrower,
            seed,
        );
        Ok(Some(Source::new(
            state,
            clock,
            self.jev.clone(),
            self.front.clone(),
        )))
    }
}
/// Only the allowed prompt identities; borrower names are withheld and guarded.
#[derive(Clone, Copy, Default)]
pub struct Identity<'a> {
    pub agent: &'a str,
    pub lender: &'a str,
    pub borrower: &'a str,
}
