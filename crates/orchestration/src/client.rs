//! Native Turn API triad. One HTTP mutation at a time per call preserves server ordering.
use crate::{Error, Future, Result, boxed};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use vogon_contracts::{
    EndCallRequest, EndCallResponse, ProcessEventsRequest, ProcessEventsResponse, StartCallRequest,
    StartCallResponse, codec,
};
/// Call-local API owner. Eager starts speculation on the application server;
/// final/resumed consume or invalidate it there. No gateway-side business logic.
pub trait TurnApi: Send + Sync + 'static {
    /// Start successfully before constructing transport from returned CallConfig.
    fn start(&self, request: StartCallRequest) -> Future<'_, StartCallResponse>;
    /// Submit facts first, then one trigger, with a stable replay ID.
    fn process(&self, request: ProcessEventsRequest) -> Future<'_, ProcessEventsResponse>;
    /// Flush remaining facts and finalize once; implementations may retry the same body.
    fn end(&self, request: EndCallRequest) -> Future<'_, EndCallResponse>;
}
/// Validated deployment/per-call binding, including the FULL .../turn/v1 root.
#[derive(Clone)]
pub struct Config {
    pub base_url: String,
    pub api_key: String,
    pub timeout: Duration,
    pub retry_delays: Vec<Duration>,
    pub epoch: f64,
}
impl Config {
    /// Resolve per-call binding before deployment defaults; never fall back to in-process Python.
    pub fn resolve(settings: &Value, call: &Value, epoch: f64) -> Result<Self> {
        let binding = &call["turn_service"];
        let pick = |a: &Value, b: &Value| {
            a.as_str()
                .filter(|s| !s.is_empty())
                .or_else(|| b.as_str())
                .unwrap_or("")
                .to_owned()
        };
        let base_url = pick(&binding["base_url"], &settings["turn_service_url"]);
        if base_url.is_empty() {
            return Err(Error::new("turn_service_not_configured"));
        }
        let timeout = settings["turn_service_timeout_sec"].as_f64().unwrap_or(40.);
        if !timeout.is_finite() || timeout <= 0. || !epoch.is_finite() {
            return Err(Error::new("invalid_turn_timing"));
        }
        let parsed = settings["turn_retry_delays"]
            .as_str()
            .unwrap_or("0.25,1,2,4,8")
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| {
                s.trim()
                    .parse::<f64>()
                    .ok()
                    .filter(|v| v.is_finite() && *v >= 0.)
            })
            .collect::<Option<Vec<_>>>()
            .unwrap_or_else(|| vec![0.25, 1., 2., 4., 8.]);
        Ok(Self {
            base_url,
            api_key: pick(&binding["auth_token"], &settings["turn_service_api_key"]),
            timeout: Duration::from_secs_f64(timeout),
            retry_delays: parsed.into_iter().map(Duration::from_secs_f64).collect(),
            epoch,
        })
    }
}
/// HTTP pool plus call-local serialization; never share this ordering lock between calls.
pub struct HttpTurnClient {
    config: Config,
    http: reqwest::Client,
    lock: Mutex<()>,
    started: AtomicBool,
    retry: Option<vogon_resilience::Policy>,
    metrics: Option<std::sync::Arc<vogon_observability::Metrics>>,
}
impl HttpTurnClient {
    /// Create a client with no automatic redirects/retries. Deployment HTTP is allowed for internal services.
    pub fn new(mut config: Config) -> Result<Self> {
        let url =
            reqwest::Url::parse(&config.base_url).map_err(|_| Error::new("invalid_turn_url"))?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::new("invalid_turn_url"));
        }
        config.base_url = config.base_url.trim_end_matches('/').into();
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| Error::new("turn_client_failed"))?;
        Ok(Self {
            config,
            http,
            lock: Mutex::new(()),
            started: AtomicBool::new(false),
            retry: None,
            metrics: None,
        })
    }
    /// Install the host retry policy; request bodies and replay IDs remain stable.
    pub fn with_retry(mut self, policy: vogon_resilience::Policy) -> Self {
        self.retry = Some(policy);
        self
    }
    /// Install aggregate latency/error metrics without sharing per-call serialization.
    pub fn with_metrics(mut self, metrics: std::sync::Arc<vogon_observability::Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }
    /// True only after a valid start response; failed starts are finalized server-side.
    pub fn started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }
    async fn post<T: serde::de::DeserializeOwned>(
        &self,
        operation: &str,
        body: Value,
        response: &str,
        end: bool,
    ) -> Result<T> {
        let start = std::time::Instant::now();
        let result = self.post_inner(operation, body, response, end).await;
        if let Some(metrics) = &self.metrics {
            metrics.turn(start.elapsed(), result.is_ok());
        }
        result
    }
    async fn post_inner<T: serde::de::DeserializeOwned>(
        &self,
        operation: &str,
        mut body: Value,
        response: &str,
        end: bool,
    ) -> Result<T> {
        if let Some(events) = body["events"].as_array() {
            body["events"] = json!(
                codec::shift_event_times(events, -self.config.epoch)
                    .map_err(|_| Error::new("invalid_event_time"))?
            );
        }
        let _ordered = self.lock.lock().await;
        let attempts = if let Some(policy) = &self.retry {
            policy.attempts
        } else if end {
            2
        } else {
            self.config.retry_delays.len() + 1
        };
        for attempt in 0..attempts {
            let request = self
                .http
                .post(format!("{}/{operation}", self.config.base_url))
                .json(&body)
                .timeout(self.config.timeout);
            let request = if self.config.api_key.is_empty() {
                request
            } else {
                request.bearer_auth(&self.config.api_key)
            };
            let operation = async {
                let mut r = request
                    .send()
                    .await
                    .map_err(|_| Error::new("turn_service_unavailable"))?;
                if r.status().as_u16() >= 500
                    || (self.retry.is_some() && matches!(r.status().as_u16(), 408 | 429))
                {
                    return Err(Error::new("turn_service_unavailable"));
                }
                if !r.status().is_success() {
                    return Err(Error::new("turn_contract_http_error"));
                }
                let mut bytes = vec![];
                while let Some(chunk) = r
                    .chunk()
                    .await
                    .map_err(|_| Error::new("turn_service_unavailable"))?
                {
                    if bytes.len() + chunk.len() > 4 * 1024 * 1024 {
                        return Err(Error::new("turn_response_too_large"));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                let value: Value =
                    serde_json::from_slice(&bytes).map_err(|_| Error::new("invalid_turn_json"))?;
                codec::from_wire(response, &value).map_err(|_| Error::new("invalid_turn_contract"))
            };
            let result = tokio::time::timeout(self.config.timeout, operation)
                .await
                .unwrap_or_else(|_| Err(Error::new("turn_service_unavailable")));
            match result {
                Err(e) if e.0 == "turn_service_unavailable" && attempt + 1 < attempts => {
                    let delay = self
                        .config
                        .retry_delays
                        .get(attempt)
                        .or_else(|| self.config.retry_delays.last())
                        .copied()
                        .unwrap_or(Duration::ZERO);
                    tokio::time::sleep(self.retry.as_ref().map_or(delay, |p| p.delay(attempt)))
                        .await;
                }
                other => return other,
            }
        }
        unreachable!()
    }
}
impl TurnApi for HttpTurnClient {
    fn start(&self, r: StartCallRequest) -> Future<'_, StartCallResponse> {
        boxed(async move {
            let result = self
                .post("start_call", json!(r), "StartCallResponse", false)
                .await?;
            self.started.store(true, Ordering::Release);
            Ok(result)
        })
    }
    fn process(&self, r: ProcessEventsRequest) -> Future<'_, ProcessEventsResponse> {
        boxed(async move {
            self.post("process_events", json!(r), "ProcessEventsResponse", false)
                .await
        })
    }
    fn end(&self, r: EndCallRequest) -> Future<'_, EndCallResponse> {
        boxed(async move {
            self.post("end_call", json!(r), "EndCallResponse", true)
                .await
        })
    }
}
