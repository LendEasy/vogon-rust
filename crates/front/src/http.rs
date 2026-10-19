//! Pooled asynchronous JSON HTTP with explicit deadlines and redacted credentials.
use crate::{Error, Result};
use reqwest::{
    Client, Url,
    header::{AUTHORIZATION, HeaderValue},
};
use serde_json::Value;
use std::time::Duration;
/// Secret bearer credential; never serialized or exposed in Debug output.
#[derive(Clone)]
pub struct Key(String);
impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Key([redacted])")
    }
}
impl Key {
    /// Reject empty credentials; callers choose settings/environment precedence.
    pub fn new(value: String) -> Result<Self> {
        if value.trim().is_empty() {
            Err(Error::new("missing_api_key"))
        } else {
            Ok(Self(value))
        }
    }
    fn header(&self) -> Result<HeaderValue> {
        let mut h = HeaderValue::from_str(&format!("Bearer {}", self.0))
            .map_err(|_| Error::new("invalid_api_key"))?;
        h.set_sensitive(true);
        Ok(h)
    }
}
/// Reusable provider connection pool; cloning shares the pool, not request state.
#[derive(Clone)]
pub struct Http {
    client: Client,
    url: Url,
    key: Key,
    retry: Option<vogon_resilience::Policy>,
}
impl Http {
    /// Require HTTPS except loopback fixtures. Redirects and implicit retries are disabled.
    pub fn new(endpoint: &str, key: Key) -> Result<Self> {
        let url = Url::parse(endpoint).map_err(|_| Error::new("invalid_provider_url"))?;
        if url.scheme() != "https"
            && !(url.scheme() == "http"
                && matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")))
        {
            return Err(Error::new("provider_requires_tls"));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(Error::new("provider_url_credentials"));
        }
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| Error::new("provider_client_failed"))?;
        Ok(Self {
            client,
            url,
            key,
            retry: None,
        })
    }
    /// Set one retry owner for front/judge requests; the outer Jev loop then disables its legacy retries.
    pub fn with_retry(mut self, policy: vogon_resilience::Policy) -> Self {
        self.retry = Some(policy);
        self
    }
    /// Whether the host supplied a retry budget rather than legacy Jev retry semantics.
    pub fn retry_configured(&self) -> bool {
        self.retry.is_some()
    }
    /// Send with configured exponential retry; cancellation stops request and backoff.
    pub async fn post(&self, body: &Value, timeout: Duration) -> Result<Value> {
        match &self.retry {
            Some(policy) => {
                policy
                    .run(|| self.post_once(body, timeout), Error::retryable)
                    .await
            }
            None => self.post_once(body, timeout).await,
        }
    }
    /// Send one JSON request. Dropping the future cancels local request work.
    async fn post_once(&self, body: &Value, timeout: Duration) -> Result<Value> {
        let operation = async {
            let mut response = self
                .client
                .post(self.url.clone())
                .header(AUTHORIZATION, self.key.header()?)
                .json(body)
                .timeout(timeout)
                .send()
                .await
                .map_err(classify)?;
            if !response.status().is_success() {
                return Err(Error::http(response.status().as_u16()));
            }
            let mut bytes = vec![];
            while let Some(chunk) = response.chunk().await.map_err(classify)? {
                if bytes.len() + chunk.len() > 4 * 1024 * 1024 {
                    return Err(Error::new("provider_response_too_large"));
                }
                bytes.extend_from_slice(&chunk);
            }
            serde_json::from_slice(&bytes).map_err(|_| Error::new("invalid_provider_json"))
        };
        tokio::time::timeout(timeout + Duration::from_millis(500), operation)
            .await
            .map_err(|_| Error::new("provider_timeout"))?
    }
}
fn classify(e: reqwest::Error) -> Error {
    Error::new(if e.is_timeout() {
        "provider_timeout"
    } else if e.is_connect() {
        "provider_connect_failed"
    } else {
        "provider_request_failed"
    })
}
