//! Gateway-owned front responses, semantic judge clients and line detection.
//! Deterministic policy is separate from provider I/O and per-call task ownership.
pub mod bootstrap;
pub mod config;
pub mod guard;
pub mod hold;
pub mod http;
pub mod jev;
pub mod judge;
pub mod line;
pub mod moves;
pub mod source;
use serde::{Deserialize, Serialize};
/// Sanitized failure category; raw HTTP response text and credentials never escape.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Error {
    pub code: String,
    pub status: Option<u16>,
}
impl Error {
    /// Construct a trusted internal category, not arbitrary provider text.
    pub fn new(code: &str) -> Self {
        Self {
            code: code.into(),
            status: None,
        }
    }
    /// Preserve status for retry classification without retaining the response body.
    pub fn http(status: u16) -> Self {
        Self {
            code: format!("provider_http_{status}"),
            status: Some(status),
        }
    }
    /// Jev retries only its explicit transient set and transport/timeout failures.
    pub fn retryable(&self) -> bool {
        match self.status {
            Some(s) => [408, 409, 425, 429, 500, 502, 503, 504].contains(&s),
            None => matches!(
                self.code.as_str(),
                "provider_timeout" | "provider_connect_failed"
            ),
        }
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.code)
    }
}
impl std::error::Error for Error {}
/// Result shared by module-8 clients and validated policy input.
pub type Result<T> = std::result::Result<T, Error>;
