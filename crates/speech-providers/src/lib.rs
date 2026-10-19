//! Streaming speech clients and deterministic provider protocol state.
//! Credentials belong to the host; no Python SDK is used by these clients.
pub mod client;
pub mod config;
pub mod stt;
pub mod tts;
pub mod wire;
use serde::{Deserialize, Serialize};

/// Stable redacted error category. Raw handshake errors can contain credentials
/// or provider payloads and are never exposed through this boundary.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Error {
    pub code: String,
}
impl Error {
    /// Construct a machine-readable category from a trusted code.
    /// This does not sanitize arbitrary text; never pass raw provider errors or secrets.
    pub fn new(code: &str) -> Self {
        Self { code: code.into() }
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.code)
    }
}
impl std::error::Error for Error {}
/// Provider result carrying a stable error category rather than raw socket errors.
pub type Result<T> = std::result::Result<T, Error>;

pub mod live;
pub mod live_client;
