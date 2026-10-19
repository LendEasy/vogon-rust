//! Versioned transport records; business state remains in the application service.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Persisted physical call lifecycle. Terminal states cannot be reopened by late callbacks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    #[default]
    Created,
    Dialing,
    Ringing,
    Connected,
    Ended,
    Failed,
    Cancelled,
}
impl State {
    /// Whether this record has reached an irreversible terminal transport state.
    pub fn terminal(self) -> bool {
        matches!(self, Self::Ended | Self::Failed | Self::Cancelled)
    }
    fn rank(self) -> u8 {
        match self {
            Self::Created => 0,
            Self::Dialing => 1,
            Self::Ringing => 2,
            Self::Connected => 3,
            _ => 4,
        }
    }
}
/// Serialized schema-v1 record. Deliberately lacks Debug: it contains bearer secrets.
#[derive(Clone, Serialize, Deserialize)]
pub struct Record {
    pub tenant: String,
    pub call_id: String,
    pub session_token: String,
    pub mode: String,
    #[serde(default)]
    pub app_base_url: String,
    #[serde(default)]
    pub app_auth_token: String,
    #[serde(default)]
    pub app_status_webhook: String,
    #[serde(default = "object")]
    pub app_payload: Value,
    #[serde(default)]
    pub route: String,
    #[serde(default)]
    pub state: State,
    #[serde(default)]
    pub provider_call_id: String,
    #[serde(default)]
    pub call_config: Option<Value>,
    #[serde(default)]
    pub gateway_instance: String,
    #[serde(default)]
    pub created_at_utc: f64,
    #[serde(default)]
    pub connected_at_utc: f64,
    #[serde(default)]
    pub ended_at_utc: f64,
    #[serde(default = "schema")]
    pub schema_version: u32,
}
fn object() -> Value {
    serde_json::json!({})
}
fn schema() -> u32 {
    1
}
impl Record {
    /// Migrate known fields on read; absent optional fields take defaults.
    pub fn decode(raw: &str) -> Result<Self> {
        let mut rec: Self =
            serde_json::from_str(raw).map_err(|_| Error::new("invalid_session_record"))?;
        rec.schema_version = 1;
        Ok(rec)
    }
    /// Apply a monotonic lifecycle transition once, retaining first timestamps.
    pub fn transition(&mut self, state: State, now: f64) -> bool {
        if self.state.terminal() || state == self.state || state.rank() < self.state.rank() {
            return false;
        }
        self.state = state;
        if state == State::Connected && self.connected_at_utc == 0. {
            self.connected_at_utc = now;
        }
        if state.terminal() && self.ended_at_utc == 0. {
            self.ended_at_utc = now;
        }
        true
    }
}
/// Safe opaque identifiers for route segments and existing Redis index delimiters.
pub fn valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 256
        && !s.contains("..")
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
}
/// Epoch timestamp for persisted records, separate from per-call monotonic clocks.
pub fn utc_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs_f64()
}
