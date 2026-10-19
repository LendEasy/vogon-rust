//! Native warm transfers: atomic call-local state, Twilio mechanics and post-call facts.
pub mod manager;
pub mod store;
pub mod twiml;
pub use manager::{Binding, Config, Manager};
pub use store::Store;

/// Stable error category; never contains credentials, phone numbers or provider bodies.
#[derive(Debug, Clone)]
pub struct Error(pub &'static str);
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

/// Wall time is used only for persisted transfer facts, independently of CallClock.
pub fn utc() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
