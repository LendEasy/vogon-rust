//! Concurrent call orchestration. Per-call Turn API mutations and speech stay ordered;
//! input, event collection, provider I/O and independent calls progress concurrently.
pub mod client;
pub mod driver;
pub mod port;
pub mod state;
use std::pin::Pin;
/// Sendable asynchronous seam for deterministic fakes and production adapters.
pub type Future<'a, T> = Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>>;
/// Stable error category; provider payloads, URLs and bearer tokens are not exposed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error(pub String);
impl Error {
    /// Construct a trusted internal error category.
    pub fn new(code: &str) -> Self {
        Self(code.into())
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Error {}
/// Module result with explicit transport/contract/outage categories.
pub type Result<T> = std::result::Result<T, Error>;
/// Box an owned or borrowed Send future without spawning or blocking a worker.
pub fn boxed<'a, T>(
    future: impl std::future::Future<Output = Result<T>> + Send + 'a,
) -> self::Future<'a, T> {
    Box::pin(future)
}
