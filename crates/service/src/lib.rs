//! HTTP/WebSocket host and session ownership for the native call orchestrator.
pub mod bootstrap;
pub mod extensions;
pub mod host;
pub mod record;
pub mod routes;
pub mod store;
pub mod twilio;
pub use vogon_orchestration::{Error, Future, Result, boxed};

/// Call-local capture and actual-send observers.
pub mod recording;

/// Native capabilities and dynamic readiness.
pub mod operations;
