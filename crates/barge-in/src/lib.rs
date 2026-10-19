//! Per-call barge-in decisions. The host supplies monotonic time, retained VAD,
//! classifier completions and transport effects; this crate performs no I/O.
//!
//! [`gate::Gate`] answers whether acoustic evidence has confirmed speech.
//! [`semantics::Policy`] and its pure functions interpret transcript text.
//! [`floor::Controller`] combines both evidence streams with playback state,
//! owns caller/agent arbitration and emits host actions. The transport keeps STT
//! open, executes actions, and supplies heartbeat/deadline events and completions.
//! Rust structs and enums here carry the roles that Python classes/dataclasses
//! held; mutable instances belong to one call, not shared global state.
/// Acoustic confirmation state, adaptive thresholds and provisional onset buffering.
pub mod gate;
/// Stateless text normalization, echo checks and heuristic/final verdict rules.
pub mod semantics;
/// Episode/floor ownership, classifier continuations and response admission policy.
pub mod floor;
