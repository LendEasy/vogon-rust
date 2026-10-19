//! Streaming caller adapters with native provider, floor, playback and timer owners.
//! The HTTP route/authentication host supplies a wire::Sink and forwards input frames.
pub mod audio;
pub mod engine;
pub mod session;
pub mod wire;
