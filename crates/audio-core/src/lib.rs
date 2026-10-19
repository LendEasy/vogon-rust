//! Stateful audio implementation for the Rust gateway.
//! Rust owns stream state and directly calls SpeexDSP and WebRTC VAD.
//! NumPy and audioop retain their CPython APIs.
pub mod aec;
pub mod formats;
pub mod libraries;
pub mod mixer;
pub mod native;
pub mod sources;

use pyo3::prelude::*;

/// Exposed only to the offline Python comparison harness, not a Python port.
#[pymodule]
pub fn vogon_audio_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<aec::AecProcessor>()?;
    m.add_class::<sources::Clip>()?;
    m.add_class::<sources::Source>()?;
    m.add_class::<mixer::AmbienceConfig>()?;
    m.add_class::<mixer::TypingState>()?;
    m.add_class::<mixer::AmbienceMixer>()?;
    m.add_class::<formats::AudioOps>()?;
    m.add_class::<formats::RateConverter>()?;
    m.add_class::<formats::PhoneFramer>()?;
    m.add_class::<formats::BrowserFramer>()?;
    m.add_class::<formats::Vad>()?;
    m.add_function(wrap_pyfunction!(formats::encode_audio_frame, m)?)?;
    m.add_function(wrap_pyfunction!(sources::build_synthetic_room_tone, m)?)?;
    m.add_function(wrap_pyfunction!(sources::build_synthetic_typing, m)?)?;
    m.add_function(wrap_pyfunction!(sources::build_synthetic_oneshots, m)?)?;
    m.add_function(wrap_pyfunction!(sources::build_wav_source, m)?)?;
    m.add_function(wrap_pyfunction!(sources::build_mixer, m)?)?;
    Ok(())
}
