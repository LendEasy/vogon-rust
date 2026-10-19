//! Explicit bootstrap: read existing gateway dotenv without changing process env
//! or writing credentials. Values supplied by the process override the file.
use crate::{Error, Result, stt::SttConfig, tts::TtsConfig};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path};

/// Secret header value. Debug always redacts it and serialization is not implemented.
#[derive(Clone)]
pub struct ApiKey(String);
impl std::fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApiKey([redacted])")
    }
}
impl ApiKey {
    /// Construct from a host secret; reject empty keys without echoing them.
    pub fn new(value: String) -> Result<Self> {
        if value.trim().is_empty() {
            Err(Error::new("missing_api_key"))
        } else {
            Ok(Self(value))
        }
    }
    /// Build a sensitive Deepgram Token header without exposing the key in diagnostics.
    pub(crate) fn header(&self) -> Result<tokio_tungstenite::tungstenite::http::HeaderValue> {
        self.auth_header("Token")
    }
    /// Build a sensitive authorization header; malformed values return a redacted error.
    pub(crate) fn auth_header(
        &self,
        scheme: &str,
    ) -> Result<tokio_tungstenite::tungstenite::http::HeaderValue> {
        let mut header = tokio_tungstenite::tungstenite::http::HeaderValue::from_str(&format!(
            "{} {}",
            scheme, self.0
        ))
        .map_err(|_| Error::new("invalid_api_key"))?;
        header.set_sensitive(true);
        Ok(header)
    }
}

/// Only speech-related configuration leaves bootstrap; dotenv content is never logged.
#[derive(Clone)]
pub struct SpeechSettings {
    pub key: ApiKey,
    pub browser: SttConfig,
    pub phone: SttConfig,
    pub tts: TtsConfig,
    pub phone_tts: TtsConfig,
}
impl SpeechSettings {
    /// Per-call voice, language and independent name-token boosts match transport startup.
    pub fn for_call(&self, call: &Value) -> Self {
        let mut next = self.clone();
        if let Some(voice) = call["persona_voice"].as_str().filter(|v| !v.is_empty()) {
            next.tts.model = voice.into();
            next.phone_tts.model = voice.into();
        }
        if let Some(language) = call["language"].as_str() {
            next.phone.language = language.into();
            next.browser.language = language.into();
        }
        let keyterms = call["name_hint"]
            .as_str()
            .unwrap_or("")
            .split_whitespace()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        next.phone.keyterms = keyterms.clone();
        next.browser.keyterms = keyterms;
        next
    }

    /// Read an existing dotenv file at the caller-supplied path; process values win.
    /// Does not discover paths, copy credentials or mutate process environment.
    pub fn load(path: &Path) -> Result<Self> {
        let file: BTreeMap<String, String> = dotenvy::from_path_iter(path)
            .map_err(|_| Error::new("dotenv_read_failed"))?
            .collect::<std::result::Result<_, _>>()
            .map_err(|_| Error::new("dotenv_parse_failed"))?;
        let environment: BTreeMap<String, String> = std::env::vars().collect();
        let values = vogon_configuration::settings(
            "settings",
            &json!({}),
            &json!(environment),
            &json!(file),
        )
        .map_err(|_| Error::new("invalid_gateway_settings"))?;
        Self::from_settings(&values)
    }
    /// Use the existing validated settings schema; call overrides remain explicit.
    pub fn from_settings(values: &Value) -> Result<Self> {
        let string = |key: &str| {
            values[key]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::new("invalid_speech_settings"))
        };
        let number = |key: &str| {
            values[key]
                .as_f64()
                .ok_or_else(|| Error::new("invalid_speech_settings"))
        };
        let browser = SttConfig {
            model: string("deepgram_stt_model")?,
            eot_threshold: number("deepgram_eot_threshold")?,
            eot_timeout_ms: number("deepgram_eot_timeout_ms")? as u64,
            eager_eot_threshold: Some(number("deepgram_eager_eot_threshold")?),
            hold_ms: number("deepgram_eot_hold_ms")? as u64,
            word_conf_threshold: number("stt_word_conf_threshold")?,
            ..SttConfig::default()
        };
        let phone = SttConfig {
            encoding: "mulaw".into(),
            sample_rate: 8000,
            eot_threshold: number("deepgram_phone_eot_threshold")?,
            eot_timeout_ms: number("deepgram_phone_eot_timeout_ms")? as u64,
            eager_eot_threshold: Some(number("deepgram_phone_eager_eot_threshold")?),
            ..browser.clone()
        };
        let tts = TtsConfig {
            model: string("deepgram_tts_model")?,
            ..TtsConfig::default()
        };
        let phone_tts = TtsConfig {
            encoding: string("tts_phone_encoding")?,
            sample_rate: number("tts_phone_sample_rate")? as u32,
            ..tts.clone()
        };
        Ok(Self {
            key: ApiKey::new(string("deepgram_api_key")?)?,
            browser,
            phone,
            tts,
            phone_tts,
        })
    }
}
