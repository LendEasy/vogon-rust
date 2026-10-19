//! Resolve deployment settings once without mutating process environment or logging secrets.
use crate::{Error, Result};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, time::Duration};

/// Validated host options plus module-1 settings and tenant overrides. No Debug for secrets.
#[derive(Clone)]
pub struct Config {
    pub settings: Value,
    pub gateway: Value,
    pub profiles: Value,
    /// Alternate OpenRouter spelling retained for the front-client bootstrap.
    pub openrouter_fallback: String,
    pub bind: String,
    pub max_active: usize,
    pub max_handshakes: usize,
    pub input_capacity: usize,
    pub max_message_bytes: usize,
    pub handshake_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub stt_endpoint: String,
    pub tts_endpoint: String,
    pub twilio_endpoint: String,
    /// Native S3 settings; optional credentials may come from the same dotenv.
    pub s3: Option<vogon_artifacts::store::S3Config>,
    /// Validated exponential retry budgets for every network dependency.
    pub retries: vogon_resilience::Policies,
}
impl Config {
    /// Load a shared dotenv file. Explicit process values override file values.
    pub fn load(path: &Path) -> Result<Self> {
        let mut file = BTreeMap::new();
        if path.exists() {
            for pair in
                dotenvy::from_path_iter(path).map_err(|_| Error::new("dotenv_read_failed"))?
            {
                let (k, v) = pair.map_err(|_| Error::new("invalid_dotenv"))?;
                file.insert(k, v);
            }
        }
        Self::resolve(
            &json!(std::env::vars().collect::<BTreeMap<_, _>>()),
            &json!(file),
        )
    }
    /// Resolve explicit environment/file inputs for deterministic bootstrap tests.
    pub fn resolve(env: &Value, file: &Value) -> Result<Self> {
        let mut settings = vogon_configuration::settings("settings", &json!({}), env, file)
            .map_err(|_| Error::new("invalid_settings"))?;
        let gateway = vogon_configuration::settings("gateway", &json!({}), env, file)
            .map_err(|_| Error::new("invalid_gateway_settings"))?;
        if !matches!(gateway["session_store"].as_str(), Some("memory" | "redis")) {
            return Err(Error::new("invalid_session_store"));
        }
        let profiles = vogon_configuration::read_overrides(Path::new(
            gateway["tenant_profiles"].as_str().unwrap_or(""),
        ))
        .map_err(|_| Error::new("invalid_tenant_profiles"))?;
        if !profiles.is_object() {
            return Err(Error::new("invalid_tenant_profiles"));
        }
        for tenant in profiles.as_object().unwrap().keys() {
            vogon_configuration::tenant_profile(tenant, &settings, &profiles)
                .map_err(|_| Error::new("invalid_tenant_profile"))?;
        }
        let get = |key: &str, default: &str| {
            env.get(key)
                .or_else(|| file.get(key))
                .and_then(Value::as_str)
                .unwrap_or(default)
                .to_owned()
        };
        let number = |key: &str, default: &str| -> Result<usize> {
            get(key, default)
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| Error::new("invalid_host_limit"))
        };
        let retries = vogon_resilience::Policies::resolve(env, file).map_err(Error::new)?;
        settings["dependency_retries"] = json!({"stt":retries.stt,"tts":retries.tts});
        let s3 = if settings["s3_enabled"] == true {
            let mut s3 = vogon_artifacts::store::S3Config::new(
                settings["s3_bucket"].as_str().unwrap_or("").into(),
                get("AWS_REGION", &get("AWS_DEFAULT_REGION", "us-east-1")),
            );
            if s3.bucket.is_empty() {
                return Err(Error::new("s3_bucket_required"));
            }
            let key = get("AWS_ACCESS_KEY_ID", "");
            let secret = get("AWS_SECRET_ACCESS_KEY", "");
            if key.is_empty() != secret.is_empty() {
                return Err(Error::new("incomplete_aws_credentials"));
            }
            if !key.is_empty() {
                let token = get("AWS_SESSION_TOKEN", "");
                s3.credentials = Some((key, secret, (!token.is_empty()).then_some(token)));
            }
            s3.max_attempts = retries.s3.attempts as u32;
            s3.initial_backoff = Duration::from_millis(retries.s3.initial_ms);
            s3.max_backoff = Duration::from_millis(retries.s3.max_ms);
            Some(s3)
        } else {
            None
        };
        Ok(Self {
            retries,
            s3,
            settings,
            gateway,
            profiles,
            openrouter_fallback: get("OPENROUTER_API_KEY", ""),
            bind: format!(
                "{}:{}",
                get("GATEWAY_HOST", "0.0.0.0"),
                get("GATEWAY_PORT", "8002")
            ),
            max_active: number("GATEWAY_MAX_ACTIVE_CALLS", "64")?,
            max_handshakes: number("GATEWAY_MAX_PENDING_HANDSHAKES", "128")?,
            input_capacity: number("GATEWAY_INPUT_CAPACITY", "256")?,
            max_message_bytes: number("GATEWAY_MAX_MESSAGE_BYTES", "1048576")?,
            handshake_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(
                number("GATEWAY_SHUTDOWN_TIMEOUT_SEC", "210")? as u64
            ),
            stt_endpoint: vogon_speech_providers::client::STT_ENDPOINT.into(),
            tts_endpoint: vogon_speech_providers::client::TTS_ENDPOINT.into(),
            twilio_endpoint: "https://api.twilio.com/2010-04-01".into(),
        })
    }
    /// Resolve one tenant without creating a second source of defaults.
    pub fn profile(&self, tenant: &str) -> Result<Value> {
        vogon_configuration::tenant_profile(tenant, &self.settings, &self.profiles)
            .map_err(|_| Error::new("invalid_tenant_profile"))
    }
}
