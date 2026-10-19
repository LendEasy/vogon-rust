//! Deployment settings and tenant/call configuration, with no Python dependency.
use serde_json::{json, Map, Value};
use std::{path::{Component, Path, PathBuf}, sync::OnceLock};
use vogon_contracts::codec::{normalize, Error};
pub mod models;
pub use models::{GatewaySettings, GatewayConfig, TenantProfile};

/// Return the generated settings schema, parsed once; no Python import is required.
pub fn schema() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA.get_or_init(|| serde_json::from_str(include_str!("schema.json")).expect("generated settings schema"))
}

/// Parsed dotenv and process environment are explicit inputs. Initializers win.
/// Loading dotenv text/secrets belongs to the service bootstrap, not this model.
pub fn settings(kind: &str, initial: &Value, env: &Value, dotenv: &Value) -> Result<Value, Error> {
    let spec = schema().get(kind).and_then(Value::as_object).ok_or_else(|| Error::value_error("unknown settings model"))?;
    let initial = initial.as_object().ok_or_else(|| Error::type_error("settings must be an object"))?;
    let lower = |v: &Value| -> Map<String, Value> {
        v.as_object().into_iter().flatten().map(|(k, v)| (k.to_lowercase(), v.clone())).collect()
    };
    let env = lower(env); let dotenv = lower(dotenv);
    let prefix = if kind == "gateway" { "gateway_" } else { "" };
    let mut output = Map::new();
    for (name, field) in spec {
        let key = format!("{prefix}{name}");
        let value = initial.get(name).or_else(|| if kind == "tenant" { None } else { env.get(&key).or_else(|| dotenv.get(&key)) })
            .unwrap_or(&field["default"]);
        output.insert(name.clone(), coerce(&field["hint"], value).map_err(|e| Error::value_error(format!("{name}: {e}")))?);
    }
    Ok(output.into())
}

fn coerce(hint: &Value, value: &Value) -> Result<Value, Error> {
    let bad = || Error::value_error("invalid settings value");
    if let Some(inner) = hint.get("list") {
        return value.as_array().ok_or_else(bad)?.iter().map(|v| coerce(inner, v)).collect::<Result<Vec<_>, _>>().map(Value::Array);
    }
    match hint["scalar"].as_str().unwrap_or("any") {
        "str" => value.as_str().map(|s| Value::String(s.into())).ok_or_else(bad),
        "bool" => {
            let parsed = match value {
                Value::Bool(b) => Some(*b),
                Value::Number(n) if n.as_f64() == Some(0.0) => Some(false),
                Value::Number(n) if n.as_f64() == Some(1.0) => Some(true),
                Value::String(s) => match s.to_lowercase().as_str() {
                    "1" | "true" | "t" | "yes" | "y" | "on" => Some(true),
                    "0" | "false" | "f" | "no" | "n" | "off" => Some(false), _ => None,
                }, _ => None,
            };
            parsed.map(Value::Bool).ok_or_else(bad)
        },
        "int" | "float" => {
            if hint["scalar"] == "int" && let Some(n) = value.as_i64() { return Ok(Value::from(n)); }
            if let Some(text) = value.as_str() {
                let chars: Vec<_> = text.trim().chars().collect();
                for (i, c) in chars.iter().enumerate() {
                    if *c == '_' && (i == 0 || i + 1 == chars.len() || !chars[i-1].is_ascii_digit() || !chars[i+1].is_ascii_digit()) { return Err(bad()); }
                }
                if hint["scalar"] == "int" {
                    if text.contains(['e', 'E']) { return Err(bad()); }
                    if let Ok(n) = text.trim().replace('_', "").parse::<i64>() { return Ok(Value::from(n)); }
                }
            }
            let number = match value {
                Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
                Value::Number(n) => n.as_f64(),
                Value::String(s) => s.trim().replace('_', "").parse::<f64>().ok(),
                _ => None,
            }.filter(|n| n.is_finite()).ok_or_else(bad)?;
            if hint["scalar"] == "int" {
                if number.fract() != 0.0 || number < i64::MIN as f64 || number >= i64::MAX as f64 { return Err(bad()); }
                Ok(Value::from(number as i64))
            } else { Ok(Value::from(number)) }
        },
        _ => Ok(value.clone()),
    }
}

/// Derive the built-in tenant profile from shared speech, ambience and barge settings.
pub fn built_in(shared: &Value) -> Result<Value, Error> {
    let mut initial = json!({});
    for (profile, setting) in [("stt_model", "deepgram_stt_model"), ("tts_model", "deepgram_tts_model"),
                              ("semantic_barge_enabled", "semantic_barge_enabled"), ("ambience_enabled", "ambience_enabled")] {
        if let Some(value) = shared.get(setting) { initial[profile] = value.clone(); }
    }
    initial["tts_voice_default"] = shared.get("tts_voice").filter(|v| !v.is_null()).cloned().unwrap_or(json!(""));
    settings("tenant", &initial, &json!({}), &json!({}))
}

/// Overlay the named tenant onto built-ins and validate the resulting profile.
pub fn tenant_profile(tenant: &str, shared: &Value, overrides: &Value) -> Result<Value, Error> {
    let mut base = built_in(shared)?;
    if let Some(row) = overrides.get(tenant).filter(|v| !v.is_null()) {
        if let Some(object) = row.as_object() { base.as_object_mut().unwrap().extend(object.clone()); }
        else if row != &json!(false) && row != &json!("") && row != &json!([]) && row != &json!(0) { return Err(Error::type_error("tenant override must be a mapping")); }
    }
    settings("tenant", &base, &json!({}), &json!({}))
}

/// Missing profile files use built-ins; malformed/unreadable existing files fail.
pub fn read_overrides(path: &Path) -> Result<Value, Error> {
    if path.as_os_str().is_empty() || !path.exists() { return Ok(json!({})); }
    let bytes = std::fs::read(path).map_err(|e| Error::value_error(e.to_string()))?;
    serde_json::from_slice(&bytes).map_err(|e| Error::value_error(e.to_string()))
}

/// Normalize call configuration and resolve tenant fallbacks into one snapshot.
/// The returned profile, compact call_config and resolved policy are host inputs.
pub fn resolve_call_snapshot(profile: &Value, call: &Value) -> Result<Value, Error> {
    let profile = settings("tenant", profile, &json!({}), &json!({}))?;
    let call = normalize("CallConfig", call)?;
    let chosen = |block: &str, field: &str, fallback: &str| -> Value {
        if !call[block].is_null() { call[block][field].clone() } else { profile[fallback].clone() }
    };
    let mut compact = call.as_object().unwrap().clone();
    compact.retain(|_, v| !(v.is_null() || v == "" || v == &json!([]) || v == &json!({})));
    Ok(json!({"profile":profile, "call_config":compact, "resolved":{
        "language": if call["language"].as_str().is_some_and(|s| !s.is_empty()) {call["language"].clone()} else {profile["language"].clone()},
        "recording_enabled": chosen("recording", "enabled", "recording_enabled"),
        "recording_channels": chosen("recording", "channels", "recording_channels"),
        "semantic_barge_enabled": chosen("barge_policy", "semantic_judge", "semantic_barge_enabled"),
        "safety_phrases": chosen("barge_policy", "safety_phrases", "safety_phrases"),
        "floor_request_phrases": chosen("barge_policy", "floor_request_phrases", "floor_request_phrases"),
        "filler": !call["filler"].is_null() || profile["filler_enabled"] == true,
        "ambience": call["ambience"].get("enabled").cloned().unwrap_or(profile["ambience_enabled"].clone()),
    }}))
}

/// Return whether a non-whitespace service key enables the authentication policy.
/// This predicate does not authenticate a request.
pub fn auth_enabled(config: &Value) -> bool {
    config["service_key"].as_str().is_some_and(|s| !s.trim().is_empty())
}

/// Resolve a front-model configuration path lexically against `cwd`.
/// No filesystem lookup occurs; an empty path remains empty.
pub fn resolved_front_config_path(path: &str, cwd: &Path) -> String {
    if path.is_empty() { return String::new(); }
    let path = if Path::new(path).is_absolute() { PathBuf::from(path) } else { cwd.join(path) };
    let mut result = PathBuf::new();
    for part in path.components() {
        match part { Component::ParentDir => { result.pop(); }, Component::CurDir => {}, _ => result.push(part.as_os_str()) }
    }
    let result=result.to_string_lossy().into_owned();
    if path.to_string_lossy().starts_with("//") && !path.to_string_lossy().starts_with("///") {format!("/{result}")} else {result}
}
