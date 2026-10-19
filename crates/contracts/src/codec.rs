//! Extend-only JSON compatibility boundary. Scalar coercion is deliberately absent.
use serde_json::{Map, Value};
use std::sync::OnceLock;

#[derive(Debug, Clone)]
/// Compatibility error preserving the Python category alongside an explanatory message.
pub struct Error { pub category: &'static str, pub message: String }
impl Error {
    /// Construct an error for a missing field or incompatible input shape.
    pub fn type_error(message: impl Into<String>) -> Self { Self { category: "TypeError", message: message.into() } }
    /// Construct an error for an unknown contract, event tag or invalid value.
    pub fn value_error(message: impl Into<String>) -> Self { Self { category: "ValueError", message: message.into() } }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}: {}", self.category, self.message) }
}
impl std::error::Error for Error {}

/// Return the generated contract schema, initialized once without Python execution.
pub fn schema() -> &'static Value {
    static SCHEMA: OnceLock<Value> = OnceLock::new();
    SCHEMA.get_or_init(|| serde_json::from_str(include_str!("schema.json")).expect("generated contract schema"))
}

/// Apply known fields, nested defaults and event tags from the named schema.
/// Unknown fields are discarded; scalar values retain the reference codec permissiveness.
/// Use typed decoding afterward when a strict Rust representation is required.
pub fn normalize(name: &str, input: &Value) -> Result<Value, Error> {
    if name == "TransportEvent" || name == "PostCallFact" { return event(name, input); }
    let spec = schema().get(name).ok_or_else(|| Error::value_error(format!("unknown contract type {name}")))?;
    let object = input.as_object().ok_or_else(|| Error::type_error("contract must be an object"))?;
    let mut result = Map::new();
    for (field, meta) in spec["fields"].as_object().unwrap() {
        let value = match object.get(field) {
            Some(value) => decode(&meta["hint"], value)?,
            None => meta.get("default").cloned().ok_or_else(|| Error::type_error(format!("{name}: missing required field {field}")))?,
        };
        result.insert(field.clone(), value);
    }
    if let Some(kind) = spec["kind"].as_str() { result.insert("kind".into(), kind.into()); }
    Ok(result.into())
}

fn decode(hint: &Value, value: &Value) -> Result<Value, Error> {
    if value.is_null() { return Ok(Value::Null); }
    if let Some(inner) = hint.get("optional") { return decode(inner, value); }
    if let Some(name) = hint.get("class").and_then(Value::as_str) { return normalize(name, value); }
    if let Some(name) = hint.get("union").and_then(Value::as_str) { return event(name, value); }
    if let Some(inner) = hint.get("list") {
        // Python's walker iterates lists, strings and dict keys without validating.
        let items: Vec<Value> = match value {
            Value::Array(v) => v.clone(),
            Value::String(v) => v.chars().map(|c| Value::String(c.to_string())).collect(),
            Value::Object(v) => v.keys().cloned().map(Value::String).collect(),
            _ => return Err(Error::type_error("value is not iterable")),
        };
        return items.iter().map(|v| decode(inner, v)).collect::<Result<Vec<_>, _>>().map(Value::Array);
    }
    Ok(value.clone())
}

/// Select the event contract by its kind within the requested union.
/// Missing, unknown or wrong-union event kinds return a compatibility error.
pub fn event(union: &str, value: &Value) -> Result<Value, Error> {
    let kind = value.get("kind").and_then(Value::as_str).ok_or_else(|| Error::value_error("missing or unknown event kind"))?;
    for (name, spec) in schema().as_object().unwrap() {
        let post_call = name == "EscalationLegEvent" || name == "EscalationResolved";
        if spec["kind"].as_str() == Some(kind) && post_call == (union == "PostCallFact") { return normalize(name, value); }
    }
    Err(Error::value_error(format!("unknown {union} kind: {kind}")))
}

/// Copy events and shift at, onset_at and carry_forward.at by offset seconds.
/// Null and zero timestamps remain unchanged; nonnumeric timestamps fail.
pub fn shift_event_times(events: &[Value], offset: f64) -> Result<Vec<Value>, Error> {
    let mut shifted = events.to_vec();
    if offset == 0.0 { return Ok(shifted); }
    for event in &mut shifted {
        let object = event.as_object_mut().ok_or_else(|| Error::type_error("event must be object"))?;
        for field in ["at", "onset_at"] {
            if let Some(value) = object.get_mut(field) { shift(value, offset)?; }
        }
        if let Some(cf) = object.get_mut("carry_forward").and_then(Value::as_object_mut)
            && let Some(value) = cf.get_mut("at") { shift(value, offset)?; }
    }
    Ok(shifted)
}

fn shift(value: &mut Value, offset: f64) -> Result<(), Error> {
    if value.is_null() { return Ok(()); }
    let number = value.as_f64().ok_or_else(|| Error::type_error("timestamp must be numeric"))?;
    if number != 0.0 { *value = Value::from(number + offset); }
    Ok(())
}

/// Serialize a concrete contract type, adding event tags and nested defaults.
pub fn to_wire<T: serde::Serialize>(name: &str, value: &T) -> Result<Value, Error> {
    let input = serde_json::to_value(value).map_err(|e| Error::type_error(e.to_string()))?;
    normalize(name, &input)
}

/// Typed decoding for well-formed internal messages, after wire normalization.
pub fn from_wire<T: serde::de::DeserializeOwned>(name: &str, value: &Value) -> Result<T, Error> {
    serde_json::from_value(normalize(name, value)?).map_err(|e| Error::type_error(e.to_string()))
}
