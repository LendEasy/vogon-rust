//! Front artifact loading and per-call runtime options. No domain phrases are invented.
use crate::{Error, Result, guard};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path};
/// Approved domain artifact; each invalid pool is rejected as a whole.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Artifact {
    pub prompt: String,
    pub beats: BTreeMap<String, Vec<String>>,
    pub must_not_say: Vec<String>,
    pub tiers: BTreeMap<String, Vec<String>>,
    pub move_criteria: Value,
    pub move_riders: Value,
    pub move_question: String,
    pub rejected: Vec<String>,
}
impl Artifact {
    /// Missing/malformed YAML disables the source; valid pools survive independent pool rejection.
    pub fn load(path: &Path, max_words: usize) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).map_err(|_| Error::new("front_config_read_failed"))?;
        // Preserve the former serde_yaml_ng -> JSON boundary: duplicate keys use
        // the last value, merge keys stay ordinary data, and YAML 1.1 yes/no
        // spellings remain strings. Only deserialization is compiled; includes,
        // environment interpolation and custom tags are not enabled.
        let options = serde_saphyr::options! {
            duplicate_keys: serde_saphyr::DuplicateKeyPolicy::LastWins,
            merge_keys: serde_saphyr::MergeKeyPolicy::AsOrdinary,
            strict_booleans: true,
            reject_unsupported_tags: true,
            non_finite_float_policy: serde_saphyr::NonFiniteFloatPolicy::PassThrough,
        };
        let value: Value = serde_saphyr::from_str_with_options(&text, options)
            .map_err(|_| Error::new("invalid_front_yaml"))?;
        Self::from_value(&value, max_words)
    }
    /// Validate artifact structure and lint phrases with the same structural guards as generated lines.
    pub fn from_value(value: &Value, max_words: usize) -> Result<Self> {
        let mut cfg: Self = serde_json::from_value(if value.is_null() {
            serde_json::json!({})
        } else {
            value.clone()
        })
        .map_err(|_| Error::new("invalid_front_config"))?;
        cfg.prompt = cfg.prompt.trim().into();
        cfg.move_question = cfg.move_question.trim().into();
        cfg.rejected.clear();
        for (family, pools) in [("beats", &mut cfg.beats), ("tiers", &mut cfg.tiers)] {
            pools.retain(|kind, pool| {
                *pool = pool
                    .iter()
                    .map(|s| s.trim().to_owned())
                    .filter(|s| !s.is_empty())
                    .collect();
                let cap = if family == "beats" || kind == "beat" {
                    guard::BEAT_MAX_WORDS
                } else {
                    max_words
                };
                let valid = pool.iter().all(|s| {
                    guard::check(
                        s,
                        &guard::Config {
                            max_words: cap,
                            must_not_say: cfg.must_not_say.clone(),
                            ..Default::default()
                        },
                    )
                    .is_empty()
                });
                if !valid {
                    cfg.rejected.push(format!("{family}:{kind}"));
                }
                valid
            });
        }
        Ok(cfg)
    }
    /// Jev needs criteria and at least one configured tier, even if that tier's pool is empty.
    pub fn can_read_moves(&self) -> bool {
        self.move_criteria
            .as_object()
            .is_some_and(|m| !m.is_empty())
            && !self.tiers.is_empty()
    }
}
/// Per-call behavior knobs resolved by module-1 settings; time values carry explicit units.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Settings {
    pub engine: String,
    pub model_effort: String,
    pub max_words: usize,
    pub min_gap_sec: f64,
    pub move_floor: f64,
    pub repeat_enabled: bool,
    pub beat_after_ms: u64,
    pub beat_after_empty_ms: u64,
    pub wait_again_after_ms: u64,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            engine: "jev".into(),
            model_effort: "none".into(),
            max_words: 12,
            min_gap_sec: 6.,
            move_floor: 0.45,
            repeat_enabled: true,
            beat_after_ms: 900,
            beat_after_empty_ms: 1500,
            wait_again_after_ms: 2500,
        }
    }
}
impl Settings {
    /// Read existing names from a validated GatewaySettings JSON object.
    pub fn from_gateway(value: &Value) -> Result<Self> {
        let fields = [
            ("engine", "front_engine"),
            ("model_effort", "front_model_effort"),
            ("max_words", "front_max_words"),
            ("min_gap_sec", "front_min_gap_sec"),
            ("move_floor", "front_move_floor"),
            ("repeat_enabled", "front_repeat_enabled"),
            ("beat_after_ms", "front_beat_after_ms"),
            ("beat_after_empty_ms", "front_beat_after_empty_ms"),
            ("wait_again_after_ms", "front_wait_again_after_ms"),
        ];
        let mut out = serde_json::Map::new();
        for (name, key) in fields {
            out.insert(
                name.into(),
                value
                    .get(key)
                    .ok_or_else(|| Error::new("missing_front_settings"))?
                    .clone(),
            );
        }
        serde_json::from_value(Value::Object(out)).map_err(|_| Error::new("invalid_front_settings"))
    }
}
