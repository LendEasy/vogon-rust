//! Structural guards for gateway-authored speech; ordered reasons match Python.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::OnceLock;
/// Four-word beats may recur; longer lines use repeat and opener guards.
pub const BEAT_MAX_WORDS: usize = 4;
/// Shared protocol constants copied from the reference by export_front_constants.py.
pub fn constants() -> &'static Value {
    static VALUE: OnceLock<Value> = OnceLock::new();
    VALUE.get_or_init(|| {
        serde_json::from_str(include_str!("constants.json")).expect("generated constants")
    })
}
/// Guard inputs contain application-supplied exclusions, never inferred domain policy.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub max_words: usize,
    pub must_not_say: Vec<String>,
    pub forbidden_names: Vec<String>,
    pub earlier: Vec<String>,
}
/// Keep ASCII letters/spaces after Unicode lowercase conversion for repetition checks.
pub fn normalize(text: &str) -> String {
    text.to_lowercase()
        .chars()
        .filter(|c| c.is_ascii_lowercase() || *c == ' ')
        .collect::<String>()
        .trim()
        .into()
}
/// Split only whitespace following . ! or ?, retaining punctuation as Python does.
pub fn sentences(text: &str) -> Vec<String> {
    let mut result = vec![];
    let mut pending = String::new();
    let mut splitting = false;
    for c in text.trim().chars() {
        if c.is_whitespace() && (splitting || pending.ends_with(['.', '!', '?'])) {
            if !pending.is_empty() {
                result.push(std::mem::take(&mut pending));
            }
            splitting = true;
        } else {
            splitting = false;
            pending.push(c);
        }
    }
    if !pending.is_empty() {
        result.push(pending);
    }
    result
}
fn word(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}
/// Match a literal phrase with Python-style alphanumeric/underscore word boundaries.
fn contains_token(text: &str, token: &str, plural: bool) -> bool {
    if token.is_empty() {
        return false;
    }
    text.match_indices(token).any(|(i, _)| {
        let left = text[..i].chars().next_back().is_some_and(word);
        let first = token.chars().next().is_some_and(word);
        if left == first {
            return false;
        }
        let end = i + token.len();
        let matches_end =
            |end: usize, last: bool| text[end..].chars().next().is_some_and(word) != last;
        let last = token.chars().next_back().is_some_and(word);
        matches_end(end, last)
            || (plural && text[end..].starts_with('s') && matches_end(end + 1, true))
    })
}
/// Return every violated rule in reference order; empty input passes unchanged.
pub fn check(text: &str, cfg: &Config) -> Vec<String> {
    if text.is_empty() {
        return vec![];
    }
    let low = text.to_lowercase();
    let words: Vec<_> = low
        .split(|c: char| !c.is_ascii_lowercase() && c != '\'')
        .filter(|w| !w.is_empty())
        .collect();
    let has_word = |key: &str| {
        constants()[key]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| words.contains(&w.as_str().unwrap()))
    };
    let has_phrase = |key: &str| {
        constants()[key]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| contains_token(&low, w.as_str().unwrap(), false))
    };
    let mut trips = vec![];
    let mut add = |reason: &str| trips.push(reason.to_owned());
    if text.contains('\n') {
        add("shape:linebreak");
    }
    if sentences(text).len() > 1 {
        add("shape:sentences");
    }
    let length = text.split_whitespace().count();
    if length > cfg.max_words {
        add("length");
    }
    if text
        .chars()
        .any(|c| c.is_ascii_digit() || c == '$' || c == '%')
    {
        add("content:number");
    }
    let one = low.match_indices("one").any(|(i, _)| {
        !low[..i].chars().next_back().is_some_and(word)
            && !low[i + 3..].chars().next().is_some_and(word)
            && ![" moment", " second", " sec"]
                .iter()
                .any(|s| low[i + 3..].starts_with(s))
    });
    if has_word("spelled_numbers") || one {
        add("content:spelled_number");
    }
    if has_word("calendar") {
        add("content:date_word");
    }
    for tok in &cfg.must_not_say {
        if contains_token(&low, &tok.to_lowercase(), true) {
            add(&format!("content:{tok}"));
        }
    }
    for name in &cfg.forbidden_names {
        if contains_token(&low, &name.to_lowercase(), false) {
            add("content:name");
        }
    }
    if has_phrase("commitment") {
        add("commitment");
    }
    if has_phrase("action_claims") {
        add("action");
    }
    if text.contains('?') {
        add("question");
    }
    if length > BEAT_MAX_WORDS
        && let Some(last) = cfg.earlier.last()
    {
        let n = normalize(text);
        if cfg.earlier.iter().any(|e| normalize(e) == n) {
            add("repeat");
        }
        let p = normalize(last);
        let previous: Vec<_> = p.split_whitespace().take(3).collect();
        if !previous.is_empty() && n.split_whitespace().take(3).collect::<Vec<_>>() == previous {
            add("opener");
        }
    }
    trips
}
