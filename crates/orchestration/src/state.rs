//! Pure inbox policy. Event collection never waits for HTTP or TTS.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, VecDeque};
/// An accepted trigger and the speech revision against which its result must be checked.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Trigger {
    pub event: Value,
    pub revision: u64,
    pub speculative: bool,
    pub caller: bool,
}
/// Call-local event identity, trigger queue and eager/final invalidation policy.
#[derive(Default)]
pub struct Inbox {
    pub queue: VecDeque<Trigger>,
    pub revision: u64,
    pub machine: bool,
    pub duplicates: u64,
    pub eager_text: Option<String>,
    seen: BTreeSet<String>,
    last_final: Option<(String, f64, f64)>,
}
impl Inbox {
    /// Accept contract events in order. Positive sequence IDs deduplicate transport replay;
    /// identical timestamped finals deduplicate callback replay without suppressing a later repeated phrase.
    pub fn accept(&mut self, event: Value) -> Result<bool> {
        let kind = event["kind"]
            .as_str()
            .ok_or_else(|| Error::new("missing_event_kind"))?;
        let seq = event["seq"].as_u64().unwrap_or(0);
        if seq > 0 && !self.seen.insert(format!("{kind}:{seq}")) {
            self.duplicates += 1;
            return Ok(false);
        }
        let text = event["text"].as_str().unwrap_or("");
        let final_key = (
            text.to_owned(),
            event["onset_at"].as_f64().unwrap_or(0.),
            event["at"].as_f64().unwrap_or(0.),
        );
        match kind {
            "utterance_eager" => {
                if self.eager_text.as_deref() == Some(text) {
                    self.duplicates += 1;
                    return Ok(false);
                }
                self.revision += 1;
                self.eager_text = Some(text.into());
                self.last_final = None;
            }
            "utterance_resumed" => {
                self.revision += 1;
                self.eager_text = None;
                self.last_final = None;
            }
            "utterance_final" => {
                if self.last_final.as_ref() == Some(&final_key) {
                    self.duplicates += 1;
                    return Ok(false);
                }
                if self.eager_text.as_deref() != Some(text) || self.last_final.is_some() {
                    self.revision += 1;
                }
                self.last_final = Some(final_key);
                self.eager_text = None;
            }
            "line_verdict" => {
                if event["answered_by"] == "machine" {
                    self.machine = true;
                }
                return Ok(true);
            }
            _ => {}
        }
        if !is_fact(&event) {
            self.queue.push_back(Trigger {
                revision: self.revision,
                speculative: kind == "utterance_eager",
                caller: matches!(kind, "utterance_final" | "utterance_eager"),
                event,
            });
        }
        Ok(true)
    }
}
/// Physical facts precede the next trigger; they never launch reasoning by themselves.
pub fn is_fact(event: &Value) -> bool {
    matches!(
        event["kind"].as_str(),
        Some(
            "delivery_outcome"
                | "playout_finished"
                | "drain_cleared"
                | "line_verdict"
                | "escalation_outcome"
        )
    )
}
