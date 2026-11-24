//! Domain-free move/rider questions and deterministic tier selection.
use crate::{Error, Result, guard, jev};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
/// Valid exclusive desk moves; unfamiliar model labels become UNSURE.
pub const MOVES: &[&str] = &[
    "ASSENT",
    "REFUSAL",
    "SYMPATHY",
    "THANKS",
    "FIGURE",
    "READBACK",
    "DONE",
    "OFFER",
    "CORRECTION",
    "EXPLANATION",
    "PROCEDURE",
    "PRESENCE",
    "UNSURE",
];
/// Independent rider questions may coexist with any move.
pub const RIDERS: &[&str] = &[
    "distress",
    "not_the_borrower",
    "identity_data",
    "unintelligible",
    "repeat_request",
    "closing",
    "plain_answer",
    "needs_lookup",
];
/// One model read; failures contain an error and default to UNSURE.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Read {
    #[serde(rename = "move")]
    pub movement: String,
    pub confidence: f64,
    pub riders: BTreeMap<String, f64>,
    pub latency_ms: f64,
    pub cost_usd: f64,
    pub error: String,
}
impl Default for Read {
    fn default() -> Self {
        Self {
            movement: "UNSURE".into(),
            confidence: 0.,
            riders: BTreeMap::new(),
            latency_ms: 0.,
            cost_usd: 0.,
            error: String::new(),
        }
    }
}
impl Read {
    /// Rider presence uses an inclusive probability threshold; missing values are zero.
    pub fn rider(&self, name: &str, threshold: f64) -> bool {
        self.riders.get(name).copied().unwrap_or(0.) >= threshold
    }
    /// Convert typed decisions to a read; wrong/missing answer kinds fail closed.
    pub fn from_result(result: &jev::DecisionResult) -> Result<Self> {
        let choice = result.choice("move")?;
        let mut riders = BTreeMap::new();
        for &name in RIDERS {
            if result.answers.contains_key(name) {
                riders.insert(name.into(), result.noul(name)?);
            }
        }
        Ok(Self {
            movement: if MOVES.contains(&choice.0) {
                choice.0.into()
            } else {
                "UNSURE".into()
            },
            confidence: choice.1,
            riders,
            latency_ms: result.latency_ms,
            cost_usd: result.cost_usd,
            error: String::new(),
        })
    }
}
/// A selected speech tier, possibly silence with an explicit reason.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Pick {
    pub tier: String,
    pub text: String,
    pub reason: String,
}
/// Construct move and configured rider questions without supplying domain wording.
pub fn questions(criteria: &Value, riders: &Value, question: &str) -> Result<Value> {
    let mut q = json!({"move":jev::choice(if question.is_empty(){guard::constants()["move_question"].as_str().unwrap()}else{question},criteria)?});
    for &name in RIDERS {
        if let Some(spec) = riders
            .get(name)
            .filter(|x| !x.is_null() && **x != json!({}))
        {
            let s = |key: &str| {
                spec[key]
                    .as_str()
                    .ok_or_else(|| Error::new("invalid_rider"))
            };
            q[name] = jev::noul(s("ask")?, s("yes_when")?, s("no_when")?);
        }
    }
    Ok(q)
}
/// Provider context includes the last six turns and no application/account state.
pub fn state(agent_last: &str, utterance: &str, history: &[(String, String)]) -> Value {
    json!({"agent_just_said":if agent_last.is_empty(){"(the call has just opened)"}else{agent_last},"caller_just_said":utterance,"recent_turns":history.iter().skip(history.len().saturating_sub(6)).map(|(who,what)|format!("{who}: {what}")).collect::<Vec<_>>()})
}
/// The silences the hold's timed beat may not cover: the desk acknowledges distress itself, and
/// nothing is owed across a goodbye.
pub const DISTRESS_SILENCE: &str = "rider:distress_defers_to_desk";
pub const CLOSING_SILENCE: &str = "rider:closing";
/// A look-up line plays only on a confident yes to the one question the read is trusted with.
pub const LOOKUP_BAR: f64 = 0.8;
/// What the read earns: silence, or a look-up line. The read is no longer asked to predict how
/// the desk's answer will open (decision 2026-11-24): that guess put a look-up line after a
/// refusal and an apology after "Hello?". Three facts about the CALLER's line are left: a goodbye
/// and distress are silent with no timed line, a confident `needs_lookup` earns a look-up line,
/// and everything else picks nothing so the hold's neutral line covers a slow turn.
/// `has_spoken` and `move_floor` are accepted for the callers that still pass them.
pub fn pick(
    read: &Read,
    tiers: &BTreeMap<String, Vec<String>>,
    mut rotate: impl FnMut(&str, &[String]) -> String,
    _has_spoken: bool,
    _move_floor: f64,
) -> Pick {
    let silent = |reason: &str| Pick {
        tier: "silence".into(),
        text: String::new(),
        reason: reason.into(),
    };
    if read.rider("closing", 0.5) {
        return silent(CLOSING_SILENCE);
    }
    if read.rider("distress", 0.5) {
        return silent(DISTRESS_SILENCE);
    }
    if read.rider("needs_lookup", LOOKUP_BAR)
        && let Some(pool) = tiers.get("lookup").filter(|pool| !pool.is_empty())
    {
        return Pick {
            tier: "lookup".into(),
            text: rotate("lookup", pool),
            reason: "rider:needs_lookup".into(),
        };
    }
    silent("no_line")
}
