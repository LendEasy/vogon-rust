//! Typed Jev questions, answers and bounded retry. No generated speech or domain policy.
use crate::{Error, Result, http::Http};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
/// Production endpoint retained from the Python gateway client.
pub const ENDPOINT: &str = "https://openrouter.ai/api/alpha/decisions";
/// Retry waits after transient failures; the fifth attempt is final.
pub const BACKOFF_MS: [u64; 4] = [125, 250, 500, 1000];
/// Build an exclusive choice with at least two opaque criteria.
pub fn choice(instructions: &str, criteria: &Value) -> Result<Value> {
    if criteria.as_object().is_none_or(|c| c.len() < 2) {
        return Err(Error::new("invalid_choice_criteria"));
    }
    Ok(json!({"type":"choice","instructions":instructions,"criteria":criteria}))
}
/// Build an independent yes/no question.
pub fn noul(instructions: &str, yes: &str, no: &str) -> Value {
    json!({"type":"noul","instructions":instructions,"criteria":{"true":yes,"false":no}})
}
/// Build an ordered scale with at least two rungs.
pub fn score(instructions: &str, criteria: &[String]) -> Result<Value> {
    if criteria.len() < 2 {
        return Err(Error::new("invalid_score_criteria"));
    }
    Ok(json!({"type":"score","instructions":instructions,"criteria":criteria}))
}
/// Validate top-level question kinds, preserving the Python client's shallow check.
pub fn check(questions: &Value) -> Result<()> {
    let qs = questions
        .as_object()
        .filter(|q| !q.is_empty())
        .ok_or_else(|| Error::new("missing_questions"))?;
    if qs
        .values()
        .any(|q| !matches!(q["type"].as_str(), Some("choice" | "noul" | "score")))
    {
        return Err(Error::new("invalid_question_type"));
    }
    Ok(())
}
/// Tagged answers keep confidence separate from yes/no probabilities and numeric scores.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Answer {
    Choice {
        choice: String,
        probabilities: Map<String, Value>,
        confidence: f64,
    },
    Noul {
        noul: f64,
    },
    Score {
        score: f64,
        confidence: f64,
    },
}
/// Parsed result and metering from one completed decisions request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecisionResult {
    pub answers: std::collections::BTreeMap<String, Answer>,
    pub latency_ms: f64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cost_usd: f64,
    pub model: String,
    pub request_id: String,
}
fn number(v: &Value, default: Option<f64>) -> Result<f64> {
    let n = match v {
        Value::Bool(b) => Some(if *b { 1. } else { 0. }),
        Value::String(s) => s.trim().parse().ok(),
        Value::Null => default,
        _ => v.as_f64(),
    };
    n.filter(|n| n.is_finite())
        .ok_or_else(|| Error::new("invalid_answer_number"))
}
impl DecisionResult {
    /// Decode SDK-equivalent answer shapes; unknown kinds fail rather than becoming guesses.
    pub fn parse(raw: &Value, latency_ms: f64) -> Result<Self> {
        let mut answers = std::collections::BTreeMap::new();
        if !raw["answers"].is_null() {
            for (name, a) in raw["answers"]
                .as_object()
                .ok_or_else(|| Error::new("invalid_answers"))?
            {
                let answer = match a["type"].as_str() {
                    Some("choice") => Answer::Choice {
                        choice: a["choice"]
                            .as_str()
                            .ok_or_else(|| Error::new("invalid_choice_answer"))?
                            .into(),
                        probabilities: if a["probabilities"].is_null() {
                            Map::new()
                        } else {
                            a["probabilities"]
                                .as_object()
                                .ok_or_else(|| Error::new("invalid_probabilities"))?
                                .clone()
                        },
                        confidence: number(&a["confidence"], Some(0.))?,
                    },
                    Some("noul") => Answer::Noul {
                        noul: number(&a["noul"], None)?,
                    },
                    Some("score") => Answer::Score {
                        score: number(&a["score"], None)?,
                        confidence: number(&a["confidence"], Some(0.))?,
                    },
                    _ => return Err(Error::new("unknown_answer_type")),
                };
                answers.insert(name.clone(), answer);
            }
        }
        Ok(Self {
            answers,
            latency_ms,
            input_tokens: number(&raw["usage"]["input_tokens"], Some(0.))? as i64,
            output_tokens: number(&raw["usage"]["output_tokens"], Some(0.))? as i64,
            cost_usd: number(&raw["usage"]["cost"], Some(0.))?,
            model: raw["model"].as_str().unwrap_or("").into(),
            request_id: raw["id"].as_str().unwrap_or("").into(),
        })
    }
    /// Require a named choice; wrong/missing kinds are errors.
    pub fn choice(&self, name: &str) -> Result<(&str, f64)> {
        match self.answers.get(name) {
            Some(Answer::Choice {
                choice, confidence, ..
            }) => Ok((choice, *confidence)),
            _ => Err(Error::new("expected_choice_answer")),
        }
    }
    /// Require a named yes/no probability without thresholding it.
    pub fn noul(&self, name: &str) -> Result<f64> {
        match self.answers.get(name) {
            Some(Answer::Noul { noul }) => Ok(*noul),
            _ => Err(Error::new("expected_noul_answer")),
        }
    }
    /// Require a numeric scale answer and confidence.
    pub fn score(&self, name: &str) -> Result<(f64, f64)> {
        match self.answers.get(name) {
            Some(Answer::Score { score, confidence }) => Ok((*score, *confidence)),
            _ => Err(Error::new("expected_score_answer")),
        }
    }
    /// Stable descending ranking preserves provider order for equal probabilities.
    pub fn ranked(&self, name: &str) -> Result<Vec<(String, f64)>> {
        match self.answers.get(name) {
            Some(Answer::Choice { probabilities, .. }) => {
                let mut values = probabilities
                    .iter()
                    .map(|(k, v)| Ok((k.clone(), number(v, None)?)))
                    .collect::<Result<Vec<_>>>()?;
                values.sort_by(|a, b| b.1.total_cmp(&a.1));
                Ok(values)
            }
            _ => Err(Error::new("expected_choice_answer")),
        }
    }
}
/// Opt-in per-call decision trace. Contains caller data; the host controls persistence.
#[derive(Clone, Debug, Serialize)]
pub struct Trace {
    pub tag: String,
    pub state: Value,
    pub questions: Value,
    pub result: Option<DecisionResult>,
    pub error: Option<Error>,
    pub model: String,
    pub latency_ms: f64,
    pub attempts: usize,
}
/// Explicit per-call trace hook; no global mutable tracer or cross-call context.
pub type Tracer = Arc<dyn Fn(Trace) + Send + Sync>;
struct TraceGuard {
    trace: Trace,
    started: Instant,
    sink: Option<Tracer>,
}
impl Drop for TraceGuard {
    fn drop(&mut self) {
        self.trace.latency_ms = self.started.elapsed().as_secs_f64() * 1000.;
        if let Some(sink) = &self.sink {
            let trace = self.trace.clone();
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink(trace)));
        }
    }
}
/// Cloneable async client. Retry budget is per attempt as in Python, not one global timeout.
#[derive(Clone)]
pub struct Client {
    pub http: Http,
    pub model: String,
    pub timeout_ms: u64,
    pub tracer: Option<Tracer>,
}
impl Client {
    /// Execute one typed request; cancellation interrupts HTTP or retry sleep immediately.
    pub async fn decide(
        &self,
        state: Value,
        questions: Value,
        timeout_ms: Option<u64>,
        tag: &str,
    ) -> Result<DecisionResult> {
        check(&questions)?;
        let started = Instant::now();
        let mut trace = TraceGuard {
            trace: Trace {
                tag: tag.into(),
                state: state.clone(),
                questions: questions.clone(),
                result: None,
                error: Some(Error::new("decision_cancelled")),
                model: self.model.clone(),
                latency_ms: 0.,
                attempts: 0,
            },
            started,
            sink: self.tracer.clone(),
        };
        let body = json!({"model":self.model,"state":state,"questions":questions});
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(self.timeout_ms));
        for (attempt, backoff) in BACKOFF_MS
            .into_iter()
            .map(Some)
            .chain(std::iter::once(None))
            .enumerate()
        {
            trace.trace.attempts = attempt + 1;
            match self.http.post(&body, timeout).await {
                Ok(raw) => {
                    let parsed =
                        DecisionResult::parse(&raw, started.elapsed().as_secs_f64() * 1000.);
                    match &parsed {
                        Ok(r) => {
                            trace.trace.result = Some(r.clone());
                            trace.trace.error = None;
                        }
                        Err(e) => trace.trace.error = Some(e.clone()),
                    };
                    return parsed;
                }
                Err(e) if backoff.is_some() && e.retryable() => {
                    tokio::time::sleep(Duration::from_millis(backoff.unwrap())).await
                }
                Err(e) => {
                    trace.trace.error = Some(e.clone());
                    return Err(e);
                }
            }
        }
        unreachable!("bounded attempt loop always returns")
    }
}
