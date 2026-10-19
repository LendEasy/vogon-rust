//! Cerebras JSON completion requests for the front generator and semantic floor judge.
use crate::{Error, Result, guard, http::Http};
use serde_json::{Value, json};
use std::time::Duration;
use vogon_barge_in::floor::{ClassifierRequest, ClassifierResult};
/// Existing Cerebras OpenAI-compatible endpoint; this is not GPT Live.
pub const ENDPOINT: &str = "https://api.cerebras.ai/v1/chat/completions";
/// Shared pooled client with independent front and judge models/deadlines.
#[derive(Clone)]
pub struct Client {
    pub http: Http,
    pub model: String,
    pub timeout_ms: u64,
}
impl Client {
    /// Execute a JSON-mode completion with no implicit retry. Invalid content is an error.
    pub async fn complete(
        &self,
        system: &str,
        user: &str,
        max_tokens: u32,
        temperature: f64,
        effort: Option<&str>,
    ) -> Result<Value> {
        let body = self.request(system, user, max_tokens, temperature, effort);
        let raw = self
            .http
            .post(&body, Duration::from_millis(self.timeout_ms))
            .await?;
        let message = raw["choices"][0]["message"]
            .as_object()
            .ok_or_else(|| Error::new("missing_completion_content"))?;
        let content = match message.get("content") {
            None | Some(Value::Null) => "",
            Some(Value::String(s)) => s.as_str(),
            _ => return Err(Error::new("invalid_completion_content")),
        };
        if content.is_empty() {
            return Ok(json!({}));
        }
        serde_json::from_str(content).map_err(|_| Error::new("invalid_completion_json"))
    }
    /// Construct the exact SDK wire body, including optional flattened reasoning_effort.
    pub fn request(
        &self,
        system: &str,
        user: &str,
        max_tokens: u32,
        temperature: f64,
        effort: Option<&str>,
    ) -> Value {
        let mut body = json!({"model":self.model,"messages":[{"role":"system","content":system},{"role":"user","content":user}],"max_tokens":max_tokens,"temperature":temperature,"response_format":{"type":"json_object"}});
        if let Some(e) = effort.filter(|e| !e.is_empty() && *e != "default") {
            body["reasoning_effort"] = json!(e);
        }
        body
    }
    /// Classify overlap; transport owns token validity and floor policy.
    pub async fn classify(&self, request: &ClassifierRequest) -> ClassifierResult {
        let user = json!({"text":request.text,"heard":request.heard,"unheard":request.unheard,"agent_line":request.agent_line,"attempts":request.attempts});
        match self
            .complete(
                guard::constants()["floor_system"].as_str().unwrap(),
                &user.to_string(),
                1024,
                0.,
                None,
            )
            .await
        {
            Ok(value)
                if matches!(
                    value["decision"].as_str(),
                    Some("CONTINUE" | "YIELD" | "UNCERTAIN")
                ) =>
            {
                ClassifierResult::Verdict(value["decision"].as_str().unwrap().into())
            }
            _ => ClassifierResult::Error,
        }
    }
}
