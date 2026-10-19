//! Explicit retry budgets. No process-global mutable state, call counters or hidden retries.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{future::Future, time::Duration};
/// Exponential full-jitter retry policy; attempts includes the initial request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Policy {
    pub attempts: usize,
    pub initial_ms: u64,
    pub max_ms: u64,
    pub jitter: bool,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            attempts: 3,
            initial_ms: 200,
            max_ms: 5000,
            jitter: true,
        }
    }
}
impl Policy {
    /// Explicit single attempt for non-replayable operations and deterministic fixtures.
    pub fn once() -> Self {
        Self {
            attempts: 1,
            ..Self::default()
        }
    }
    /// Resolve global defaults plus per-dependency overrides from immutable environment data.
    /// Example: GATEWAY_RETRY_STT_ATTEMPTS overrides GATEWAY_RETRY_ATTEMPTS.
    pub fn resolve(env: &Value, file: &Value, dependency: &str) -> Result<Self, &'static str> {
        let get = |suffix: &str, default: &str| {
            let specific = format!("GATEWAY_RETRY_{dependency}_{suffix}");
            let global = format!("GATEWAY_RETRY_{suffix}");
            env.get(&specific)
                .or_else(|| file.get(&specific))
                .or_else(|| env.get(&global))
                .or_else(|| file.get(&global))
                .and_then(Value::as_str)
                .unwrap_or(default)
                .to_owned()
        };
        let attempts = get("ATTEMPTS", "3")
            .parse::<usize>()
            .map_err(|_| "invalid_retry_attempts")?;
        let initial_ms = get("INITIAL_MS", "200")
            .parse::<u64>()
            .map_err(|_| "invalid_retry_initial")?;
        let max_ms = get("MAX_MS", "5000")
            .parse::<u64>()
            .map_err(|_| "invalid_retry_max")?;
        let jitter = get("JITTER", "true")
            .parse::<bool>()
            .map_err(|_| "invalid_retry_jitter")?;
        if !(1..=16).contains(&attempts) || initial_ms > max_ms || max_ms > 60_000 {
            return Err("invalid_retry_bounds");
        }
        Ok(Self {
            attempts,
            initial_ms,
            max_ms,
            jitter,
        })
    }
    /// Delay after failed attempt zero, then double up to the configured cap.
    pub fn delay(&self, failed_attempt: usize) -> Duration {
        let cap = self
            .initial_ms
            .saturating_mul(
                1u64.checked_shl(failed_attempt.min(63) as u32)
                    .unwrap_or(u64::MAX),
            )
            .min(self.max_ms);
        let ms = if self.jitter && cap > 0 {
            (uuid::Uuid::new_v4().as_u128() % (cap as u128 + 1)) as u64
        } else {
            cap
        };
        Duration::from_millis(ms)
    }
    /// Retry only classified transient failures; dropping this future cancels request/sleep.
    pub async fn run<T, E, F, Fut>(
        &self,
        mut operation: F,
        transient: impl Fn(&E) -> bool,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        for attempt in 0..self.attempts.max(1) {
            match operation().await {
                Err(error) if attempt + 1 < self.attempts && transient(&error) => {
                    tokio::time::sleep(self.delay(attempt)).await
                }
                result => return result,
            }
        }
        unreachable!("last attempt always returns")
    }
}
/// Immutable policies for each network boundary; copied into client ownership at startup.
#[derive(Clone, Debug)]
pub struct Policies {
    pub turn: Policy,
    pub front: Policy,
    pub stt: Policy,
    pub tts: Policy,
    pub twilio: Policy,
    pub redis: Policy,
    pub report: Policy,
    pub s3: Policy,
}
impl Policies {
    /// Validate all configured budgets before admission opens.
    pub fn resolve(env: &Value, file: &Value) -> Result<Self, &'static str> {
        Ok(Self {
            turn: Policy::resolve(env, file, "TURN")?,
            front: Policy::resolve(env, file, "FRONT")?,
            stt: Policy::resolve(env, file, "STT")?,
            tts: Policy::resolve(env, file, "TTS")?,
            twilio: Policy::resolve(env, file, "TWILIO")?,
            redis: Policy::resolve(env, file, "REDIS")?,
            report: Policy::resolve(env, file, "REPORT")?,
            s3: Policy::resolve(env, file, "S3")?,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    #[test]
    fn doubles_caps_and_validates() {
        let p = Policy {
            attempts: 6,
            initial_ms: 10,
            max_ms: 25,
            jitter: false,
        };
        assert_eq!(
            (0..5).map(|i| p.delay(i).as_millis()).collect::<Vec<_>>(),
            vec![10, 20, 25, 25, 25]
        );
        assert!(Policy::resolve(&json!({"GATEWAY_RETRY_ATTEMPTS":"0"}), &json!({}), "S3").is_err());
        let p = Policy::resolve(
            &json!({"GATEWAY_RETRY_ATTEMPTS":"4"}),
            &json!({"GATEWAY_RETRY_S3_ATTEMPTS":"5"}),
            "S3",
        )
        .unwrap();
        assert_eq!(p.attempts, 5);
    }
    #[tokio::test(start_paused = true)]
    async fn transient_budget_and_permanent_error() {
        let p = Policy {
            jitter: false,
            initial_ms: 10,
            ..Default::default()
        };
        let mut n = 0;
        let start = tokio::time::Instant::now();
        let r = p
            .run(
                || {
                    n += 1;
                    async { Err::<(), _>("temporary") }
                },
                |_| true,
            )
            .await;
        assert_eq!(r, Err("temporary"));
        assert_eq!(n, 3);
        assert_eq!(start.elapsed(), Duration::from_millis(30));
        let mut n = 0;
        let _ = p
            .run(
                || {
                    n += 1;
                    async { Err::<(), _>("auth") }
                },
                |_| false,
            )
            .await;
        assert_eq!(n, 1);
    }
    #[tokio::test(start_paused = true)]
    async fn cancellation_stops_retries() {
        let n = Arc::new(AtomicUsize::new(0));
        let count = n.clone();
        let task = tokio::spawn(async move {
            Policy {
                initial_ms: 10000,
                max_ms: 10000,
                jitter: false,
                ..Default::default()
            }
            .run(
                || {
                    count.fetch_add(1, Ordering::SeqCst);
                    async { Err::<(), _>(()) }
                },
                |_| true,
            )
            .await
        });
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;
        tokio::time::advance(Duration::from_secs(60)).await;
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn jitter_stays_inside_each_exponential_window() {
        let p = Policy::default();
        for i in 0..10 {
            for _ in 0..100 {
                assert!(
                    p.delay(i).as_millis()
                        <= p.initial_ms.saturating_mul(1 << i).min(p.max_ms) as u128
                );
            }
        }
    }
}
