//! Per-call module 11/12 hook composition. Shared managers contain no call clock.
use crate::{Error, Future, boxed, record::Record};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::Mutex;
use vogon_contracts::{CallConfig, EscalationOrder};
use vogon_orchestration::port::Hooks;

/// Owns one authenticated call binding and persona selection, forwarding custom hooks.
pub struct CallHooks {
    transfers: Arc<vogon_escalation::Manager>,
    record: Record,
    voice: Mutex<String>,
    next: Arc<dyn Hooks>,
}
impl CallHooks {
    /// Create after socket identity is authenticated; never derive binding from callbacks.
    pub fn new(
        transfers: Arc<vogon_escalation::Manager>,
        record: Record,
        next: Arc<dyn Hooks>,
    ) -> Self {
        Self {
            transfers,
            record,
            voice: Mutex::new(String::new()),
            next,
        }
    }
}
impl Hooks for CallHooks {
    fn configure(&self, config: CallConfig) -> Future<'_, ()> {
        boxed(async move {
            *self.voice.lock().await = config.persona_voice.clone();
            self.next.configure(config).await
        })
    }
    fn prepare_escalation(&self, order: EscalationOrder) -> Future<'_, ()> {
        boxed(async move {
            if self.record.provider_call_id.is_empty() {
                return Ok(());
            }
            let binding = vogon_escalation::Binding {
                call_id: self.record.call_id.clone(),
                caller_sid: self.record.provider_call_id.clone(),
                report_url: self.record.app_base_url.clone(),
                report_token: self.record.app_auth_token.clone(),
            };
            self.transfers
                .prepare(binding, order, self.voice.lock().await.clone())
                .await
                .map_err(|e| Error::new(e.0))
        })
    }
    fn escalate(&self, order: EscalationOrder) -> Future<'_, Value> {
        boxed(async move {
            let binding = vogon_escalation::Binding {
                call_id: self.record.call_id.clone(),
                caller_sid: self.record.provider_call_id.clone(),
                report_url: self.record.app_base_url.clone(),
                report_token: self.record.app_auth_token.clone(),
            };
            let target = order
                .targets
                .first()
                .map(|t| t.label.clone())
                .unwrap_or_default();
            let id = order.escalation_id.clone();
            let status = self
                .transfers
                .execute(binding, order, self.voice.lock().await.clone())
                .await
                .map_err(|e| Error::new(e.0))?;
            Ok(
                json!({"kind":"escalation_outcome","status":status,"target":target,"escalation_id":id,"detail":"","at":0.0}),
            )
        })
    }
    fn finish(&self, id: String, reason: String) -> Future<'_, ()> {
        boxed(async move {
            // The call's own end-of-call work runs whatever the transfer store answers; its failure
            // is still returned.
            let discarded = self
                .transfers
                .discard_prepared(&self.record.provider_call_id)
                .await;
            let finished = self.next.finish(id, reason).await;
            discarded.map_err(|e| Error::new(e.0))?;
            finished
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    struct Finished(AtomicBool);
    impl Hooks for Finished {
        fn configure(&self, _: CallConfig) -> Future<'_, ()> {
            boxed(async { Ok(()) })
        }
        fn escalate(&self, _: EscalationOrder) -> Future<'_, Value> {
            boxed(async { Ok(json!({})) })
        }
        fn finish(&self, _: String, _: String) -> Future<'_, ()> {
            self.0.store(true, Ordering::SeqCst);
            boxed(async { Ok(()) })
        }
    }

    /// Production 2026-11-26 (call e5b7b2eb): a transfer-store timeout at call end returned before the
    /// next finish hook ran, so the call's own end-of-call work was skipped as well as reported failed.
    #[tokio::test]
    async fn a_transfer_store_failure_at_call_end_still_runs_the_next_finish_hook() {
        let unreachable = vogon_escalation::Store::redis("redis://127.0.0.1:1/")
            .unwrap()
            .with_retry(vogon_resilience::Policy::once());
        let policy = vogon_resilience::Policy::once();
        let transfers = vogon_escalation::Manager::new(
            vogon_escalation::Config {
                endpoint: String::new(),
                account: String::new(),
                token: String::new(),
                from: String::new(),
                public_base: String::new(),
                say_voice: String::new(),
                tts_endpoint: String::new(),
                tts_key: String::new(),
                timeout: Duration::from_secs(1),
                retry_delays: vec![],
                report_retry: None,
                rest_retry: policy.clone(),
                tts_retry: policy,
            },
            unreachable,
        )
        .unwrap();
        let record = Record::decode(
            &json!({"tenant":"t","call_id":"c","session_token":"s","mode":"twilio",
            "provider_call_id":"CA0123"})
            .to_string(),
        )
        .unwrap();
        let next = Arc::new(Finished(AtomicBool::new(false)));
        let hooks = CallHooks::new(transfers, record, next.clone());
        let outcome = hooks.finish("c".into(), "caller_farewell".into()).await;
        assert!(
            outcome.is_err(),
            "the transfer-store failure is still reported"
        );
        assert!(
            next.0.load(Ordering::SeqCst),
            "the next finish hook was skipped"
        );
    }
}
