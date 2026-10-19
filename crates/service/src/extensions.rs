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
            self.transfers
                .discard_prepared(&self.record.provider_call_id)
                .await
                .map_err(|e| Error::new(e.0))?;
            self.next.finish(id, reason).await
        })
    }
}
