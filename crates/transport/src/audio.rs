//! Async bridge to the serialized native audio owner. Blocking responses and
//! worker construction/destruction are isolated from Tokio executor threads.
use serde_json::Value;
use std::sync::{Arc, Mutex};
use vogon_audio_runtime::{AudioRuntime, Command, Config};
use vogon_speech_providers::{Error, Result};
#[derive(Clone)]
/// Cloneable async access to one serialized audio worker.
/// Clones share state; they do not create independent resampler or AEC instances.
pub struct Audio {
    inner: Arc<Mutex<AudioRuntime>>,
}
impl Audio {
    /// Construct the worker on Tokio blocking capacity and expose redacted startup errors.
    pub async fn start(config: Config) -> Result<Self> {
        let runtime = tokio::task::spawn_blocking(move || AudioRuntime::start(config))
            .await
            .map_err(|_| Error::new("audio_start_task_failed"))?
            .map_err(|_| Error::new("audio_start_failed"))?;
        Ok(Self {
            inner: Arc::new(Mutex::new(runtime)),
        })
    }
    /// Submit in queue order and await the reply outside executor threads.
    /// Dropping this future after submission does not retract accepted audio work.
    pub async fn call(&self, command: Command) -> Result<Value> {
        let receiver = self
            .inner
            .lock()
            .map_err(|_| Error::new("audio_owner_failed"))?
            .submit(command)
            .map_err(|_| Error::new("audio_submit_failed"))?;
        tokio::task::spawn_blocking(move || receiver.recv())
            .await
            .map_err(|_| Error::new("audio_wait_failed"))?
            .map_err(|_| Error::new("audio_disconnected"))?
            .map_err(|_| Error::new("audio_operation_failed"))
    }
    /// Drain and join the shared worker outside executor threads; all clones then reject work.
    pub async fn close(&self) -> Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            inner
                .lock()
                .map_err(|_| Error::new("audio_owner_failed"))?
                .close()
                .map_err(|_| Error::new("audio_close_failed"))
        })
        .await
        .map_err(|_| Error::new("audio_close_task_failed"))?
    }
}
