//! Expiring opaque values with compare-and-set. Rust-only namespace avoids unsafe
//! rolling interoperability with Python's differently serialized transfer hashes.
use crate::{Error, Result};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, OnceCell},
    time::Instant,
};

type Entries = HashMap<String, (Vec<u8>, Instant)>;
/// Memory for single-process development; Redis for callbacks landing on other workers.
#[derive(Clone)]
pub struct Store {
    backend: Arc<Backend>,
    retry: vogon_resilience::Policy,
}
enum Backend {
    Memory(Mutex<Entries>),
    Redis {
        client: redis::Client,
        connection: OnceCell<redis::aio::ConnectionManager>,
    },
}
impl Default for Store {
    fn default() -> Self {
        Self {
            retry: vogon_resilience::Policy::once(),
            backend: Arc::new(Backend::Memory(Mutex::new(HashMap::new()))),
        }
    }
}
impl Store {
    /// Validate configuration; clones share one lazily initialized multiplexed connection manager.
    pub fn redis(url: &str) -> Result<Self> {
        Ok(Self {
            retry: vogon_resilience::Policy::once(),
            backend: Arc::new(Backend::Redis {
                client: redis::Client::open(url).map_err(|_| Error("invalid_transfer_redis"))?,
                connection: OnceCell::new(),
            }),
        })
    }
    /// Set retries for safe reads and connection establishment, never replay ambiguous CAS writes.
    pub fn with_retry(mut self, retry: vogon_resilience::Policy) -> Self {
        self.retry = retry;
        self
    }
    /// Single-flight initialization; failed initialization can be retried by the next operation.
    /// Manager clones multiplex the same socket and reconnect after transport failures.
    async fn connection(
        &self,
        client: &redis::Client,
        cell: &OnceCell<redis::aio::ConnectionManager>,
    ) -> redis::RedisResult<redis::aio::ConnectionManager> {
        let manager = cell
            .get_or_try_init(|| async {
                let config = redis::aio::ConnectionManagerConfig::new()
                    .set_number_of_retries(self.retry.attempts.saturating_sub(1))
                    .set_exponent_base(2)
                    .set_factor(self.retry.initial_ms)
                    .set_max_delay(self.retry.max_ms);
                client.get_connection_manager_with_config(config).await
            })
            .await?;
        Ok(manager.clone())
    }
    /// Read a value without extending its expiration.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.retry
            .run(
                || self.get_once(key),
                |e| matches!(e.0, "transfer_store_timeout" | "transfer_store_unavailable"),
            )
            .await
    }
    async fn get_once(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self.backend.as_ref() {
            Backend::Memory(m) => {
                let mut m = m.lock().await;
                m.retain(|_, (_, until)| *until > Instant::now());
                Ok(m.get(key).map(|(v, _)| v.clone()))
            }
            Backend::Redis { client, connection } => {
                tokio::time::timeout(Duration::from_secs(5), async {
                    let mut conn = self.connection(client, connection).await?;
                    redis::cmd("GET").arg(key).query_async(&mut conn).await
                })
                .await
                .map_err(|_| Error("transfer_store_timeout"))?
                .map_err(|e: redis::RedisError| {
                    Error(if e.is_io_error() || e.is_timeout() {
                        "transfer_store_unavailable"
                    } else {
                        "transfer_store_rejected"
                    })
                })
            }
        }
    }
    /// Atomically create/replace/delete only the version read by the caller.
    pub async fn cas(
        &self,
        key: &str,
        old: Option<&[u8]>,
        new: Option<&[u8]>,
        ttl: Duration,
    ) -> Result<bool> {
        match self.backend.as_ref() {
            Backend::Memory(m) => {
                let mut m = m.lock().await;
                m.retain(|_, (_, until)| *until > Instant::now());
                if m.get(key).map(|(v, _)| v.as_slice()) != old {
                    return Ok(false);
                }
                if let Some(v) = new {
                    m.insert(key.into(), (v.to_vec(), Instant::now() + ttl));
                } else {
                    m.remove(key);
                }
                Ok(true)
            }
            Backend::Redis { client, connection } => {
                let script = "local v=redis.call('get',KEYS[1]); if ARGV[1]=='0' then if v then return 0 end elseif v~=ARGV[2] then return 0 end; if ARGV[3]=='0' then redis.call('del',KEYS[1]) else redis.call('set',KEYS[1],ARGV[4],'PX',ARGV[5]) end; return 1";
                let value: i64 = tokio::time::timeout(Duration::from_secs(5), async {
                    // Establish/reconnect safely, but never replay an ambiguous write.
                    let mut conn = self.connection(client, connection).await?;
                    redis::cmd("EVAL")
                        .arg(script)
                        .arg(1)
                        .arg(key)
                        .arg(if old.is_some() { 1 } else { 0 })
                        .arg(old.unwrap_or_default())
                        .arg(if new.is_some() { 1 } else { 0 })
                        .arg(new.unwrap_or_default())
                        .arg(ttl.as_millis().max(1) as u64)
                        .query_async(&mut conn)
                        .await
                })
                .await
                .map_err(|_| Error("transfer_store_timeout"))?
                .map_err(|_| Error("transfer_store_unavailable"))?;
                Ok(value == 1)
            }
        }
    }
}
