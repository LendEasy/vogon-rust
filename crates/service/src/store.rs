//! Atomic session records and exclusive media claims in memory or standalone Redis.
//! Redis CAS writes complete Rust-serialized JSON, preserving empty arrays exactly.
use crate::{
    Error, Future, Result, boxed,
    record::{Record, State, utc_now},
};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

// Script keys/arguments are positional and kept beside their callers. JSON remains
// opaque to Redis: decoding/re-encoding in Lua changes empty arrays into objects.
const CREATE: &str = r#"
if redis.call('exists', KEYS[1]) == 1 or redis.call('exists', KEYS[2]) == 1 then return 0 end
redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[3])
redis.call('set', KEYS[2], ARGV[2], 'PX', ARGV[3])
return 1
"#;
// Record CAS plus all lookup indexes share the refreshed record expiry.
const CHANGE: &str = r#"
if redis.call('get', KEYS[1]) ~= ARGV[1] then return 0 end
for i = 2, #KEYS do
    local ref = redis.call('get', KEYS[i])
    if ref and ref ~= ARGV[4] then return -1 end
end
redis.call('set', KEYS[1], ARGV[2], 'PX', ARGV[3])
for i = 2, #KEYS do redis.call('set', KEYS[i], ARGV[4], 'PX', ARGV[3]) end
return 1
"#;
// Unique media owner and connected-state snapshot become visible atomically.
const CLAIM: &str = r#"
if redis.call('get', KEYS[1]) ~= ARGV[1] then return -1 end
local holder = redis.call('get', KEYS[2])
if holder and holder ~= ARGV[3] then return 0 end
for i = 3, #KEYS do
    local ref = redis.call('get', KEYS[i])
    if ref and ref ~= ARGV[5] then return 0 end
end
redis.call('set', KEYS[2], ARGV[3], 'PX', ARGV[4])
redis.call('set', KEYS[1], ARGV[2], 'PX', ARGV[4])
for i = 3, #KEYS do redis.call('set', KEYS[i], ARGV[5], 'PX', ARGV[4]) end
return 1
"#;
const RELEASE: &str = r#"
if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end
return 0
"#;

/// A tenant-qualified key prevents one tenant from reading another tenant's record.
pub type Key = (String, String);
/// Configurable expiry budgets; injectable short values support real expiry tests.
#[derive(Clone, Copy)]
pub struct Ttl {
    pub live: Duration,
    pub ended: Duration,
}
impl Default for Ttl {
    fn default() -> Self {
        Self {
            live: Duration::from_secs(86400),
            ended: Duration::from_secs(3600),
        }
    }
}
impl Ttl {
    fn duration(self, r: &Record) -> Duration {
        if r.state.terminal() {
            self.ended
        } else {
            self.live
        }
    }
}
/// Atomic record update; provider identity is immutable once bound.
#[derive(Clone)]
pub enum Change {
    Transition(State),
    Provider(String),
    Snapshot(Value),
}
impl Change {
    fn apply(&self, r: &mut Record) -> Result<bool> {
        match self {
            Self::Transition(s) => Ok(r.transition(*s, utc_now())),
            Self::Provider(s) => {
                if !r.provider_call_id.is_empty() && r.provider_call_id != *s {
                    return Err(Error::new("provider_already_bound"));
                }
                r.provider_call_id = s.clone();
                Ok(true)
            }
            Self::Snapshot(s) => {
                r.call_config = Some(s.clone());
                Ok(true)
            }
        }
    }
}
/// Asynchronous storage boundary; each operation is atomic within its documented scope.
pub trait Store: Send + Sync {
    /// Insert a new identity atomically; existing keys/tokens are errors.
    fn create(&self, record: Record) -> Future<'_, ()>;
    /// Read a nonexpired record by tenant-qualified identity.
    fn get(&self, key: Key) -> Future<'_, Option<Record>>;
    /// Resolve a token or provider SID and verify the indexed identity.
    fn lookup(&self, provider: bool, id: String) -> Future<'_, Option<Record>>;
    /// Apply one field update without losing concurrent changes.
    fn change(&self, key: Key, change: Change) -> Future<'_, bool>;
    /// Exclusively attach an owner and mark the record connected.
    fn claim(&self, key: Key, owner: String) -> Future<'_, bool>;
    /// Release only the matching owner; other owners are unaffected.
    fn release(&self, key: Key, owner: String) -> Future<'_, ()>;
}
struct Entry {
    record: Record,
    expires: tokio::time::Instant,
    owner: Option<String>,
}
/// Single-process store with the same expiry/claim/transition rules as Redis.
pub struct Memory {
    entries: Mutex<HashMap<Key, Entry>>,
    ttl: Ttl,
}
impl Default for Memory {
    fn default() -> Self {
        Self::new(Ttl::default())
    }
}
impl Memory {
    /// Construct an isolated store with live and terminal retention budgets.
    pub fn new(ttl: Ttl) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl,
        }
    }
}
fn prune(map: &mut HashMap<Key, Entry>) {
    let now = tokio::time::Instant::now();
    map.retain(|_, e| e.expires > now);
}
impl Store for Memory {
    fn create(&self, mut r: Record) -> Future<'_, ()> {
        boxed(async move {
            let mut m = self.entries.lock().await;
            prune(&mut m);
            let key = (r.tenant.clone(), r.call_id.clone());
            if m.contains_key(&key)
                || m.values()
                    .any(|e| e.record.session_token == r.session_token)
            {
                return Err(Error::new("session_exists"));
            }
            if r.created_at_utc == 0. {
                r.created_at_utc = utc_now();
            }
            m.insert(
                key,
                Entry {
                    expires: tokio::time::Instant::now() + self.ttl.duration(&r),
                    record: r,
                    owner: None,
                },
            );
            Ok(())
        })
    }
    fn get(&self, key: Key) -> Future<'_, Option<Record>> {
        boxed(async move {
            let mut m = self.entries.lock().await;
            prune(&mut m);
            Ok(m.get(&key).map(|e| e.record.clone()))
        })
    }
    fn lookup(&self, provider: bool, id: String) -> Future<'_, Option<Record>> {
        boxed(async move {
            let mut m = self.entries.lock().await;
            prune(&mut m);
            Ok(m.values()
                .find(|e| {
                    !id.is_empty()
                        && (if provider {
                            &e.record.provider_call_id
                        } else {
                            &e.record.session_token
                        }) == &id
                })
                .map(|e| e.record.clone()))
        })
    }
    fn change(&self, key: Key, change: Change) -> Future<'_, bool> {
        boxed(async move {
            let mut m = self.entries.lock().await;
            prune(&mut m);
            if let Change::Provider(s) = &change
                && m.iter()
                    .any(|(k, e)| k != &key && e.record.provider_call_id == *s)
            {
                return Err(Error::new("provider_already_bound"));
            }
            let Some(e) = m.get_mut(&key) else {
                return Ok(false);
            };
            let changed = change.apply(&mut e.record)?;
            if changed {
                e.expires = tokio::time::Instant::now() + self.ttl.duration(&e.record);
            }
            Ok(changed)
        })
    }
    fn claim(&self, key: Key, owner: String) -> Future<'_, bool> {
        boxed(async move {
            let mut m = self.entries.lock().await;
            prune(&mut m);
            let Some(e) = m.get_mut(&key) else {
                return Ok(false);
            };
            if e.record.state.terminal() || e.owner.as_ref().is_some_and(|s| s != &owner) {
                return Ok(false);
            };
            e.owner = Some(owner.clone());
            e.record.gateway_instance = owner;
            e.record.transition(State::Connected, utc_now());
            e.expires = tokio::time::Instant::now() + self.ttl.live;
            Ok(true)
        })
    }
    fn release(&self, key: Key, owner: String) -> Future<'_, ()> {
        boxed(async move {
            if let Some(e) = self.entries.lock().await.get_mut(&key)
                && e.owner.as_ref() == Some(&owner)
            {
                e.owner = None;
            }
            Ok(())
        })
    }
}

/// Standalone Redis backend; scripts use existing gw:* key layout and atomic CAS.
pub struct Redis {
    connection: redis::aio::ConnectionManager,
    ttl: Ttl,
    retry: vogon_resilience::Policy,
}
impl Redis {
    /// Connect without logging URLs or passwords. Cluster mode is rejected by bootstrap.
    pub async fn connect(url: &str, ttl: Ttl) -> Result<Self> {
        Self::connect_with_retry(url, ttl, vogon_resilience::Policy::default()).await
    }
    /// Configure reconnect backoff; mutating Lua commands are never blindly replayed.
    pub async fn connect_with_retry(
        url: &str,
        ttl: Ttl,
        retry: vogon_resilience::Policy,
    ) -> Result<Self> {
        let client = redis::Client::open(url).map_err(|_| Error::new("invalid_redis_url"))?;
        let config = redis::aio::ConnectionManagerConfig::new()
            .set_number_of_retries(retry.attempts.saturating_sub(1))
            .set_exponent_base(2)
            .set_factor(retry.initial_ms)
            .set_max_delay(retry.max_ms);
        let connection = tokio::time::timeout(
            Duration::from_secs(15),
            client.get_connection_manager_with_config(config),
        )
        .await
        .map_err(|_| Error::new("redis_timeout"))?
        .map_err(|_| Error::new("redis_unavailable"))?;
        Ok(Self {
            connection,
            ttl,
            retry,
        })
    }
    fn key(k: &Key) -> String {
        format!("gw:{}:call:{}", k.0, k.1)
    }
    fn index(provider: bool, id: &str) -> String {
        format!("gw:idx:{}:{id}", if provider { "sid" } else { "token" })
    }
    async fn raw(&self, key: &str) -> Result<Option<String>> {
        self.retry
            .run(
                || self.raw_once(key),
                |e| matches!(e.0.as_str(), "redis_timeout" | "redis_unavailable"),
            )
            .await
    }
    async fn raw_once(&self, key: &str) -> Result<Option<String>> {
        tokio::time::timeout(
            Duration::from_secs(5),
            redis::cmd("GET")
                .arg(key)
                .query_async(&mut self.connection.clone()),
        )
        .await
        .map_err(|_| Error::new("redis_timeout"))?
        .map_err(|e: redis::RedisError| {
            Error::new(if e.is_io_error() || e.is_timeout() {
                "redis_unavailable"
            } else {
                "redis_rejected"
            })
        })
    }
    async fn script(&self, script: &str, keys: Vec<String>, args: Vec<String>) -> Result<i64> {
        let mut c = redis::cmd("EVAL");
        c.arg(script).arg(keys.len());
        for k in keys {
            c.arg(k);
        }
        for a in args {
            c.arg(a);
        }
        tokio::time::timeout(
            Duration::from_secs(5),
            c.query_async(&mut self.connection.clone()),
        )
        .await
        .map_err(|_| Error::new("redis_timeout"))?
        .map_err(|_| Error::new("redis_unavailable"))
    }
}
impl Store for Redis {
    fn create(&self, mut r: Record) -> Future<'_, ()> {
        boxed(async move {
            if r.created_at_utc == 0. {
                r.created_at_utc = utc_now();
            }
            let key = (r.tenant.clone(), r.call_id.clone());
            let n = self
                .script(
                    CREATE,
                    vec![Self::key(&key), Self::index(false, &r.session_token)],
                    vec![
                        serde_json::to_string(&r).unwrap(),
                        format!("{}|{}", key.0, key.1),
                        self.ttl.duration(&r).as_millis().max(1).to_string(),
                    ],
                )
                .await?;
            if n == 1 {
                Ok(())
            } else {
                Err(Error::new("session_exists"))
            }
        })
    }
    fn get(&self, key: Key) -> Future<'_, Option<Record>> {
        boxed(async move {
            self.raw(&Self::key(&key))
                .await?
                .map(|s| Record::decode(&s))
                .transpose()
        })
    }
    fn lookup(&self, provider: bool, id: String) -> Future<'_, Option<Record>> {
        boxed(async move {
            if id.is_empty() {
                return Ok(None);
            }
            let Some(k) = self.raw(&Self::index(provider, &id)).await? else {
                return Ok(None);
            };
            let Some((t, c)) = k.split_once('|') else {
                return Err(Error::new("invalid_session_index"));
            };
            Ok(self.get((t.into(), c.into())).await?.filter(|r| {
                (if provider {
                    &r.provider_call_id
                } else {
                    &r.session_token
                }) == &id
            }))
        })
    }
    fn change(&self, key: Key, change: Change) -> Future<'_, bool> {
        boxed(async move {
            for _ in 0..32 {
                let Some(old) = self.raw(&Self::key(&key)).await? else {
                    return Ok(false);
                };
                let mut r = Record::decode(&old)?;
                if !change.apply(&mut r)? {
                    return Ok(false);
                };
                let mut keys = vec![Self::key(&key), Self::index(false, &r.session_token)];
                if !r.provider_call_id.is_empty() {
                    keys.push(Self::index(true, &r.provider_call_id));
                }
                let n = self
                    .script(
                        CHANGE,
                        keys,
                        vec![
                            old,
                            serde_json::to_string(&r).unwrap(),
                            self.ttl.duration(&r).as_millis().max(1).to_string(),
                            format!("{}|{}", key.0, key.1),
                        ],
                    )
                    .await?;
                if n == 1 {
                    return Ok(true);
                }
                if n < 0 {
                    return Err(Error::new("provider_already_bound"));
                }
            }
            Err(Error::new("session_contention"))
        })
    }
    fn claim(&self, key: Key, owner: String) -> Future<'_, bool> {
        boxed(async move {
            for _ in 0..32 {
                let Some(old) = self.raw(&Self::key(&key)).await? else {
                    return Ok(false);
                };
                let mut r = Record::decode(&old)?;
                if r.state.terminal() {
                    return Ok(false);
                };
                r.gateway_instance = owner.clone();
                r.transition(State::Connected, utc_now());
                let mut keys = vec![
                    Self::key(&key),
                    format!("gw:{}:claim:{}", key.0, key.1),
                    Self::index(false, &r.session_token),
                ];
                if !r.provider_call_id.is_empty() {
                    keys.push(Self::index(true, &r.provider_call_id));
                }
                let n = self
                    .script(
                        CLAIM,
                        keys,
                        vec![
                            old,
                            serde_json::to_string(&r).unwrap(),
                            owner.clone(),
                            self.ttl.live.as_millis().max(1).to_string(),
                            format!("{}|{}", key.0, key.1),
                        ],
                    )
                    .await?;
                if n >= 0 {
                    return Ok(n == 1);
                }
            }
            Err(Error::new("session_contention"))
        })
    }
    fn release(&self, key: Key, owner: String) -> Future<'_, ()> {
        boxed(async move {
            self.script(
                RELEASE,
                vec![format!("gw:{}:claim:{}", key.0, key.1)],
                vec![owner],
            )
            .await?;
            Ok(())
        })
    }
}
/// Shared backend ownership, with no shared per-call clock or speech counters.
pub type SharedStore = Arc<dyn Store>;
