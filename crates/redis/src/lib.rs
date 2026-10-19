//! Shared Redis connection ownership, cluster routing and credential-free diagnostics.
//! No session keys or call state live here. Mutation commands are never replayed.
use redis::{Cmd, FromRedisValue, RedisResult};
use std::{sync::Arc, time::Duration};
use tokio::sync::OnceCell;
use vogon_resilience::Policy;

enum Backend {
    Standalone(redis::Client),
    Cluster(redis::cluster::ClusterClient),
}
#[derive(Clone)]
enum Connection {
    Standalone(redis::aio::ConnectionManager),
    Cluster(redis::cluster_async::ClusterConnection),
}
struct Inner {
    backend: Backend,
    connection: OnceCell<Connection>,
}

/// Clones share one lazy connection initializer and the client's underlying sockets.
#[derive(Clone)]
pub struct Client(Arc<Inner>);
impl Client {
    /// Validate a seed URL and select standalone or topology-aware cluster routing.
    pub fn new(url: &str, cluster: bool) -> RedisResult<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let backend = if cluster {
            Backend::Cluster(
                redis::cluster::ClusterClient::builder(vec![url])
                    // redis-rs retries include ambiguous I/O errors. Disable them:
                    // repeating a CAS after a lost response can change its outcome.
                    .retries(0)
                    .connection_timeout(Duration::from_secs(5))
                    .response_timeout(Duration::from_secs(5))
                    .build()?,
            )
        } else {
            Backend::Standalone(redis::Client::open(url)?)
        };
        Ok(Self(Arc::new(Inner {
            backend,
            connection: OnceCell::new(),
        })))
    }

    async fn connection(&self, retry: &Policy) -> RedisResult<Connection> {
        self.0
            .connection
            .get_or_try_init(|| async {
                match &self.0.backend {
                    Backend::Standalone(client) => {
                        let config = redis::aio::ConnectionManagerConfig::new()
                            .set_number_of_retries(retry.attempts.saturating_sub(1))
                            .set_exponent_base(2)
                            .set_factor(retry.initial_ms)
                            .set_max_delay(retry.max_ms);
                        client
                            .get_connection_manager_with_config(config)
                            .await
                            .map(Connection::Standalone)
                    }
                    Backend::Cluster(client) => retry
                        .run(|| client.get_async_connection(), transient)
                        .await
                        .map(Connection::Cluster),
                }
            })
            .await
            .cloned()
    }

    /// Establish startup connectivity without issuing application commands.
    pub async fn connect(&self, retry: &Policy) -> RedisResult<()> {
        self.connection(retry).await.map(|_| ())
    }

    /// Execute once; callers may retry safe reads, but never an ambiguous mutation.
    pub async fn query<T: FromRedisValue>(&self, command: &Cmd, retry: &Policy) -> RedisResult<T> {
        match self.connection(retry).await? {
            Connection::Standalone(mut c) => command.query_async(&mut c).await,
            Connection::Cluster(mut c) => command.query_async(&mut c).await,
        }
    }
}

/// Fixed categories only: never expose server text, keys, URLs, credentials or values.
pub fn category(error: &redis::RedisError) -> &'static str {
    match error.code() {
        Some("CROSSSLOT") => "crossslot",
        Some("NOPERM") => "permission_denied",
        Some("READONLY") => "read_only",
        Some("NOAUTH" | "WRONGPASS") => "authentication_failed",
        Some("MOVED") => "moved",
        Some("ASK") => "ask",
        Some("CLUSTERDOWN") => "cluster_down",
        Some("MASTERDOWN") => "cluster_down",
        Some("TRYAGAIN") => "try_again",
        _ if error.kind() == redis::ErrorKind::AuthenticationFailed => "authentication_failed",
        _ if error.is_timeout() => "timeout",
        _ if error.is_io_error() => "connection_failed",
        _ => "rejected",
    }
}

/// Only safe reads/connection establishment may use this transient classification.
pub fn transient(error: &redis::RedisError) -> bool {
    matches!(
        category(error),
        "timeout" | "connection_failed" | "moved" | "ask" | "cluster_down" | "try_again"
    )
}

/// Emit a bounded structured diagnostic, omitting the original error payload.
pub fn log_error(operation: &'static str, error: &redis::RedisError) {
    eprintln!(
        "{}",
        serde_json::json!({"event":"redis_error","operation":operation,"code":category(error)})
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn diagnostics_allowlist_codes_without_server_details() {
        for (kind, expected) in [
            (redis::ErrorKind::CrossSlot, "crossslot"),
            (redis::ErrorKind::ReadOnly, "read_only"),
            (redis::ErrorKind::Moved, "moved"),
            (redis::ErrorKind::Ask, "ask"),
            (
                redis::ErrorKind::AuthenticationFailed,
                "authentication_failed",
            ),
        ] {
            let error = redis::RedisError::from((kind, "secret credentials and call data"));
            assert_eq!(super::category(&error), expected);
        }
    }
}
