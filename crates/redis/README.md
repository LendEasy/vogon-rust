# Shared Redis transport

`Client` owns either a standalone connection manager or an asynchronous Redis
cluster client. Clones share a single `OnceCell` initialization and underlying
connections. Call/session state, keys and clocks remain in their owning modules.
`new` validates configuration and initializes Rustls; `connect` performs eager
startup connectivity; `query` routes and executes a command once.

Cluster mode discovers slot ownership from its seed, authenticates to discovered
nodes, and supports `redis://` and certificate-verified `rediss://`. It does not
implement IAM token generation/refresh. `cluster-async` is explicit so both the
gateway and transfer store use the same routing implementation.

Connection establishment may retry under the supplied policy. Cluster command
retries are zero: redis-rs otherwise retries ambiguous I/O errors, which could
repeat already-executed mutations. The owning stores may retry safe GETs.
Redirects/failover during a mutation can therefore fail explicitly; this module
does not promise transparent mutation replay or reconciliation.

`category` allowlists server/error codes, `transient` identifies errors suitable
for read/connect retries, and `log_error` emits only operation/category JSON.
Server details, keys, credentials and URLs are not included. Unit tests cover
classification; real Redis/TLS/cluster/pooling tests live in service, escalation,
`validation/redis_tls` and `validation/redis_cluster`.
