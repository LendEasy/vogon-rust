# Three-primary Redis cluster acceptance

Run from the repository root with native/Python build dependencies available:

```sh
REDIS_SERVER_BIN=/path/to/redis-server REDIS_CLI_BIN=/path/to/redis-cli \
  python3 validation/redis_cluster/run.py
REDIS_SERVER_BIN=/path/to/redis-server REDIS_CLI_BIN=/path/to/redis-cli \
  python3 validation/redis_cluster/run.py --tls
```

The runner owns three real Redis primaries covering all 16,384 slots. Each node
requires a test password and advertises a loopback proxy port. TLS mode uses a
temporary CA and server certificate; the cluster bus stays on separate loopback
ports. Proxies forward unchanged bytes to actual Redis. No responses, commands
or Lua scripts are emulated. Temporary Redis processes/files are cleaned up.

Tests cover:

- Reproduction of the old cross-slot script failure and same-slot placement of
  the corrected session record, token/SID indexes and claim keys.
- Routing from all three seeds, including when the seed does not own the slot.
- Concurrent creates from independent clients (one winner), plus the existing
  lifecycle, exclusive-claim, concurrent field-update and expiry/index-refresh
  tests executed against the cluster without weakening their assertions.
- Transfer CAS/read operations distributed across the three shards.
- Real HTTP POST call creation and subsequent token lookup from another client.
- ACL denial of EVAL classified as `permission_denied` rather than network loss.
- A deliberately dropped reply after Redis executes a mutation. The command
  returns an error and the persisted counter remains exactly one, proving no
  hidden retry re-executes the side effect.

Native CI runs both modes. TLS certificate/hostname/password rejection is also
tested by the separate TLS runner. No paid providers or AWS resources are used.

To test the packaged gateway's actual main/configuration/HTTP path against the
same TLS cluster instead of Cargo integration tests:

```sh
REDIS_SERVER_BIN=/path/to/redis-server REDIS_CLI_BIN=/path/to/redis-cli \
  python3 validation/redis_cluster/run.py --tls --image vogon-rust:al2023-redis-cluster
```

This uses Linux Docker host networking, injects the fixture CA and cluster mode,
checks readiness, creates a text call without attaching media or contacting the
Turn service, reads the call back, and verifies graceful process exit.

Cluster session metadata intentionally occupies one slot to retain atomicity;
it does not distribute session writes across shards. Transfer values do remain
distributed. These tests do not establish production load capacity, AWS IAM-token
refresh, or transparent mutation recovery during failover/resharding. Drain old
Python/standalone calls before switching namespaces; there is no live migration.
