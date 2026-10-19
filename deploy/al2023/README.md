# Amazon Linux 2023 binary distribution

For the complete `codelexin` installation, `voice-gateway.service` cutover and
rollback commands, use [the EC2 installation guide](INSTALL.md).

Target: x86-64, Amazon Linux repository snapshot `2023.10.20260302`, CPython
3.11 and glibc 2.34. Binaries use the generic x86-64 CPU baseline. NumPy/audioop
are still embedded through CPython; this is not a static executable. Native
WebRTC VAD is compiled in; SpeexDSP remains a system shared library.

Redis TLS (`rediss://`) uses the system CA certificates and validates the hostname.
Keep the existing encrypted AWS URL. The original `707021a` archive lacked Redis
TLS; use a corrected build when connecting to encrypted Redis endpoints.
Cluster/serverless deployments additionally require the cluster fix and
`REDIS_CLUSTER_MODE=true`; the TLS-only replacement is insufficient.

## Build on a development machine

Run from the repository root with Docker/BuildKit. No EC2 build instance is needed.
The build runs natively in an amd64 container (ARM hosts need amd64 emulation).
Both the Rust toolchain and base image are digest-pinned. The Amazon Linux base
is synchronized to the EC2 repository snapshot before installing packages.

```sh
docker buildx build --platform linux/amd64 -f Dockerfile.al2023 \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  --target artifacts --output type=local,dest=target/al2023-release .
docker buildx build --platform linux/amd64 -f Dockerfile.al2023 \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  --target runtime --load -t vogon-rust:al2023 .
docker run --rm -e FRONT_MODEL_ENABLED=false -e GATEWAY_SESSION_STORE=memory \
  vogon-rust:al2023 --check --env /nonexistent
python3 deploy/al2023/smoke.py
```

Use a clean committed checkout for published artifacts. `SOURCE_REVISION` labels
the source; it does not verify a clean tree. Generated outputs stay under ignored
`target/`. The archive includes both executables, notices, build metadata, ELF
symbol-version requirements and executable checksums. It excludes configuration,
credentials, the Python environment and recordings. It does not bundle all dynamic
runtime dependencies; install those below. No published release is implied by a
local build.

## Prepare EC2

Keep the system Python and existing gateway environment unchanged:

```sh
sudo dnf install -y python3.11 python3.11-pip python3.11-libs speexdsp ca-certificates
sudo mkdir -p /opt/vogon-rust
sudo python3.11 -m venv /opt/vogon-rust/venv
sudo /opt/vogon-rust/venv/bin/python -m pip install 'numpy==2.2.6'
```

Copy `vogon-rust-al2023-x86_64.tar.gz` from the build output to the instance using
scp. Extract into a new versioned directory under `/opt/vogon-rust/releases/`.
Run `sha256sum -c SHA256SUMS` there, and `ldd ./vogon-gateway`; no library should
show `not found`. The checksum detects transfer corruption, not publisher identity.
Point `/opt/vogon-rust/current` to the new directory after verification.

Copy the existing gateway dotenv to `/etc/vogon-rust/gateway.env` with restricted
permissions. Set `GATEWAY_HOST=127.0.0.1`, `GATEWAY_PORT=8003` for initial checks,
and a writable `RECORDINGS_PATH`. Preserve service/provider credentials, the Turn
service binding and the actual external `GATEWAY_PUBLIC_BASE_URL` including any
proxy prefix. Resolve tenant/front/ambience file paths explicitly. Set `REDIS_CLUSTER_MODE=true` for cluster/serverless Redis, or leave it false
for standalone Redis. The earlier TLS-only binary does not include cluster support;
use a build containing the cluster fix. See OPERATIONS.md for namespace and
single-shard session metadata limits.

```sh
export PYTHONPATH=/opt/vogon-rust/venv/lib/python3.11/site-packages
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
/opt/vogon-rust/current/vogon-gateway --check --env /etc/vogon-rust/gateway.env
/opt/vogon-rust/current/vogon-gateway --env /etc/vogon-rust/gateway.env
# In another terminal:
curl --fail 'http://127.0.0.1:8003/ready?mode=twilio'
```

Run as a dedicated unprivileged service account with access to the dotenv and
recording directory. Carry the three environment values above into the supervisor;
allow at least 480 seconds for SIGTERM shutdown with the default gateway budgets.
Readiness validates local capabilities and configuration, not real provider/IAM
access. Pause new calls and finish existing Python calls before switching the
proxy. Route call creation, WebSockets and callbacks consistently to Rust. Do not
transfer ownership of active calls between implementations. Test normal speech,
barge-in, eager/final EOT, parallel calls and artifacts before wider traffic.
Rollback by draining Rust calls and restoring the Python upstream; retain state
and recordings. See the repository's OPERATIONS.md for acceptance limitations.
