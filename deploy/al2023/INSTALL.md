# Install and switch the EC2 gateway to Rust

Deployment: Amazon Linux 2023 x86-64, glibc 2.34, Python 3.11. All services run
as `codelexin`. The existing Python unit is **`voice-gateway.service`**; the new
unit is **`vogon-rust.service`**. This guide replaces only the gateway. Keep the
Python Turn service, speech credentials, Redis and external callback URLs.

The live listening address/port must be checked on EC2. Examples below assume
the existing gateway listens on `127.0.0.1:8002`. Rust first runs on 8003 for
local checks, then takes over the existing listener so proxy routes stay intact.
If the current gateway has a different bind address/port, use those exact values
for the final cutover, including `0.0.0.0` when a remote proxy/load balancer needs it.

## 1. Install runtime prerequisites (once)

```bash
sudo dnf install -y python3.11 python3.11-pip python3.11-libs speexdsp ca-certificates
sudo mkdir -p /opt/vogon-rust
sudo python3.11 -m venv /opt/vogon-rust/venv
sudo /opt/vogon-rust/venv/bin/python -m pip install 'numpy==2.2.6'
```

Leave system Python 3.9 and the existing Python gateway environment unchanged.
No Rust compiler is needed on EC2. Build instructions are in [README.md](README.md).

## 2. Copy the corrected cluster/TLS build and verify it

From the development machine (replace SSH placeholders):

```bash
scp -i /path/to/key.pem \
  /mnt/disk2/codelexin/vogon-rust/target/al2023-redis-cluster/vogon-rust-al2023-redis-cluster-x86_64.tar.gz \
  ec2-user@YOUR_EC2_HOST:~/vogon-rust-al2023-redis-cluster-x86_64.tar.gz
```

On EC2:

```bash
sudo install -d -m 755 /opt/vogon-rust/releases/redis-cluster-fixed
sudo tar -xzf ~/vogon-rust-al2023-redis-cluster-x86_64.tar.gz \
  -C /opt/vogon-rust/releases/redis-cluster-fixed
cd /opt/vogon-rust/releases/redis-cluster-fixed
sha256sum -c SHA256SUMS
sha256sum vogon-gateway
ldd ./vogon-gateway
```

The corrected artifact from this session has gateway SHA-256:

```text
443ad1e2577471f719c67d320e727159a16cfb3e28cf1ae8d09cc4c4b936e3c7
```

Do not proceed with `6dd4b6a6564b4bce4d2bbeac638c7fa07877d67fdceb7d6da8ba05e44fa25d9c`:
that is the old binary without Redis TLS. Merely renaming its installation
directory does not update the executable. Future builds have different checksums;
use their corresponding trusted build metadata. `ldd` must not report missing
libraries. Extract subsequent releases into new directories, not over a running
executable.

## 3. Set ownership and configuration

```bash
SERVICE_GROUP="$(id -gn codelexin)"
sudo chown -R root:root /opt/vogon-rust/releases/redis-cluster-fixed
sudo find /opt/vogon-rust/releases/redis-cluster-fixed -type d -exec chmod 755 {} +
sudo find /opt/vogon-rust/releases/redis-cluster-fixed -type f -exec chmod 644 {} +
sudo chmod 755 /opt/vogon-rust/releases/redis-cluster-fixed/vogon-gateway \
  /opt/vogon-rust/releases/redis-cluster-fixed/vogon-echo-turn-service
sudo install -d -o root -g "$SERVICE_GROUP" -m 750 /etc/vogon-rust
sudo install -d -o codelexin -g "$SERVICE_GROUP" -m 750 /var/lib/vogon-rust/recordings
```

If `/etc/vogon-rust/gateway.env` is already configured, retain it. Otherwise copy
the existing Python dotenv once, substituting its actual path:

```bash
sudo install -o root -g "$SERVICE_GROUP" -m 640 \
  /PATH/TO/voice-gateway/.env /etc/vogon-rust/gateway.env
```

For either case, enforce restricted permissions and edit settings:

```bash
sudo chown root:"$SERVICE_GROUP" /etc/vogon-rust/gateway.env
sudo chmod 640 /etc/vogon-rust/gateway.env
sudoedit /etc/vogon-rust/gateway.env
```

For initial checks, replace existing entries rather than adding duplicate keys:

```dotenv
GATEWAY_HOST=127.0.0.1
GATEWAY_PORT=8003
GATEWAY_SESSION_STORE=redis
GATEWAY_CAPABILITY_POLICY=refuse
GATEWAY_MAX_ACTIVE_CALLS=4
GATEWAY_SHUTDOWN_TIMEOUT_SEC=210
RECORDINGS_PATH=/var/lib/vogon-rust/recordings
```

Keep the existing `rediss://` URL, credentials and external
`GATEWAY_PUBLIC_BASE_URL` (including any proxy prefix). Keep Turn service URLs and
per-call app bindings reachable. Convert relative tenant-profile/front/ambience
paths to absolute paths readable by `codelexin`. For cluster/serverless Redis set
`REDIS_CLUSTER_MODE=true` using a build containing the cluster fix. Automatic
IAM-token refresh is not implemented. Cluster sessions use a separate same-slot
namespace; drain old calls before switching. The TLS-only artifact
from the earlier deployment predates this fix and must not be used for cluster deployments.
The archive and checksum in this updated guide include both TLS and cluster support.
For a standalone Redis deployment, keep `REDIS_CLUSTER_MODE=false`.

## 4. Validate the candidate as the service account

```bash
sudo -u codelexin test -r /etc/vogon-rust/gateway.env
sudo -u codelexin test -w /var/lib/vogon-rust/recordings
sudo -u codelexin env \
  PYTHONPATH=/opt/vogon-rust/venv/lib/python3.11/site-packages \
  OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  /opt/vogon-rust/releases/redis-cluster-fixed/vogon-gateway \
    --check --env /etc/vogon-rust/gateway.env
```

Require successful native probes and `"ready":true`. Root-owned binaries and
the Python environment only need read/execute access; the service needs write
access to recordings. Do not use `chmod 777` or world-readable dotenv files.
Readiness does not validate live provider calls or AWS S3 permissions.

## 5. Install the Rust systemd unit

Use the supplied [vogon-rust.service](vogon-rust.service), or create it on EC2:

```bash
sudo tee /etc/systemd/system/vogon-rust.service >/dev/null <<'EOF'
[Unit]
Description=Vogon Rust Gateway
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=codelexin
WorkingDirectory=/opt/vogon-rust/current
Environment=PYTHONPATH=/opt/vogon-rust/venv/lib/python3.11/site-packages
Environment=OPENBLAS_NUM_THREADS=1
Environment=OMP_NUM_THREADS=1
ExecStart=/opt/vogon-rust/current/vogon-gateway --env /etc/vogon-rust/gateway.env
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=480
UMask=0027

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
```

The service inherits `codelexin`'s primary group. No EnvironmentFile is needed:
the executable reads the dotenv directly. The supervisor grace covers the
gateway's sequential shutdown phases. If Rust is already serving calls, drain
them before stopping/repointing its release symlink.

## 6. Switch the existing listener to Rust

First inspect the live deployment locally; do not paste configurations containing
secrets into chat:

```bash
sudo systemctl status voice-gateway.service --no-pager
sudo ss -ltnp
sudo systemctl is-enabled voice-gateway.service
```

Record the original bind address/port and boot-enable state. Pause new calls in
the calling application and wait for active calls to finish. Stopping either
gateway tears down calls; it does not migrate them. Keep creation, media sockets,
TwiML, status and transfer callbacks owned by one gateway for each call. If there
are multiple instances, do not mix active Python/Rust ownership via a load balancer.

After calls drain, stop any candidate Rust process on 8003 and edit its dotenv:

```bash
sudo systemctl stop vogon-rust.service
sudoedit /etc/vogon-rust/gateway.env
```

Set `GATEWAY_HOST` and `GATEWAY_PORT` to the original Python listener (normally
`127.0.0.1` and `8002`). Then:

```bash
sudo ln -sfnT /opt/vogon-rust/releases/redis-cluster-fixed /opt/vogon-rust/current
sudo systemctl stop voice-gateway.service
sudo systemctl start vogon-rust.service
sudo systemctl status vogon-rust.service --no-pager
sudo journalctl -u vogon-rust.service -n 60 --no-pager
curl --fail 'http://127.0.0.1:8002/ready?mode=twilio'
```

Adjust curl's port if needed. Require `"ready":true`. No proxy change is needed
when the original listener is preserved. If the proxy was previously changed to
8003, restore its upstream to the chosen final port and use that proxy's config
validation/reload procedure. A load-balancer health-check path may need updating
if it uses a Python-specific route. Check readiness through the external route
as well, then resume a small number of test calls.

Test ordinary conversation, several turns, interruption during playback, a pause
followed by additional words (eager/final EOT), two concurrent calls, hangup and
recording output. Keep the Python Turn service running. Once these pass:

```bash
sudo systemctl disable voice-gateway.service
sudo systemctl enable vogon-rust.service
```

Do not leave both gateways enabled to contend for the same port after reboot.

## 7. Rollback

Pause new calls and let Rust calls finish. Then restore Python on its original
listener and preserve its original configuration:

```bash
sudo systemctl stop vogon-rust.service
sudo systemctl disable vogon-rust.service
sudo systemctl start voice-gateway.service
sudo systemctl status voice-gateway.service --no-pager
```

Restore Python's prior boot-enable state (`sudo systemctl enable
voice-gateway.service` if it was originally enabled). Restore any proxy changes,
verify the original health check, and resume calls. Keep Redis state, recordings,
both releases and both dotenv files. No uninstall or data deletion is required.

## Troubleshooting

- `invalid_redis_url`: first check the executable SHA-256. The original artifact
  rejects every `rediss://` URL. With the corrected artifact, check URL syntax,
  percent-encoded credentials, numeric database path and fragments without
  disclosing credentials.
- `redis_unavailable`: URL parsing succeeded; inspect endpoint reachability,
  Redis authentication, CA trust and hostname verification. Do not disable TLS
  or certificate validation as a workaround.
- `Address already in use`: check the old gateway and any manually launched Rust
  process with `ss -ltnp`; do not kill an unrelated service occupying the port.
- Permission failure: check directory traversal permissions on `/opt/vogon-rust`,
  environment readability, asset readability and recording-directory ownership
  as `codelexin`. Keep binaries/configuration root-owned.

See [validation evidence](VALIDATION.md) and [operations](../../OPERATIONS.md).
The local build and tests are complete; the remote cutover must be verified on
the actual instance. This document does not claim it has already happened.
