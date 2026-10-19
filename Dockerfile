# syntax=docker/dockerfile:1
FROM rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-dev python3-venv libspeexdsp-dev pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY requirements-runtime.txt tools/fetch_webrtcvad.py ./setup/
RUN python3 -m venv /opt/venv && /opt/venv/bin/python -m pip install --no-cache-dir -r setup/requirements-runtime.txt
COPY . .
RUN /opt/venv/bin/python tools/fetch_webrtcvad.py
ENV PYO3_PYTHON=/opt/venv/bin/python
ENV PYTHONPATH=/opt/venv/lib/python3.11/site-packages
RUN cargo build --locked --release -j 2 -p vogon-service --bin vogon-gateway -p vogon-echo-turn-service --bin vogon-echo-turn-service

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 libpython3.11 libspeexdsp1 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 vogon && useradd --uid 10001 --gid vogon --no-create-home vogon \
    && mkdir -p /data/recordings && chown -R vogon:vogon /data
COPY --from=build /opt/venv /opt/venv
COPY --from=build /src/target/release/vogon-gateway /usr/local/bin/
COPY --from=build /src/target/release/vogon-echo-turn-service /usr/local/bin/
COPY LICENSE crates/audio-core/WEBRTC_LICENSE /usr/share/licenses/vogon/
ENV PYTHONPATH=/opt/venv/lib/python3.11/site-packages \
    OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
    RECORDINGS_PATH=/data/recordings GATEWAY_HOST=0.0.0.0 GATEWAY_PORT=8002
USER 10001:10001
WORKDIR /data
EXPOSE 8002
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=15s --timeout=4s --start-period=15s --retries=3 \
  CMD ["python3", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8002/ready?mode=text', timeout=3)"]
ENTRYPOINT ["/usr/local/bin/vogon-gateway"]
CMD ["--env", "/run/secrets/vogon.env"]
