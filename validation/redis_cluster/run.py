"""Own three real Redis cluster primaries and run store/API parity over plain TCP or TLS.

TLS terminators forward unchanged bytes to each Redis node, whose advertised
client ports point at those terminators. Redis cluster bus ports stay private on
loopback. No Redis commands/responses are emulated and no AWS access is needed.
"""
import argparse
import asyncio
import contextlib
import os
import json
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from validation.redis_tls.run import certificates


def free_port():
    """Reserve an available loopback port briefly for a test-owned Redis process."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--image", help="exercise the packaged gateway HTTP API instead of Cargo tests")
    args = parser.parse_args()
    server = os.environ.get("REDIS_SERVER_BIN") or shutil.which("redis-server")
    cli = os.environ.get("REDIS_CLI_BIN") or shutil.which("redis-cli")
    if not cli and server:
        cli = str(Path(server).with_name("redis-cli"))
    if not server or not cli:
        raise SystemExit("Set REDIS_SERVER_BIN and REDIS_CLI_BIN")
    processes, proxies = [], []
    with tempfile.TemporaryDirectory(prefix="vogon-cluster-") as temp:
        root = Path(temp)
        context, ca = certificates(root, "cluster", "DNS:localhost,IP:127.0.0.1")
        env = dict(os.environ, REDISCLI_AUTH="cluster-test-secret")
        if args.tls:
            env["SSL_CERT_FILE"] = str(ca)

        def command(port, *values):
            return subprocess.check_output([cli, "--raw", "-h", "127.0.0.1", "-p", str(port), *map(str, values)],
                                           env=env, text=True, stderr=subprocess.DEVNULL).strip()

        dropped_reply = False

        async def relay(reader, writer, port):
            peer = None
            jobs = []
            discard_response = False
            try:
                incoming, peer = await asyncio.open_connection("127.0.0.1", port)
                async def pump(source, destination, requests):
                    nonlocal dropped_reply, discard_response
                    seen = b""
                    while data := await source.read(65536):
                        if requests:
                            seen = (seen + data)[-4096:]
                            if not dropped_reply and b"EVAL" in seen and b"vogon-test-lost-reply" in seen:
                                dropped_reply = True
                                discard_response = True
                        elif discard_response:
                            # Redis executed the mutation. Lose the reply deliberately,
                            # proving the connector does not silently execute it again.
                            assert data == b":1\r\n", data
                            return
                        destination.write(data)
                        await destination.drain()
                jobs = [asyncio.create_task(pump(reader, peer, True)), asyncio.create_task(pump(incoming, writer, False))]
                await asyncio.wait(jobs, return_when=asyncio.FIRST_COMPLETED)
            except OSError:
                pass
            finally:
                for job in jobs:
                    job.cancel()
                await asyncio.gather(*jobs, return_exceptions=True)
                writer.close()
                if peer:
                    peer.close()

        try:
            nodes = []
            for index in range(3):
                port, bus = free_port(), free_port()
                proxy = await asyncio.start_server(lambda r, w, p=port: relay(r, w, p), "127.0.0.1", 0, ssl=context if args.tls else None)
                proxies.append(proxy)
                advertised = proxy.sockets[0].getsockname()[1]
                folder = root / str(index)
                folder.mkdir()
                process = await asyncio.create_subprocess_exec(
                    server, "--bind", "127.0.0.1", "--port", str(port),
                    "--cluster-enabled", "yes", "--cluster-port", str(bus),
                    "--cluster-announce-ip", "127.0.0.1", "--cluster-announce-port", str(advertised),
                    "--cluster-announce-bus-port", str(bus), "--cluster-node-timeout", "1000",
                    "--dir", str(folder), "--save", "", "--appendonly", "no",
                    "--requirepass", "cluster-test-secret", "--masterauth", "cluster-test-secret",
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
                processes.append(process)
                nodes.append((port, bus, advertised))
            for port, _, _ in nodes:
                for attempt in range(100):
                    try:
                        if command(port, "PING") == "PONG":
                            break
                    except subprocess.CalledProcessError:
                        pass
                    await asyncio.sleep(0.05)
                else:
                    raise RuntimeError("Redis node did not start")
                assert command(port, "ACL", "SETUSER", "noeval", "on", ">cluster-test-secret", "~*", "+@all", "-eval") == "OK"
            for index, (port, _, _) in enumerate(nodes):
                low, high = [(0, 5460), (5461, 10922), (10923, 16383)][index]
                assert command(port, "CLUSTER", "ADDSLOTSRANGE", low, high) == "OK"
                for other, bus, _ in nodes:
                    if other != port:
                        assert command(port, "CLUSTER", "MEET", "127.0.0.1", other, bus) == "OK"
            deadline = time.monotonic() + 15
            while not all("cluster_state:ok" in command(p, "CLUSTER", "INFO") for p, _, _ in nodes):
                if time.monotonic() > deadline:
                    raise RuntimeError("Cluster did not converge")
                await asyncio.sleep(0.1)
            scheme = "rediss" if args.tls else "redis"
            urls = [f"{scheme}://:cluster-test-secret@127.0.0.1:{p}/0" for _, _, p in nodes]
            env["VOGON_TEST_REDIS_CLUSTER_URL"] = urls[0]
            env["VOGON_TEST_REDIS_CLUSTER_SEEDS"] = ",".join(urls)
            print(f"Three-primary {'TLS' if args.tls else 'plain'} cluster ready", flush=True)
            if not args.image:
                child = await asyncio.create_subprocess_exec(
                    "cargo", "test", "--locked", "-p", "vogon-service", "--test", "store", "--test", "redis_cluster",
                    "--", "--include-ignored", "--test-threads=1", cwd=ROOT, env=env)
                assert await child.wait() == 0, "Cluster regression tests failed"
                assert dropped_reply, "lost-response fault was not exercised"
            else:
                async def docker(*values):
                    child = await asyncio.create_subprocess_exec("docker", *values, stdout=asyncio.subprocess.PIPE)
                    output, _ = await child.communicate()
                    assert child.returncode == 0, values[0]
                    return output.decode().strip()

                gateway_port = free_port()
                launch = ["run", "-d", "--network", "host", "-e", "GATEWAY_HOST=127.0.0.1",
                          "-e", f"GATEWAY_PORT={gateway_port}", "-e", "GATEWAY_SESSION_STORE=redis",
                          "-e", "REDIS_CLUSTER_MODE=true", "-e", f"REDIS_URL={urls[0]}",
                          "-e", "FRONT_MODEL_ENABLED=false", "-e", "GATEWAY_SERVICE_KEY=cluster-test-key"]
                if args.tls:
                    launch += ["-v", f"{ca}:/tls-ca.pem:ro", "-e", "SSL_CERT_FILE=/tls-ca.pem"]
                cid = await docker(*launch, args.image, "--env", "/nonexistent")
                try:
                    base = f"http://127.0.0.1:{gateway_port}"
                    def request(path, body=None):
                        data = None if body is None else json.dumps(body).encode()
                        req = urllib.request.Request(base + path, data=data, headers={
                            "Content-Type": "application/json", "Authorization": "Bearer cluster-test-key"})
                        with urllib.request.urlopen(req, timeout=5) as response:
                            assert response.status == 200
                            return json.load(response)
                    for attempt in range(100):
                        try:
                            readiness = await asyncio.to_thread(request, "/ready?mode=text")
                            assert readiness["ready"] is True
                            break
                        except OSError:
                            if attempt == 99:
                                raise
                            await asyncio.sleep(0.1)
                    body = await asyncio.to_thread(request, "/v1/tenants/cluster-image/calls", {
                        "mode": "text", "app": {"base_url": "http://127.0.0.1:9/turn/v1", "auth_token": ""}})
                    await asyncio.to_thread(request, "/v1/tenants/cluster-image/calls/" + body["call_id"])
                    print("Packaged gateway cluster startup, readiness, HTTP create and lookup: PASS", flush=True)
                    await docker("stop", "--time", "480", cid)
                    assert await docker("wait", cid) == "0"
                finally:
                    await docker("logs", cid)
                    await docker("rm", "-f", cid)
        finally:
            for proxy in proxies:
                proxy.close()
                await proxy.wait_closed()
            for process in processes:
                with contextlib.suppress(ProcessLookupError):
                    process.terminate()
            for process in processes:
                await process.wait()


if __name__ == "__main__":
    asyncio.run(main())
