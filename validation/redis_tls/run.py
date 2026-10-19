"""Test Rust stores or the packaged gateway against real Redis over verified TLS.

Requires openssl and redis-server (or REDIS_SERVER_BIN). A local TLS terminator
forwards bytes to real Redis; no Redis commands or responses are emulated.
"""

import argparse
import asyncio
import contextlib
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
PASSWORD = "tls-test:password"


def certificates(directory, name, san="DNS:localhost"):
    """Generate a private test CA and localhost-only server certificate."""
    base = directory / name
    base.mkdir()
    commands = [
        ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
         "-subj", f"/CN={name}", "-keyout", "ca.key", "-out", "ca.pem"],
        ["req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
         "-keyout", "server.key", "-out", "server.csr"],
        ["x509", "-req", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
         "-CAcreateserial", "-days", "2", "-extfile", "extensions", "-out", "server.pem"],
    ]
    (base / "extensions").write_text(
        f"subjectAltName={san}\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n"
    )
    for args in commands:
        subprocess.run(["openssl", *args], cwd=base, check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(base / "server.pem", base / "server.key")
    return context, base / "ca.pem"


async def main():
    """Own isolated Redis, TLS endpoints and child test processes; always clean up."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", help="test a packaged gateway image instead of Cargo tests")
    args = parser.parse_args()
    redis = os.environ.get("REDIS_SERVER_BIN") or shutil.which("redis-server")
    if not redis:
        raise SystemExit("Set REDIS_SERVER_BIN to a real redis-server executable")
    with tempfile.TemporaryDirectory(prefix="vogon-redis-tls-") as temp:
        directory = Path(temp)
        trusted, ca = certificates(directory, "trusted")
        untrusted, _ = certificates(directory, "untrusted")
        # A Unix socket avoids port-allocation races and network exposure for plaintext Redis.
        sock = str(directory / "redis.sock")
        process = await asyncio.create_subprocess_exec(
            redis, "--port", "0", "--unixsocket", sock, "--save", "",
            "--appendonly", "no", "--requirepass", PASSWORD,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )

        async def forward(reader, writer):
            """Relay an encrypted client session to the actual Redis socket."""
            upstream = None
            jobs = []
            try:
                upstream_reader, upstream = await asyncio.open_unix_connection(sock)

                async def pump(source, destination):
                    while data := await source.read(65536):
                        destination.write(data)
                        await destination.drain()

                jobs = [asyncio.create_task(pump(reader, upstream)),
                        asyncio.create_task(pump(upstream_reader, writer))]
                await asyncio.wait(jobs, return_when=asyncio.FIRST_COMPLETED)
            except (OSError, ConnectionError):
                pass
            finally:
                for job in jobs:
                    job.cancel()
                await asyncio.gather(*jobs, return_exceptions=True)
                writer.close()
                if upstream:
                    upstream.close()

        try:
            for _ in range(100):
                if Path(sock).exists():
                    break
                await asyncio.sleep(0.05)
            else:
                raise RuntimeError("Redis did not start")
            async with await asyncio.start_server(forward, "127.0.0.1", 0, ssl=trusted) as good, \
                    await asyncio.start_server(forward, "127.0.0.1", 0, ssl=untrusted) as bad:
                port = good.sockets[0].getsockname()[1]
                bad_port = bad.sockets[0].getsockname()[1]
                urls = {
                    "URL": f"rediss://:tls-test%3Apassword@localhost:{port}/0",
                    "BAD_HOST": f"rediss://:tls-test%3Apassword@127.0.0.1:{port}/0",
                    "UNTRUSTED": f"rediss://:tls-test%3Apassword@localhost:{bad_port}/0",
                    "BAD_AUTH": f"rediss://:wrong@localhost:{port}/0",
                }
                env = dict(os.environ, SSL_CERT_FILE=str(ca))
                env.update({"VOGON_TEST_REDIS_TLS_" + key: value for key, value in urls.items()})
                if not args.image:
                    child = await asyncio.create_subprocess_exec(
                        "cargo", "test", "--locked", "-p", "vogon-service", "--test", "redis_tls",
                        "--", "--include-ignored", cwd=ROOT, env=env,
                    )
                    assert await child.wait() == 0, "Redis TLS store tests failed"
                else:
                    for case, url in urls.items():
                        command = ["docker", "run", "--rm", "--network", "host",
                                   "-v", f"{ca}:/tls-ca.pem:ro",
                                   "-e", "SSL_CERT_FILE=/tls-ca.pem",
                                   "-e", "FRONT_MODEL_ENABLED=false",
                                   "-e", "GATEWAY_SESSION_STORE=redis",
                                   "-e", "GATEWAY_RETRY_REDIS_ATTEMPTS=1",
                                   "-e", f"REDIS_URL={url}", args.image,
                                   "--check", "--env", "/nonexistent"]
                        child = await asyncio.create_subprocess_exec(
                            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        stdout, stderr = await child.communicate()
                        if case == "URL":
                            import json
                            assert child.returncode == 0, stderr.decode()
                            assert json.loads(stdout)["ready"] is True, stdout
                        else:
                            assert child.returncode != 0, f"accepted {case}"
                            assert b"redis_unavailable" in stderr, stderr
                            assert PASSWORD.encode() not in stderr
                        print(f"Packaged gateway {case}: PASS", flush=True)
        finally:
            with contextlib.suppress(ProcessLookupError):
                process.terminate()
            await process.wait()


if __name__ == "__main__":
    asyncio.run(main())
