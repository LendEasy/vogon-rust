"""Verify the packaged binaries in a clean runtime image without provider calls.

Run with Python 3 and Docker access. Only containers created here are removed.
"""

import argparse
import json
import subprocess
import time
import urllib.request
import uuid


def docker(*args):
    """Run Docker and return stdout, preserving failures and stderr."""
    return subprocess.check_output(["docker", *args], text=True).strip()


def main():
    """Check ABI/runtime dependencies, capabilities, readiness and clean shutdown."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="vogon-rust:al2023")
    image = parser.parse_args().image
    containers = []
    try:
        abi = docker("run", "--rm", "--entrypoint", "/usr/bin/ldd", image,
                     "/usr/local/bin/vogon-gateway")
        assert "not found" not in abi, abi
        print("LINKED LIBRARIES\n" + abi, flush=True)
        runtime = docker("run", "--rm", "--entrypoint", "/usr/bin/python3.11", image,
                         "-c", "import sys,platform,numpy,audioop; "
                         "assert sys.version_info[:2] == (3,11); "
                         "assert platform.libc_ver() == ('glibc','2.34'); "
                         "assert numpy.__version__ == '2.2.6'; "
                         "print(sys.version,platform.libc_ver(),numpy.__version__)")
        print("RUNTIME", runtime, flush=True)
        check = docker("run", "--rm", "-e", "FRONT_MODEL_ENABLED=false",
                       "-e", "GATEWAY_SESSION_STORE=memory", image,
                       "--check", "--env", "/nonexistent")
        assert json.loads(check)["ready"] is True, check
        print("STARTUP", check, flush=True)
        for kind, port in [("gateway", 8002), ("echo", 8090)]:
            args = ["run", "-d", "--name", f"vogon-al2023-{kind}-{uuid.uuid4().hex[:8]}",
                    "-p", f"127.0.0.1::{port}"]
            if kind == "echo":
                args += ["--entrypoint", "/usr/local/bin/vogon-echo-turn-service",
                         "-e", "ECHO_BIND=0.0.0.0:8090"]
            else:
                args += ["-e", "FRONT_MODEL_ENABLED=false", "-e", "GATEWAY_SESSION_STORE=memory"]
            cid = docker(*args, image)
            containers.append(cid)
            base = "http://" + docker("port", cid, f"{port}/tcp")
            path = "/ready?mode=text" if kind == "gateway" else "/health"
            for attempt in range(30):
                try:
                    with urllib.request.urlopen(base + path, timeout=2) as response:
                        result = json.load(response)
                    break
                except (OSError, ValueError):
                    if attempt == 29:
                        raise
                    time.sleep(1)
            if kind == "gateway":
                assert result["ready"] is True, result
            assert docker("exec", cid, "id", "-u") == "10001"
            print(kind, "READY", result, "UID 10001", flush=True)
            if kind == "echo":
                docker("kill", "--signal", "SIGINT", cid)
            else:
                docker("stop", "--time", "480", cid)
            code = docker("wait", cid)
            assert code == "0", (kind, code, docker("logs", cid))
            print(kind, "EXIT", code, flush=True)
    finally:
        for cid in containers:
            docker("rm", "-f", cid)


if __name__ == "__main__":
    main()
