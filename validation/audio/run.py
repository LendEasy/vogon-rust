"""Build the Rust audio core and run offline compatibility checks. No timing."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import sysconfig
import importlib

ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parents[1]
MONOREPO = WORKSPACE.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("aec", "mixer", "formats", "runtime", "echo_runtime", "native", "all"), default="all")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    env = dict(os.environ)
    env.update(OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1", MKL_NUM_THREADS="1",
               NUMEXPR_NUM_THREADS="1", PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1",
               PYO3_PYTHON=sys.executable, RUSTFLAGS="-C target-cpu=native",
               PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
    env["PYTHONPATH"] = os.pathsep.join(dict.fromkeys([sysconfig.get_path("purelib"),
        sysconfig.get_path("platlib"), str(MONOREPO / "voice-gateway"),
        str(MONOREPO / "voice-gateway/tests"), str(MONOREPO / "vogon-contract/src")]))
    cargo = shutil.which("cargo")
    if cargo is None:
        cargo = "/tmp/vogon-rust-toolchain/cargo/bin/cargo"
        env.update(CARGO_HOME="/tmp/vogon-rust-toolchain/cargo", RUSTUP_HOME="/tmp/vogon-rust-toolchain/rustup")
    if not args.skip_build:
        subprocess.run([cargo, "build", "--release", "--offline", "--locked", "-p", "vogon-audio-core", "-p", "vogon-audio-runtime"],
                       cwd=WORKSPACE, env=env, check=True)
    stages = ("aec", "mixer", "formats", "runtime", "echo_runtime", "native") if args.stage == "all" else (args.stage,)
    results = ROOT / "results"
    results.mkdir(exist_ok=True)
    binary = WORKSPACE / "target/release/vogon-audio-validation"
    for stage in stages:
        stage_binary = WORKSPACE / "target/release/vogon-runtime-validation" if stage in ("runtime", "echo_runtime", "native") else binary
        subprocess.run([str(stage_binary), str(ROOT / f"{stage}_checks.py")], cwd=WORKSPACE, env=env, check=True)
    files = [*sorted((WORKSPACE / "crates/audio-core").rglob("*.rs")), *sorted((WORKSPACE / "crates/audio-runtime").rglob("*.rs")), *sorted(ROOT.glob("*.py")),
             WORKSPACE / "Cargo.lock", WORKSPACE / "crates/audio-core/Cargo.toml", WORKSPACE / "crates/audio-runtime/Cargo.toml",
             *sorted(p for p in (WORKSPACE / "crates/audio-core/vendor").rglob("*") if p.is_file()),
             *sorted(p for p in (ROOT / "fixtures").rglob("*") if p.is_file())]
    references = [*sorted((MONOREPO / "voice-gateway/vogon/gateway/transport/ambience").glob("*.py")),
                  MONOREPO / "voice-gateway/vogon/gateway/transport/aec.py",
                  MONOREPO / "voice-gateway/vogon/gateway/transport/twilio_transport.py",
                  MONOREPO / "voice-gateway/vogon/gateway/transport/voice_transport.py",
                  MONOREPO / "voice-gateway/vogon/gateway/transport/barge_gate.py",
                  *sorted((MONOREPO / "voice-gateway/tests/aec").glob("*.py")),
                  MONOREPO / "voice-gateway/tests/test_ambience_mixer.py",
                  MONOREPO / "voice-gateway/tests/transport_fixtures.py"]
    libraries = {}
    for name in ("numpy", "audioop", "speexdsp._speexdsp", "webrtcvad", "_webrtcvad"):
        module = importlib.import_module(name)
        path = Path(module.__file__)
        libraries[name] = {"version": getattr(module, "__version__", None), "path":str(path),
                           "module_file_sha256":hashlib.sha256(path.read_bytes()).hexdigest()}
    linkage = subprocess.check_output(["ldd", str(binary)], text=True)
    speex_path = Path(next(line.split("=>", 1)[1].strip().split()[0]
                           for line in linkage.splitlines() if "libspeexdsp.so" in line)).resolve()
    libraries["native_speexdsp"] = {"path": str(speex_path),
                                   "sha256": hashlib.sha256(speex_path.read_bytes()).hexdigest()}
    meta = {"stages": stages, "python": sys.version, "python_executable": sys.executable,
            "rustc": subprocess.check_output([str(Path(cargo).with_name("rustc")), "-Vv"], env=env, text=True),
            "source_hashes": {str(p.relative_to(MONOREPO)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files + references},
            "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "runtime_binary_sha256": hashlib.sha256((WORKSPACE / "target/release/vogon-runtime-validation").read_bytes()).hexdigest(),
            "libraries": libraries,
            "native_linkage": linkage,
            "native_speex_version": subprocess.check_output(["pkg-config", "--modversion", "speexdsp"], text=True).strip(),
            "c_compiler": subprocess.check_output([env.get("CC", "cc"), "--version"], text=True).splitlines()[0],
            "scope": "Offline correctness only; no wrapper, gateway or numerical performance measurement"}
    (results / "environment.json").write_text(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
