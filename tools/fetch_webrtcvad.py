"""Fetch and verify the existing pinned VAD C source; never called implicitly by Cargo."""
import argparse
import hashlib
import io
from pathlib import Path, PurePosixPath
import tarfile
from urllib.request import urlopen

URL = "https://codeload.github.com/wiseman/py-webrtcvad/tar.gz/refs/tags/2.0.10"
SHA256 = "003c7681f7616221f6c4cfb748c9bd2b95a8f5367f16d276c7e984786e510c62"
ROOT = Path(__file__).resolve().parents[1]


def install(data, destination):
    """Install regular C/header/license files after checking the complete archive hash.

    Existing modified files are rejected rather than overwritten. Archive paths
    are mapped explicitly; symlinks and parent traversal are never extracted.
    """
    if hashlib.sha256(data).hexdigest() != SHA256:
        raise ValueError("WebRTC VAD archive checksum mismatch")
    files = {}
    prefix = "py-webrtcvad-2.0.10/"
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        for member in archive.getmembers():
            if not member.isfile() or not member.name.startswith(prefix):
                continue
            name = member.name[len(prefix):]
            if name == "LICENSE":
                relative = PurePosixPath("LICENSE")
            elif name.startswith("cbits/webrtc/"):
                relative = PurePosixPath(name[len("cbits/"):])
            else:
                continue
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("Invalid archive path")
            path = destination.joinpath(*relative.parts)
            content = archive.extractfile(member).read()
            if path.exists() and path.read_bytes() != content:
                raise ValueError(f"Refusing to overwrite modified source: {path}")
            files[path] = content
    if not files:
        raise ValueError("Archive contains no VAD sources")
    for path, content in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return len(files)


def main():
    """Allow a verified local archive for offline setup, or download the pinned release."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, help="Use an already downloaded archive")
    parser.add_argument("--destination", type=Path, default=ROOT / "crates/audio-core/vendor/webrtcvad")
    args = parser.parse_args()
    if args.archive:
        data = args.archive.read_bytes()
    else:
        with urlopen(URL, timeout=60) as response:
            data = response.read()
    count = install(data, args.destination)
    print(f"Verified WebRTC VAD 2.0.10: {count} files in {args.destination}")


if __name__ == "__main__":
    main()
