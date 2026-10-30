"""Generate fixtures/: sample files per type, an EICAR test file, a zero-byte
file, a multipart-sized file, an unsupported type, and (with --huge) a 6 GB
sparse file for the size limit.

A "file with a deliberately wrong checksum" is a client-side condition: send
a sha256 that doesn't match the bytes (see tests/test_upload_api.py)."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from fileuploader.adapters.vendors import EICAR, PLACEHOLDER_PNG  # noqa: E402

OUT = Path(__file__).resolve().parent.parent / "fixtures"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--huge", action="store_true", help="also create a 6 GB sparse file")
    args = p.parse_args()
    OUT.mkdir(exist_ok=True)
    (OUT / "sample.txt").write_text("hello from the fixtures\n" * 100)
    (OUT / "sample.csv").write_text("id,name\n" + "".join(f"{i},row {i}\n" for i in range(500)))
    (OUT / "sample.pdf").write_bytes(b"%PDF-1.4\n% fake pdf for tests\n" + b"0" * 4096 + b"\n%%EOF\n")
    (OUT / "sample.png").write_bytes(PLACEHOLDER_PNG)
    (OUT / "sample.json").write_text('{"hello": "world"}\n')
    (OUT / "eicar.txt").write_bytes(EICAR)
    (OUT / "empty.txt").write_bytes(b"")
    (OUT / "not-allowed.exe").write_bytes(b"MZ\x90\x00")
    with open(OUT / "multipart-20mb.zip", "wb") as f:
        block = os.urandom(1024 * 1024)
        for _ in range(20):
            f.write(block)
    if args.huge:
        with open(OUT / "too-big-6gb.mp4", "wb") as f:
            f.truncate(6 * 1024 ** 3)
    print(f"wrote fixtures to {OUT}")


if __name__ == "__main__":
    main()
