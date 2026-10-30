"""File families and magic-byte sniffing.

Type is checked by content (magic bytes), not by extension: in the create call
from the first bytes the client declares, and again at `complete` from the
stored object, because the client can't be trusted to tell the truth.
"""
from __future__ import annotations

import os

SNIFF_BYTES = 32

_ZIP = (b"PK\x03\x04", b"PK\x05\x06")
_OLE = (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1",)
MAGIC: dict[str, tuple[bytes, ...]] = {
    ".pdf": (b"%PDF",),
    ".png": (b"\x89PNG\r\n\x1a\n",),
    ".jpg": (b"\xff\xd8\xff",),
    ".jpeg": (b"\xff\xd8\xff",),
    ".gif": (b"GIF87a", b"GIF89a"),
    ".zip": _ZIP, ".docx": _ZIP, ".xlsx": _ZIP, ".pptx": _ZIP,
    ".doc": _OLE, ".xls": _OLE, ".ppt": _OLE,
}
TEXT_EXTENSIONS = {".txt", ".csv", ".json"}
_ISO_BOXES = (b"ftyp", b"moov", b"mdat", b"wide", b"free")  # mp4 / mov: box type at offset 4


def ext(name: str) -> str:
    return os.path.splitext(name)[1].lower()


def _known_binary(head: bytes) -> bool:
    return (any(head.startswith(m) for ms in MAGIC.values() for m in ms)
            or (head[:4] == b"RIFF" and head[8:12] == b"WEBP") or head[4:8] in _ISO_BOXES)


def matches(name: str, head: bytes) -> bool:
    """True if the first bytes of a file are consistent with its extension."""
    e = ext(name)
    if e in MAGIC:
        return any(head.startswith(m) for m in MAGIC[e])
    if e == ".webp":
        return head[:4] == b"RIFF" and head[8:12] == b"WEBP"
    if e in (".mp4", ".mov"):
        return head[4:8] in _ISO_BOXES
    if e in TEXT_EXTENSIONS:
        return not _known_binary(head)  # plain text has no magic; reject a binary in disguise
    return True


def family(name: str) -> str:
    e = ext(name).lstrip(".")
    if e == "pdf":
        return "pdf"
    if e in {"png", "jpg", "jpeg", "gif", "webp"}:
        return "image"
    if e in {"txt", "csv", "json"}:
        return "text"
    if e in {"doc", "docx", "xls", "xlsx", "ppt", "pptx"}:
        return "office"
    if e in {"mp4", "mov"}:
        return "video"
    return "other"


def size_bucket(size: int) -> str:
    for limit, label in ((1 << 20, "<1MB"), (10 << 20, "<10MB"), (100 << 20, "<100MB"), (1 << 30, "<1GB")):
        if size < limit:
            return label
    return ">=1GB"
