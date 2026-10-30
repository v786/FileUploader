"""Adapter wiring: config picks the implementation for every port."""
from __future__ import annotations

import os
from dataclasses import dataclass

from ..config import get_settings
from .object_store import LocalObjectStore
from .vendors import FaultStore, MockExtractor, MockScanner, MockThumbnailer


@dataclass
class Adapters:
    store: LocalObjectStore
    faults: FaultStore
    scanner: MockScanner
    extractor: MockExtractor
    thumbnailer: MockThumbnailer

    def vendor(self, name: str):
        return {"scanner": self.scanner, "extractor": self.extractor, "thumbnailer": self.thumbnailer}[name]


_adapters: Adapters | None = None


def build() -> Adapters:
    s = get_settings()
    if s.vendor_mode != "mock":
        # Open question in the design doc: which real vendors replace the mocks.
        # Live adapters implement the same ports (ports.py) and are wired here.
        raise NotImplementedError(f"VENDOR_MODE={s.vendor_mode!r}: no live adapters configured yet")
    store = LocalObjectStore(s.storage_dir, s.bucket, s.public_base_url, s.signing_secret)
    faults = FaultStore(os.path.join(s.storage_dir, "..", "mock_faults.json"))
    return Adapters(store, faults, MockScanner(faults, store), MockExtractor(faults, store),
                    MockThumbnailer(faults, store))


def get() -> Adapters:
    global _adapters
    if _adapters is None:
        _adapters = build()
    return _adapters


def reset() -> None:
    global _adapters
    _adapters = None
