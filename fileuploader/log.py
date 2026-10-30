"""Structured JSON logs. Callers pass request_id / batch_id / file_id / job_id as fields."""
from __future__ import annotations

import json
import logging
import sys
import time

_logger = logging.getLogger("fileuploader")
if not _logger.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(_h)
    _logger.setLevel(logging.INFO)
    _logger.propagate = False


def log(event: str, level: int = logging.INFO, **fields) -> None:
    _logger.log(level, json.dumps({"ts": round(time.time(), 3), "event": event, **fields}, default=str))
