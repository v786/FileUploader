"""Token buckets shared by every process through SQLite.

The doc uses a Redis Lua script for an atomic check-and-take; here one
BEGIN IMMEDIATE transaction gives the same atomicity across pods/processes.
"""
from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass

from . import clock, db
from .config import get_limits


@dataclass
class BucketSpec:
    key: str
    rate: float   # tokens/second
    burst: float  # capacity


@dataclass
class Decision:
    allowed: bool
    wait_s: float        # time until the request could succeed (0 if allowed)
    remaining: float     # tokens left in the tightest bucket
    limit: float         # capacity of the tightest bucket
    reset_s: float       # seconds until the tightest bucket is full again


def _load(conn: sqlite3.Connection, spec: BucketSpec, now: float) -> float:
    row = conn.execute("SELECT tokens, updated_at FROM rate_buckets WHERE key = ?", (spec.key,)).fetchone()
    if row is None:
        return spec.burst
    return min(spec.burst, row["tokens"] + max(0.0, now - row["updated_at"]) * spec.rate)


def take(specs: list[BucketSpec], cost: float = 1.0) -> Decision:
    """All-or-nothing take of `cost` tokens from every bucket."""
    now = clock.now()
    with db.tx() as conn:
        levels = [(_load(conn, s, now), s) for s in specs]
        waits = [0.0 if lvl >= cost else (cost - lvl) / s.rate for lvl, s in levels]
        allowed = all(w == 0.0 for w in waits)
        for lvl, s in levels:
            new = lvl - cost if allowed else lvl
            conn.execute(
                "INSERT INTO rate_buckets (key, tokens, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET tokens = excluded.tokens, updated_at = excluded.updated_at",
                (s.key, new, now),
            )
    tight_lvl, tight = min(levels, key=lambda p: p[0] / p[1].burst)
    remaining = tight_lvl - cost if allowed else tight_lvl
    return Decision(
        allowed=allowed,
        wait_s=max(waits),
        remaining=max(0.0, remaining),
        limit=tight.burst,
        reset_s=(tight.burst - max(0.0, remaining)) / tight.rate,
    )


def drain(keys: list[str], wait_s: float, specs: list[BucketSpec]) -> None:
    """A vendor 429 empties our local bucket so it self-corrects to the real quota.

    We set tokens negative enough that the next token arrives after `wait_s`.
    """
    now = clock.now()
    with db.tx() as conn:
        for s in specs:
            conn.execute(
                "INSERT INTO rate_buckets (key, tokens, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET tokens = excluded.tokens, updated_at = excluded.updated_at",
                (s.key, 1.0 - wait_s * s.rate, now),
            )


# ---- named limits from config/limits.yaml ----

def inbound_spec(name: str, user_id: str) -> BucketSpec:
    cfg = get_limits()["inbound"][name]
    return BucketSpec(f"in:{name}:{user_id}", float(cfg["rate"]), float(cfg["burst"]))


def vendor_specs(vendor: str) -> list[BucketSpec]:
    out = []
    for i, cfg in enumerate(get_limits()["outbound"].get(vendor, [])):
        out.append(BucketSpec(f"out:{vendor}:{cfg.get('name', i)}", float(cfg["rate"]), float(cfg["burst"])))
    return out


def acquire_vendor(vendor: str) -> float:
    """Returns 0 if a token was taken, else the exact wait in seconds."""
    specs = vendor_specs(vendor)
    if not specs:
        return 0.0
    d = take(specs)
    return 0.0 if d.allowed else d.wait_s


def headers(d: Decision) -> dict[str, str]:
    h = {
        "RateLimit-Limit": str(int(d.limit)),
        "RateLimit-Remaining": str(int(d.remaining)),
        "RateLimit-Reset": str(math.ceil(d.reset_s)),
    }
    if not d.allowed:
        h["Retry-After"] = str(max(1, math.ceil(d.wait_s)))
    return h
