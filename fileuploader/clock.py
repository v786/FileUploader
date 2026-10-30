"""Wall clock behind one function so tests can move time forward."""
from __future__ import annotations

import time


class Clock:
    def now(self) -> float:
        return time.time()


class FakeClock(Clock):
    def __init__(self, start: float = 1_800_000_000.0) -> None:
        self.t = start

    def now(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


_clock: Clock = Clock()


def now() -> float:
    return _clock.now()


def set_clock(c: Clock) -> None:
    global _clock
    _clock = c
