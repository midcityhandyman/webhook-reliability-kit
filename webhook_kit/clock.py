"""A clock you can inject. Tests and the demo use FakeClock so nothing sleeps."""

from __future__ import annotations

import time


class SystemClock:
    def now(self) -> float:
        return time.time()


class FakeClock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("time only moves forward")
        self._now += seconds

    def set(self, when: float) -> None:
        self.advance(max(0.0, when - self._now))
