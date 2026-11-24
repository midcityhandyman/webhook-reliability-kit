"""Exponential backoff with full jitter.

The ceiling doubles on every attempt up to a cap. The actual delay is drawn
uniformly between zero and that ceiling, which spreads retries out so a crowd
of failed rows does not hammer a recovering downstream at the same instant.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class BackoffPolicy:
    base_seconds: float = 1.0
    cap_seconds: float = 300.0
    max_attempts: int = 6

    def ceiling(self, attempt: int) -> float:
        """Largest possible delay after the given attempt number (1 based)."""
        if attempt < 1:
            raise ValueError("attempt is 1 based")
        # Clamp the exponent so a huge attempt count cannot overflow a float.
        return min(self.cap_seconds, self.base_seconds * (2 ** min(attempt - 1, 60)))


def next_delay(policy: BackoffPolicy, attempt: int, rng: random.Random) -> float:
    return rng.uniform(0.0, policy.ceiling(attempt))
