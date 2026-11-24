"""A fake downstream service that fails on purpose.

It keeps a small ledger in its own SQLite database, separate from the sender's
database, the way a real third party would. Failures come from a seeded
random generator so every run is reproducible.

Two failure shapes matter:

  fail before  the request is rejected and nothing is applied
  fail after   the effect is applied, then the response is lost

The second is the dangerous one. The sender cannot tell it from the first and
must retry, so only a dedupe on the idempotency key keeps the ledger correct.
"""

from __future__ import annotations

import json
import random
import sqlite3

from .signing import SignatureError, verify


class DownstreamError(Exception):
    """The call failed from the sender's point of view."""


class FlakyDownstream:
    def __init__(
        self,
        seed: int,
        secret: str,
        p_fail_before: float = 0.0,
        p_fail_after: float = 0.0,
        idempotent: bool = True,
        path: str = ":memory:",
    ) -> None:
        self.rng = random.Random(seed)
        self.secret = secret
        self.p_fail_before = p_fail_before
        self.p_fail_after = p_fail_after
        self.idempotent = idempotent
        self.calls = 0
        self.duplicates_suppressed = 0
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.executescript(
            """CREATE TABLE IF NOT EXISTS seen_keys (idempotency_key TEXT PRIMARY KEY);
               CREATE TABLE IF NOT EXISTS ledger (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   idempotency_key TEXT NOT NULL,
                   account TEXT NOT NULL,
                   amount_cents INTEGER NOT NULL
               );"""
        )

    def heal(self) -> None:
        self.p_fail_before = 0.0
        self.p_fail_after = 0.0

    def handle(self, idempotency_key: str, body: bytes, signature_header: str, now: float) -> None:
        """Apply one credit. Raises DownstreamError when chaos strikes."""
        self.calls += 1
        try:
            verify(self.secret, body, signature_header, now)
        except SignatureError as exc:
            raise DownstreamError(f"401 bad signature: {exc}") from None
        # Draw both rolls on every call so the random stream does not depend
        # on which branch was taken.
        roll_before, roll_after = self.rng.random(), self.rng.random()
        if roll_before < self.p_fail_before:
            raise DownstreamError("503 unavailable, nothing applied")
        self._apply(idempotency_key, json.loads(body))
        if roll_after < self.p_fail_after:
            raise DownstreamError("timeout, effect applied but response lost")

    def _apply(self, key: str, payload: dict) -> None:
        self.db.execute("BEGIN IMMEDIATE")
        if self.idempotent:
            cur = self.db.execute("INSERT OR IGNORE INTO seen_keys (idempotency_key) VALUES (?)", (key,))
            if cur.rowcount == 0:
                self.db.execute("ROLLBACK")
                self.duplicates_suppressed += 1
                return
        self.db.execute(
            "INSERT INTO ledger (idempotency_key, account, amount_cents) VALUES (?, ?, ?)",
            (key, payload["account"], int(payload["amount_cents"])),
        )
        self.db.execute("COMMIT")

    # Read side, used by tests and the demo to check the outcome.

    def effect_counts(self) -> dict:
        return dict(self.db.execute("SELECT idempotency_key, COUNT(*) FROM ledger GROUP BY 1"))

    def balances(self) -> dict:
        return dict(self.db.execute("SELECT account, SUM(amount_cents) FROM ledger GROUP BY 1"))
