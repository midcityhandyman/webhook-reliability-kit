"""Transactional outbox with a drain worker.

Delivery is at least once: a row is retried until the downstream confirms it
or the attempt budget runs out. Every call carries the row's idempotency key,
so a downstream that dedupes on that key sees each effect exactly once.

Row lifecycle:

    pending -> in_flight -> sent
                  |  ^
                  v  |  (failure: back to pending after a jittered delay)
                 dead   (attempt budget spent; replay() puts it back)
"""

from __future__ import annotations

import random
import sqlite3
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from .backoff import BackoffPolicy, next_delay

SendFn = Callable[[sqlite3.Row], None]


class WorkerCrash(BaseException):
    """Simulates the worker process dying. Deliberately not an Exception, so
    the drain loop cannot catch it and tidy up, just like a real kill."""


@dataclass
class DrainStats:
    claimed: int = 0
    sent: int = 0
    retried: int = 0
    dead: int = 0
    events: List[str] = field(default_factory=list)

    def add(self, other: "DrainStats") -> None:
        self.claimed += other.claimed
        self.sent += other.sent
        self.retried += other.retried
        self.dead += other.dead
        self.events.extend(other.events)


def enqueue(
    conn: sqlite3.Connection,
    idempotency_key: str,
    destination: str,
    payload: str,
    now: float,
    max_attempts: int = BackoffPolicy.max_attempts,
) -> bool:
    """Add an outbound call. Returns False if the key is already queued.

    Safe to call inside a caller's open transaction, which is the point: the
    business write and the outbox row commit together.
    """
    cur = conn.execute(
        """INSERT INTO outbox (idempotency_key, destination, payload, max_attempts,
                               next_attempt_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT (idempotency_key) DO NOTHING""",
        (idempotency_key, destination, payload, max_attempts, now, now),
    )
    return cur.rowcount == 1


def _log(conn, row_id, attempt, now, outcome, error=None) -> None:
    conn.execute(
        "INSERT INTO delivery_attempts (outbox_id, attempt, at, outcome, error) VALUES (?, ?, ?, ?, ?)",
        (row_id, attempt, now, outcome, error),
    )


def _claim(conn: sqlite3.Connection, now: float, batch: int, lease_seconds: float, stats: DrainStats):
    """Atomically take due rows. A row left in_flight by a dead worker becomes
    claimable again once its lease expires."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        # A row whose final attempt never reported back has an unknown outcome.
        # Park it as dead for a human or a replay instead of retrying forever.
        orphans = conn.execute(
            """SELECT id, attempts FROM outbox
               WHERE status = 'in_flight' AND lease_expires_at <= ? AND attempts >= max_attempts""",
            (now,),
        ).fetchall()
        for row in orphans:
            conn.execute(
                "UPDATE outbox SET status = 'dead', lease_expires_at = NULL, last_error = ? WHERE id = ?",
                ("lease expired on final attempt, outcome unknown", row["id"]),
            )
            _log(conn, row["id"], row["attempts"], now, "dead", "lease expired on final attempt")
            stats.dead += 1
            stats.events.append(f"row {row['id']} dead (lease expired on final attempt)")
        ids = [
            r["id"]
            for r in conn.execute(
                """SELECT id FROM outbox
                   WHERE (status = 'pending' AND next_attempt_at <= ?)
                      OR (status = 'in_flight' AND lease_expires_at <= ?)
                   ORDER BY next_attempt_at, id LIMIT ?""",
                (now, now, batch),
            )
        ]
        rows = []
        for row_id in ids:
            conn.execute(
                """UPDATE outbox SET status = 'in_flight', attempts = attempts + 1,
                                     lease_expires_at = ? WHERE id = ?""",
                (now + lease_seconds, row_id),
            )
            rows.append(conn.execute("SELECT * FROM outbox WHERE id = ?", (row_id,)).fetchone())
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    return rows


def drain_once(
    conn: sqlite3.Connection,
    send: SendFn,
    now: float,
    rng: random.Random,
    policy: BackoffPolicy = BackoffPolicy(),
    batch: int = 10,
    lease_seconds: float = 30.0,
) -> DrainStats:
    """Claim due rows and try each one once.

    send(row) must raise an Exception on failure. The attempt counter is
    bumped at claim time, before the call, so a crash mid call still counts
    against the budget.
    """
    stats = DrainStats()
    rows = _claim(conn, now, batch, lease_seconds, stats)
    stats.claimed = len(rows)
    for row in rows:
        try:
            send(row)
        except Exception as exc:  # WorkerCrash passes straight through
            error = f"{type(exc).__name__}: {exc}"[:500]
            if row["attempts"] >= row["max_attempts"]:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE outbox SET status = 'dead', lease_expires_at = NULL, last_error = ? WHERE id = ?",
                    (error, row["id"]),
                )
                _log(conn, row["id"], row["attempts"], now, "dead", error)
                conn.execute("COMMIT")
                stats.dead += 1
                stats.events.append(f"row {row['id']} dead after {row['attempts']} attempts ({error})")
            else:
                delay = next_delay(policy, row["attempts"], rng)
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """UPDATE outbox SET status = 'pending', lease_expires_at = NULL,
                                         next_attempt_at = ?, last_error = ? WHERE id = ?""",
                    (now + delay, error, row["id"]),
                )
                _log(conn, row["id"], row["attempts"], now, "retry", error)
                conn.execute("COMMIT")
                stats.retried += 1
                stats.events.append(
                    f"row {row['id']} attempt {row['attempts']} failed, retry in {delay:.2f}s ({error})"
                )
        else:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """UPDATE outbox SET status = 'sent', lease_expires_at = NULL,
                                     sent_at = ?, last_error = NULL WHERE id = ?""",
                (now, row["id"]),
            )
            _log(conn, row["id"], row["attempts"], now, "sent")
            conn.execute("COMMIT")
            stats.sent += 1
            stats.events.append(f"row {row['id']} sent on attempt {row['attempts']}")
    return stats


def next_wakeup(conn: sqlite3.Connection) -> Optional[float]:
    """Earliest time at which some row becomes claimable, or None if idle."""
    row = conn.execute(
        """SELECT MIN(CASE status WHEN 'pending' THEN next_attempt_at
                                  ELSE lease_expires_at END) AS t
           FROM outbox WHERE status IN ('pending', 'in_flight')"""
    ).fetchone()
    return row["t"]


def run_until_idle(conn, send, clock, rng, policy=BackoffPolicy(), batch=10,
                   lease_seconds=30.0, max_rounds=100_000) -> DrainStats:
    """Drive drain_once with a FakeClock until every row is sent or dead.

    A WorkerCrash from send is treated as a process restart: the round ends
    and the next round starts with whatever the database says.
    """
    total = DrainStats()
    for _ in range(max_rounds):
        wake = next_wakeup(conn)
        if wake is None:
            return total
        clock.set(wake)
        try:
            total.add(drain_once(conn, send, clock.now(), rng, policy, batch, lease_seconds))
        except WorkerCrash as crash:
            total.events.append(f"worker crashed ({crash}), restarting")
    raise RuntimeError("outbox did not go idle; check for a row that can never finish")


def replay(conn: sqlite3.Connection, outbox_id: int, now: float) -> None:
    """Put a dead or sent row back in the queue with a fresh attempt budget.

    The idempotency key is unchanged, so replaying a row whose effect already
    landed is harmless for a downstream that dedupes on the key.
    """
    cur = conn.execute(
        """UPDATE outbox SET status = 'pending', attempts = 0, next_attempt_at = ?,
                             lease_expires_at = NULL, sent_at = NULL
           WHERE id = ? AND status IN ('dead', 'sent')""",
        (now, outbox_id),
    )
    if cur.rowcount != 1:
        raise ValueError(f"outbox row {outbox_id} is not dead or sent")
    _log(conn, outbox_id, 0, now, "replay")


def replay_dead(conn: sqlite3.Connection, now: float) -> int:
    ids = [r["id"] for r in conn.execute("SELECT id FROM outbox WHERE status = 'dead' ORDER BY id")]
    for row_id in ids:
        replay(conn, row_id, now)
    return len(ids)
