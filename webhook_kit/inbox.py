"""Inbound webhook handling: verify, dedupe, then record the work to do.

The dedupe row and every outbox row the handler enqueues are written in one
transaction. Either the event is recorded together with all of its outbound
work, or none of it is, so a crash can never leave an accepted event with no
follow up, and a redelivery can never enqueue the work twice.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Callable, Iterable, Union

from .signing import verify


class IdempotencyConflict(Exception):
    """The same idempotency key arrived again with a different body."""


Handler = Callable[[sqlite3.Connection, dict], None]


def receive(
    conn: sqlite3.Connection,
    body: bytes,
    signature_header: str,
    secrets: Union[str, Iterable[str]],
    now: float,
    handler: Handler,
    key_field: str = "id",
) -> str:
    """Process one webhook delivery. Returns "accepted" or "duplicate".

    Raises SignatureError for a bad or stale signature, ValueError for a body
    without a usable key, and IdempotencyConflict when a key is reused with a
    different body.
    """
    verify(secrets, body, signature_header, now)
    try:
        event = json.loads(body)
        key = event[key_field]
    except (ValueError, KeyError, TypeError):
        raise ValueError(f"body must be a JSON object with a {key_field!r} field") from None
    if not isinstance(key, str) or not key:
        raise ValueError(f"{key_field!r} must be a non empty string")
    body_hash = hashlib.sha256(body).hexdigest()

    conn.execute("BEGIN IMMEDIATE")
    try:
        seen = conn.execute(
            "SELECT body_sha256 FROM inbox_events WHERE idempotency_key = ?", (key,)
        ).fetchone()
        if seen is not None:
            conn.execute("ROLLBACK")
            if seen["body_sha256"] != body_hash:
                raise IdempotencyConflict(f"key {key!r} was reused with a different body")
            return "duplicate"
        conn.execute(
            "INSERT INTO inbox_events (idempotency_key, body_sha256, received_at) VALUES (?, ?, ?)",
            (key, body_hash, now),
        )
        handler(conn, event)
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    return "accepted"
