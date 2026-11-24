"""SQLite schema and connection helper."""

from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS inbox_events (
    idempotency_key TEXT PRIMARY KEY,
    body_sha256     TEXT NOT NULL,
    received_at     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key  TEXT NOT NULL UNIQUE,
    destination      TEXT NOT NULL,
    payload          TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'in_flight', 'sent', 'dead')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL,
    next_attempt_at  REAL NOT NULL,
    lease_expires_at REAL,
    last_error       TEXT,
    created_at       REAL NOT NULL,
    sent_at          REAL
);

CREATE INDEX IF NOT EXISTS outbox_due ON outbox (status, next_attempt_at);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    outbox_id  INTEGER NOT NULL REFERENCES outbox (id),
    attempt    INTEGER NOT NULL,
    at         REAL NOT NULL,
    outcome    TEXT NOT NULL,
    error      TEXT
);
"""


def connect(path: str = ":memory:") -> sqlite3.Connection:
    """Open a database and make sure the schema exists.

    isolation_level=None puts the connection in autocommit mode so every
    transaction in this package is opened and closed explicitly.
    """
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    if path != ":memory:":
        conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA)
    return conn
