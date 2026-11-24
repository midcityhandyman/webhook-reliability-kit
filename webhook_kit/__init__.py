"""Reliability patterns for webhooks and outbound calls, backed by SQLite."""

from .backoff import BackoffPolicy, next_delay
from .db import connect
from .downstream import DownstreamError, FlakyDownstream
from .inbox import IdempotencyConflict, receive
from .outbox import DrainStats, WorkerCrash, drain_once, enqueue, replay, run_until_idle
from .signing import SignatureError, sign, verify

__all__ = [
    "BackoffPolicy",
    "DownstreamError",
    "DrainStats",
    "FlakyDownstream",
    "IdempotencyConflict",
    "SignatureError",
    "WorkerCrash",
    "connect",
    "drain_once",
    "enqueue",
    "next_delay",
    "receive",
    "replay",
    "run_until_idle",
    "sign",
    "verify",
]
