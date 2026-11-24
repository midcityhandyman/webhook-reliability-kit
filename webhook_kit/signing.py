"""HMAC-SHA256 webhook signatures with a timestamp to limit replay.

Header format:  t=<unix seconds>,v1=<hex digest>
Signed string:  "<t>.<raw body>"

The timestamp is part of the signed string, so an attacker cannot refresh an
old capture. Verification accepts a list of secrets so a secret can be rotated
without dropping traffic.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Iterable, Union

DEFAULT_TOLERANCE_SECONDS = 300.0


class SignatureError(Exception):
    """The request is not authentic, is malformed, or is too old."""


def _digest(secret: str, timestamp: int, body: bytes) -> str:
    signed = str(timestamp).encode("ascii") + b"." + body
    return hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()


def sign(secret: str, body: bytes, timestamp: float) -> str:
    ts = int(timestamp)
    return f"t={ts},v1={_digest(secret, ts, body)}"


def verify(
    secrets: Union[str, Iterable[str]],
    body: bytes,
    header: str,
    now: float,
    tolerance_seconds: float = DEFAULT_TOLERANCE_SECONDS,
) -> int:
    """Return the signed timestamp, or raise SignatureError."""
    if isinstance(secrets, str):
        secrets = [secrets]
    timestamp = None
    candidates = []
    for part in (header or "").split(","):
        name, _, value = part.strip().partition("=")
        if name == "t":
            try:
                timestamp = int(value)
            except ValueError:
                raise SignatureError("timestamp is not an integer") from None
        elif name == "v1":
            candidates.append(value)
    if timestamp is None or not candidates:
        raise SignatureError("signature header is missing t or v1")
    if abs(now - timestamp) > tolerance_seconds:
        raise SignatureError("timestamp is outside the tolerance window")
    for secret in secrets:
        expected = _digest(secret, timestamp, body).encode("ascii")
        for candidate in candidates:
            # compare_digest runs in constant time, so the check does not leak
            # how many leading characters matched. Compare bytes: on str it
            # raises TypeError for non ASCII input, and the header is untrusted.
            if hmac.compare_digest(expected, candidate.encode("utf-8")):
                return timestamp
    raise SignatureError("signature does not match")
