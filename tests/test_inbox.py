import json

import pytest

from webhook_kit import IdempotencyConflict, SignatureError, connect, enqueue, receive, sign

SECRET = "unit-test-secret"
NOW = 1_700_000_000.0


def body_for(event_id="evt_1", amount=500):
    return json.dumps({"id": event_id, "account": "acct_demo_01", "amount_cents": amount}).encode()


def handler(tx, event):
    enqueue(tx, f"{event['id']}:ledger", "ledger", json.dumps(event), NOW)


def count(conn, table):
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_first_delivery_is_accepted_and_enqueues_once():
    conn = connect()
    body = body_for()
    assert receive(conn, body, sign(SECRET, body, NOW), SECRET, NOW, handler) == "accepted"
    assert receive(conn, body, sign(SECRET, body, NOW + 5), SECRET, NOW + 5, handler) == "duplicate"
    assert count(conn, "inbox_events") == 1
    assert count(conn, "outbox") == 1


def test_bad_signature_writes_nothing():
    conn = connect()
    body = body_for()
    with pytest.raises(SignatureError):
        receive(conn, body, sign("wrong", body, NOW), SECRET, NOW, handler)
    assert count(conn, "inbox_events") == 0
    assert count(conn, "outbox") == 0


def test_key_reuse_with_different_body_is_a_conflict():
    conn = connect()
    first, second = body_for(amount=500), body_for(amount=999)
    receive(conn, first, sign(SECRET, first, NOW), SECRET, NOW, handler)
    with pytest.raises(IdempotencyConflict):
        receive(conn, second, sign(SECRET, second, NOW), SECRET, NOW, handler)
    assert count(conn, "outbox") == 1


def test_handler_failure_rolls_back_the_dedupe_row_so_a_retry_works():
    conn = connect()
    body = body_for()

    def broken(tx, event):
        handler(tx, event)
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        receive(conn, body, sign(SECRET, body, NOW), SECRET, NOW, broken)
    assert count(conn, "inbox_events") == 0
    assert count(conn, "outbox") == 0
    assert receive(conn, body, sign(SECRET, body, NOW), SECRET, NOW, handler) == "accepted"
    assert count(conn, "outbox") == 1


@pytest.mark.parametrize("raw", [b"not json", b"[1, 2]", b'{"no_id": 1}', b'{"id": ""}', b'{"id": 7}'])
def test_body_without_a_usable_key_is_rejected(raw):
    conn = connect()
    with pytest.raises(ValueError):
        receive(conn, raw, sign(SECRET, raw, NOW), SECRET, NOW, handler)
    assert count(conn, "inbox_events") == 0
