import random

import pytest

from webhook_kit import BackoffPolicy, WorkerCrash, connect, drain_once, enqueue, replay, run_until_idle
from webhook_kit.clock import FakeClock
from webhook_kit.outbox import next_wakeup, replay_dead

NOW = 1_000_000.0
POLICY = BackoffPolicy(base_seconds=1.0, cap_seconds=8.0, max_attempts=3)


def row(conn, row_id=1):
    return conn.execute("SELECT * FROM outbox WHERE id = ?", (row_id,)).fetchone()


def fail(_row):
    raise ConnectionError("downstream is down")


def test_enqueue_is_idempotent_on_the_key():
    conn = connect()
    assert enqueue(conn, "k1", "ledger", "{}", NOW) is True
    assert enqueue(conn, "k1", "ledger", "{}", NOW) is False
    assert conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1


def test_success_marks_the_row_sent():
    conn = connect()
    enqueue(conn, "k1", "ledger", "{}", NOW, POLICY.max_attempts)
    seen = []
    stats = drain_once(conn, lambda r: seen.append(r["idempotency_key"]), NOW, random.Random(0), POLICY)
    assert seen == ["k1"] and stats.sent == 1
    assert row(conn)["status"] == "sent" and row(conn)["attempts"] == 1
    assert drain_once(conn, fail, NOW + 999, random.Random(0), POLICY).claimed == 0


def test_failure_schedules_a_jittered_retry_inside_the_ceiling():
    conn = connect()
    enqueue(conn, "k1", "ledger", "{}", NOW, POLICY.max_attempts)
    stats = drain_once(conn, fail, NOW, random.Random(0), POLICY)
    r = row(conn)
    assert stats.retried == 1 and r["status"] == "pending"
    assert NOW <= r["next_attempt_at"] <= NOW + POLICY.ceiling(1)
    assert "ConnectionError" in r["last_error"]
    # Not due yet, so a drain one instant before the retry time claims nothing.
    assert drain_once(conn, fail, r["next_attempt_at"] - 0.001, random.Random(0), POLICY).claimed == 0


def test_row_goes_dead_after_max_attempts_and_stops_being_retried():
    conn = connect()
    clock = FakeClock(NOW)
    enqueue(conn, "k1", "ledger", "{}", NOW, POLICY.max_attempts)
    stats = run_until_idle(conn, fail, clock, random.Random(0), POLICY)
    r = row(conn)
    assert r["status"] == "dead" and r["attempts"] == 3
    assert (stats.retried, stats.dead) == (2, 1)
    assert next_wakeup(conn) is None
    outcomes = [x["outcome"] for x in conn.execute("SELECT outcome FROM delivery_attempts ORDER BY id")]
    assert outcomes == ["retry", "retry", "dead"]


def test_replay_gives_a_dead_row_a_fresh_budget_with_the_same_key():
    conn = connect()
    clock = FakeClock(NOW)
    enqueue(conn, "k1", "ledger", "{}", NOW, POLICY.max_attempts)
    run_until_idle(conn, fail, clock, random.Random(0), POLICY)
    assert replay_dead(conn, clock.now()) == 1
    seen = []
    run_until_idle(conn, lambda r: seen.append(r["idempotency_key"]), clock, random.Random(0), POLICY)
    assert seen == ["k1"]
    assert row(conn)["status"] == "sent" and row(conn)["attempts"] == 1


def test_replay_refuses_rows_that_are_still_in_play():
    conn = connect()
    enqueue(conn, "k1", "ledger", "{}", NOW)
    with pytest.raises(ValueError):
        replay(conn, 1, NOW)
    with pytest.raises(ValueError):
        replay(conn, 404, NOW)


def test_crashed_worker_leaves_a_lease_that_expires_and_is_reclaimed():
    conn = connect()
    enqueue(conn, "k1", "ledger", "{}", NOW, POLICY.max_attempts)

    def crash(_row):
        raise WorkerCrash("killed")

    with pytest.raises(WorkerCrash):
        drain_once(conn, crash, NOW, random.Random(0), POLICY, lease_seconds=30)
    assert row(conn)["status"] == "in_flight"
    # A second worker cannot take the row while the lease is live.
    assert drain_once(conn, fail, NOW + 29, random.Random(0), POLICY).claimed == 0
    stats = drain_once(conn, lambda r: None, NOW + 30, random.Random(0), POLICY)
    assert stats.sent == 1 and row(conn)["attempts"] == 2


def test_crash_on_the_final_attempt_is_dead_lettered_not_retried_forever():
    conn = connect()
    clock = FakeClock(NOW)
    enqueue(conn, "k1", "ledger", "{}", NOW, 1)
    calls = []

    def crash(r):
        calls.append(r["id"])
        raise WorkerCrash("killed")

    stats = run_until_idle(conn, crash, clock, random.Random(0), POLICY)
    assert len(calls) == 1 and stats.dead == 1
    assert row(conn)["status"] == "dead" and "outcome unknown" in row(conn)["last_error"]


def test_two_workers_never_claim_the_same_row(tmp_path):
    path = str(tmp_path / "shared.db")
    a, b = connect(path), connect(path)
    for n in range(20):
        enqueue(a, f"k{n}", "ledger", "{}", NOW)
    seen = []
    drain_once(a, lambda r: seen.append(r["id"]), NOW, random.Random(0), POLICY, batch=12)
    drain_once(b, lambda r: seen.append(r["id"]), NOW, random.Random(0), POLICY, batch=12)
    assert sorted(seen) == list(range(1, 21))
    a.close()
    b.close()


def test_late_result_from_a_worker_that_lost_its_lease_is_discarded(tmp_path):
    """Worker A's call outlives its lease. Worker B reclaims the row and
    delivers it. A's late failure must not flip the sent row back to pending."""
    path = str(tmp_path / "shared.db")
    a, b = connect(path), connect(path)
    enqueue(a, "k1", "ledger", "{}", NOW, POLICY.max_attempts)

    def slow_then_fail(_row):
        # While A is still inside its call, the lease expires and B takes over.
        stats_b = drain_once(b, lambda r: None, NOW + 31, random.Random(0), POLICY)
        assert stats_b.sent == 1
        raise ConnectionError("timed out")

    stats_a = drain_once(a, slow_then_fail, NOW, random.Random(0), POLICY, lease_seconds=30)
    assert (stats_a.stale, stats_a.retried, stats_a.dead) == (1, 0, 0)
    assert row(a)["status"] == "sent" and row(a)["attempts"] == 2
    outcomes = [x["outcome"] for x in a.execute("SELECT outcome FROM delivery_attempts ORDER BY id")]
    assert outcomes == ["sent"]
    a.close()
    b.close()


def test_budget_comes_from_the_row_not_the_drain_policy():
    conn = connect()
    clock = FakeClock(NOW)
    enqueue(conn, "k1", "ledger", "{}", NOW, 2)
    stats = run_until_idle(conn, fail, clock, random.Random(0), BackoffPolicy(max_attempts=9))
    assert stats.dead == 1 and row(conn)["attempts"] == 2
