import random

import pytest

from webhook_kit.backoff import BackoffPolicy, next_delay


def test_ceiling_doubles_then_caps():
    policy = BackoffPolicy(base_seconds=1.0, cap_seconds=10.0)
    assert [policy.ceiling(n) for n in range(1, 7)] == [1.0, 2.0, 4.0, 8.0, 10.0, 10.0]
    assert policy.ceiling(10_000) == 10.0


def test_delay_stays_inside_the_ceiling_and_is_spread_out():
    policy = BackoffPolicy(base_seconds=2.0, cap_seconds=60.0)
    rng = random.Random(1)
    for attempt in range(1, 9):
        delays = [next_delay(policy, attempt, rng) for _ in range(200)]
        assert all(0.0 <= d <= policy.ceiling(attempt) for d in delays)
        assert max(delays) - min(delays) > policy.ceiling(attempt) * 0.5


def test_same_seed_gives_same_delays():
    policy = BackoffPolicy()
    a = [next_delay(policy, n, random.Random(42)) for n in range(1, 6)]
    b = [next_delay(policy, n, random.Random(42)) for n in range(1, 6)]
    assert a == b


def test_attempt_is_one_based():
    with pytest.raises(ValueError):
        BackoffPolicy().ceiling(0)
