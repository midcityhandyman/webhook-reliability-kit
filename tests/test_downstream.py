import json

import pytest

from webhook_kit import DownstreamError, FlakyDownstream, sign

SECRET = "unit-test-secret"
NOW = 1_700_000_000.0
BODY = json.dumps({"account": "acct_demo_01", "amount_cents": 250}).encode()


def call(ds, key="k1", secret=SECRET):
    ds.handle(key, BODY, sign(secret, BODY, NOW), NOW)


def test_repeat_key_is_applied_once():
    ds = FlakyDownstream(seed=0, secret=SECRET)
    call(ds)
    call(ds)
    assert ds.balances() == {"acct_demo_01": 250}
    assert ds.duplicates_suppressed == 1


def test_fail_after_applies_the_effect_but_reports_failure():
    ds = FlakyDownstream(seed=0, secret=SECRET, p_fail_after=1.0)
    with pytest.raises(DownstreamError, match="response lost"):
        call(ds)
    assert ds.effect_counts() == {"k1": 1}


def test_fail_before_applies_nothing():
    ds = FlakyDownstream(seed=0, secret=SECRET, p_fail_before=1.0)
    with pytest.raises(DownstreamError, match="nothing applied"):
        call(ds)
    assert ds.effect_counts() == {}


def test_unsigned_or_wrongly_signed_calls_are_refused():
    ds = FlakyDownstream(seed=0, secret=SECRET)
    with pytest.raises(DownstreamError, match="401"):
        call(ds, secret="wrong")
    assert ds.effect_counts() == {}
