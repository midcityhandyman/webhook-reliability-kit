import pytest

from webhook_kit.signing import SignatureError, sign, verify

SECRET = "unit-test-secret"
BODY = b'{"id": "evt_1"}'
NOW = 1_700_000_000.0


def test_round_trip_returns_timestamp():
    assert verify(SECRET, BODY, sign(SECRET, BODY, NOW), NOW + 10) == int(NOW)


def test_tampered_body_is_rejected():
    with pytest.raises(SignatureError):
        verify(SECRET, BODY + b" ", sign(SECRET, BODY, NOW), NOW)


def test_wrong_secret_is_rejected():
    with pytest.raises(SignatureError):
        verify(SECRET, BODY, sign("other", BODY, NOW), NOW)


def test_old_capture_is_rejected_as_replay():
    header = sign(SECRET, BODY, NOW)
    verify(SECRET, BODY, header, NOW + 300)
    with pytest.raises(SignatureError, match="tolerance"):
        verify(SECRET, BODY, header, NOW + 301)


def test_timestamp_cannot_be_refreshed_without_the_secret():
    digest = sign(SECRET, BODY, NOW).split("v1=")[1]
    with pytest.raises(SignatureError, match="match"):
        verify(SECRET, BODY, f"t={int(NOW) + 1000},v1={digest}", NOW + 1000)


def test_secret_rotation_accepts_old_and_new():
    assert verify(["new", "old"], BODY, sign("old", BODY, NOW), NOW)
    assert verify(["new", "old"], BODY, sign("new", BODY, NOW), NOW)


@pytest.mark.parametrize("header", ["", "garbage", "t=abc,v1=00", "v1=00", "t=1"])
def test_malformed_headers_are_rejected(header):
    with pytest.raises(SignatureError):
        verify(SECRET, BODY, header, NOW)
