"""The demo entry point: exit codes, and the README's quoted output."""

import pathlib

from webhook_kit.__main__ import main

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_default_demo_exits_zero_and_matches_the_captured_output(capsys, monkeypatch):
    for name in ("WEBHOOK_KIT_DB", "WEBHOOK_KIT_INBOUND_SECRET", "WEBHOOK_KIT_OUTBOUND_SECRET"):
        monkeypatch.delenv(name, raising=False)
    assert main(["demo"]) == 0
    out = capsys.readouterr().out
    assert out == (ROOT / "demo" / "output.txt").read_text(encoding="utf-8")


def test_readme_what_you_will_see_block_is_real_demo_output():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    block = readme.split("## What you will see", 1)[1].split("```", 2)[1]
    captured = (ROOT / "demo" / "output.txt").read_text(encoding="utf-8").splitlines()
    quoted = [line for line in block.splitlines() if line.strip() and line.strip() != "..."]
    assert len(quoted) > 10
    for line in quoted:
        assert line in captured, line


def test_demo_refuses_a_sender_database_left_over_from_an_earlier_run(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("WEBHOOK_KIT_DB", str(tmp_path / "kit.db"))
    assert main(["demo", "--events", "10", "--log-lines", "0"]) == 0
    capsys.readouterr()
    assert main(["demo", "--events", "10", "--log-lines", "0"]) == 2
    assert "already holds events" in capsys.readouterr().out
