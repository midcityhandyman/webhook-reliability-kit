"""The headline test: random downstream failure, exactly once effects."""

from dataclasses import replace

import pytest

from webhook_kit.chaos import ChaosConfig, run_scenario

SEEDS = [1, 7, 42, 1337, 2026, 90210]


@pytest.mark.parametrize("seed", SEEDS)
def test_every_accepted_event_lands_exactly_once(seed):
    report = run_scenario(ChaosConfig(seed=seed, events=120))

    assert report.accepted == 120
    assert report.final_status == {"sent": 120}
    assert report.missing == []
    assert report.doubled == []
    assert sorted(report.effect_counts) == sorted(report.expected_keys)
    assert report.actual_balances == report.expected_balances
    assert report.exactly_once

    # Guard against a vacuous pass: the chaos really happened.
    assert report.retries > 0
    assert report.downstream_calls > report.accepted
    assert report.duplicates_suppressed > 0
    assert report.duplicates > 0
    assert report.rejected_signatures > 0


@pytest.mark.parametrize("seed", SEEDS)
def test_control_without_downstream_dedupe_double_applies(seed):
    """Same traffic, key ignored. If this ever passes as exactly once, the
    chaos is too gentle to prove anything."""
    report = run_scenario(ChaosConfig(seed=seed, events=120, idempotent_downstream=False))
    assert report.missing == []
    assert report.doubled
    assert not report.exactly_once


def test_heavy_chaos_dead_letters_then_replay_recovers_everything():
    config = ChaosConfig(seed=3, events=150, p_fail_before=0.5, p_fail_after=0.3, p_crash=0.1, max_attempts=3)
    report = run_scenario(config)
    assert report.crashes > 0
    assert report.dead_before_replay > 0
    assert report.replayed == report.dead_before_replay
    assert report.exactly_once


def test_same_seed_is_reproducible_and_different_seeds_differ():
    a = run_scenario(ChaosConfig(seed=11))
    b = run_scenario(ChaosConfig(seed=11))
    c = run_scenario(ChaosConfig(seed=12))
    assert a.log == b.log and a.downstream_calls == b.downstream_calls
    assert a.log != c.log


def test_file_backed_database_behaves_the_same(tmp_path):
    base = ChaosConfig(seed=5, events=40)
    on_disk = run_scenario(replace(base, db_path=str(tmp_path / "kit.db")))
    assert on_disk.exactly_once
    assert on_disk.log == run_scenario(base).log
