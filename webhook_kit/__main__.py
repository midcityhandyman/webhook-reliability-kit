"""Command line entry: python -m webhook_kit demo [--seed N] [--events N]"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import replace

from .chaos import ChaosConfig, ChaosReport, run_scenario
from .db import connect


def _money(cents: int) -> str:
    return f"{cents / 100:,.2f}"


def _print_report(title: str, report: ChaosReport, log_lines: int) -> None:
    c = report.config
    print(title)
    print("-" * len(title))
    print(f"  inbound deliveries        {report.deliveries}")
    print(f"    accepted                {report.accepted}")
    print(f"    deduped redeliveries    {report.duplicates}")
    print(f"    rejected bad signature  {report.rejected_signatures}")
    print(f"  downstream calls          {report.downstream_calls}")
    print(f"    retries scheduled       {report.retries}")
    print(f"    worker crashes          {report.crashes}")
    print(f"    dead lettered           {report.dead_before_replay} (max {c.max_attempts} attempts)")
    print(f"    replayed after heal     {report.replayed}")
    print(f"    duplicates suppressed   {report.duplicates_suppressed}")
    print(f"  final outbox status       {dict(sorted(report.final_status.items()))}")
    if log_lines:
        print(f"  first {log_lines} worker log lines:")
        for line in report.log[:log_lines]:
            print(f"    {line}")
        print("  first log line of each other kind:")
        for marker in ("worker crashed", " dead ", "downstream healed"):
            hit = next((line for line in report.log[log_lines:] if marker in line), None)
            if hit:
                print(f"    {hit}")
    print("  ledger check (account: expected / actual):")
    for account in sorted(report.expected_balances):
        want = report.expected_balances[account]
        got = report.actual_balances.get(account, 0)
        flag = "ok" if want == got else f"WRONG by {_money(got - want)}"
        print(f"    {account}: {_money(want)} / {_money(got)}  {flag}")
    print(f"  effects missing: {len(report.missing)}   effects applied twice or more: {len(report.doubled)}")
    print(f"  exactly once: {'YES' if report.exactly_once else 'NO'}")
    print()


def _has_events(path: str) -> bool:
    if path == ":memory:" or not os.path.exists(path):
        return False
    conn = connect(path)
    try:
        return conn.execute("SELECT COUNT(*) FROM inbox_events").fetchone()[0] > 0
    finally:
        conn.close()


def demo(seed: int, events: int, log_lines: int) -> int:
    config = ChaosConfig(
        seed=seed,
        events=events,
        inbound_secret=os.environ.get("WEBHOOK_KIT_INBOUND_SECRET", ChaosConfig.inbound_secret),
        outbound_secret=os.environ.get("WEBHOOK_KIT_OUTBOUND_SECRET", ChaosConfig.outbound_secret),
        db_path=os.environ.get("WEBHOOK_KIT_DB", ChaosConfig.db_path),
    )
    if _has_events(config.db_path):
        # The fake downstream ledger is in memory and starts empty each run, so
        # a sender database left over from an earlier run would dedupe every
        # event and report a failure that is not real.
        print(f"WEBHOOK_KIT_DB={config.db_path} already holds events from an earlier run.")
        print("Delete it or point at a new file, then run the demo again.")
        return 2
    print(f"webhook-reliability-kit chaos demo (seed={seed}, events={events})")
    print(
        f"downstream fails before applying {config.p_fail_before:.0%} of calls, "
        f"loses the response after applying {config.p_fail_after:.0%}, "
        f"worker crashes after {config.p_crash:.0%} of successful calls"
    )
    print()
    safe = run_scenario(config)
    _print_report("Run 1: downstream dedupes on the idempotency key", safe, log_lines)
    naive = run_scenario(replace(config, idempotent_downstream=False, db_path=":memory:"))
    _print_report("Run 2: same seed, downstream ignores the key (control)", naive, 0)
    print("Same traffic and same failures in both runs. Retries are unavoidable;")
    print("the idempotency key is what turns at least once delivery into exactly once effects.")
    return 0 if safe.exactly_once and not naive.exactly_once else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="webhook_kit")
    sub = parser.add_subparsers(dest="command", required=True)
    d = sub.add_parser("demo", help="run the seeded chaos scenario offline")
    d.add_argument("--seed", type=int, default=7)
    d.add_argument("--events", type=int, default=60)
    d.add_argument("--log-lines", type=int, default=8)
    args = parser.parse_args(argv)
    return demo(args.seed, args.events, args.log_lines)


if __name__ == "__main__":
    sys.exit(main())
