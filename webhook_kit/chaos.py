"""End to end chaos scenario, shared by the test suite and the demo.

Everything random is drawn from generators seeded from one integer, and time
comes from a FakeClock, so a given seed always produces the same run.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from typing import Dict, List

from .backoff import BackoffPolicy
from .clock import FakeClock
from .db import connect
from .downstream import FlakyDownstream
from .inbox import receive
from .outbox import WorkerCrash, enqueue, replay_dead, run_until_idle
from .signing import SignatureError, sign

ACCOUNTS = [f"acct_demo_{n:02d}" for n in range(1, 6)]


@dataclass
class ChaosConfig:
    seed: int = 7
    events: int = 60
    p_fail_before: float = 0.30
    p_fail_after: float = 0.20
    p_crash: float = 0.05
    p_redelivery: float = 0.40
    p_forged: float = 0.10
    max_attempts: int = 4
    idempotent_downstream: bool = True
    inbound_secret: str = "inbound-secret-not-real"
    outbound_secret: str = "outbound-secret-not-real"
    db_path: str = ":memory:"


@dataclass
class ChaosReport:
    config: ChaosConfig
    deliveries: int = 0
    accepted: int = 0
    duplicates: int = 0
    rejected_signatures: int = 0
    downstream_calls: int = 0
    duplicates_suppressed: int = 0
    retries: int = 0
    crashes: int = 0
    dead_before_replay: int = 0
    replayed: int = 0
    final_status: Dict[str, int] = field(default_factory=dict)
    expected_balances: Dict[str, int] = field(default_factory=dict)
    actual_balances: Dict[str, int] = field(default_factory=dict)
    effect_counts: Dict[str, int] = field(default_factory=dict)
    expected_keys: List[str] = field(default_factory=list)
    log: List[str] = field(default_factory=list)

    @property
    def missing(self) -> List[str]:
        return [k for k in self.expected_keys if self.effect_counts.get(k, 0) == 0]

    @property
    def doubled(self) -> List[str]:
        return [k for k, n in sorted(self.effect_counts.items()) if n > 1]

    @property
    def exactly_once(self) -> bool:
        return (
            not self.missing
            and not self.doubled
            and set(self.effect_counts) == set(self.expected_keys)
            and self.actual_balances == self.expected_balances
        )


def run_scenario(config: ChaosConfig) -> ChaosReport:
    report = ChaosReport(config=config)
    clock = FakeClock()
    conn = connect(config.db_path)
    traffic = random.Random(f"{config.seed}:traffic")
    jitter = random.Random(f"{config.seed}:jitter")
    crash = random.Random(f"{config.seed}:crash")
    downstream = FlakyDownstream(
        seed=config.seed,
        secret=config.outbound_secret,
        p_fail_before=config.p_fail_before,
        p_fail_after=config.p_fail_after,
        idempotent=config.idempotent_downstream,
    )
    policy = BackoffPolicy(base_seconds=1.0, cap_seconds=60.0, max_attempts=config.max_attempts)
    crashing = {"on": True}

    def handler(tx, event):
        payload = json.dumps(
            {"account": event["account"], "amount_cents": event["amount_cents"]}, sort_keys=True
        )
        enqueue(tx, f"{event['id']}:ledger", "ledger", payload, clock.now(), policy.max_attempts)

    def send(row):
        body = row["payload"].encode("utf-8")
        downstream.handle(row["idempotency_key"], body, sign(config.outbound_secret, body, clock.now()), clock.now())
        if crashing["on"] and crash.random() < config.p_crash:
            report.crashes += 1
            raise WorkerCrash("killed after the downstream replied, before the row was marked sent")

    # 1. Inbound webhooks: redeliveries, forged signatures, shuffled order.
    deliveries = []
    for n in range(1, config.events + 1):
        event = {
            "id": f"evt_{n:04d}",
            "account": traffic.choice(ACCOUNTS),
            "amount_cents": traffic.randint(100, 9999),
        }
        body = json.dumps(event, sort_keys=True).encode("utf-8")
        report.expected_keys.append(f"{event['id']}:ledger")
        report.expected_balances[event["account"]] = (
            report.expected_balances.get(event["account"], 0) + event["amount_cents"]
        )
        deliveries.append((body, "good"))
        while traffic.random() < config.p_redelivery:
            deliveries.append((body, "good"))
        if traffic.random() < config.p_forged:
            forged = json.dumps(dict(event, amount_cents=event["amount_cents"] * 100), sort_keys=True)
            deliveries.append((forged.encode("utf-8"), "forged"))
    traffic.shuffle(deliveries)
    for body, kind in deliveries:
        clock.advance(0.05)
        # A forged delivery carries a signature made with the wrong secret.
        secret = config.inbound_secret if kind == "good" else "attacker-guess"
        report.deliveries += 1
        try:
            outcome = receive(conn, body, sign(secret, body, clock.now()), config.inbound_secret, clock.now(), handler)
        except SignatureError:
            report.rejected_signatures += 1
            continue
        if outcome == "accepted":
            report.accepted += 1
        else:
            report.duplicates += 1

    # 2. Drain under chaos.
    stats = run_until_idle(conn, send, clock, jitter, policy)
    report.retries = stats.retried
    report.log.extend(stats.events)
    report.dead_before_replay = conn.execute("SELECT COUNT(*) FROM outbox WHERE status = 'dead'").fetchone()[0]

    # 3. The downstream recovers; replay the dead letters.
    downstream.heal()
    crashing["on"] = False
    report.replayed = replay_dead(conn, clock.now())
    report.log.append(f"downstream healed, replayed {report.replayed} dead rows")
    stats = run_until_idle(conn, send, clock, jitter, policy)
    report.log.extend(stats.events)

    report.downstream_calls = downstream.calls
    report.duplicates_suppressed = downstream.duplicates_suppressed
    report.final_status = dict(conn.execute("SELECT status, COUNT(*) FROM outbox GROUP BY 1"))
    report.actual_balances = downstream.balances()
    report.effect_counts = downstream.effect_counts()
    conn.close()
    return report
