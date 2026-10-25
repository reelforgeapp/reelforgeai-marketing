"""Spending controller: policy decisions and concurrent enforcement in Postgres."""

import asyncio
from decimal import Decimal as D

import pytest

from rpi.administration.budget import (
    Budget,
    BudgetPeriod,
    BudgetScope,
    BudgetStore,
    InMemoryBudget,
    Priority,
    ReservationRequest,
    SpendSnapshot,
    evaluate_reservation,
)

MONTHLY = Budget(BudgetScope.GLOBAL, BudgetPeriod.MONTHLY, D("100"), D("0.8"))
DAILY = Budget(BudgetScope.GLOBAL, BudgetPeriod.DAILY, D("10"), D("0.8"))
CDB = Budget(BudgetScope.PROVIDER, BudgetPeriod.MONTHLY, D("50"), D("0.8"), provider="creatordb")


def req(amount, provider="creatordb", priority=Priority.NORMAL, **kw):
    return ReservationRequest(provider, "op", D(str(amount)), priority, **kw)


def test_within_budget_allowed():
    d = evaluate_reservation(req("1"), [MONTHLY, DAILY, CDB], SpendSnapshot())
    assert d.allowed and not d.alerts


def test_hard_limit_blocks():
    d = evaluate_reservation(req("2"), [MONTHLY, DAILY], SpendSnapshot(global_daily=D("9")))
    assert not d.allowed and "global daily" in d.reason


def test_provider_budget_only_applies_to_that_provider():
    snap = SpendSnapshot(provider_monthly={"creatordb": D("49.5")})
    assert not evaluate_reservation(req("1", "creatordb"), [CDB], snap).allowed
    assert evaluate_reservation(req("1", "sparktoro"), [CDB], snap).allowed


def test_low_priority_paused_past_alert_threshold():
    snap = SpendSnapshot(global_monthly=D("79"))
    low = evaluate_reservation(req("2", priority=Priority.LOW), [MONTHLY], snap)
    normal = evaluate_reservation(req("2"), [MONTHLY], snap)
    assert not low.allowed and "low-priority work paused" in low.reason
    assert normal.allowed and normal.alerts


def test_job_and_creator_caps():
    d = evaluate_reservation(req("3", job_id="j", job_budget_usd=D("5")), [], SpendSnapshot(job_total=D("3")))
    assert not d.allowed and "job" in d.reason
    d = evaluate_reservation(req("1", creator_id="c"), [], SpendSnapshot(creator_total=D("1")),
                             max_cost_per_creator_usd=D("1.5"))
    assert not d.allowed and "creator" in d.reason


def test_negative_amount_rejected():
    assert not evaluate_reservation(req("-1"), [MONTHLY], SpendSnapshot()).allowed


async def test_in_memory_budget_reserve_settle_release():
    b = InMemoryBudget([Budget(BudgetScope.GLOBAL, BudgetPeriod.MONTHLY, D("1"), D("1"))])
    r1 = await b.reserve(req("0.6"))
    assert r1.allowed
    assert not (await b.reserve(req("0.6"))).allowed  # reserved amount counts
    await b.settle(r1.id, D("0.2"))
    assert (await b.reserve(req("0.6"))).allowed  # actual (0.2) replaces reserved (0.6)


@pytest.mark.db
async def test_concurrent_reservations_never_exceed_limit(pool):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE rpi_budgets SET limit_usd = 1.00 WHERE scope = 'global' AND period = 'daily'")
    store = BudgetStore(pool)
    results = await asyncio.gather(*[store.reserve(req("0.10")) for _ in range(30)])
    allowed = [r for r in results if r.allowed]
    assert len(allowed) == 10  # exactly $1.00 of $0.10 reservations
    async with pool.acquire() as conn:
        denied = await conn.fetchval("SELECT COUNT(*) FROM rpi_spend_ledger WHERE status = 'denied'")
        reserved = await conn.fetchval("SELECT SUM(reserved_usd) FROM rpi_spend_ledger WHERE status = 'reserved'")
    assert denied == 20 and reserved == D("1.00")


@pytest.mark.db
async def test_alert_recorded_once_per_day(pool):
    store = BudgetStore(pool)
    async with pool.acquire() as conn:
        await conn.execute("UPDATE rpi_budgets SET limit_usd = 1.00 WHERE scope = 'global' AND period = 'daily'")
    for _ in range(3):
        r = await store.reserve(req("0.30"))
        await store.settle(r.id, D("0.30"))
    async with pool.acquire() as conn:
        alerts = await conn.fetch("SELECT message FROM rpi_alerts WHERE kind = 'budget'")
    assert len([a for a in alerts if "global daily" in a["message"]]) == 1
