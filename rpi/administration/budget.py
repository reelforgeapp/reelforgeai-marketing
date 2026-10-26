"""
Global data and inference spending controller.

Every paid provider or Claude call reserves its estimated cost before the
request and settles the actual cost afterwards. Reservations are checked
against daily, monthly, per-provider, per-job and per-creator limits. When
spend passes a budget's alert threshold, low-priority work is refused so that
interactive and high-priority jobs keep running; at the hard limit everything
is refused. Raising a limit requires an approved increase request.

`evaluate_reservation` is pure and unit-tested. `BudgetStore` persists
reservations in Postgres under a transaction-scoped advisory lock so
concurrent workers cannot overspend.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional, Protocol

import structlog

logger = structlog.get_logger()

ZERO = Decimal("0")


class Priority(str, Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"


class BudgetScope(str, Enum):
    GLOBAL = "global"
    PROVIDER = "provider"
    JOB = "job"
    CREATOR = "creator"


class BudgetPeriod(str, Enum):
    DAILY = "daily"
    MONTHLY = "monthly"
    LIFETIME = "lifetime"  # used for per-job and per-creator caps


@dataclass(frozen=True)
class Budget:
    scope: BudgetScope
    period: BudgetPeriod
    limit_usd: Decimal
    alert_ratio: Decimal = Decimal("0.8")
    provider: Optional[str] = None  # for PROVIDER scope
    id: Optional[str] = None

    def applies_to(self, provider: str) -> bool:
        return self.scope != BudgetScope.PROVIDER or self.provider == provider


@dataclass
class SpendSnapshot:
    """Reserved + committed spend relevant to one reservation request."""

    global_daily: Decimal = ZERO
    global_monthly: Decimal = ZERO
    provider_daily: dict[str, Decimal] = field(default_factory=dict)
    provider_monthly: dict[str, Decimal] = field(default_factory=dict)
    job_total: Decimal = ZERO
    creator_total: Decimal = ZERO

    def spent_for(self, budget: Budget, provider: str) -> Decimal:
        if budget.scope == BudgetScope.GLOBAL:
            return self.global_daily if budget.period == BudgetPeriod.DAILY else self.global_monthly
        if budget.scope == BudgetScope.PROVIDER:
            source = self.provider_daily if budget.period == BudgetPeriod.DAILY else self.provider_monthly
            return source.get(provider, ZERO)
        if budget.scope == BudgetScope.JOB:
            return self.job_total
        return self.creator_total


@dataclass
class ReservationRequest:
    provider: str
    operation: str
    amount_usd: Decimal
    priority: Priority = Priority.NORMAL
    job_id: Optional[str] = None
    creator_id: Optional[str] = None
    job_budget_usd: Optional[Decimal] = None


@dataclass
class Decision:
    allowed: bool
    reason: str
    alerts: list[str] = field(default_factory=list)
    blocking_budget: Optional[Budget] = None


def evaluate_reservation(
    request: ReservationRequest,
    budgets: list[Budget],
    spent: SpendSnapshot,
    *,
    max_cost_per_creator_usd: Optional[Decimal] = None,
) -> Decision:
    """Decide whether a reservation fits every applicable budget."""
    if request.amount_usd < ZERO:
        return Decision(False, "negative reservation amount")

    applicable = [b for b in budgets if b.applies_to(request.provider)]
    if request.job_id and request.job_budget_usd is not None:
        applicable.append(Budget(BudgetScope.JOB, BudgetPeriod.LIFETIME, request.job_budget_usd, Decimal("1")))
    if request.creator_id and max_cost_per_creator_usd is not None:
        applicable.append(Budget(BudgetScope.CREATOR, BudgetPeriod.LIFETIME, max_cost_per_creator_usd, Decimal("1")))

    alerts: list[str] = []
    for budget in applicable:
        if budget.scope == BudgetScope.JOB and not request.job_id:
            continue
        if budget.scope == BudgetScope.CREATOR and not request.creator_id:
            continue
        current = spent.spent_for(budget, request.provider)
        after = current + request.amount_usd
        label = _label(budget)
        if after > budget.limit_usd:
            return Decision(
                False,
                f"{label} limit ${budget.limit_usd} would be exceeded "
                f"(spent ${current.quantize(Decimal('0.0001'))}, request ${request.amount_usd})",
                alerts,
                budget,
            )
        threshold = budget.limit_usd * budget.alert_ratio
        if after >= threshold:
            if request.priority == Priority.LOW and budget.alert_ratio < Decimal("1"):
                return Decision(
                    False,
                    f"{label} past {int(budget.alert_ratio * 100)}% alert threshold; low-priority work paused",
                    alerts + [f"{label} at {_pct(after, budget.limit_usd)}%"],
                    budget,
                )
            alerts.append(f"{label} at {_pct(after, budget.limit_usd)}% of ${budget.limit_usd}")
    return Decision(True, "within budget", alerts)


def _label(budget: Budget) -> str:
    if budget.scope == BudgetScope.PROVIDER:
        return f"{budget.provider} {budget.period.value}"
    return f"{budget.scope.value} {budget.period.value}"


def _pct(value: Decimal, limit: Decimal) -> int:
    if limit <= ZERO:
        return 100
    return int((value / limit) * 100)


def period_start(period: BudgetPeriod, now: datetime) -> datetime:
    now = now.astimezone(timezone.utc)
    if period == BudgetPeriod.DAILY:
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == BudgetPeriod.MONTHLY:
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


# --------------------------------------------------------------------------
# Persistence
# --------------------------------------------------------------------------


@dataclass
class Reservation:
    id: Optional[str]
    decision: Decision

    @property
    def allowed(self) -> bool:
        return self.decision.allowed


class BudgetBackend(Protocol):
    async def reserve(self, request: ReservationRequest) -> Reservation: ...

    async def settle(self, reservation_id: str, actual_usd: Decimal, actual_units: Decimal = ZERO) -> None: ...

    async def release(self, reservation_id: str) -> None: ...

    async def units_spent(self, job_id: str, provider: str) -> Decimal: ...


# Arbitrary constant key for pg_advisory_xact_lock serializing reservations.
BUDGET_LOCK_KEY = 7_340_118_221

STALE_RESERVATION_MINUTES = 30


class BudgetStore:
    """Postgres-backed budget enforcement (tables rpi_budgets, rpi_spend_ledger)."""

    def __init__(self, pool, *, max_cost_per_creator_usd: Optional[Decimal] = None, workspace_id: Optional[str] = None):
        self.pool = pool
        self.max_cost_per_creator_usd = max_cost_per_creator_usd
        self.workspace_id = workspace_id

    async def load_budgets(self, conn) -> list[Budget]:
        rows = await conn.fetch(
            "SELECT id, scope, period, provider, limit_usd, alert_ratio FROM rpi_budgets WHERE active = TRUE"
        )
        return [
            Budget(
                scope=BudgetScope(r["scope"]),
                period=BudgetPeriod(r["period"]),
                limit_usd=Decimal(r["limit_usd"]),
                alert_ratio=Decimal(r["alert_ratio"]),
                provider=r["provider"],
                id=str(r["id"]),
            )
            for r in rows
        ]

    async def _snapshot(self, conn, request: ReservationRequest, now: datetime) -> SpendSnapshot:
        day = period_start(BudgetPeriod.DAILY, now)
        month = period_start(BudgetPeriod.MONTHLY, now)
        stale_before = now - timedelta(minutes=STALE_RESERVATION_MINUTES)
        # Committed rows count at their actual amount; open reservations count
        # at the reserved amount unless abandoned (older than the stale window).
        active_amount = """
            CASE WHEN status = 'committed' THEN actual_usd
                 WHEN status = 'reserved' AND created_at >= $3 THEN reserved_usd
                 ELSE 0 END
        """
        row = await conn.fetchrow(
            f"""
            SELECT
              COALESCE(SUM({active_amount}) FILTER (WHERE created_at >= $1), 0) AS global_daily,
              COALESCE(SUM({active_amount}) FILTER (WHERE created_at >= $2), 0) AS global_monthly,
              COALESCE(SUM({active_amount}) FILTER (WHERE created_at >= $1 AND provider = $4), 0) AS provider_daily,
              COALESCE(SUM({active_amount}) FILTER (WHERE created_at >= $2 AND provider = $4), 0) AS provider_monthly,
              COALESCE(SUM({active_amount}) FILTER (WHERE job_id = $5), 0) AS job_total,
              COALESCE(SUM({active_amount}) FILTER (WHERE creator_id = $6), 0) AS creator_total
            FROM rpi_spend_ledger
            WHERE created_at >= $2 OR job_id = $5 OR creator_id = $6
            """,
            day,
            month,
            stale_before,
            request.provider,
            _uuid_or_none(request.job_id),
            _uuid_or_none(request.creator_id),
        )
        return SpendSnapshot(
            global_daily=Decimal(row["global_daily"]),
            global_monthly=Decimal(row["global_monthly"]),
            provider_daily={request.provider: Decimal(row["provider_daily"])},
            provider_monthly={request.provider: Decimal(row["provider_monthly"])},
            job_total=Decimal(row["job_total"]),
            creator_total=Decimal(row["creator_total"]),
        )

    async def reserve(self, request: ReservationRequest, now: Optional[datetime] = None) -> Reservation:
        now = now or datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock($1)", BUDGET_LOCK_KEY)
                budgets = await self.load_budgets(conn)
                spent = await self._snapshot(conn, request, now)
                decision = evaluate_reservation(
                    request, budgets, spent, max_cost_per_creator_usd=self.max_cost_per_creator_usd
                )
                if not decision.allowed:
                    await conn.execute(
                        """INSERT INTO rpi_spend_ledger
                           (provider, operation, job_id, creator_id, reserved_usd, actual_usd, status, priority, reason, created_at)
                           VALUES ($1, $2, $3, $4, $5, 0, 'denied', $6, $7, $8)""",
                        request.provider,
                        request.operation,
                        _uuid_or_none(request.job_id),
                        _uuid_or_none(request.creator_id),
                        request.amount_usd,
                        request.priority.value,
                        decision.reason,
                        now,
                    )
                    logger.warning("rpi_budget_denied", provider=request.provider, reason=decision.reason)
                    return Reservation(None, decision)
                reservation_id = await conn.fetchval(
                    """INSERT INTO rpi_spend_ledger
                       (provider, operation, job_id, creator_id, reserved_usd, actual_usd, status, priority, reason, created_at)
                       VALUES ($1, $2, $3, $4, $5, 0, 'reserved', $6, $7, $8) RETURNING id""",
                    request.provider,
                    request.operation,
                    _uuid_or_none(request.job_id),
                    _uuid_or_none(request.creator_id),
                    request.amount_usd,
                    request.priority.value,
                    "; ".join(decision.alerts) or None,
                    now,
                )
                for alert in decision.alerts:
                    await conn.execute(
                        """INSERT INTO rpi_alerts (kind, severity, message, dedupe_key)
                           VALUES ('budget', 'warning', $1, $2)
                           ON CONFLICT (dedupe_key) DO NOTHING""",
                        alert,
                        f"budget:{alert.split(' at ')[0]}:{now.date().isoformat()}",
                    )
                return Reservation(str(reservation_id), decision)

    async def settle(self, reservation_id: str, actual_usd: Decimal, actual_units: Decimal = ZERO) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE rpi_spend_ledger
                   SET status = 'committed', actual_usd = $2, actual_units = $3, settled_at = NOW()
                   WHERE id = $1 AND status = 'reserved'""",
                uuid.UUID(reservation_id),
                actual_usd,
                actual_units,
            )

    async def release(self, reservation_id: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE rpi_spend_ledger SET status = 'released', settled_at = NOW() WHERE id = $1 AND status = 'reserved'",
                uuid.UUID(reservation_id),
            )

    async def units_spent(self, job_id: str, provider: str) -> Decimal:
        """Provider units (credits) a job has committed so far."""
        async with self.pool.acquire() as conn:
            v = await conn.fetchval(
                """SELECT COALESCE(SUM(actual_units), 0) FROM rpi_spend_ledger
                   WHERE job_id = $1 AND provider = $2 AND status = 'committed'""",
                _uuid_or_none(job_id), provider)
        return Decimal(v)


class InMemoryBudget:
    """Budget backend for unit tests and offline evaluation runs."""

    def __init__(self, budgets: list[Budget], max_cost_per_creator_usd: Optional[Decimal] = None):
        self.budgets = budgets
        self.max_cost_per_creator_usd = max_cost_per_creator_usd
        self.entries: dict[str, dict] = {}

    def _snapshot(self, request: ReservationRequest) -> SpendSnapshot:
        snap = SpendSnapshot()
        for e in self.entries.values():
            if e["status"] not in ("reserved", "committed"):
                continue
            amount = e["actual"] if e["status"] == "committed" else e["reserved"]
            snap.global_daily += amount
            snap.global_monthly += amount
            if e["provider"] == request.provider:
                snap.provider_daily[request.provider] = snap.provider_daily.get(request.provider, ZERO) + amount
                snap.provider_monthly[request.provider] = snap.provider_monthly.get(request.provider, ZERO) + amount
            if request.job_id and e["job_id"] == request.job_id:
                snap.job_total += amount
            if request.creator_id and e["creator_id"] == request.creator_id:
                snap.creator_total += amount
        return snap

    async def reserve(self, request: ReservationRequest) -> Reservation:
        decision = evaluate_reservation(
            request, self.budgets, self._snapshot(request), max_cost_per_creator_usd=self.max_cost_per_creator_usd
        )
        if not decision.allowed:
            return Reservation(None, decision)
        rid = str(uuid.uuid4())
        self.entries[rid] = {
            "provider": request.provider,
            "job_id": request.job_id,
            "creator_id": request.creator_id,
            "reserved": request.amount_usd,
            "actual": ZERO,
            "units": ZERO,
            "status": "reserved",
        }
        return Reservation(rid, decision)

    async def settle(self, reservation_id: str, actual_usd: Decimal, actual_units: Decimal = ZERO) -> None:
        entry = self.entries[reservation_id]
        entry["actual"] = actual_usd
        entry["units"] = actual_units
        entry["status"] = "committed"

    async def release(self, reservation_id: str) -> None:
        self.entries[reservation_id]["status"] = "released"

    async def units_spent(self, job_id: str, provider: str) -> Decimal:
        return sum((e["units"] for e in self.entries.values()
                    if e["status"] == "committed" and e["job_id"] == job_id and e["provider"] == provider), ZERO)

    @property
    def committed_total(self) -> Decimal:
        return sum((e["actual"] for e in self.entries.values() if e["status"] == "committed"), ZERO)


def _uuid_or_none(value: Optional[str]):
    if not value:
        return None
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
