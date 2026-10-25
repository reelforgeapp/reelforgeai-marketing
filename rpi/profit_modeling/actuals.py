"""
Replace default assumptions with ledger actuals when there is enough data.

Per plan, with at least MIN_CUSTOMERS paying customers observed:
  * price  → average net first charge (charge + discount entries), ACTUAL
  * mix    → share of first charges, ACTUAL (renormalized across plans)
  * survival curve → observed monthly retention for cohorts old enough, ACTUAL
Otherwise the default stays and a note explains why.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

from rpi.profit_modeling.model import Basis, Input, PlanEconomics

MIN_CUSTOMERS = 30


async def inputs_from_ledger(conn, plans: tuple[PlanEconomics, ...]) -> tuple[tuple[PlanEconomics, ...], list[str]]:
    rows = await conn.fetch(
        """SELECT e.customer_ref, lower(e.plan) AS plan, e.entry_type, e.amount_usd, e.occurred_at, e.idempotency_key,
                  c.first_paid_at
           FROM rpi_ledger_entries e JOIN rpi_customers c ON c.customer_ref = e.customer_ref
           WHERE e.entry_type IN ('subscription_charge', 'renewal', 'discount') AND e.billing_period = 'monthly'
             AND c.first_paid_at IS NOT NULL""")
    if not rows:
        return plans, ["no ledger actuals yet; all plan inputs are defaults (estimates/assumptions)"]

    first_charge: dict[str, dict[str, Decimal]] = defaultdict(dict)
    paid_months: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    first_paid: dict[str, datetime] = {}
    for r in rows:
        plan = r["plan"] or "unknown"
        first_paid[r["customer_ref"]] = r["first_paid_at"]
        k = (r["occurred_at"].year - r["first_paid_at"].year) * 12 + (r["occurred_at"].month - r["first_paid_at"].month)
        if r["entry_type"] in ("subscription_charge", "renewal"):
            paid_months[plan][r["customer_ref"]].add(k)
        if k == 0:
            first_charge[plan][r["customer_ref"]] = first_charge[plan].get(r["customer_ref"], Decimal(0)) + Decimal(r["amount_usd"])

    notes: list[str] = []
    total_first = sum(len(v) for v in first_charge.values()) or 1
    now = datetime.now(timezone.utc)
    updated = []
    for p in plans:
        customers = first_charge.get(p.plan.lower(), {})
        n = len(customers)
        if n < MIN_CUSTOMERS:
            notes.append(f"{p.plan}: {n} paying customers observed (< {MIN_CUSTOMERS}); using default inputs")
            updated.append(p)
            continue
        price = (sum(customers.values(), Decimal(0)) / n).quantize(Decimal("0.01"))
        mix = Decimal(n) / Decimal(total_first)
        curve = []
        for k in range(12):
            eligible = [c for c in paid_months[p.plan.lower()]
                        if (now.year - first_paid[c].year) * 12 + (now.month - first_paid[c].month) >= k]
            if len(eligible) < MIN_CUSTOMERS:
                break
            curve.append(Decimal(sum(1 for c in eligible if k in paid_months[p.plan.lower()][c])) / Decimal(len(eligible)))
        src = f"billing ledger, {n} customers"
        p2 = replace(p, price=Input(price, Basis.ACTUAL, src), mix=Input(mix, Basis.ACTUAL, src))
        if len(curve) >= 3:
            p2 = replace(p2, survival_curve=tuple(curve), survival_basis=Basis.ACTUAL)
            notes.append(f"{p.plan}: retention from {len(curve)} observed months")
        else:
            notes.append(f"{p.plan}: price and mix from ledger; retention still assumed (cohorts too young)")
        updated.append(p2)

    # Keep the mix summing to 1 when only some plans have actuals.
    total_mix = sum((p.mix.value for p in updated), Decimal(0))
    if total_mix and abs(total_mix - 1) > Decimal("0.001"):
        updated = [replace(p, mix=Input(p.mix.value / total_mix, p.mix.basis, p.mix.source + " (renormalized)")) for p in updated]
    return tuple(updated), notes
