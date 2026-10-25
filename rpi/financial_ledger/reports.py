"""
Partner profitability, subscriber cohorts and reconciliation.

All figures come from rpi_ledger_entries (actuals plus explicitly flagged
estimates). A report is marked `complete` only when every required cost
category is present for the customers it covers; otherwise it says which
categories are missing and that profit is not exact.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from rpi.financial_ledger.ledger import ACQUISITION_COST_TYPES, REVENUE_TYPES, VARIABLE_COST_TYPES

REQUIRED_COST_CATEGORIES = ("payment_fee", "render_cost")
Q = Decimal("0.01")


def _d(v) -> Decimal:
    return Decimal(v or 0)


def _month_index(start: datetime, at: datetime) -> int:
    return (at.year - start.year) * 12 + (at.month - start.month)


async def partner_report(conn, *, attribution_model: str = "last_touch", campaign_id=None) -> list[dict]:
    col = "last_touch_campaign_id" if attribution_model == "last_touch" else "first_touch_campaign_id"
    campaigns = await conn.fetch(
        f"""SELECT ca.id, ca.name, ca.referral_code, ca.creator_id, cr.display_name, ca.status, ca.start_date
            FROM rpi_campaigns ca JOIN rpi_creators cr ON cr.id = ca.creator_id
            WHERE ($1::uuid IS NULL OR ca.id = $1) ORDER BY ca.created_at""",
        campaign_id,
    )
    out = []
    for ca in campaigns:
        customers = await conn.fetch(
            f"SELECT customer_ref, signed_up_at, first_paid_at, canceled_at FROM rpi_customers WHERE {col} = $1", ca["id"])
        refs = [c["customer_ref"] for c in customers]
        sums = await conn.fetch(
            """SELECT entry_type, SUM(amount_usd) AS usd, BOOL_OR(is_estimate) AS any_estimate,
                      SUM(CASE WHEN is_estimate THEN amount_usd ELSE 0 END) AS est_usd,
                      COUNT(*) FILTER (WHERE fx_source = 'MISSING') AS fx_missing
               FROM rpi_ledger_entries WHERE customer_ref = ANY($1::text[]) GROUP BY entry_type""",
            refs,
        )
        campaign_costs = await conn.fetch(
            """SELECT entry_type, SUM(amount_usd) AS usd, COUNT(*) FILTER (WHERE fx_source = 'MISSING') AS fx_missing
               FROM rpi_ledger_entries WHERE campaign_id = $1 AND entry_type IN ('influencer_fee', 'other_campaign_cost', 'affiliate_commission')
               GROUP BY entry_type""",
            ca["id"],
        )
        by_type = {r["entry_type"]: r for r in sums}
        revenue = sum((_d(by_type[t]["usd"]) for t in REVENUE_TYPES if t in by_type), Decimal("0"))
        variable = sum((_d(by_type[t]["usd"]) for t in VARIABLE_COST_TYPES if t in by_type), Decimal("0"))
        estimated_variable = sum((_d(by_type[t]["est_usd"]) for t in VARIABLE_COST_TYPES if t in by_type), Decimal("0"))
        acq = {r["entry_type"]: _d(r["usd"]) for r in campaign_costs}
        acquisition = sum(acq.values(), Decimal("0"))  # negative
        contribution = revenue + variable
        profit = contribution + acquisition
        paying = [c for c in customers if c["first_paid_at"]]
        missing = [t for t in REQUIRED_COST_CATEGORIES if paying and t not in by_type]
        if not any(t in acq for t in ("influencer_fee", "affiliate_commission")):
            missing.append("acquisition cost (no influencer fee or commission recorded)")
        fx_missing = sum(int(r["fx_missing"]) for r in list(sums) + list(campaign_costs))
        if fx_missing:
            missing.append(f"{fx_missing} entries without an exchange rate")
        out.append({
            "campaign_id": str(ca["id"]),
            "campaign": ca["name"],
            "creator": ca["display_name"],
            "attribution_model": attribution_model,
            "signups": len(customers),
            "paying_customers": len(paying),
            "active_customers": sum(1 for c in paying if not c["canceled_at"]),
            "net_revenue_usd": str(revenue.quantize(Q)),
            "variable_costs_usd": str(variable.quantize(Q)),
            "variable_costs_estimated_usd": str(estimated_variable.quantize(Q)),
            "contribution_usd": str(contribution.quantize(Q)),
            "acquisition_costs_usd": {k: str(v.quantize(Q)) for k, v in acq.items()},
            "campaign_profit_usd": str(profit.quantize(Q)),
            "contribution_per_acquisition_dollar": str((contribution / -acquisition).quantize(Decimal("0.001")))
            if acquisition < 0 else None,
            "cost_per_paying_customer_usd": str((-acquisition / len(paying)).quantize(Q)) if paying and acquisition < 0 else None,
            "complete": not missing,
            "missing": missing,
            "statement": "Observed ledger results." if not missing else
            "Profit is not exact: " + "; ".join(missing) + ". Estimated amounts are flagged in the ledger.",
        })
    return out


async def cohorts(conn, *, campaign_id=None, attribution_model: str = "last_touch", horizon: int = 12) -> dict:
    col = "last_touch_campaign_id" if attribution_model == "last_touch" else "first_touch_campaign_id"
    customers = await conn.fetch(
        f"""SELECT customer_ref, first_paid_at, billing_period FROM rpi_customers
            WHERE first_paid_at IS NOT NULL AND ($1::uuid IS NULL OR {col} = $1)""",
        campaign_id,
    )
    if not customers:
        return {"cohorts": [], "note": "no paying customers yet"}
    refs = [c["customer_ref"] for c in customers]
    entries = await conn.fetch(
        """SELECT customer_ref, entry_type, amount_usd, occurred_at, billing_period FROM rpi_ledger_entries
           WHERE customer_ref = ANY($1::text[])""",
        refs,
    )
    by_customer = defaultdict(list)
    for e in entries:
        by_customer[e["customer_ref"]].append(e)
    now = datetime.now(timezone.utc)
    groups: dict[str, list] = defaultdict(list)
    for c in customers:
        groups[c["first_paid_at"].strftime("%Y-%m")].append(c)

    result = []
    for month, members in sorted(groups.items()):
        start = members[0]["first_paid_at"].replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        observable = min(horizon, _month_index(start, now) + 1)
        active = [0] * observable
        contribution = [Decimal("0")] * observable
        for c in members:
            paid_months = set()
            for e in by_customer[c["customer_ref"]]:
                k = _month_index(c["first_paid_at"], e["occurred_at"])
                if 0 <= k < observable:
                    if e["entry_type"] in ("subscription_charge", "renewal"):
                        span = 12 if (e["billing_period"] or c["billing_period"]) == "annual" else 1
                        paid_months.update(range(k, min(k + span, observable)))
                    if e["entry_type"] in REVENUE_TYPES + VARIABLE_COST_TYPES:
                        contribution[k] += _d(e["amount_usd"])
            for k in paid_months:
                active[k] += 1
        n = len(members)
        cumulative, running = [], Decimal("0")
        for k in range(observable):
            running += contribution[k]
            cumulative.append(str((running / n).quantize(Q)))
        result.append({
            "cohort": month,
            "customers": n,
            "months_observed": observable,
            "retention": [round(a / n, 3) for a in active],
            "cumulative_contribution_per_customer_usd": cumulative,
        })
    return {"attribution_model": attribution_model, "cohorts": result,
            "note": "Observed retention from paid charges; annual plans count as retained through their paid term."}


async def reconciliation(conn) -> dict:
    events = await conn.fetch(
        "SELECT processing_status, COUNT(*) AS n FROM rpi_billing_events GROUP BY processing_status")
    unverified = await conn.fetchval("SELECT COUNT(*) FROM rpi_billing_events WHERE NOT signature_verified")
    invoices_without_entry = await conn.fetchval(
        """SELECT COUNT(*) FROM rpi_billing_events b
           WHERE b.event_type IN ('invoice.paid', 'invoice.payment_succeeded')
             AND COALESCE((b.payload->'data'->'object'->>'amount_paid')::bigint, 0) > 0
             AND NOT EXISTS (SELECT 1 FROM rpi_ledger_entries e
                             WHERE e.idempotency_key = 'stripe:invoice:' || (b.payload->'data'->'object'->>'id') || ':charge')""")
    totals = await conn.fetchrow(
        """SELECT COALESCE(SUM((payload->'data'->'object'->>'amount_paid')::bigint), 0) AS paid_minor
           FROM (SELECT DISTINCT ON (payload->'data'->'object'->>'id') payload FROM rpi_billing_events
                 WHERE event_type IN ('invoice.paid', 'invoice.payment_succeeded') AND processing_status = 'processed'
                   AND COALESCE(payload->'data'->'object'->>'currency', 'usd') = 'usd') s""")
    ledger_paid = await conn.fetchval(
        """SELECT COALESCE(SUM(amount_minor), 0) FROM rpi_ledger_entries
           WHERE entry_type IN ('subscription_charge', 'renewal', 'discount', 'tax') AND currency = 'usd'
             AND source = 'billing_webhook'""")
    fx_missing = await conn.fetchval("SELECT COUNT(*) FROM rpi_ledger_entries WHERE fx_source = 'MISSING'")
    unidentified = await conn.fetchval(
        "SELECT COUNT(DISTINCT customer_ref) FROM rpi_ledger_entries WHERE customer_ref LIKE 'stripe:%'")
    unattributed_chargebacks = await conn.fetchval(
        "SELECT COUNT(*) FROM rpi_ledger_entries WHERE entry_type = 'chargeback' AND customer_ref IS NULL")
    checks = {
        "usd_invoice_totals_match": int(totals["paid_minor"]) == int(ledger_paid),
        "every_paid_invoice_has_entry": invoices_without_entry == 0,
        "all_events_signature_verified": unverified == 0,
        "no_missing_fx": fx_missing == 0,
    }
    return {
        "checks": checks,
        "ok": all(checks.values()),
        "billing_events": {r["processing_status"]: r["n"] for r in events},
        "usd_invoices_paid_minor": int(totals["paid_minor"]),
        "usd_ledger_billed_minor": int(ledger_paid),
        "paid_invoices_without_ledger_entry": invoices_without_entry,
        "entries_missing_fx": fx_missing,
        "customers_not_linked_to_product_user": unidentified,
        "chargebacks_without_customer": unattributed_chargebacks,
        "external_reconciliation": "Not implemented: compare against Stripe balance transactions / payouts with a "
        "Stripe API key (see KNOWN_LIMITATIONS.md).",
    }
