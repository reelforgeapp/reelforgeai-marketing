"""
Commercial ledger.

Server-side billing events are the authoritative source for revenue and
payment state; PostHog is never used as a financial ledger.

Entry conventions (amount_minor is signed, in the currency's minor units):
  subscription_charge / renewal   +gross (pre-discount, pre-tax)
  discount                        −discount
  tax                             +tax  (recorded, excluded from revenue)
  refund / chargeback             −amount
  payment_fee, *_cost             −amount  (is_estimate=True when not reported)
  affiliate_commission            −accrued commission per deal terms
  influencer_fee, other_campaign_cost  −amount, campaign-level

Net revenue = charges + renewals + discounts + refunds + chargebacks.
Every write has an idempotency key, so replays and duplicate webhooks
(Stripe sends both invoice.paid and invoice.payment_succeeded) never double
count.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Optional

import structlog

logger = structlog.get_logger()

REVENUE_TYPES = ("subscription_charge", "renewal", "discount", "refund", "chargeback")
VARIABLE_COST_TYPES = ("payment_fee", "render_cost", "storage_cost", "delivery_cost", "ai_cost", "other_variable_cost")
ACQUISITION_COST_TYPES = ("affiliate_commission", "influencer_fee", "other_campaign_cost")
# Stripe zero-decimal currencies (amounts are already in major units).
ZERO_DECIMAL_CURRENCIES = {"bif", "clp", "djf", "gnf", "jpy", "kmf", "krw", "mga", "pyg", "rwf", "ugx", "vnd", "vuv",
                           "xaf", "xof", "xpf"}

DISPUTE_FEE_ESTIMATE_MINOR = 1500  # USD 15.00 Stripe dispute fee; estimate until balance transactions are read


@dataclass
class FxResult:
    usd: Decimal
    source: str


def minor_to_major(amount_minor: int, currency: str) -> Decimal:
    if currency.lower() in ZERO_DECIMAL_CURRENCIES:
        return Decimal(amount_minor)
    return Decimal(amount_minor) / Decimal(100)


async def to_usd(conn, amount_minor: int, currency: str, on: date) -> FxResult:
    major = minor_to_major(amount_minor, currency)
    if currency.lower() == "usd":
        return FxResult(major, "identity")
    rate = await conn.fetchrow(
        """SELECT usd_per_unit, source, rate_date FROM rpi_fx_rates WHERE currency = upper($1) AND rate_date <= $2
           ORDER BY rate_date DESC LIMIT 1""",
        currency, on,
    )
    if not rate:
        return FxResult(Decimal("0"), "MISSING")
    return FxResult((major * Decimal(rate["usd_per_unit"])).quantize(Decimal("0.0001"), ROUND_HALF_UP),
                    f"{rate['source']}@{rate['rate_date']}")


async def add_entry(conn, *, entry_type: str, amount_minor: int, currency: str, occurred_at: datetime,
                    idempotency_key: str, source: str, customer_ref: Optional[str] = None,
                    campaign_id=None, plan: Optional[str] = None, billing_period: Optional[str] = None,
                    is_estimate: bool = False, estimate_basis: Optional[str] = None, source_event_id=None) -> bool:
    fx = await to_usd(conn, amount_minor, currency, occurred_at.date())
    res = await conn.fetchval(
        """INSERT INTO rpi_ledger_entries
           (customer_ref, campaign_id, entry_type, amount_minor, currency, amount_usd, fx_source, plan, billing_period,
            occurred_at, is_estimate, estimate_basis, source, source_event_id, idempotency_key)
           VALUES ($1,$2,$3,$4,lower($5),$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
           ON CONFLICT (idempotency_key) DO NOTHING RETURNING id""",
        customer_ref, campaign_id, entry_type, amount_minor, currency, fx.usd, fx.source, plan, billing_period,
        occurred_at, is_estimate, estimate_basis, source, source_event_id, idempotency_key,
    )
    return res is not None


# --------------------------------------------------------------------------
# Stripe event ingestion
# --------------------------------------------------------------------------


async def store_billing_event(conn, event: dict, raw: bytes, signature_verified: bool) -> tuple[Optional[str], bool]:
    """Returns (billing_event_id, is_new)."""
    occurred = datetime.fromtimestamp(int(event.get("created", 0)), tz=timezone.utc)
    obj = (event.get("data") or {}).get("object") or {}
    customer = obj.get("customer") if isinstance(obj.get("customer"), str) else None
    row_id = await conn.fetchval(
        """INSERT INTO rpi_billing_events (provider, provider_event_id, event_type, billing_customer_id, occurred_at,
                                           livemode, payload_sha256, payload, signature_verified)
           VALUES ('stripe', $1, $2, $3, $4, $5, $6, $7::jsonb, $8)
           ON CONFLICT (provider, provider_event_id) DO NOTHING RETURNING id""",
        event["id"], event.get("type"), customer, occurred, event.get("livemode"),
        hashlib.sha256(raw).hexdigest(), json.dumps(event), signature_verified,
    )
    if row_id is None:
        existing = await conn.fetchval(
            "SELECT id FROM rpi_billing_events WHERE provider = 'stripe' AND provider_event_id = $1", event["id"])
        return (str(existing) if existing else None), False
    return str(row_id), True


async def customer_ref_for(conn, billing_customer_id: Optional[str]) -> Optional[str]:
    if not billing_customer_id:
        return None
    ref = await conn.fetchval("SELECT customer_ref FROM rpi_customers WHERE billing_customer_id = $1", billing_customer_id)
    if ref:
        return ref
    placeholder = f"stripe:{billing_customer_id}"
    await conn.execute(
        """INSERT INTO rpi_customers (customer_ref, billing_customer_id) VALUES ($1, $2)
           ON CONFLICT DO NOTHING""",
        placeholder, billing_customer_id,
    )
    return placeholder


def _plan_from_invoice(invoice: dict, price_map: dict[str, str]) -> tuple[Optional[str], Optional[str]]:
    lines = ((invoice.get("lines") or {}).get("data")) or []
    for line in lines:
        price = line.get("price") or {}
        if price.get("id"):
            plan = price_map.get(price["id"]) or price.get("nickname") or price["id"]
            interval = (price.get("recurring") or {}).get("interval")
            period = {"month": "monthly", "year": "annual"}.get(interval, interval)
            return plan, period
    return None, None


async def process_stripe_event(conn, event: dict, event_row_id: str, *, price_map: dict[str, str],
                               fee_rate: Decimal, fee_fixed_minor: int) -> str:
    etype = event.get("type", "")
    obj = (event.get("data") or {}).get("object") or {}
    occurred = datetime.fromtimestamp(int(event.get("created", 0)), tz=timezone.utc)

    if etype in ("invoice.paid", "invoice.payment_succeeded"):
        amount_paid = int(obj.get("amount_paid") or 0)
        if amount_paid <= 0:
            return "ignored: zero-amount invoice"
        currency = obj.get("currency", "usd")
        invoice_id = obj["id"]
        customer_ref = await customer_ref_for(conn, obj.get("customer"))
        tax = int(obj.get("tax") or 0) or sum(int(t.get("amount", 0)) for t in obj.get("total_tax_amounts") or [])
        discount = sum(int(d.get("amount", 0)) for d in obj.get("total_discount_amounts") or [])
        net = amount_paid - tax
        gross = net + discount
        plan, period = _plan_from_invoice(obj, price_map)
        entry_type = "renewal" if obj.get("billing_reason") == "subscription_cycle" else "subscription_charge"
        paid_at = datetime.fromtimestamp(int((obj.get("status_transitions") or {}).get("paid_at") or event.get("created", 0)),
                                         tz=timezone.utc)
        common = dict(currency=currency, occurred_at=paid_at, customer_ref=customer_ref, plan=plan, billing_period=period,
                      source="billing_webhook", source_event_id=_u(event_row_id))
        await add_entry(conn, entry_type=entry_type, amount_minor=gross, idempotency_key=f"stripe:invoice:{invoice_id}:charge", **common)
        if discount:
            await add_entry(conn, entry_type="discount", amount_minor=-discount, idempotency_key=f"stripe:invoice:{invoice_id}:discount", **common)
        if tax:
            await add_entry(conn, entry_type="tax", amount_minor=tax, idempotency_key=f"stripe:invoice:{invoice_id}:tax", **common)
        fee = int((Decimal(amount_paid) * fee_rate).quantize(Decimal("1"), ROUND_HALF_UP)) + fee_fixed_minor
        await add_entry(conn, entry_type="payment_fee", amount_minor=-fee, idempotency_key=f"stripe:invoice:{invoice_id}:fee",
                        is_estimate=True, estimate_basis=f"configured {fee_rate} + {fee_fixed_minor} minor units; "
                        "replace with balance transaction fee", **common)
        await conn.execute(
            """UPDATE rpi_customers SET first_paid_at = LEAST(COALESCE(first_paid_at, $2), $2),
                   current_plan = COALESCE($3, current_plan), billing_period = COALESCE($4, billing_period),
                   attribution_locked_at = CASE WHEN signed_up_at IS NOT NULL THEN COALESCE(attribution_locked_at, NOW())
                                                ELSE attribution_locked_at END,
                   updated_at = NOW()
               WHERE customer_ref = $1""",
            customer_ref, paid_at, plan, period,
        )
        return f"recorded {entry_type}"

    if etype == "charge.refunded":
        currency = obj.get("currency", "usd")
        customer_ref = await customer_ref_for(conn, obj.get("customer"))
        # amount_refunded is cumulative per charge. Recording only the delta
        # against what is already booked for this charge works whether or not
        # the API version expands `refunds`, and never double counts. The
        # per-charge advisory lock serializes concurrent deliveries.
        await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"refund:{obj['id']}")
        already = await conn.fetchval(
            "SELECT COALESCE(-SUM(amount_minor), 0) FROM rpi_ledger_entries WHERE idempotency_key LIKE $1",
            f"stripe:charge:{obj['id']}:refund:%",
        )
        delta = int(obj.get("amount_refunded") or 0) - int(already)
        if delta <= 0:
            return "ignored: refund already recorded"
        await add_entry(conn, entry_type="refund", amount_minor=-delta, currency=currency, occurred_at=occurred,
                        idempotency_key=f"stripe:charge:{obj['id']}:refund:{obj.get('amount_refunded')}",
                        source="billing_webhook", customer_ref=customer_ref, source_event_id=_u(event_row_id))
        return "recorded refund"

    if etype == "charge.dispute.created":
        currency = obj.get("currency", "usd")
        charge = obj.get("charge")
        customer_ref = None
        if isinstance(charge, dict):
            customer_ref = await customer_ref_for(conn, charge.get("customer"))
        await add_entry(conn, entry_type="chargeback", amount_minor=-int(obj.get("amount", 0)), currency=currency,
                        occurred_at=occurred, idempotency_key=f"stripe:dispute:{obj['id']}", source="billing_webhook",
                        customer_ref=customer_ref, source_event_id=_u(event_row_id))
        await add_entry(conn, entry_type="payment_fee", amount_minor=-DISPUTE_FEE_ESTIMATE_MINOR, currency="usd",
                        occurred_at=occurred, idempotency_key=f"stripe:dispute:{obj['id']}:fee", source="billing_webhook",
                        customer_ref=customer_ref, is_estimate=True, estimate_basis="standard dispute fee estimate",
                        source_event_id=_u(event_row_id))
        return "recorded chargeback"

    if etype == "charge.dispute.closed" and obj.get("status") == "won":
        await add_entry(conn, entry_type="chargeback", amount_minor=int(obj.get("amount", 0)), currency=obj.get("currency", "usd"),
                        occurred_at=occurred, idempotency_key=f"stripe:dispute:{obj['id']}:won", source="billing_webhook",
                        source_event_id=_u(event_row_id))
        return "recorded dispute reversal"

    if etype == "customer.subscription.deleted":
        customer_ref = await customer_ref_for(conn, obj.get("customer"))
        ended = obj.get("ended_at") or obj.get("canceled_at") or event.get("created")
        await conn.execute("UPDATE rpi_customers SET canceled_at = $2, updated_at = NOW() WHERE customer_ref = $1",
                           customer_ref, datetime.fromtimestamp(int(ended), tz=timezone.utc))
        return "recorded cancellation"

    if etype in ("customer.subscription.created", "customer.subscription.updated"):
        customer_ref = await customer_ref_for(conn, obj.get("customer"))
        items = ((obj.get("items") or {}).get("data")) or []
        price = (items[0].get("price") or {}) if items else {}
        plan = price_map.get(price.get("id", "")) or price.get("nickname") or price.get("id")
        period = {"month": "monthly", "year": "annual"}.get((price.get("recurring") or {}).get("interval"))
        await conn.execute(
            """UPDATE rpi_customers SET current_plan = COALESCE($2, current_plan), billing_period = COALESCE($3, billing_period),
                   canceled_at = CASE WHEN $4 THEN canceled_at ELSE NULL END, updated_at = NOW() WHERE customer_ref = $1""",
            customer_ref, plan, period, obj.get("status") in ("canceled", "incomplete_expired"),
        )
        return "recorded subscription state"

    return "ignored: event type not used by RPI"


# --------------------------------------------------------------------------
# Commission accrual and cost allocation
# --------------------------------------------------------------------------


async def accrue_commissions(conn, *, attribution_model: str = "last_touch", refund_clawback_days: int = 60) -> int:
    """Accrue deal commission on attributed net revenue. Idempotent per
    revenue entry. Refunds within the clawback window reverse commission."""
    campaign_col = "last_touch_campaign_id" if attribution_model == "last_touch" else "first_touch_campaign_id"
    rows = await conn.fetch(
        f"""SELECT e.id, e.entry_type, e.amount_minor, e.currency, e.occurred_at, e.customer_ref,
                   cu.first_paid_at, ca.id AS campaign_id, d.commission_rate, d.commission_months
            FROM rpi_ledger_entries e
            JOIN rpi_customers cu ON cu.customer_ref = e.customer_ref
            JOIN rpi_campaigns ca ON ca.id = cu.{campaign_col}
            JOIN rpi_deals d ON d.id = ca.deal_id
            WHERE e.entry_type IN ('subscription_charge', 'renewal', 'discount', 'refund')
              AND d.commission_rate IS NOT NULL AND d.commission_rate > 0
              AND d.status IN ('agreed', 'completed')
              AND NOT EXISTS (SELECT 1 FROM rpi_ledger_entries c WHERE c.idempotency_key = 'commission:' || e.id::text)"""
    )
    n = 0
    for r in rows:
        if r["commission_months"] is not None and r["first_paid_at"] is not None:
            months = (r["occurred_at"].year - r["first_paid_at"].year) * 12 + (r["occurred_at"].month - r["first_paid_at"].month)
            if months >= r["commission_months"]:
                continue
        if r["entry_type"] == "refund" and r["first_paid_at"] is not None:
            if (r["occurred_at"] - r["first_paid_at"]).days > refund_clawback_days:
                continue
        commission = -int((Decimal(r["amount_minor"]) * Decimal(r["commission_rate"])).quantize(Decimal("1"), ROUND_HALF_UP))
        if commission == 0:
            continue
        added = await add_entry(
            conn, entry_type="affiliate_commission", amount_minor=commission, currency=r["currency"],
            occurred_at=r["occurred_at"], idempotency_key=f"commission:{r['id']}", source="allocation",
            customer_ref=r["customer_ref"], campaign_id=r["campaign_id"], is_estimate=True,
            estimate_basis=f"accrued at {r['commission_rate']} per deal terms; reconcile with affiliate payouts",
        )
        n += added
    return n


async def allocate_variable_costs(conn, month_start: date, cost_by_plan_usd: dict[str, Decimal], basis: str) -> int:
    """Estimated delivery cost (render + storage + AI) per paying customer
    for a month, until the product emits per-render cost events."""
    rows = await conn.fetch(
        """SELECT DISTINCT e.customer_ref, e.plan FROM rpi_ledger_entries e
           WHERE e.entry_type IN ('subscription_charge', 'renewal')
             AND e.occurred_at >= $1 AND e.occurred_at < ($1::date + INTERVAL '1 month')""",
        month_start,
    )
    n = 0
    for r in rows:
        cost = cost_by_plan_usd.get((r["plan"] or "").lower())
        if cost is None:
            continue
        n += await add_entry(
            conn, entry_type="render_cost", amount_minor=-int((cost * 100).quantize(Decimal("1"))), currency="usd",
            occurred_at=datetime.combine(month_start, datetime.min.time(), tzinfo=timezone.utc),
            idempotency_key=f"alloc:render:{r['customer_ref']}:{month_start.isoformat()}", source="allocation",
            customer_ref=r["customer_ref"], plan=r["plan"], is_estimate=True, estimate_basis=basis,
        )
    return n


def _u(value: Any):
    import uuid

    return uuid.UUID(str(value)) if value else None
