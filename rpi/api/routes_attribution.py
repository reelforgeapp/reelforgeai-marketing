"""Attribution ingest, Stripe billing webhook, cost feed and financial reports."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from rpi.administration.auth import Principal
from rpi.api.context import RPIContext
from rpi.api.deps import get_ctx, require
from rpi.api.serialize import clean
from rpi.attribution import touches
from rpi.attribution.posthog import PostHogBridge
from rpi.attribution.stripe_signature import SignatureError, verify
from rpi.core.audit import audit
from rpi.financial_ledger import ledger, reports

logger = structlog.get_logger()
router = APIRouter()


class TouchBody(BaseModel):
    anonymous_id: Optional[str] = Field(default=None, max_length=200)
    referral_code: Optional[str] = Field(default=None, max_length=60)
    utm_source: Optional[str] = Field(default=None, max_length=120)
    utm_medium: Optional[str] = Field(default=None, max_length=120)
    utm_campaign: Optional[str] = Field(default=None, max_length=120)
    landing_page_variant: Optional[str] = Field(default=None, max_length=120)
    consent_state: str = Field(default="unknown", pattern="^(granted|denied|unknown)$")
    touched_at: datetime
    idempotency_key: Optional[str] = Field(default=None, max_length=200)


@router.post("/attribution/touch")
async def touch(body: TouchBody, ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("ingest"))):
    if not (body.referral_code or body.utm_campaign):
        raise HTTPException(422, "a referral_code or utm_campaign is required")
    async with ctx.pool.acquire() as conn:
        return await touches.record_touch(conn, body.model_dump())


class IdentifyBody(BaseModel):
    customer_ref: str = Field(min_length=1, max_length=120, description="ReelForge product user id")
    signed_up_at: datetime
    anonymous_id: Optional[str] = Field(default=None, max_length=200)
    billing_customer_id: Optional[str] = Field(default=None, max_length=120)
    signup_referral_code: Optional[str] = Field(default=None, max_length=60)
    consent_state: str = Field(default="unknown", pattern="^(granted|denied|unknown)$")


@router.post("/attribution/identify")
async def identify(body: IdentifyBody, ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("ingest"))):
    async with ctx.pool.acquire() as conn:
        async with conn.transaction():
            result = await touches.identify(conn, body.model_dump())
        await ledger.accrue_commissions(conn)
        campaign = None
        if result["last_touch_campaign_id"]:
            campaign = await conn.fetchrow("SELECT creator_id, referral_code FROM rpi_campaigns WHERE id = $1",
                                           result["last_touch_campaign_id"])
    bridge = PostHogBridge(ctx.settings.posthog_api_key, ctx.settings.posthog_host,
                           requires_consent=ctx.settings.rpi_posthog_requires_consent)
    if campaign:
        result["posthog"] = await bridge.attribution_assigned(
            distinct_id=body.customer_ref, consent_state=body.consent_state,
            properties={"partner_first_touch_campaign_id": result["first_touch_campaign_id"],
                        "partner_last_touch_campaign_id": result["last_touch_campaign_id"],
                        "partner_creator_id": str(campaign["creator_id"]), "partner_referral_code": campaign["referral_code"],
                        "partner_attribution_model": "last_touch", "campaign_id": result["last_touch_campaign_id"],
                        "attribution_model": "last_touch"})
    return clean(result)


class MilestoneBody(BaseModel):
    customer_ref: str
    milestone: str = Field(pattern="^(first_render|first_export)$")
    at: datetime


@router.post("/attribution/milestone")
async def milestone(body: MilestoneBody, ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("ingest"))):
    async with ctx.pool.acquire() as conn:
        return {"updated": await touches.record_milestone(conn, body.customer_ref, body.milestone, body.at)}


# --------------------------------------------------------------------------
# Stripe webhook (RPI's own endpoint and secret)
# --------------------------------------------------------------------------


@router.post("/webhooks/stripe", include_in_schema=False)
async def stripe_webhook(request: Request, ctx: RPIContext = Depends(get_ctx)):
    raw = await request.body()
    try:
        verify(raw, request.headers.get("stripe-signature", ""), ctx.settings.rpi_stripe_webhook_secret,
               ctx.settings.rpi_stripe_webhook_tolerance_seconds)
    except SignatureError as exc:
        logger.warning("rpi_stripe_signature_rejected", reason=str(exc))
        raise HTTPException(400, "invalid signature")
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(400, "invalid JSON")
    if not isinstance(event, dict) or "id" not in event or "type" not in event:
        raise HTTPException(400, "not a Stripe event")
    price_map = json.loads(ctx.settings.rpi_stripe_price_plan_map or "{}")
    async with ctx.pool.acquire() as conn:
        async with conn.transaction():
            event_row, is_new = await ledger.store_billing_event(conn, event, raw, signature_verified=True)
            if not is_new:
                prior = await conn.fetchval("SELECT processing_status FROM rpi_billing_events WHERE id = $1", event_row)
                if prior in ("processed", "ignored"):
                    return {"status": "duplicate"}
        # New event, or a redelivery of one that previously failed: process it.
        try:
            async with conn.transaction():
                outcome = await ledger.process_stripe_event(conn, event, event_row, price_map=price_map,
                                                            fee_rate=Decimal("0.029"), fee_fixed_minor=30)
                status = "ignored" if outcome.startswith("ignored") else "processed"
                await conn.execute(
                    "UPDATE rpi_billing_events SET processing_status = $2, processed_at = NOW() WHERE id = $1",
                    event_row, status)
            await ledger.accrue_commissions(conn)
        except Exception as exc:
            await conn.execute(
                "UPDATE rpi_billing_events SET processing_status = 'failed', processing_error = $2 WHERE id = $1",
                event_row, str(exc)[:500])
            logger.error("rpi_stripe_processing_failed", event_id=event.get("id"), error=str(exc))
            raise HTTPException(500, "processing failed; event stored for retry")
    return {"status": status, "outcome": outcome}


@router.post("/ledger/reprocess-failed")
async def reprocess(ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("admin"))):
    price_map = json.loads(ctx.settings.rpi_stripe_price_plan_map or "{}")
    done = 0
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, payload FROM rpi_billing_events WHERE processing_status = 'failed' ORDER BY occurred_at")
        for r in rows:
            event = r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"])
            async with conn.transaction():
                outcome = await ledger.process_stripe_event(conn, event, str(r["id"]), price_map=price_map,
                                                            fee_rate=Decimal("0.029"), fee_fixed_minor=30)
                await conn.execute("UPDATE rpi_billing_events SET processing_status = $2, processing_error = NULL, "
                                   "processed_at = NOW() WHERE id = $1", r["id"],
                                   "ignored" if outcome.startswith("ignored") else "processed")
            done += 1
        await ledger.accrue_commissions(conn)
        await audit(conn, p.actor, "ledger.reprocess", None, None, {"events": done})
    return {"reprocessed": done}


# --------------------------------------------------------------------------
# Cost feed, FX, reports
# --------------------------------------------------------------------------


class CostBody(BaseModel):
    customer_ref: str
    entry_type: str = Field(pattern="^(render_cost|storage_cost|delivery_cost|ai_cost|other_variable_cost)$")
    amount_usd: Decimal = Field(gt=0)
    occurred_at: datetime
    idempotency_key: str = Field(min_length=6, max_length=200)
    is_estimate: bool = False
    estimate_basis: Optional[str] = None


@router.post("/ledger/costs")
async def cost_feed(body: CostBody, ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("ingest"))):
    async with ctx.pool.acquire() as conn:
        added = await ledger.add_entry(
            conn, entry_type=body.entry_type, amount_minor=-int((body.amount_usd * 100).quantize(Decimal("1"))),
            currency="usd", occurred_at=body.occurred_at, idempotency_key=f"cost:{body.idempotency_key}",
            source="product_cost_feed", customer_ref=body.customer_ref, is_estimate=body.is_estimate,
            estimate_basis=body.estimate_basis)
    return {"recorded": added, "duplicate": not added}


class AllocateBody(BaseModel):
    month: date
    use_default_estimates: bool = True


@router.post("/ledger/allocate-variable-costs")
async def allocate(body: AllocateBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("admin"))):
    from rpi.profit_modeling.defaults import COGS_SRC, default_plans

    costs = {pl.plan: pl.variable_cost_monthly.value for pl in default_plans()}
    async with ctx.pool.acquire() as conn:
        n = await ledger.allocate_variable_costs(conn, body.month.replace(day=1), costs, basis=COGS_SRC)
        await audit(conn, p.actor, "ledger.allocate_costs", None, None, {"month": body.month.isoformat(), "entries": n})
    return {"entries": n, "is_estimate": True, "basis": COGS_SRC}


class FxBody(BaseModel):
    rate_date: date
    currency: str = Field(pattern="^[A-Za-z]{3}$")
    usd_per_unit: Decimal = Field(gt=0)
    source: str = Field(min_length=2, max_length=120)


@router.put("/ledger/fx-rates")
async def fx(body: FxBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("admin"))):
    async with ctx.pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO rpi_fx_rates (rate_date, currency, usd_per_unit, source) VALUES ($1, upper($2), $3, $4)
               ON CONFLICT (rate_date, currency) DO UPDATE SET usd_per_unit = EXCLUDED.usd_per_unit, source = EXCLUDED.source""",
            body.rate_date, body.currency, body.usd_per_unit, body.source)
        await audit(conn, p.actor, "fx.upsert", "fx_rate", f"{body.currency}:{body.rate_date}", body.model_dump(mode="json"))
    return {"ok": True}


@router.get("/reports/partners")
async def partners(attribution_model: str = "last_touch", ctx: RPIContext = Depends(get_ctx),
                   _: Principal = Depends(require("viewer"))):
    if attribution_model not in ("last_touch", "first_touch"):
        raise HTTPException(422, "attribution_model must be last_touch or first_touch")
    async with ctx.pool.acquire() as conn:
        return clean(await reports.partner_report(conn, attribution_model=attribution_model))


@router.get("/reports/cohorts")
async def cohort_report(attribution_model: str = "last_touch", campaign_id: Optional[str] = None,
                        ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer"))):
    async with ctx.pool.acquire() as conn:
        return clean(await reports.cohorts(conn, campaign_id=campaign_id, attribution_model=attribution_model))


@router.get("/reports/reconciliation")
async def reconciliation(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("analyst"))):
    async with ctx.pool.acquire() as conn:
        return clean(await reports.reconciliation(conn))
