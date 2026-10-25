"""Profit modeling, deals, campaigns, CRM and outreach."""

from __future__ import annotations

import json
import secrets
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from rpi.administration.auth import Principal
from rpi.api.context import RPIContext
from rpi.api.deps import get_ctx, require
from rpi.api.serialize import clean, jsonb
from rpi.core.audit import audit
from rpi.crm import suppression
from rpi.crm.pipeline import Status, TransitionError, transition
from rpi.enrichment.service import creator_competitor_deals
from rpi.financial_ledger.ledger import add_entry
from rpi.outreach.drafts import DraftContext, generate, save_draft
from rpi.outreach.sender import send_draft
from rpi.profit_modeling import defaults
from rpi.profit_modeling.actuals import inputs_from_ledger
from rpi.profit_modeling.deals import candidate_structures, compare_deals
from rpi.profit_modeling.model import (
    Basis,
    BonusTerms,
    CampaignInputs,
    CommissionTerms,
    DealKind,
    DealStructure,
    Funnel,
    Input,
    PaymentFees,
    PlanEconomics,
    Range,
    forecast,
)

router = APIRouter()


# --------------------------------------------------------------------------
# Profit modeling
# --------------------------------------------------------------------------


class RangeIn(BaseModel):
    low: Decimal
    base: Decimal
    high: Decimal
    basis: Basis = Basis.ASSUMPTION
    source: str = "operator input"

    def to(self) -> Range:
        return Range.of(self.low, self.base, self.high, self.basis, self.source)


class PlanIn(BaseModel):
    plan: str
    billing_period: str = Field(default="monthly", pattern="^(monthly|annual)$")
    price: Decimal = Field(ge=0)
    mix: Decimal = Field(ge=0, le=1)
    variable_cost_monthly: Decimal = Field(ge=0)
    monthly_churn: Optional[RangeIn] = None
    annual_renewal_rate: Optional[RangeIn] = None
    first_payment_refund_rate: Decimal = Field(default=Decimal("0"), ge=0, le=1)


class DealIn(BaseModel):
    kind: DealKind = DealKind.FIXED
    upfront_fee: Decimal = Field(default=Decimal("0"), ge=0)
    commission_rate: Optional[Decimal] = Field(default=None, ge=0, le=1)
    commission_months: Optional[int] = Field(default=None, ge=1, le=120)
    bonus_threshold_customers: Optional[Decimal] = None
    bonus_per_customer_usd: Optional[Decimal] = None
    bonus_flat_usd: Optional[Decimal] = None
    bonus_cap_usd: Optional[Decimal] = None

    def to(self) -> DealStructure:
        commission = CommissionTerms(self.commission_rate, self.commission_months) if self.commission_rate else None
        bonus = None
        if self.bonus_threshold_customers is not None:
            bonus = BonusTerms(self.bonus_threshold_customers, self.bonus_per_customer_usd or Decimal(0),
                               self.bonus_flat_usd or Decimal(0), self.bonus_cap_usd)
        return DealStructure(self.kind, self.upfront_fee, commission, bonus)


class ForecastBody(BaseModel):
    expected_views: Optional[RangeIn] = None
    click_through_rate: Optional[RangeIn] = None
    visitor_to_paid_rate: Optional[RangeIn] = None
    plans: Optional[list[PlanIn]] = None
    use_ledger_actuals: bool = True
    payment_fee_rate: Optional[Decimal] = None
    payment_fee_fixed: Optional[Decimal] = None
    other_campaign_costs: Decimal = Decimal("0")
    incrementality: Decimal = Field(default=Decimal("1"), ge=0, le=1)
    horizon_months: int = Field(default=12, ge=1, le=60)
    required_roi: Decimal = Decimal("0")
    deal: DealIn = Field(default_factory=DealIn)
    compare_structures: bool = False


async def _inputs(body: ForecastBody, ctx: RPIContext) -> tuple[CampaignInputs, list[str]]:
    notes: list[str] = []
    base = defaults.default_inputs()
    plans = base.plans
    if body.plans:
        plans = tuple(PlanEconomics(
            plan=p.plan, billing_period=p.billing_period, price=Input(p.price), mix=Input(p.mix),
            variable_cost_monthly=Input(p.variable_cost_monthly),
            monthly_churn=p.monthly_churn.to() if p.monthly_churn else None,
            annual_renewal_rate=p.annual_renewal_rate.to() if p.annual_renewal_rate else None,
            first_payment_refund_rate=Input(p.first_payment_refund_rate)) for p in body.plans)
    elif body.use_ledger_actuals:
        async with ctx.pool.acquire() as conn:
            actual_plans, actual_notes = await inputs_from_ledger(conn, plans)
        plans = actual_plans
        notes.extend(actual_notes)
    fees = base.payment_fees
    if body.payment_fee_rate is not None or body.payment_fee_fixed is not None:
        fees = PaymentFees(Input(body.payment_fee_rate if body.payment_fee_rate is not None else fees.rate.value),
                           Input(body.payment_fee_fixed if body.payment_fee_fixed is not None else fees.fixed.value))
    funnel = Funnel(
        body.expected_views.to() if body.expected_views else base.funnel.expected_views,
        body.click_through_rate.to() if body.click_through_rate else base.funnel.click_through_rate,
        body.visitor_to_paid_rate.to() if body.visitor_to_paid_rate else base.funnel.visitor_to_paid_rate,
    )
    inputs = CampaignInputs(plans, fees, funnel, Input(body.other_campaign_costs),
                            Input(body.incrementality, Basis.ASSUMPTION,
                                  "operator input" if body.incrementality != 1 else "no holdout data: all attributed customers assumed incremental"),
                            body.horizon_months)
    problems = inputs.validate()
    if problems:
        raise HTTPException(422, "; ".join(problems))
    return inputs, notes


@router.post("/profit/forecast")
async def profit_forecast(body: ForecastBody, ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer"))):
    inputs, notes = await _inputs(body, ctx)
    result = forecast(inputs, body.deal.to(), required_roi=body.required_roi)
    result["notes"] = notes
    result["program_terms"] = {"rate": str(ctx.program.rate), "duration_months": ctx.program.duration_months,
                               "attribution_window_days": ctx.program.attribution_window_days}
    if body.compare_structures:
        result["deal_comparison"] = compare_deals(
            inputs, candidate_structures(body.deal.upfront_fee or Decimal("500"), ctx.program), ctx.program)
    return clean(result)


# --------------------------------------------------------------------------
# Deals and campaigns
# --------------------------------------------------------------------------


class DealBody(DealIn):
    creator_id: str
    deliverables: Optional[str] = None
    contract_reference: Optional[str] = None
    status: str = Field(default="proposed", pattern="^(proposed|negotiating|agreed|rejected|completed|canceled)$")
    forecast_snapshot: dict = Field(default_factory=dict)


@router.post("/deals")
async def create_deal(body: DealBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("analyst"))):
    standard = ctx.program.is_standard(CommissionTerms(body.commission_rate, body.commission_months) if body.commission_rate else None)
    needs_doc = (not standard) or body.bonus_threshold_customers is not None or body.kind != DealKind.AFFILIATE_ONLY
    if body.status == "agreed" and needs_doc and not body.contract_reference:
        raise HTTPException(422, "agreed deals with fees, bonuses or non-standard commission need a contract_reference")
    async with ctx.pool.acquire() as conn:
        deal_id = await conn.fetchval(
            """INSERT INTO rpi_deals (creator_id, structure, upfront_fee_usd, commission_rate, commission_months, bonus_terms,
                                      deliverables, status, requires_documented_agreement, contract_reference,
                                      forecast_snapshot, created_by)
               VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,$11::jsonb,$12) RETURNING id""",
            body.creator_id, body.kind.value, body.upfront_fee, body.commission_rate, body.commission_months,
            json.dumps({"threshold": str(body.bonus_threshold_customers) if body.bonus_threshold_customers is not None else None,
                        "per_customer_usd": str(body.bonus_per_customer_usd or 0), "flat_usd": str(body.bonus_flat_usd or 0),
                        "cap_usd": str(body.bonus_cap_usd) if body.bonus_cap_usd is not None else None}),
            body.deliverables, body.status, needs_doc, body.contract_reference, json.dumps(body.forecast_snapshot, default=str),
            p.actor)
        await conn.execute(
            "INSERT INTO rpi_notes (creator_id, kind, body, data, author) VALUES ($1, 'negotiation', $2, $3::jsonb, $4)",
            body.creator_id, f"Deal proposed: {body.kind.value}, fee ${body.upfront_fee}",
            json.dumps({"deal_id": str(deal_id)}), p.actor)
        await audit(conn, p.actor, "deal.created", "deal", deal_id, {"kind": body.kind.value, "fee": str(body.upfront_fee)})
    return {"id": str(deal_id), "requires_documented_agreement": needs_doc}


class DealUpdate(BaseModel):
    status: str = Field(pattern="^(proposed|negotiating|agreed|rejected|completed|canceled)$")
    contract_reference: Optional[str] = None
    note: Optional[str] = None


@router.patch("/deals/{deal_id}")
async def update_deal(deal_id: str, body: DealUpdate, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("approver"))):
    async with ctx.pool.acquire() as conn:
        async with conn.transaction():
            deal = await conn.fetchrow("SELECT * FROM rpi_deals WHERE id = $1 FOR UPDATE", deal_id)
            if not deal:
                raise HTTPException(404, "deal not found")
            ref = body.contract_reference or deal["contract_reference"]
            if body.status == "agreed" and deal["requires_documented_agreement"] and not ref:
                raise HTTPException(422, "this deal requires a documented agreement (contract_reference) before it is agreed")
            await conn.execute(
                "UPDATE rpi_deals SET status = $2, contract_reference = $3, updated_at = NOW() WHERE id = $1",
                deal_id, body.status, ref)
            if body.note:
                await conn.execute(
                    "INSERT INTO rpi_notes (creator_id, kind, body, data, author) VALUES ($1, 'negotiation', $2, $3::jsonb, $4)",
                    deal["creator_id"], body.note, json.dumps({"deal_id": deal_id, "status": body.status}), p.actor)
            await audit(conn, p.actor, "deal.updated", "deal", deal_id, {"status": body.status, "contract_reference": ref})
    return {"id": deal_id, "status": body.status}


@router.get("/deals")
async def list_deals(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer")), creator_id: Optional[str] = None):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM rpi_deals WHERE ($1::uuid IS NULL OR creator_id = $1) ORDER BY created_at DESC", creator_id)
    return clean([dict(r) | {"bonus_terms": jsonb(r["bonus_terms"]), "forecast_snapshot": jsonb(r["forecast_snapshot"])} for r in rows])


class CampaignBody(BaseModel):
    creator_id: str
    deal_id: Optional[str] = None
    name: str
    referral_code: Optional[str] = Field(default=None, pattern="^[A-Za-z0-9_-]{3,40}$")
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = "influencer"
    utm_campaign: Optional[str] = None
    landing_page: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    attribution_window_days: Optional[int] = Field(default=None, ge=1, le=365)


@router.post("/campaigns")
async def create_campaign(body: CampaignBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("approver"))):
    code = body.referral_code or ("rf" + secrets.token_hex(4))
    window = body.attribution_window_days or ctx.program.attribution_window_days
    async with ctx.pool.acquire() as conn:
        try:
            cid = await conn.fetchval(
                """INSERT INTO rpi_campaigns (creator_id, deal_id, name, referral_code, utm_source, utm_medium, utm_campaign,
                                              landing_page, start_date, end_date, attribution_window_days)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id""",
                body.creator_id, body.deal_id, body.name, code, body.utm_source, body.utm_medium,
                body.utm_campaign or code, body.landing_page, body.start_date, body.end_date, window)
        except asyncpg.UniqueViolationError:
            raise HTTPException(409, "referral code or utm_campaign already in use")
        await audit(conn, p.actor, "campaign.created", "campaign", cid, {"referral_code": code})
    return {"id": str(cid), "referral_code": code, "utm_campaign": body.utm_campaign or code, "attribution_window_days": window}


@router.get("/campaigns")
async def list_campaigns(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer"))):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT ca.*, cr.display_name FROM rpi_campaigns ca JOIN rpi_creators cr ON cr.id = ca.creator_id
               ORDER BY ca.created_at DESC""")
    return clean([dict(r) for r in rows])


class CampaignCostBody(BaseModel):
    entry_type: str = Field(pattern="^(influencer_fee|other_campaign_cost)$")
    amount: Decimal = Field(gt=0)
    currency: str = Field(default="usd", pattern="^[a-zA-Z]{3}$")
    occurred_on: date
    reference: str = Field(min_length=3, max_length=120, description="invoice or payment reference (idempotency)")


@router.post("/campaigns/{campaign_id}/costs")
async def campaign_cost(campaign_id: str, body: CampaignCostBody, ctx: RPIContext = Depends(get_ctx),
                        p: Principal = Depends(require("approver"))):
    from datetime import datetime, timezone

    minor = int((body.amount * 100).quantize(Decimal("1")))
    async with ctx.pool.acquire() as conn:
        async with conn.transaction():
            added = await add_entry(conn, entry_type=body.entry_type, amount_minor=-minor, currency=body.currency,
                                    occurred_at=datetime.combine(body.occurred_on, datetime.min.time(), tzinfo=timezone.utc),
                                    idempotency_key=f"campaign_cost:{campaign_id}:{body.reference}", source="operator",
                                    campaign_id=campaign_id)
            await audit(conn, p.actor, "campaign.cost_recorded", "campaign", campaign_id,
                        {"type": body.entry_type, "amount": str(body.amount), "reference": body.reference, "new": added})
    return {"recorded": added, "duplicate": not added}


# --------------------------------------------------------------------------
# CRM
# --------------------------------------------------------------------------


class TransitionBody(BaseModel):
    status: Status
    reason: Optional[str] = None


@router.post("/crm/creators/{creator_id}/transition")
async def crm_transition(creator_id: str, body: TransitionBody, ctx: RPIContext = Depends(get_ctx),
                         p: Principal = Depends(require("analyst"))):
    if body.status == Status.OUTREACH_APPROVED and not p.has("approver"):
        raise HTTPException(403, "approving outreach requires the approver role")
    async with ctx.pool.acquire() as conn:
        async with conn.transaction():
            try:
                return await transition(conn, creator_id, body.status, actor=p.actor, reason=body.reason, is_admin=p.has("admin"))
            except TransitionError as exc:
                raise HTTPException(409, str(exc))


@router.get("/crm/board")
async def crm_board(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer"))):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT c.id, c.display_name, c.pipeline_status, c.next_action, c.next_action_due, c.owner,
                      q.overall, q.recommendation
               FROM rpi_creators c LEFT JOIN LATERAL (SELECT overall, recommendation FROM rpi_qualification_scores z
                   WHERE z.creator_id = c.id ORDER BY computed_at DESC LIMIT 1) q ON TRUE
               WHERE c.merged_into_id IS NULL AND (c.pipeline_status <> 'discovered' OR q.recommendation IN ('recommend', 'investigate'))
               ORDER BY q.overall DESC NULLS LAST LIMIT 1000""")
    board: dict[str, list] = {s.value: [] for s in Status}
    for r in rows:
        board[r["pipeline_status"]].append(dict(r))
    return clean(board)


class NoteBody(BaseModel):
    kind: str = Field(default="note", pattern="^(note|sponsorship_request|negotiation|call|manual_analytics)$")
    body: str = Field(min_length=1, max_length=5000)
    data: dict = Field(default_factory=dict)
    next_action: Optional[str] = None
    next_action_due: Optional[date] = None


@router.post("/crm/creators/{creator_id}/notes")
async def add_note(creator_id: str, body: NoteBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("analyst"))):
    if body.kind == "sponsorship_request" and "fee_usd" in body.data:
        try:
            body.data["fee_usd"] = float(body.data["fee_usd"])
        except (TypeError, ValueError):
            raise HTTPException(422, "fee_usd must be a number")
    if body.kind == "manual_analytics":
        body.data["measurement"] = "manual_submitted"
    async with ctx.pool.acquire() as conn:
        nid = await conn.fetchval(
            "INSERT INTO rpi_notes (creator_id, kind, body, data, author) VALUES ($1,$2,$3,$4::jsonb,$5) RETURNING id",
            creator_id, body.kind, body.body, json.dumps(body.data, default=str), p.actor)
        if body.next_action or body.next_action_due:
            await conn.execute("UPDATE rpi_creators SET next_action = $2, next_action_due = $3, updated_at = NOW() WHERE id = $1",
                               creator_id, body.next_action, body.next_action_due)
    return {"id": str(nid)}


class SuppressionBody(BaseModel):
    kind: str = Field(pattern="^(email|domain)$")
    value: str = Field(min_length=3, max_length=320)
    reason: str = Field(min_length=3, max_length=200)


@router.post("/crm/suppression")
async def suppress(body: SuppressionBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("analyst"))):
    async with ctx.pool.acquire() as conn:
        await suppression.add(conn, body.kind, body.value, body.reason, p.actor)
        await audit(conn, p.actor, "suppression.added", "suppression", None, {"kind": body.kind, "reason": body.reason})
    return {"suppressed": True}


# --------------------------------------------------------------------------
# Outreach (drafts, approval, controlled sending)
# --------------------------------------------------------------------------


class DraftBody(BaseModel):
    proposition: str = Field(default="paid sponsorship", pattern="^(paid sponsorship|affiliate partnership)$")
    request_audience_data: bool = True
    contact_id: Optional[str] = None
    use_claude: bool = True


@router.post("/outreach/creators/{creator_id}/drafts")
async def create_draft(creator_id: str, body: DraftBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("analyst"))):
    async with ctx.pool.acquire() as conn:
        creator = await conn.fetchrow("SELECT display_name, pipeline_status FROM rpi_creators WHERE id = $1", creator_id)
        if not creator:
            raise HTTPException(404, "creator not found")
        if creator["pipeline_status"] in ("rejected", "do_not_contact"):
            raise HTTPException(409, f"creator is {creator['pipeline_status']}")
        settings_row = await conn.fetchrow("SELECT sender_name, sender_email FROM rpi_outreach_settings LIMIT 1")
        cls = await conn.fetchrow(
            "SELECT labels, product_capabilities, evidence FROM rpi_content_classifications WHERE creator_id = $1 "
            "AND content_id IS NULL ORDER BY created_at DESC LIMIT 1", creator_id)
        if not cls:
            raise HTTPException(409, "research the creator before drafting outreach (no classification yet)")
        evidence = jsonb(cls["evidence"]) or []
        refs = [e.get("ref") for e in evidence if e.get("ref")]
        videos = await conn.fetch(
            "SELECT id, title, url FROM rpi_creator_content WHERE creator_id = $1 AND id::text = ANY($2::text[])", creator_id, refs)
        if not videos:
            raise HTTPException(409, "no verified relevant videos to ground the message")
        contact = None
        if body.contact_id:
            contact = await conn.fetchrow("SELECT id FROM rpi_creator_contacts WHERE id = $1 AND creator_id = $2",
                                          body.contact_id, creator_id)
            if not contact:
                raise HTTPException(422, "contact does not belong to this creator")
        else:
            contact = await conn.fetchrow(
                "SELECT id FROM rpi_creator_contacts WHERE creator_id = $1 AND contact_type = 'email' ORDER BY is_business_contact DESC NULLS LAST, created_at LIMIT 1",
                creator_id)
        labels = jsonb(cls["labels"]) or {}
        deals = await creator_competitor_deals(conn, creator_id, now=datetime.now(timezone.utc))
        months = ctx.program.duration_months
        dctx = DraftContext(
            creator_id=creator_id, creator_name=creator["display_name"],
            videos=[{"ref": str(v["id"]), "title": v["title"], "url": v["url"]} for v in videos],
            primary_labels=labels.get("primary", []), capabilities=jsonb(cls["product_capabilities"]) or [],
            commission_rate_pct=str(int(ctx.program.rate * 100)),
            commission_duration_text=f"for {months} months" if months else "for the lifetime of referred customers",
            proposition=body.proposition, request_audience_data=body.request_audience_data,
            sender_name=(settings_row["sender_name"] if settings_row and settings_row["sender_name"] else "The ReelForge AI team"),
            competitor_deals=tuple(brand for brand, _ in deals),
            avoid_title_terms=tuple(n for row in await conn.fetch("SELECT name, aliases FROM rpi_competitors WHERE active")
                                    for n in (row["name"], *row["aliases"]) if n),
        )
        draft = await generate(ctx.llm if body.use_claude else None, ctx.fast_price, dctx,
                               sender_email=settings_row["sender_email"] if settings_row else None)
        async with conn.transaction():
            draft_id = await save_draft(conn, creator_id, contact["id"] if contact else None, draft, p.actor)
    return clean({"id": draft_id, **draft, "contact_id": str(contact["id"]) if contact else None})


class DraftEdit(BaseModel):
    subject: str = Field(min_length=1, max_length=120)
    body: str = Field(min_length=1, max_length=4000)


@router.put("/outreach/drafts/{draft_id}")
async def edit_draft(draft_id: str, body: DraftEdit, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("analyst"))):
    from rpi.outreach.drafts import regrounding_from_row
    from rpi.outreach.validator import validate_draft

    async with ctx.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM rpi_outreach_drafts WHERE id = $1", draft_id)
        if not row or row["status"] != "draft":
            raise HTTPException(409, "only drafts in 'draft' status can be edited")
        sender = await conn.fetchval("SELECT sender_email FROM rpi_outreach_settings LIMIT 1")
        check = validate_draft(body.subject, body.body, regrounding_from_row(dict(row), sender))
        await conn.execute("UPDATE rpi_outreach_drafts SET subject = $2, body_text = $3, validation = $4::jsonb WHERE id = $1",
                           draft_id, body.subject, body.body, json.dumps(check.as_dict()))
        await audit(conn, p.actor, "outreach.draft_edited", "outreach_draft", draft_id, {"valid": check.ok})
    return check.as_dict()


@router.post("/outreach/drafts/{draft_id}/approve")
async def approve_draft(draft_id: str, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("approver"))):
    async with ctx.pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("SELECT * FROM rpi_outreach_drafts WHERE id = $1 FOR UPDATE", draft_id)
            if not row or row["status"] != "draft":
                raise HTTPException(409, "draft not in 'draft' status")
            validation = jsonb(row["validation"]) or {}
            if not validation.get("ok"):
                raise HTTPException(422, "draft fails validation; edit it first: " + "; ".join(validation.get("errors", [])[:3]))
            if not row["contact_id"]:
                raise HTTPException(422, "draft has no contact; add a verified contact first")
            try:
                await conn.execute(
                    "UPDATE rpi_outreach_drafts SET status = 'approved', approved_by = $2, approved_at = NOW() WHERE id = $1",
                    draft_id, p.actor)
            except asyncpg.UniqueViolationError:
                raise HTTPException(409, "another message to this creator is already approved or sending")
            status = await conn.fetchval("SELECT pipeline_status FROM rpi_creators WHERE id = $1", row["creator_id"])
            if status == Status.RECOMMENDED.value:
                await transition(conn, row["creator_id"], Status.CONTACT_IDENTIFIED, actor=p.actor, reason="contact on file")
                status = Status.CONTACT_IDENTIFIED.value
            if status == Status.CONTACT_IDENTIFIED.value:
                await transition(conn, row["creator_id"], Status.OUTREACH_APPROVED, actor=p.actor, reason="draft approved")
            await audit(conn, p.actor, "outreach.approved", "outreach_draft", draft_id, {})
    return {"id": draft_id, "status": "approved"}


@router.post("/outreach/drafts/{draft_id}/reject")
async def reject_draft(draft_id: str, reason: str, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("approver"))):
    async with ctx.pool.acquire() as conn:
        res = await conn.execute(
            "UPDATE rpi_outreach_drafts SET status = 'rejected', failure_reason = $2 WHERE id = $1 AND status IN ('draft', 'approved')",
            draft_id, reason)
        await audit(conn, p.actor, "outreach.rejected", "outreach_draft", draft_id, {"reason": reason})
    return {"rejected": res.endswith("1")}


@router.post("/outreach/drafts/{draft_id}/send")
async def send(draft_id: str, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("approver"))):
    if ctx.send_fn is None:
        raise HTTPException(503, "no email provider configured")
    if not ctx.settings.rpi_public_base_url:
        raise HTTPException(503, "RPI_PUBLIC_BASE_URL must be set; it is used for working unsubscribe links")
    result = await send_draft(ctx.pool, draft_id, p.actor, ctx.send_fn, public_base_url=ctx.settings.rpi_public_base_url)
    if not result.get("sent"):
        raise HTTPException(403 if any("disabled" in r for r in result.get("reasons", [])) else 409, result)
    return result


@router.get("/outreach/drafts")
async def list_drafts(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer")), status: Optional[str] = None):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT d.*, c.display_name FROM rpi_outreach_drafts d JOIN rpi_creators c ON c.id = d.creator_id
               WHERE ($1::text IS NULL OR d.status = $1) ORDER BY d.created_at DESC LIMIT 200""", status)
    return clean([dict(r) | {"grounding": jsonb(r["grounding"]), "validation": jsonb(r["validation"])} for r in rows])


class OutreachSettingsBody(BaseModel):
    sending_enabled: bool
    daily_send_limit: int = Field(ge=0, le=200)
    min_days_between_contacts: int = Field(ge=1, le=365)
    sender_name: Optional[str] = None
    sender_email: Optional[str] = None
    postal_address: Optional[str] = None


@router.get("/outreach/settings")
async def get_outreach_settings(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer"))):
    async with ctx.pool.acquire() as conn:
        return clean(dict(await conn.fetchrow("SELECT * FROM rpi_outreach_settings LIMIT 1")))


@router.put("/outreach/settings")
async def put_outreach_settings(body: OutreachSettingsBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("admin"))):
    async with ctx.pool.acquire() as conn:
        await conn.execute(
            """UPDATE rpi_outreach_settings SET sending_enabled = $1, daily_send_limit = $2, min_days_between_contacts = $3,
                   sender_name = $4, sender_email = $5, postal_address = $6, updated_at = NOW(),
                   enabled_by = CASE WHEN $1 THEN $7 ELSE enabled_by END, enabled_at = CASE WHEN $1 THEN NOW() ELSE enabled_at END""",
            body.sending_enabled, body.daily_send_limit, body.min_days_between_contacts, body.sender_name, body.sender_email,
            body.postal_address, p.actor)
        await audit(conn, p.actor, "outreach.settings_changed", "outreach_settings", None, body.model_dump())
    return {"updated": True}
