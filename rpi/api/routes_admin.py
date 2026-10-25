"""Dashboard, providers, budgets, compliance, audit, principals, OAuth, public endpoints."""

from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from rpi import RPI_VERSION
from rpi.administration.auth import Principal, create_principal
from rpi.analytics.dashboard import executive, research_costs
from rpi.api.context import RPIContext
from rpi.api.deps import get_ctx, require
from rpi.api.serialize import clean, jsonb
from rpi.compliance import retention
from rpi.compliance.policies import POLICIES
from rpi.core.audit import audit
from rpi.crm import suppression
from rpi.outreach.sender import verify_unsubscribe_token
from rpi.providers import youtube_analytics

router = APIRouter()
public = APIRouter()


@router.get("/dashboard/executive")
async def dashboard(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer"))):
    async with ctx.pool.acquire() as conn:
        return clean(await executive(conn))


@router.get("/dashboard/costs")
async def costs(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer"))):
    async with ctx.pool.acquire() as conn:
        return clean(await research_costs(conn))


@router.get("/admin/providers")
async def providers(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("analyst"))):
    out = []
    async with ctx.pool.acquire() as conn:
        recent = {r["provider"]: dict(r) for r in await conn.fetch(
            """SELECT provider, COUNT(*) AS calls, COUNT(*) FILTER (WHERE status = 'error') AS errors,
                      COUNT(*) FILTER (WHERE status = 'budget_denied') AS denied, MAX(created_at) AS last_call,
                      ROUND(AVG(duration_ms)) AS avg_ms
               FROM rpi_provider_requests WHERE created_at > NOW() - INTERVAL '24 hours' GROUP BY provider""")}
    for item in ctx.registry.describe():
        adapter = ctx.registry.get(item["provider"])
        health = await adapter.health_check()
        policy = POLICIES.get(adapter.policy_key)
        out.append(item | {"health": asdict(health), "last_24h": recent.get(item["provider"]),
                           "policy_verified": policy.verified if policy else False})
    configured = {p["provider"] for p in out}
    for missing in ("creatordb", "sparktoro", "supadata", "youtube_data_api"):
        if missing not in configured:
            out.append({"provider": missing, "configured": False, "capabilities": [], "detail": "no API key configured"})
    out.append({"provider": "anthropic", "configured": bool(ctx.llm), "models": [ctx.fast_price.model, ctx.deep_price.model]})
    return clean({"version": RPI_VERSION, "providers": out})


@router.get("/admin/budgets")
async def budgets(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer"))):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT b.*, COALESCE((SELECT SUM(CASE WHEN l.status = 'committed' THEN l.actual_usd ELSE l.reserved_usd END)
                                      FROM rpi_spend_ledger l WHERE l.status IN ('committed', 'reserved')
                                        AND (b.scope = 'global' OR l.provider = b.provider)
                                        AND l.created_at >= CASE WHEN b.period = 'daily' THEN date_trunc('day', NOW())
                                                                 ELSE date_trunc('month', NOW()) END), 0) AS spent_usd
               FROM rpi_budgets b WHERE b.active ORDER BY b.scope, b.period, b.provider""")
        requests = await conn.fetch("SELECT * FROM rpi_budget_change_requests ORDER BY created_at DESC LIMIT 50")
    return clean({"budgets": [dict(r) for r in rows], "change_requests": [dict(r) for r in requests]})


class BudgetChange(BaseModel):
    requested_limit_usd: Decimal = Field(ge=0, le=100000)
    reason: str = Field(min_length=5, max_length=500)


@router.post("/admin/budgets/{budget_id}/change-requests")
async def request_change(budget_id: str, body: BudgetChange, ctx: RPIContext = Depends(get_ctx),
                         p: Principal = Depends(require("analyst"))):
    async with ctx.pool.acquire() as conn:
        current = await conn.fetchval("SELECT limit_usd FROM rpi_budgets WHERE id = $1 AND active", budget_id)
        if current is None:
            raise HTTPException(404, "budget not found")
        if body.requested_limit_usd <= current:
            # Decreases take effect immediately; they only reduce risk.
            await conn.execute("UPDATE rpi_budgets SET limit_usd = $2, updated_at = NOW() WHERE id = $1", budget_id,
                               body.requested_limit_usd)
            await audit(conn, p.actor, "budget.decreased", "budget", budget_id,
                        {"from": str(current), "to": str(body.requested_limit_usd), "reason": body.reason})
            return {"applied": True}
        rid = await conn.fetchval(
            """INSERT INTO rpi_budget_change_requests (budget_id, requested_limit_usd, reason, requested_by)
               VALUES ($1, $2, $3, $4) RETURNING id""", budget_id, body.requested_limit_usd, body.reason, p.actor)
        await audit(conn, p.actor, "budget.increase_requested", "budget", budget_id,
                    {"to": str(body.requested_limit_usd), "reason": body.reason})
    return {"applied": False, "request_id": str(rid), "status": "pending_approval"}


class BudgetDecision(BaseModel):
    approve: bool


@router.post("/admin/budget-requests/{request_id}")
async def decide_change(request_id: str, body: BudgetDecision, ctx: RPIContext = Depends(get_ctx),
                        p: Principal = Depends(require("admin"))):
    async with ctx.pool.acquire() as conn:
        async with conn.transaction():
            req = await conn.fetchrow("SELECT * FROM rpi_budget_change_requests WHERE id = $1 FOR UPDATE", request_id)
            if not req or req["status"] != "pending":
                raise HTTPException(409, "request not pending")
            if req["requested_by"] == p.actor:
                raise HTTPException(403, "a budget increase must be approved by someone other than the requester")
            await conn.execute(
                "UPDATE rpi_budget_change_requests SET status = $2, decided_by = $3, decided_at = NOW() WHERE id = $1",
                request_id, "approved" if body.approve else "rejected", p.actor)
            if body.approve:
                await conn.execute("UPDATE rpi_budgets SET limit_usd = $2, updated_at = NOW() WHERE id = $1",
                                   req["budget_id"], req["requested_limit_usd"])
            await audit(conn, p.actor, "budget.increase_" + ("approved" if body.approve else "rejected"), "budget",
                        req["budget_id"], {"request_id": request_id, "to": str(req["requested_limit_usd"])})
    return {"request_id": request_id, "approved": body.approve}


@router.get("/admin/spend")
async def spend(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("analyst")), limit: int = 200):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM rpi_spend_ledger ORDER BY created_at DESC LIMIT $1", min(limit, 1000))
    return clean([dict(r) for r in rows])


@router.get("/admin/requests")
async def provider_requests(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("analyst")), limit: int = 200):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM rpi_provider_requests ORDER BY created_at DESC LIMIT $1", min(limit, 1000))
    return clean([dict(r) for r in rows])


@router.post("/admin/alerts/{alert_id}/ack")
async def ack(alert_id: int, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("analyst"))):
    async with ctx.pool.acquire() as conn:
        await conn.execute("UPDATE rpi_alerts SET acknowledged_at = NOW() WHERE id = $1", alert_id)
    return {"acknowledged": alert_id}


@router.get("/admin/audit")
async def audit_log(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("admin")),
                    entity_type: Optional[str] = None, entity_id: Optional[str] = None, limit: int = 200):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM rpi_audit_log WHERE ($1::text IS NULL OR entity_type = $1) AND ($2::text IS NULL OR entity_id = $2)
               ORDER BY created_at DESC LIMIT $3""", entity_type, entity_id, min(limit, 1000))
    return clean([dict(r) | {"detail": jsonb(r["detail"])} for r in rows])


@router.get("/admin/compliance")
async def compliance(_: Principal = Depends(require("viewer"))):
    return clean({k: asdict(v) for k, v in POLICIES.items()})


@router.post("/admin/retention/run")
async def run_retention(dry_run: bool = True, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("admin"))):
    async with ctx.pool.acquire() as conn:
        result = await retention.enforce(conn, dry_run=dry_run)
    return clean(result)


class PrincipalBody(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    role: str = Field(pattern="^(viewer|analyst|approver|admin|ingest)$")


@router.post("/admin/principals")
async def new_principal(body: PrincipalBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("admin"))):
    async with ctx.pool.acquire() as conn:
        pid, key = await create_principal(conn, body.name, body.role)
        await audit(conn, p.actor, "principal.created", "principal", pid, {"name": body.name, "role": body.role})
    return {"id": pid, "key": key, "note": "Store this key now; it is not retrievable later."}


@router.get("/admin/principals")
async def principals(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("admin"))):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, role, key_prefix, active, created_at, last_used_at FROM rpi_principals ORDER BY created_at")
    return clean([dict(r) for r in rows])


@router.delete("/admin/principals/{principal_id}")
async def revoke_principal(principal_id: str, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("admin"))):
    async with ctx.pool.acquire() as conn:
        await conn.execute("UPDATE rpi_principals SET active = FALSE WHERE id = $1", principal_id)
        await audit(conn, p.actor, "principal.revoked", "principal", principal_id, {})
    return {"revoked": principal_id}


# --------------------------------------------------------------------------
# Creator-authorized analytics (disabled by default)
# --------------------------------------------------------------------------


@router.post("/creators/{creator_id}/analytics/consent-url")
async def analytics_consent(creator_id: str, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("approver"))):
    try:
        async with ctx.pool.acquire() as conn:
            url = await youtube_analytics.consent_url(conn, ctx.settings, creator_id)
    except youtube_analytics.AnalyticsDisabled as exc:
        raise HTTPException(503, str(exc))
    return {"url": url, "expires_in_minutes": 30}


@router.post("/creators/{creator_id}/analytics/revoke")
async def analytics_revoke(creator_id: str, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("approver"))):
    async with ctx.pool.acquire() as conn:
        result = await youtube_analytics.revoke(conn, ctx.settings, creator_id, source=p.actor)
        await audit(conn, p.actor, "oauth.revoked", "creator", creator_id, result)
    return clean(result)


@public.get("/oauth/google/callback", response_class=HTMLResponse, include_in_schema=False)
async def oauth_callback(code: str, state: str, ctx: RPIContext = Depends(get_ctx)):
    try:
        async with ctx.pool.acquire() as conn:
            await youtube_analytics.handle_callback(conn, ctx.settings, code, state)
    except (youtube_analytics.AnalyticsDisabled, PermissionError):
        return HTMLResponse("<p>Authorization could not be completed. You can close this window.</p>", status_code=400)
    return HTMLResponse("<p>Thanks — access granted. You can revoke it at any time from your Google account "
                        "or by replying to our email.</p>")


@public.get("/unsubscribe/{draft_id}", response_class=HTMLResponse, include_in_schema=False)
async def unsubscribe(draft_id: str, t: str, ctx: RPIContext = Depends(get_ctx)):
    if not verify_unsubscribe_token(draft_id, t):
        return HTMLResponse("<p>This link is not valid.</p>", status_code=400)
    async with ctx.pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """SELECT d.creator_id, c.value_normalized FROM rpi_outreach_drafts d
                   LEFT JOIN rpi_creator_contacts c ON c.id = d.contact_id WHERE d.id = $1""", draft_id)
            if row:
                if row["value_normalized"]:
                    await suppression.add(conn, "email", row["value_normalized"], "unsubscribe link", "recipient")
                await suppression.suppress_creator(conn, row["creator_id"], "unsubscribe link", "recipient")
                await conn.execute(
                    "INSERT INTO rpi_outreach_events (draft_id, creator_id, event_type) VALUES ($1, $2, 'unsubscribed')",
                    draft_id, row["creator_id"])
                await audit(conn, "recipient", "outreach.unsubscribed", "creator", row["creator_id"], {})
    return HTMLResponse("<p>You're unsubscribed. ReelForge AI will not contact you again.</p>")
