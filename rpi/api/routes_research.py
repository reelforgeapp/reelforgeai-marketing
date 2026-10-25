"""Discovery, creators, research jobs, identity review, competitors, ICPs."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from rpi.administration.auth import Principal
from rpi.administration.jobs import create_job, run_job
from rpi.api.context import RPIContext
from rpi.api.deps import get_ctx, require
from rpi.api.serialize import clean, jsonb
from rpi.audience_intelligence.fit import DEFAULT_ICPS
from rpi.core.audit import audit
from rpi.discovery.engine import STRATEGIES, DiscoveryRequest
from rpi.discovery.presets import DEFAULT_PRESET, PRESETS, apply_preset
from rpi.providers.base import CreatorSearchCriteria
from rpi.providers.gateway import CallContext
from rpi.storage.creators import creator_profile, delete_creator

router = APIRouter()


class DiscoveryBody(BaseModel):
    criteria: CreatorSearchCriteria = Field(default_factory=CreatorSearchCriteria)
    # Fills every criteria field not set explicitly (rpi/discovery/presets.py); "none" disables.
    preset: Optional[str] = DEFAULT_PRESET
    strategies: list[str] = Field(default_factory=lambda: ["niche"])
    max_candidates: int = Field(default=25, ge=1, le=5000)
    audience_query: Optional[str] = None
    competitor_names: list[str] = Field(default_factory=list)
    budget_usd: Optional[Decimal] = Field(default=None, ge=0)
    inline: bool = False


@router.post("/discovery/estimate")
async def discovery_estimate(body: DiscoveryBody, ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("analyst"))):
    req = DiscoveryRequest(_criteria(body), body.strategies, body.max_candidates, body.audience_query, body.competitor_names)
    return clean(ctx.discovery.estimate(req) | {"preset": body.preset, "criteria": req.criteria.model_dump(mode="json")})


def _criteria(body: DiscoveryBody) -> CreatorSearchCriteria:
    try:
        return apply_preset(body.criteria, body.preset)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.get("/discovery/presets")
async def discovery_presets(_: Principal = Depends(require("viewer"))):
    return {"default": DEFAULT_PRESET, "presets": PRESETS}


@router.post("/discovery/runs")
async def discovery_run(body: DiscoveryBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("analyst"))):
    bad = [s for s in body.strategies if s not in STRATEGIES]
    if bad:
        raise HTTPException(422, f"unknown strategies {bad}; valid: {list(STRATEGIES)}")
    budget = body.budget_usd if body.budget_usd is not None else Decimal(str(ctx.settings.rpi_default_job_budget_usd))
    criteria = _criteria(body)
    async with ctx.pool.acquire() as conn:
        job_id = await create_job(conn, "discovery", {
            "criteria": criteria.model_dump(mode="json"), "preset": body.preset, "strategies": body.strategies,
            "max_candidates": body.max_candidates, "audience_query": body.audience_query,
            "competitor_names": body.competitor_names}, p.actor, budget_usd=budget)
    if body.inline:
        return clean(await run_job(ctx.pool, job_id, discovery=ctx.discovery, research=ctx.research, stages=ctx.stages))
    from tasks.rpi_tasks import run_rpi_job

    run_rpi_job.delay(job_id)
    return {"job_id": job_id, "status": "queued"}


@router.get("/discovery/runs")
async def discovery_runs(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer")), limit: int = 50):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM rpi_discovery_runs ORDER BY created_at DESC LIMIT $1", min(limit, 200))
    return clean([dict(r) | {k: jsonb(r[k]) for k in ("criteria", "niche_distribution", "strategy_breakdown", "warnings")} for r in rows])


@router.get("/discovery/runs/{run_id}")
async def discovery_run_detail(run_id: str, ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer"))):
    async with ctx.pool.acquire() as conn:
        run = await conn.fetchrow("SELECT * FROM rpi_discovery_runs WHERE id = $1", run_id)
        if not run:
            raise HTTPException(404, "run not found")
        cands = await conn.fetch(
            """SELECT d.*, c.display_name, c.pipeline_status, a.platform_account_id, a.url, snap.subscribers
               FROM rpi_discovery_candidates d JOIN rpi_creators c ON c.id = d.creator_id
               LEFT JOIN LATERAL (SELECT platform_account_id, url FROM rpi_creator_social_accounts s WHERE s.creator_id = c.id LIMIT 1) a ON TRUE
               LEFT JOIN LATERAL (SELECT subscribers FROM rpi_creator_profile_snapshots p WHERE p.creator_id = c.id
                                  ORDER BY p.observed_at DESC LIMIT 1) snap ON TRUE
               WHERE d.run_id = $1 ORDER BY d.preliminary_score DESC NULLS LAST LIMIT 1000""", run_id)
    return clean({"run": dict(run) | {k: jsonb(run[k]) for k in ("criteria", "niche_distribution", "strategy_breakdown", "warnings")},
                  "candidates": [dict(c) | {"inclusion_reasons": jsonb(c["inclusion_reasons"]),
                                            "missing_fields": jsonb(c["missing_fields"])} for c in cands]})


class SavedSearchBody(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    criteria: CreatorSearchCriteria
    strategies: list[str]


@router.post("/discovery/saved-searches")
async def save_search(body: SavedSearchBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("analyst"))):
    async with ctx.pool.acquire() as conn:
        sid = await conn.fetchval(
            "INSERT INTO rpi_saved_searches (name, criteria, strategies, created_by) VALUES ($1, $2::jsonb, $3, $4) RETURNING id",
            body.name, body.criteria.model_dump_json(), body.strategies, p.actor)
    return {"id": str(sid)}


@router.get("/discovery/saved-searches")
async def list_saved(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer"))):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM rpi_saved_searches ORDER BY created_at DESC")
    return clean([dict(r) | {"criteria": jsonb(r["criteria"])} for r in rows])


# --------------------------------------------------------------------------
# Creators
# --------------------------------------------------------------------------


@router.get("/creators")
async def list_creators(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer")),
                        status: Optional[str] = None, recommendation: Optional[str] = None,
                        q: Optional[str] = None, limit: int = Query(100, le=500), offset: int = 0):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT c.id, c.display_name, c.pipeline_status, c.funnel_stage, c.country, c.disqualified_reason,
                      c.next_action, c.next_action_due, a.platform_account_id, a.handle, a.url,
                      s.subscribers, s.provider AS snapshot_provider, q.overall, q.confidence, q.recommendation,
                      q.niche_relevance, q.risk_level
               FROM rpi_creators c
               LEFT JOIN LATERAL (SELECT platform_account_id, handle, url FROM rpi_creator_social_accounts x
                                  WHERE x.creator_id = c.id ORDER BY first_seen_at LIMIT 1) a ON TRUE
               LEFT JOIN LATERAL (SELECT subscribers, provider FROM rpi_creator_profile_snapshots y
                                  WHERE y.creator_id = c.id ORDER BY observed_at DESC LIMIT 1) s ON TRUE
               LEFT JOIN LATERAL (SELECT overall, confidence, recommendation, niche_relevance, risk_level
                                  FROM rpi_qualification_scores z WHERE z.creator_id = c.id ORDER BY computed_at DESC LIMIT 1) q ON TRUE
               WHERE c.merged_into_id IS NULL
                 AND ($1::text IS NULL OR c.pipeline_status = $1)
                 AND ($2::text IS NULL OR q.recommendation = $2)
                 AND ($3::text IS NULL OR c.display_name ILIKE '%' || $3 || '%' OR a.handle ILIKE '%' || $3 || '%')
               ORDER BY q.overall DESC NULLS LAST, c.created_at DESC
               LIMIT $4 OFFSET $5""",
            status, recommendation, q, limit, offset)
    return clean([dict(r) for r in rows])


@router.get("/creators/compare")
async def compare(ids: str, ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer"))):
    out = []
    async with ctx.pool.acquire() as conn:
        for cid in [i for i in ids.split(",") if i][:6]:
            prof = await creator_profile(conn, cid)
            if not prof:
                continue
            q = prof["qualification"] or {}
            snap = prof["snapshots"][0] if prof["snapshots"] else {}
            out.append({
                "creator_id": cid, "name": prof["creator"]["display_name"], "status": prof["creator"]["pipeline_status"],
                "subscribers": snap.get("subscribers"), "subscribers_source": snap.get("provider"),
                "niche_relevance": q.get("niche_relevance"), "audience_fit": jsonb(q.get("audience_fit")),
                "risk_level": q.get("risk_level"), "overall": q.get("overall"), "confidence": q.get("confidence"),
                "recommendation": q.get("recommendation"), "missing_data": jsonb(q.get("missing_data")),
                "relevant_videos": [c for c in prof["content"]][:3],
                "sponsorships": [{"brand": s["brand_name"], "type": s["relationship_type"]} for s in prof["sponsorships"]][:6],
                "requested_fee_notes": [n["data"] for n in prof["notes"] if n["kind"] == "sponsorship_request"][:1],
            })
    return clean(out)


@router.get("/creators/{creator_id}")
async def get_creator(creator_id: str, ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer"))):
    async with ctx.pool.acquire() as conn:
        prof = await creator_profile(conn, creator_id)
    if not prof:
        raise HTTPException(404, "creator not found")
    for key in ("classification", "qualification"):
        if prof[key]:
            prof[key] = {k: jsonb(v) for k, v in prof[key].items()}
    prof["sponsorships"] = [s | {"evidence": jsonb(s["evidence"])} for s in prof["sponsorships"]]
    prof["identity_matches"] = [m | {"signals": jsonb(m["signals"])} for m in prof["identity_matches"]]
    prof["notes"] = [n | {"data": jsonb(n["data"])} for n in prof["notes"]]
    prof["data_labels"] = {
        "observed": "public values read from a provider",
        "third_party_modeled": "provider estimates (not measurements)",
        "content_inferred": "RPI classification of creator content",
        "manual_submitted": "creator-sent figures, unverified",
        "fixture": "SYNTHETIC development data",
    }
    return clean(prof)


@router.delete("/creators/{creator_id}")
async def remove_creator(creator_id: str, reason: str, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("admin"))):
    async with ctx.pool.acquire() as conn:
        async with conn.transaction():
            try:
                result = await delete_creator(conn, creator_id, p.actor, reason)
            except ValueError as exc:
                raise HTTPException(409, str(exc))
            await audit(conn, p.actor, "creator.deleted", "creator", creator_id, {"reason": reason})
    return clean(result)


class ResearchBody(BaseModel):
    creator_ids: list[str] = Field(min_length=1, max_length=500)
    stage: str = Field(pattern="^(screen|deep)$")
    inline: bool = False
    budget_usd: Optional[Decimal] = Field(default=None, ge=0)


@router.post("/research")
async def research(body: ResearchBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("analyst"))):
    budget = body.budget_usd if body.budget_usd is not None else Decimal(str(ctx.settings.rpi_default_job_budget_usd))
    async with ctx.pool.acquire() as conn:
        job_id = await create_job(conn, body.stage, {"creator_ids": body.creator_ids}, p.actor, budget_usd=budget)
    if body.inline:
        return clean(await run_job(ctx.pool, job_id, discovery=ctx.discovery, research=ctx.research, stages=ctx.stages))
    from tasks.rpi_tasks import run_rpi_job

    run_rpi_job.delay(job_id)
    return {"job_id": job_id, "status": "queued"}


class FunnelBody(BaseModel):
    screen: Optional[int] = Field(default=None, ge=1, le=2000)
    analyze: Optional[int] = Field(default=None, ge=1, le=500)
    budget_usd: Optional[Decimal] = Field(default=None, ge=0)
    priority: str = Field(default="normal", pattern="^(low|normal|high)$")
    inline: bool = False


@router.post("/research/funnel")
async def funnel(body: FunnelBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("analyst"))):
    params = {k: v for k, v in {"screen": body.screen, "analyze": body.analyze}.items() if v}
    budget = body.budget_usd if body.budget_usd is not None else Decimal(str(ctx.settings.rpi_default_job_budget_usd))
    async with ctx.pool.acquire() as conn:
        job_id = await create_job(conn, "funnel", params, p.actor, priority=body.priority, budget_usd=budget)
    if body.inline:
        return clean(await run_job(ctx.pool, job_id, discovery=ctx.discovery, research=ctx.research, stages=ctx.stages))
    from tasks.rpi_tasks import run_rpi_job

    run_rpi_job.delay(job_id)
    return {"job_id": job_id, "status": "queued"}


@router.get("/jobs")
async def jobs(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer")), limit: int = 50):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM rpi_research_jobs ORDER BY created_at DESC LIMIT $1", min(limit, 200))
    return clean([dict(r) | {"params": jsonb(r["params"]), "progress": jsonb(r["progress"])} for r in rows])


@router.post("/jobs/{job_id}/resume")
async def resume(job_id: str, inline: bool = False, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("analyst"))):
    if inline:
        return clean(await run_job(ctx.pool, job_id, discovery=ctx.discovery, research=ctx.research, stages=ctx.stages))
    from tasks.rpi_tasks import run_rpi_job

    run_rpi_job.delay(job_id)
    return {"job_id": job_id, "status": "queued"}


# --------------------------------------------------------------------------
# Identity review
# --------------------------------------------------------------------------


@router.get("/identity/matches")
async def identity_matches(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer")),
                           decision: str = "pending_review"):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT m.*, a.display_name AS name_a, b.display_name AS name_b FROM rpi_creator_identity_matches m
               JOIN rpi_creators a ON a.id = m.creator_a JOIN rpi_creators b ON b.id = m.creator_b
               WHERE m.decision = $1 ORDER BY m.confidence DESC""", decision)
    return clean([dict(r) | {"signals": jsonb(r["signals"])} for r in rows])


class MatchDecisionBody(BaseModel):
    decision: str = Field(pattern="^(confirmed|rejected)$")


@router.post("/identity/matches/{match_id}")
async def decide_match(match_id: str, body: MatchDecisionBody, ctx: RPIContext = Depends(get_ctx),
                       p: Principal = Depends(require("analyst"))):
    async with ctx.pool.acquire() as conn:
        async with conn.transaction():
            m = await conn.fetchrow("SELECT * FROM rpi_creator_identity_matches WHERE id = $1 FOR UPDATE", match_id)
            if not m or m["decision"] != "pending_review":
                raise HTTPException(409, "match not pending review")
            await conn.execute(
                "UPDATE rpi_creator_identity_matches SET decision = $2, reviewed_by = $3, reviewed_at = NOW() WHERE id = $1",
                match_id, body.decision, p.actor)
            if body.decision == "confirmed":
                # Merge b into a: accounts move, b is marked merged.
                await conn.execute("UPDATE rpi_creator_social_accounts SET creator_id = $1 WHERE creator_id = $2",
                                   m["creator_a"], m["creator_b"])
                await conn.execute("UPDATE rpi_creators SET merged_into_id = $1, updated_at = NOW() WHERE id = $2",
                                   m["creator_a"], m["creator_b"])
            await audit(conn, p.actor, "identity.decision", "identity_match", match_id, {"decision": body.decision})
    return {"match_id": match_id, "decision": body.decision}


# --------------------------------------------------------------------------
# Competitor registry and ICPs
# --------------------------------------------------------------------------


@router.get("/competitors")
async def competitors(ctx: RPIContext = Depends(get_ctx), _: Principal = Depends(require("viewer"))):
    async with ctx.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM rpi_competitors ORDER BY overlap, name")
    return clean([dict(r) for r in rows])


class CompetitorBody(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    domains: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    category: str
    overlap: str = Field(default="adjacent", pattern="^(direct|adjacent|complementary)$")
    active: bool = True


@router.put("/competitors")
async def upsert_competitor(body: CompetitorBody, ctx: RPIContext = Depends(get_ctx), p: Principal = Depends(require("analyst"))):
    async with ctx.pool.acquire() as conn:
        cid = await conn.fetchval(
            """INSERT INTO rpi_competitors (name, domains, aliases, category, overlap, active) VALUES ($1,$2,$3,$4,$5,$6)
               ON CONFLICT (name) DO UPDATE SET domains = EXCLUDED.domains, aliases = EXCLUDED.aliases,
                 category = EXCLUDED.category, overlap = EXCLUDED.overlap, active = EXCLUDED.active RETURNING id""",
            body.name, [d.lower() for d in body.domains], body.aliases, body.category, body.overlap, body.active)
        await audit(conn, p.actor, "competitor.upsert", "competitor", cid, body.model_dump())
    return {"id": str(cid)}


@router.get("/icps")
async def icps(_: Principal = Depends(require("viewer"))):
    return clean([{"key": i.key, "name": i.name, "description": i.description, "content_labels": i.content_labels,
                   "audience_problems": i.audience_problems, "countries": i.countries, "age_buckets": i.age_buckets,
                   "weights": i.weights} for i in DEFAULT_ICPS])
