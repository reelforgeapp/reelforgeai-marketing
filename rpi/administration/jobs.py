"""
Traceable, recoverable research jobs.

A job row records type, parameters, budget, progress and status. Workers run
jobs idempotently: progress lists the creator ids already processed, so a
failed or budget-paused job resumes where it stopped instead of paying for
the same work twice. A job whose next paid call is refused by the budget
controller moves to `paused_budget`; resuming requires either budget
headroom or an approved budget increase.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

import structlog

from rpi.administration.budget import Priority
from rpi.core.audit import audit
from rpi.providers.base import CreatorSearchCriteria
from rpi.providers.gateway import CallContext

logger = structlog.get_logger()

JOB_TYPES = ("discovery", "funnel", "screen", "deep", "autopilot")


@dataclass
class StageSizes:
    discover: int = 100
    screen: int = 25
    analyze: int = 10
    qualify: int = 10
    recommend: int = 5


async def create_job(conn, job_type: str, params: dict, actor: str, *, priority: str = "normal",
                     budget_usd: Optional[Decimal] = None) -> str:
    if job_type not in JOB_TYPES:
        raise ValueError(f"unknown job type {job_type}")
    job_id = await conn.fetchval(
        """INSERT INTO rpi_research_jobs (job_type, params, priority, budget_usd, created_by)
           VALUES ($1, $2::jsonb, $3, $4, $5) RETURNING id""",
        job_type, json.dumps(params, default=str), priority, budget_usd, actor,
    )
    await audit(conn, actor, "job.created", "job", job_id, {"type": job_type, "budget_usd": str(budget_usd)})
    return str(job_id)


async def _set(pool, job_id, **fields) -> None:
    sets, args = [], [job_id]
    for k, v in fields.items():
        args.append(json.dumps(v, default=str) if k in ("progress",) else v)
        cast = "::jsonb" if k == "progress" else ""
        sets.append(f"{k} = ${len(args)}{cast}")
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE rpi_research_jobs SET {', '.join(sets)} WHERE id = $1", *args)


async def run_job(pool, job_id: str, *, discovery, research, stages: StageSizes, autopilot: bool = False) -> dict:
    async with pool.acquire() as conn:
        job = await conn.fetchrow("SELECT * FROM rpi_research_jobs WHERE id = $1", job_id)
        if not job:
            raise ValueError("job not found")
        if job["job_type"] == "autopilot" and not autopilot:
            # Its credit cap was computed for one cycle; a later resume could
            # pass the weekly allowance or run while the autopilot is off.
            return {"job_id": job_id, "status": job["status"],
                    "detail": "autopilot jobs run only inside an autopilot cycle; run the autopilot instead"}
        claimed = await conn.fetchval(
            """UPDATE rpi_research_jobs SET status = 'running', attempts = attempts + 1, started_at = COALESCE(started_at, NOW())
               WHERE id = $1 AND status IN ('queued', 'paused_budget', 'failed') RETURNING id""",
            job_id,
        )
    if not claimed:
        return {"job_id": job_id, "status": job["status"], "detail": "job not runnable in its current status"}

    params = job["params"] if isinstance(job["params"], dict) else json.loads(job["params"])
    progress = job["progress"] if isinstance(job["progress"], dict) else json.loads(job["progress"] or "{}")
    priority = Priority(job["priority"])
    budget = Decimal(job["budget_usd"]) if job["budget_usd"] is not None else None
    caps = {k: Decimal(str(v)) for k, v in (params.get("unit_caps") or {}).items()}
    ctx = CallContext(job_id=job_id, priority=priority, job_budget_usd=budget, unit_caps=caps)
    try:
        if job["job_type"] == "discovery":
            from rpi.discovery.engine import DiscoveryRequest

            req = DiscoveryRequest(
                criteria=CreatorSearchCriteria(**params.get("criteria", {})),
                strategies=params.get("strategies", ["niche"]),
                max_candidates=int(params.get("max_candidates", stages.discover)),
                audience_query=params.get("audience_query"),
                competitor_names=params.get("competitor_names", []),
                job_id=job_id, job_budget_usd=budget,
            )
            result = await discovery.run(req, job["created_by"])
            status = "paused_budget" if any("budget" in w for w in result.get("warnings", [])) else "succeeded"
            progress = {"discovery_run_id": result["run_id"], "summary": {k: result[k] for k in ("total_found", "new_creators", "preliminary_qualified", "non_english")}}
        elif job["job_type"] in ("screen", "deep"):
            ids = params.get("creator_ids", [])
            status, progress = await _process(pool, research, job["job_type"], ids, ctx, progress, job["created_by"], job_id)
        elif job["job_type"] == "autopilot":
            status, progress = await _autopilot(pool, discovery, research, ctx, progress, stages, job["created_by"], job_id,
                                                params)
        else:  # funnel
            status, progress = await _funnel(pool, research, ctx, progress, stages, job["created_by"], job_id, params)
        spent = await _spent(pool, job_id)
        await _set(pool, job_id, status=status, progress=progress, spent_usd=spent,
                   finished_at=None if status == "paused_budget" else _now())
        return {"job_id": job_id, "status": status, "progress": progress, "spent_usd": str(spent)}
    except Exception as exc:
        logger.error("rpi_job_failed", job_id=job_id, error=str(exc))
        # Progress is already saved after each creator; writing this run's starting
        # copy back would lose it and make a resume pay for the same work again.
        await _set(pool, job_id, status="failed", error=str(exc)[:1000])
        raise


async def _process(pool, research, kind, ids, ctx, progress, actor, job_id, stop=None):
    done = set(progress.get("done", []))
    results = progress.get("results", {})
    for cid in ids:
        if cid in done:
            continue
        if stop is not None and await stop():
            return "stopped", {"done": sorted(done), "results": results}
        r = await (research.screen(cid, ctx, actor) if kind == "screen" else research.deep(cid, ctx, actor))
        results[cid] = (r.get("qualification") or {}).get("recommendation") or r.get("error")
        done.add(cid)
        progress = {"done": sorted(done), "results": results}
        await _set(pool, job_id, progress=progress)
        if any("budget" in n for n in r.get("notes", [])):
            return "paused_budget", progress
    return "succeeded", progress


# Creators an RPI discovery run found and preliminarily qualified, not yet
# screened. Creators imported from Partner Scout (or added by hand) have no
# discovery candidate row and are screened only when selected explicitly.
SCREEN_QUEUE_SQL = """
    FROM rpi_creators c
    JOIN LATERAL (SELECT MAX(preliminary_score) AS s FROM rpi_discovery_candidates d
                  WHERE d.creator_id = c.id AND d.preliminary_qualified) d ON d.s IS NOT NULL
    WHERE c.pipeline_status = 'discovered' AND c.disqualified_reason IS NULL
      AND NOT EXISTS (SELECT 1 FROM rpi_qualification_scores q WHERE q.creator_id = c.id)"""


async def _funnel(pool, research, ctx, progress, stages: StageSizes, actor, job_id, params, stop=None):
    """Stage 2 screens the best stage-1 candidates; stage 3-4 deep-analyzes
    the best screened creators. Sizes are configurable per job."""
    screen_n = int(params.get("screen", stages.screen))
    analyze_n = int(params.get("analyze", stages.analyze))
    async with pool.acquire() as conn:
        to_screen = await conn.fetch(
            f"SELECT c.id {SCREEN_QUEUE_SQL} ORDER BY d.s DESC, c.created_at LIMIT $1", screen_n)
    status, p2 = await _process(pool, research, "screen", [str(r["id"]) for r in to_screen], ctx,
                                progress.get("screen", {}), actor, job_id, stop)
    progress = {**progress, "screen": p2}
    await _set(pool, job_id, progress=progress)
    if status in ("paused_budget", "stopped"):
        return status, progress
    async with pool.acquire() as conn:
        to_deep = await conn.fetch(
            """SELECT DISTINCT ON (q.creator_id) q.creator_id, q.overall FROM rpi_qualification_scores q
               JOIN rpi_creators c ON c.id = q.creator_id
               WHERE c.pipeline_status = 'preliminary_qualified' AND c.disqualified_reason IS NULL
               ORDER BY q.creator_id, q.computed_at DESC""")
    ranked = sorted(to_deep, key=lambda r: r["overall"] or 0, reverse=True)[:analyze_n]
    status, p3 = await _process(pool, research, "deep", [str(r["creator_id"]) for r in ranked], ctx,
                                progress.get("deep", {}), actor, job_id, stop)
    progress = {**progress, "deep": p3}
    return status, progress


async def _autopilot(pool, discovery, research, ctx, progress, stages: StageSizes, actor, job_id, params):
    """Autopilot research (rpi.administration.autopilot): one discovery page
    when too few qualified candidates are waiting, then the funnel. Stopping
    at a spending cap ends the job normally; the next run gets a fresh allowance."""
    from rpi.discovery.engine import DiscoveryRequest
    from rpi.discovery.presets import apply_preset

    screen_n = int(params.get("screen", stages.screen))

    async def stop() -> bool:  # turning the autopilot off stops its research between creators
        async with pool.acquire() as conn:
            return not await conn.fetchval("SELECT enabled FROM rpi_autopilot_settings LIMIT 1")

    if await stop():
        return "succeeded", {**progress, "stopped": "autopilot turned off"}
    if "discovery" not in progress:
        async with pool.acquire() as conn:
            waiting = await conn.fetchval(f"SELECT COUNT(*) {SCREEN_QUEUE_SQL}")
        if waiting >= screen_n or not params.get("preset"):
            progress = {**progress, "discovery": {"skipped": f"{waiting} qualified candidate(s) already waiting"
                                                  if params.get("preset") else "no discovery preset left"}}
        else:
            criteria = apply_preset(CreatorSearchCriteria(page_token=params.get("page_token")), params["preset"])
            result = await discovery.run(DiscoveryRequest(
                criteria=criteria, strategies=["niche"], max_candidates=int(params.get("discover", stages.discover)),
                job_id=job_id, job_budget_usd=ctx.job_budget_usd, unit_caps=dict(ctx.unit_caps)), actor)
            tokens = result.get("next_page_tokens", {})
            progress = {**progress, "discovery": {
                "run_id": result["run_id"], "preset": params["preset"], "page_token": params.get("page_token"),
                "searched": "niche" in tokens, "next_page_token": tokens.get("niche"),
                **{k: result[k] for k in ("total_found", "new_creators", "preliminary_qualified", "non_english")},
                "warnings": result.get("warnings", [])}}
        await _set(pool, job_id, progress=progress)
        if any("budget" in w for w in progress["discovery"].get("warnings", [])):
            return "succeeded", {**progress, "stopped": "spending cap reached during discovery"}
    status, progress = await _funnel(pool, research, ctx, progress, stages, actor, job_id, params, stop)
    if status == "succeeded":
        # Recommended creators whose contact lookup an earlier cap refused (or a
        # provider error interrupted) get it now; a lookup that found nothing is not repeated.
        async with pool.acquire() as conn:
            retry = await conn.fetch(CONTACT_RETRY_SQL, int(params.get("analyze", stages.analyze)))
        status, p4 = await _process(pool, research, "deep", [str(r["id"]) for r in retry], ctx,
                                    progress.get("contacts", {}), actor, job_id, stop)
        progress = {**progress, "contacts": p4}
    if status == "paused_budget":
        return "succeeded", {**progress, "stopped": "spending cap reached"}
    if status == "stopped":
        return "succeeded", {**progress, "stopped": "autopilot turned off"}
    return status, progress


CONTACT_RETRY_SQL = """
    SELECT c.id FROM rpi_creators c
    WHERE c.pipeline_status = 'recommended' AND c.disqualified_reason IS NULL
      AND NOT EXISTS (SELECT 1 FROM rpi_creator_contacts k WHERE k.creator_id = c.id AND k.contact_type = 'email')
      AND NOT EXISTS (SELECT 1 FROM rpi_provider_requests r WHERE r.creator_id = c.id
                      AND r.operation = 'get_creator_contacts' AND r.status NOT IN ('budget_denied', 'error'))
    ORDER BY c.updated_at LIMIT $1"""


async def _spent(pool, job_id) -> Decimal:
    async with pool.acquire() as conn:
        v = await conn.fetchval(
            "SELECT COALESCE(SUM(actual_usd), 0) FROM rpi_spend_ledger WHERE job_id = $1 AND status = 'committed'", job_id)
    return Decimal(v)


def _now():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)
