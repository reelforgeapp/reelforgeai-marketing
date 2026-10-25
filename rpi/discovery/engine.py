"""
Discovery engine: independent strategies, one deduplicated candidate set.

  niche                   direct topic/keyword search (primary provider)
  audience_first          SparkToro audience research → channels → index
                          (explicit opt-in per run; never automatic)
  competitor_sponsorship  creators associated with registry brands
  emerging                smaller, recently active creators in the niche
  lookalike               not available until campaign outcomes exist

Every run records totals, duplicates, the reason each candidate was
included, missing fields, niche distribution and the estimated cost of the
next enrichment stage. For audience_first it also records how many candidates
no other strategy found, which is how SparkToro's incremental value is
measured over time.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

import structlog

from rpi.administration.budget import Priority
from rpi.content_intelligence.prefilter import match_text
from rpi.content_intelligence.taxonomy import RELEVANT_LABELS
from rpi.core.audit import audit
from rpi.crm.suppression import is_suppressed
from rpi.identity.store import resolve_after_upsert
from rpi.providers.base import Capability, CreatorSearchCriteria, NormalizedCreator, ProviderAdapter, ResultStatus
from rpi.providers.gateway import CallContext, ProviderGateway
from rpi.providers.registry import ProviderRegistry
from rpi.storage.creators import record_snapshot, upsert_creator

logger = structlog.get_logger()

STRATEGIES = ("niche", "audience_first", "competitor_sponsorship", "emerging", "lookalike")

DEFAULT_NICHE_KEYWORDS = [
    "faceless youtube channel", "ai youtube shorts", "youtube automation", "ai video generator",
    "text to video ai", "ai voiceover", "faceless video monetization", "auto post shorts",
]

PRIMARY_PREFERENCE = ("creatordb", "fixture")


@dataclass
class DiscoveryRequest:
    criteria: CreatorSearchCriteria
    strategies: list[str]
    max_candidates: int = 1000
    audience_query: Optional[str] = None
    competitor_names: list[str] = field(default_factory=list)
    job_id: Optional[str] = None
    job_budget_usd: Optional[Decimal] = None
    dry_run: bool = False


@dataclass
class Candidate:
    creator: NormalizedCreator
    strategy: str
    provider: str
    reasons: list[str]


def preliminary_screen(c: NormalizedCreator, criteria: CreatorSearchCriteria) -> tuple[bool, float, list[str], list[str]]:
    """Cheap stage-1 screen on fields already returned by discovery.
    Returns (qualified, score, reasons, missing)."""
    reasons, missing = [], []
    score = 0.0
    if c.language is None:
        missing.append("language")
        score += 0.1
    elif c.language.lower()[:2] in [l.lower()[:2] for l in criteria.languages or ["en"]]:
        score += 0.2
        reasons.append(f"language {c.language}")
    else:
        return False, 0.0, [f"language {c.language} outside target"], missing

    if c.last_upload_at is None and c.uploads_last_90d is None:
        missing.append("recent activity")
        score += 0.1
    elif (c.uploads_last_90d or 0) > 0:
        score += 0.2
        reasons.append(f"{c.uploads_last_90d} uploads in 90 days")
    else:
        reasons.append("no recent uploads reported")

    text_match = match_text("profile", c.display_name, " ".join([c.description or "", *c.categories]))
    relevant = set(text_match.labels) & RELEVANT_LABELS
    if relevant:
        score += 0.4
        reasons.append("profile mentions " + ", ".join(sorted(relevant)))
    else:
        missing.append("content evidence")

    if c.subscribers is None:
        missing.append("subscribers")
    elif (criteria.min_subscribers or 0) <= c.subscribers <= (criteria.max_subscribers or 10**12):
        score += 0.2
    if c.country is None:
        missing.append("country")
    return score >= 0.4, round(score, 3), reasons, missing


class DiscoveryEngine:
    def __init__(self, pool, registry: ProviderRegistry, gateway: ProviderGateway):
        self.pool = pool
        self.registry = registry
        self.gateway = gateway

    def primary(self) -> Optional[ProviderAdapter]:
        return self.registry.first_supporting(Capability.SEARCH_CREATORS, PRIMARY_PREFERENCE)

    def estimate(self, request: DiscoveryRequest) -> dict:
        primary = self.primary()
        lines, total = [], Decimal("0")
        if primary:
            per_strategy_limit = max(1, request.max_candidates // max(1, len([s for s in request.strategies if s != "audience_first"])))
            for s in request.strategies:
                if s in ("niche", "competitor_sponsorship", "emerging"):
                    est = primary.estimate_cost(Capability.SEARCH_CREATORS, limit=per_strategy_limit)
                    lines.append({"strategy": s, "provider": primary.name, "usd": str(est.usd), "basis": est.basis})
                    total += est.usd
        if "audience_first" in request.strategies:
            spark = self.registry.get("sparktoro")
            if spark:
                est = spark.estimate_cost(Capability.AUDIENCE_RESEARCH)
                lines.append({"strategy": "audience_first", "provider": "sparktoro", "usd": str(est.usd), "basis": est.basis})
                total += est.usd
        return {"lines": lines, "total_usd": str(total), "primary_provider": primary.name if primary else None}

    async def _search(self, adapter, criteria: CreatorSearchCriteria, strategy: str, limit: int, ctx: CallContext,
                      warnings: list[str]) -> list[Candidate]:
        out: list[Candidate] = []
        page = criteria.model_copy(update={"limit": min(100, limit)})
        while len(out) < limit:
            result = await self.gateway.call(adapter, Capability.SEARCH_CREATORS, page, ctx=ctx)
            if not result.ok:
                warnings.append(f"{strategy}: {adapter.name} {result.status.value}: {result.error}")
                break
            if result.unapplied_criteria:
                warnings.append(f"{strategy}: {adapter.name} could not apply {', '.join(result.unapplied_criteria)}")
            for c in result.data or []:
                out.append(Candidate(c, strategy, adapter.name, [f"{strategy} search via {adapter.name}"]))
            if not result.next_page_token or not result.data:
                break
            page = page.model_copy(update={"page_token": result.next_page_token})
        return out[:limit]

    async def run(self, request: DiscoveryRequest, actor: str) -> dict:
        unknown = [s for s in request.strategies if s not in STRATEGIES]
        if unknown:
            raise ValueError(f"unknown strategies: {unknown}")
        estimate = self.estimate(request)
        if request.dry_run:
            return {"dry_run": True, "estimate": estimate}

        primary = self.primary()
        warnings: list[str] = []
        ctx = CallContext(job_id=request.job_id, priority=Priority.NORMAL, job_budget_usd=request.job_budget_usd)
        async with self.pool.acquire() as conn:
            run_id = await conn.fetchval(
                """INSERT INTO rpi_discovery_runs (job_id, criteria, strategies, created_by)
                   VALUES ($1, $2::jsonb, $3, $4) RETURNING id""",
                request.job_id, request.criteria.model_dump_json(), request.strategies, actor,
            )

        candidates: list[Candidate] = []
        search_strategies = [s for s in request.strategies if s in ("niche", "competitor_sponsorship", "emerging")]
        per_limit = max(1, request.max_candidates // max(1, len(search_strategies) or 1))
        if search_strategies and primary is None:
            warnings.append("no configured provider supports creator search (CreatorDB search endpoint unverified or no key)")

        for strategy in search_strategies:
            if primary is None:
                break
            if strategy == "niche":
                crit = request.criteria.model_copy(update={"keywords": request.criteria.keywords or DEFAULT_NICHE_KEYWORDS})
            elif strategy == "emerging":
                crit = request.criteria.model_copy(update={
                    "keywords": request.criteria.keywords or DEFAULT_NICHE_KEYWORDS,
                    "min_subscribers": 1_000, "max_subscribers": min(request.criteria.min_subscribers or 15_000, 15_000),
                    "active_within_days": 30,
                })
            else:
                names = request.competitor_names or await self._competitor_names()
                crit = request.criteria.model_copy(update={"sponsored_brands": names, "keywords": []})
            found = await self._search(primary, crit, strategy, per_limit, ctx, warnings)
            if strategy == "competitor_sponsorship":
                for c in found:
                    c.reasons.append("associated with competitor brands: " + ", ".join(crit.sponsored_brands[:5]))
            candidates.extend(found)

        if "audience_first" in request.strategies:
            candidates.extend(await self._audience_first(request, primary, ctx, warnings))
        if "lookalike" in request.strategies:
            warnings.append("lookalike discovery needs outcome data from completed campaigns; not yet available (Milestone 7)")

        return await self._persist(run_id, request, candidates, warnings, estimate, actor)

    async def _competitor_names(self) -> list[str]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT name FROM rpi_competitors WHERE active ORDER BY overlap, name")
        return [r["name"] for r in rows]

    async def _audience_first(self, request, primary, ctx, warnings) -> list[Candidate]:
        spark = self.registry.get("sparktoro")
        if spark is None:
            warnings.append("audience_first requested but SparkToro is not configured")
            return []
        query = request.audience_query or (
            "people who want to create faceless YouTube channels and AI-generated Shorts without editing skills"
        )
        result = await self.gateway.call(spark, Capability.AUDIENCE_RESEARCH, query, ctx=ctx)
        if not result.ok or not result.data:
            warnings.append(f"audience_first: SparkToro {result.status.value}: {result.error}")
            return []
        out = []
        for src in result.data.sources:
            account_id = src.get("platform_account_id")
            if not account_id or not str(account_id).startswith("UC"):
                continue
            if primary is None or not primary.supports(Capability.CREATOR_PROFILE):
                warnings.append("audience_first: no profile provider to resolve SparkToro channels")
                break
            prof = await self.gateway.call(primary, Capability.CREATOR_PROFILE, str(account_id), ctx=ctx)
            if prof.ok and prof.data:
                out.append(Candidate(prof.data, "audience_first", "sparktoro",
                                     [f"SparkToro: audience '{query[:60]}' pays attention to this channel"]))
        return out

    async def _persist(self, run_id, request, candidates: list[Candidate], warnings, estimate, actor) -> dict:
        seen: dict[str, str] = {}
        strategy_hits: dict[str, set] = {}
        stats = Counter()
        niche = Counter()
        new_creators = duplicates = prelim = 0
        async with self.pool.acquire() as conn:
            for cand in candidates[: request.max_candidates * 2]:
                key = f"{cand.creator.platform}:{cand.creator.platform_account_id}"
                strategy_hits.setdefault(cand.strategy, set()).add(key)
                stats[cand.strategy] += 1
                async with conn.transaction():
                    in_run = key in seen
                    creator_id, account_id, created = await upsert_creator(conn, cand.creator)
                    if not in_run:
                        await record_snapshot(conn, creator_id, account_id, cand.creator)
                        for d in await resolve_after_upsert(conn, creator_id, cand.creator):
                            cand.reasons.append(f"identity: {d['decision']} with {d['other']} ({d['reason']})")
                    seen[key] = str(creator_id)
                    duplicate = in_run or not created
                    if duplicate:
                        duplicates += 1
                    else:
                        new_creators += 1
                    if await is_suppressed(conn, creator_id=creator_id):
                        cand.reasons.append("on suppression list: will not be contacted")
                    ok, score, reasons, missing = preliminary_screen(cand.creator, request.criteria)
                    prelim += int(ok and not in_run)
                    labels = match_text("p", cand.creator.display_name, " ".join([cand.creator.description or "", *cand.creator.categories])).labels
                    if not in_run:
                        for label in labels or {"unclassified": []}:
                            niche[label] += 1
                    await conn.execute(
                        """INSERT INTO rpi_discovery_candidates
                           (run_id, creator_id, strategy, source_provider, inclusion_reasons, missing_fields, was_duplicate,
                            preliminary_score, preliminary_qualified)
                           VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9)
                           ON CONFLICT (run_id, creator_id, strategy) DO NOTHING""",
                        run_id, creator_id, cand.strategy, cand.provider, json.dumps(cand.reasons + reasons),
                        json.dumps(missing), duplicate, score, ok,
                    )
                    if ok and created:
                        await conn.execute(
                            "UPDATE rpi_creators SET funnel_stage = GREATEST(funnel_stage, 1) WHERE id = $1", creator_id)

            others = set().union(*[v for k, v in strategy_hits.items() if k != "audience_first"]) if strategy_hits else set()
            breakdown = {s: {"found": stats[s], "unique_to_strategy": len(v - set().union(*[w for k, w in strategy_hits.items() if k != s]))}
                         for s, v in strategy_hits.items()}
            if "audience_first" in strategy_hits:
                breakdown["audience_first"]["not_found_by_other_strategies"] = len(strategy_hits["audience_first"] - others)

            spent = await conn.fetchval(
                "SELECT COALESCE(SUM(actual_usd), 0) FROM rpi_spend_ledger WHERE job_id = $1 AND status = 'committed'",
                request.job_id) if request.job_id else Decimal("0")
            primary = self.primary()
            content_cost = primary.estimate_cost(Capability.CREATOR_CONTENT).usd if primary else Decimal("0")
            stage2_estimate = content_cost * min(prelim, 200)
            await conn.execute(
                """UPDATE rpi_discovery_runs SET status = 'succeeded', total_found = $2, duplicate_count = $3,
                       new_creators = $4, preliminary_qualified = $5, niche_distribution = $6::jsonb,
                       strategy_breakdown = $7::jsonb, estimated_enrichment_cost_usd = $8, actual_cost_usd = $9,
                       warnings = $10::jsonb, finished_at = NOW()
                   WHERE id = $1""",
                run_id, len(candidates), duplicates, new_creators, prelim, json.dumps(niche), json.dumps(breakdown),
                stage2_estimate, spent, json.dumps(warnings),
            )
            await audit(conn, actor, "discovery.run", "discovery_run", run_id,
                        {"strategies": request.strategies, "found": len(candidates), "new": new_creators})
        return {
            "run_id": str(run_id),
            "total_found": len(candidates),
            "unique_creators": len(seen),
            "duplicates": duplicates,
            "new_creators": new_creators,
            "preliminary_qualified": prelim,
            "niche_distribution": dict(niche),
            "strategy_breakdown": breakdown,
            "estimated_next_stage_cost_usd": str(stage2_estimate),
            "actual_cost_usd": str(spent),
            "pre_run_estimate": estimate,
            "warnings": warnings,
        }
