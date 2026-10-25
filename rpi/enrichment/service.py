"""
Per-creator research pipeline (funnel stages 2-5).

  screen()  stage 2: recent content → sponsorship evidence → niche
            classification → risk screen → audience fit → score
  deep()    stages 3-4: demographics, provider sponsorship history,
            selective transcripts, re-classification, contacts → re-score

Every paid call goes through the value gate and the provider gateway (budget,
cache, metering). Derived analysis runs only on data whose provider policy
permits it; otherwise the step is skipped with an explicit reason.
"""

from __future__ import annotations

import json
from statistics import median
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import structlog

from rpi.audience_intelligence.fit import AudienceEvidence, score_all
from rpi.audience_intelligence.quality import SnapshotStat, VideoStats, screen as risk_screen
from rpi.compliance.policies import scoring_allowed
from rpi.content_intelligence.claude_client import LLM, ModelPrice
from rpi.content_intelligence.classifier import CreatorAnalysis, VideoInput, analyze_creator, buyer_intent_summary
from rpi.core.audit import audit
from rpi.core.provenance import MeasurementType
from rpi.crm.pipeline import Status, TransitionError, transition
from rpi.enrichment.value_gate import CreatorState, evaluate as gate
from rpi.providers.base import Capability
from rpi.providers.gateway import CallContext, ProviderGateway
from rpi.providers.registry import ProviderRegistry
from rpi.qualification.scoring import QualificationInput, Sourced, score as qualification_score
from rpi.sponsorship_intelligence.detection import Competitor, detect_sponsorships
from rpi.storage.creators import record_audience, record_contacts, record_content

logger = structlog.get_logger()

CONTENT_PREFERENCE = ("creatordb", "fixture")
TRANSCRIPT_PREFERENCE = ("creatordb", "supadata", "fixture")


# Paid results that only matter through the qualification score.
SCORING_INPUTS = {Capability.CREATOR_CONTENT, Capability.AUDIENCE_DEMOGRAPHICS, Capability.SPONSORSHIP_HISTORY,
                  Capability.TRANSCRIPT}


@dataclass
class ResearchConfig:
    fast: Optional[ModelPrice]
    deep: Optional[ModelPrice]
    creatordb_scoring_permitted: bool
    # 10 CreatorDB credits per creator; discovery presets already filter on
    # the main audience country, so this is off unless an ICP needs detail.
    fetch_audience_demographics: bool = False
    preferred_min_subs: int = 10_000
    preferred_max_subs: int = 150_000
    preferred_max_fee: float = 1_000.0


class ResearchService:
    def __init__(self, pool, registry: ProviderRegistry, gateway: ProviderGateway, llm: Optional[LLM], config: ResearchConfig):
        self.pool = pool
        self.registry = registry
        self.gateway = gateway
        self.llm = llm
        self.config = config

    # ------------------------------------------------------------------
    def allowed(self, provider: Optional[str]) -> bool:
        if provider is None:
            return True
        return scoring_allowed(provider, creatordb_scoring_permitted=self.config.creatordb_scoring_permitted)

    async def _account(self, conn, creator_id) -> Optional[str]:
        return await conn.fetchval(
            "SELECT platform_account_id FROM rpi_creator_social_accounts WHERE creator_id = $1 AND platform = 'youtube' "
            "ORDER BY first_seen_at LIMIT 1", creator_id)

    async def state(self, conn, creator_id) -> CreatorState:
        c = await conn.fetchrow("SELECT pipeline_status, funnel_stage FROM rpi_creators WHERE id = $1", creator_id)
        cls = await conn.fetchrow(
            "SELECT relevance, confidence FROM rpi_content_classifications WHERE creator_id = $1 AND content_id IS NULL "
            "ORDER BY created_at DESC LIMIT 1", creator_id)
        sc = await conn.fetchrow(
            "SELECT recommendation FROM rpi_qualification_scores WHERE creator_id = $1 ORDER BY computed_at DESC LIMIT 1", creator_id)
        fetched = await conn.fetch(
            """SELECT operation, MAX(created_at) AS at FROM rpi_provider_requests
               WHERE creator_id = $1 AND status IN ('ok', 'partial', 'cached') GROUP BY operation""", creator_id)
        transcripts = await conn.fetchval(
            "SELECT COUNT(*) FROM rpi_creator_content WHERE creator_id = $1 AND transcript_excerpt IS NOT NULL", creator_id)
        return CreatorState(
            pipeline_status=c["pipeline_status"], funnel_stage=c["funnel_stage"],
            niche_relevance=cls["relevance"] if cls else None, classification_confidence=cls["confidence"] if cls else None,
            recommendation=sc["recommendation"] if sc else None,
            last_fetched={r["operation"]: r["at"] for r in fetched}, transcripts_fetched=transcripts or 0,
        )

    async def _paid(self, conn, creator_id, capability: Capability, prefer, *args, ctx: CallContext, force=False):
        st = await self.state(conn, creator_id)
        decision = gate(capability, st, force=force)
        if not decision.allowed:
            return None, f"skipped {capability.value}: {decision.reason}"
        adapters = self.registry.supporting(capability, prefer)
        if not adapters:
            return None, f"skipped {capability.value}: no configured provider supports it"
        if capability in SCORING_INPUTS:
            # Never pay for data the qualification score is not allowed to use.
            usable = [a for a in adapters if self.allowed(a.policy_key)]
            if not usable:
                return None, (f"skipped {capability.value}: {adapters[0].name} data may not be used for scoring "
                              f"(set RPI_CREATORDB_SCORING_PERMITTED after reviewing the provider agreement)")
            adapters = usable
        adapter = adapters[0]
        result = await self.gateway.call(adapter, capability, *args, ctx=ctx)
        if not result.ok:
            return None, f"{capability.value} via {adapter.name}: {result.status.value} {result.error or ''}".strip()
        return result, None

    # ------------------------------------------------------------------
    async def screen(self, creator_id, ctx: CallContext, actor: str = "system") -> dict:
        notes: list[str] = []
        ctx.creator_id = str(creator_id)
        async with self.pool.acquire() as conn:
            account = await self._account(conn, creator_id)
            if not account:
                return {"creator_id": str(creator_id), "error": "no YouTube account on record"}
            result, note = await self._paid(conn, creator_id, Capability.CREATOR_CONTENT, CONTENT_PREFERENCE, account, ctx=ctx)
            if note:
                notes.append(note)
            if result and result.data:
                async with conn.transaction():
                    await record_content(conn, creator_id, result.data)
            await self._detect_sponsorships(conn, creator_id)
            analysis = await self._classify(conn, creator_id, ctx, allow_deep=False, notes=notes)
            qual = await self._qualify(conn, creator_id, analysis, stage=2, notes=notes)
            await self._advance_after_screen(conn, creator_id, qual, actor)
            await audit(conn, actor, "research.screen", "creator", creator_id,
                        {"recommendation": qual.get("recommendation"), "notes": notes[:5]})
        return {"creator_id": str(creator_id), "stage": 2, "qualification": qual, "notes": notes}

    async def deep(self, creator_id, ctx: CallContext, actor: str = "system", include_contacts: bool = True) -> dict:
        notes: list[str] = []
        ctx.creator_id = str(creator_id)
        async with self.pool.acquire() as conn:
            st = await self.state(conn, creator_id)
            if st.recommendation not in ("recommend", "investigate"):
                return {"creator_id": str(creator_id), "error": f"not qualified for deep analysis (recommendation={st.recommendation})"}
            await conn.execute("UPDATE rpi_creators SET funnel_stage = GREATEST(funnel_stage, 3), updated_at = NOW() WHERE id = $1", creator_id)
            if st.pipeline_status == Status.PRELIMINARY_QUALIFIED.value:
                await transition(conn, creator_id, Status.DEEP_ANALYSIS, actor=actor, reason="stage 3 analysis")
            account = await self._account(conn, creator_id)

            aud = None
            if self.config.fetch_audience_demographics:
                aud, note = await self._paid(conn, creator_id, Capability.AUDIENCE_DEMOGRAPHICS, CONTENT_PREFERENCE, account,
                                             ctx=ctx)
                if note:
                    notes.append(note)
            else:
                notes.append("audience demographics not fetched (RPI_FETCH_AUDIENCE_DEMOGRAPHICS is off)")
            if aud and aud.data:
                async with conn.transaction():
                    await record_audience(conn, creator_id, aud.data)

            spons, note = await self._paid(conn, creator_id, Capability.SPONSORSHIP_HISTORY, CONTENT_PREFERENCE, account, ctx=ctx)
            if note:
                notes.append(note)
            if spons and spons.data:
                await self._store_provider_sponsorships(conn, creator_id, spons.data)

            await self._fetch_transcripts(conn, creator_id, ctx, notes)
            analysis = await self._classify(conn, creator_id, ctx, allow_deep=True, notes=notes)
            qual = await self._qualify(conn, creator_id, analysis, stage=3, notes=notes)

            if include_contacts and qual.get("recommendation") == "recommend":
                await conn.execute("UPDATE rpi_creators SET funnel_stage = GREATEST(funnel_stage, 4) WHERE id = $1", creator_id)
                contacts, note = await self._paid(conn, creator_id, Capability.CREATOR_CONTACTS, CONTENT_PREFERENCE, account, ctx=ctx)
                if note:
                    notes.append(note)
                if contacts and contacts.data:
                    async with conn.transaction():
                        await record_contacts(conn, creator_id, contacts.data)
                qual = await self._qualify(conn, creator_id, analysis, stage=4, notes=notes)

            if qual.get("recommendation") == "recommend":
                status = await conn.fetchval("SELECT pipeline_status FROM rpi_creators WHERE id = $1", creator_id)
                if status == Status.DEEP_ANALYSIS.value:
                    await transition(conn, creator_id, Status.RECOMMENDED, actor=actor, reason="deep qualification: recommend")
                    await conn.execute("UPDATE rpi_creators SET funnel_stage = 5 WHERE id = $1", creator_id)
            await audit(conn, actor, "research.deep", "creator", creator_id,
                        {"recommendation": qual.get("recommendation"), "notes": notes[:8]})
        return {"creator_id": str(creator_id), "stage": 4, "qualification": qual, "notes": notes}

    # ------------------------------------------------------------------
    async def _content_rows(self, conn, creator_id) -> list[dict]:
        rows = await conn.fetch(
            """SELECT c.id, c.platform_content_id, c.title, c.description, c.url, c.published_at, c.views, c.likes,
                      c.comments, c.sponsored_flag, c.is_short, c.transcript_excerpt, p.provider
               FROM rpi_creator_content c JOIN rpi_data_provenance p ON p.id = c.provenance_id
               WHERE c.creator_id = $1 ORDER BY c.published_at DESC NULLS LAST LIMIT 30""", creator_id)
        return [dict(r) for r in rows]

    async def _competitors(self, conn) -> list[Competitor]:
        rows = await conn.fetch("SELECT id, name, domains, aliases, category, overlap FROM rpi_competitors WHERE active")
        return [Competitor(r["name"], tuple(r["domains"]), tuple(r["aliases"]), r["category"], r["overlap"], str(r["id"]))
                for r in rows]

    async def _detect_sponsorships(self, conn, creator_id) -> int:
        competitors = await self._competitors(conn)
        n = 0
        for row in await self._content_rows(conn, creator_id):
            if not self.allowed(row["provider"]):
                continue
            text = f"{row['title'] or ''}\n{row['description'] or ''}"
            for f in detect_sponsorships(text, competitors, content_ref=str(row["id"]), content_url=row["url"],
                                         platform_paid_flag=row["sponsored_flag"]):
                if f.brand_name == "(unregistered brand)" and f.relationship_type != "possible_promotional":
                    continue
                res = await conn.execute(
                    """INSERT INTO rpi_sponsorship_relationships
                       (creator_id, brand_name, brand_domain, competitor_id, relationship_type, content_id, evidence_url,
                        evidence, published_at, source)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,'rpi_detection')
                       ON CONFLICT (creator_id, brand_name, content_id, relationship_type) DO NOTHING""",
                    creator_id, f.brand_name, f.brand_domain, _uuid(f.competitor_id), f.relationship_type, row["id"],
                    row["url"], json.dumps(f.evidence), row["published_at"],
                )
                n += res.endswith("1")
        return n

    async def _store_provider_sponsorships(self, conn, creator_id, items) -> None:
        from rpi.storage.creators import insert_provenance

        for s in items:
            label = (s.provider_label or "").lower()
            rtype = "confirmed_paid" if label in ("paid", "sponsored", "paid_sponsorship") else "possible_promotional"
            prov_id = await insert_provenance(conn, s.provenance)
            await conn.execute(
                """INSERT INTO rpi_sponsorship_relationships
                   (creator_id, brand_name, brand_domain, relationship_type, evidence_url, evidence, published_at, source, provenance_id)
                   VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9)
                   ON CONFLICT (creator_id, brand_name, content_id, relationship_type) DO NOTHING""",
                creator_id, s.brand_name, s.brand_domain, rtype, s.content_url,
                json.dumps([{"kind": "provider_field", "ref": s.content_id, "provider": s.provenance.provider,
                             "signal": s.provider_label}]), s.published_at, s.provenance.provider, prov_id,
            )

    async def _fetch_transcripts(self, conn, creator_id, ctx: CallContext, notes: list[str]) -> None:
        rows = [r for r in await self._content_rows(conn, creator_id) if not r["transcript_excerpt"]][:3]
        for row in rows:
            res, note = await self._paid(conn, creator_id, Capability.TRANSCRIPT, TRANSCRIPT_PREFERENCE,
                                         row["platform_content_id"], ctx=ctx)
            if note:
                notes.append(note)
                if "skipped" in note:
                    break
                continue
            if res and res.data:
                from rpi.storage.creators import insert_provenance

                prov_id = await insert_provenance(conn, res.data.provenance)
                await conn.execute(
                    "UPDATE rpi_creator_content SET transcript_excerpt = $2, transcript_provenance_id = $3 WHERE id = $1",
                    row["id"], res.data.text[:1500], prov_id)

    async def _classify(self, conn, creator_id, ctx: CallContext, *, allow_deep: bool, notes: list[str]) -> Optional[CreatorAnalysis]:
        rows = await self._content_rows(conn, creator_id)
        permitted = [r for r in rows if self.allowed(r["provider"])]
        if len(permitted) < len(rows):
            notes.append(f"{len(rows) - len(permitted)} content item(s) excluded by provider data policy")
        if not permitted:
            notes.append("no policy-permitted content to classify")
            return None
        videos = [VideoInput(str(r["id"]), r["title"], r["description"], r["transcript_excerpt"], r["url"]) for r in permitted]
        analysis = await analyze_creator(
            self.llm if self.config.fast else None, videos, fast=self.config.fast or ModelPrice("none", Decimal(0), Decimal(0)),
            deep=self.config.deep, allow_deep=allow_deep, job_id=ctx.job_id, creator_id=str(creator_id), priority=ctx.priority,
        )
        source_providers = sorted({r["provider"] for r in permitted})
        await conn.execute(
            """INSERT INTO rpi_content_classifications
               (creator_id, taxonomy_version, method, model, prompt_version, labels, relevance, audience_problem,
                product_capabilities, buyer_intent, evidence, confidence, uncertainty_reasons, source_providers, cost_usd)
               VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9::jsonb,$10::jsonb,$11::jsonb,$12,$13::jsonb,$14,$15)""",
            creator_id, analysis.taxonomy_version, analysis.method, analysis.model, analysis.prompt_version,
            json.dumps({"primary": analysis.primary_labels,
                        "videos": [{"ref": v.ref, "labels": v.labels, "relevance": v.relevance, "note": v.note,
                                    "buyer_intent": v.buyer_intent, "problems": v.audience_problems} for v in analysis.videos],
                        "model_relevance": analysis.model_relevance}),
            analysis.computed_relevance, analysis.audience_problem, json.dumps(analysis.product_capabilities),
            json.dumps(buyer_intent_summary(analysis.videos)), json.dumps(analysis.evidence), analysis.confidence,
            json.dumps(analysis.uncertainty_reasons), source_providers, analysis.cost_usd,
        )
        return analysis

    async def _qualify(self, conn, creator_id, analysis: Optional[CreatorAnalysis], *, stage: int, notes: list[str]) -> dict:
        snap = await conn.fetchrow(
            """SELECT s.subscribers, s.last_upload_at, s.uploads_last_90d, s.language, s.provider
               FROM rpi_creator_profile_snapshots s WHERE s.creator_id = $1 ORDER BY s.observed_at DESC LIMIT 1""", creator_id)
        snapshots = await conn.fetch(
            "SELECT observed_at, subscribers, provider FROM rpi_creator_profile_snapshots WHERE creator_id = $1", creator_id)
        content = await self._content_rows(conn, creator_id)
        audience_rows = await conn.fetch(
            "SELECT dimension, bucket, share, measurement, provider FROM rpi_audience_estimates WHERE creator_id = $1", creator_id)

        # Audience evidence grouped by (dimension, provider), policy-filtered.
        grouped: dict[tuple, dict] = {}
        for r in audience_rows:
            if not self.allowed(r["provider"]):
                continue
            g = grouped.setdefault((r["dimension"], r["provider"], r["measurement"]), {})
            g[r["bucket"]] = r["share"]
        evidence = [AudienceEvidence(d, shares, MeasurementType(m), p) for (d, p, m), shares in grouped.items()]

        video_labels = [v.labels for v in analysis.videos] if analysis else []
        problems = sorted({p for v in analysis.videos for p in v.audience_problems}) if analysis else []
        fits = score_all(video_labels=video_labels, audience_problems=problems, audience=evidence,
                         creator_language=snap["language"] if snap else None)
        best = max((f for f in fits if f.fit is not None), key=lambda f: f.fit, default=None)

        allowed_content = [r for r in content if self.allowed(r["provider"])]
        risk = risk_screen(
            [VideoStats(r["published_at"], r["views"], r["likes"], r["comments"]) for r in allowed_content],
            [SnapshotStat(s["observed_at"], s["subscribers"]) for s in snapshots if self.allowed(s["provider"])],
            subscribers=snap["subscribers"] if snap and self.allowed(snap["provider"]) else None,
        )
        async with conn.transaction():
            await conn.execute("DELETE FROM rpi_audience_quality_signals WHERE creator_id = $1", creator_id)
            for s in risk["signals"]:
                await conn.execute(
                    """INSERT INTO rpi_audience_quality_signals (creator_id, signal_type, severity, value, detail, data_basis)
                       VALUES ($1,$2,$3,$4,$5,$6::jsonb)""",
                    creator_id, s["signal_type"], s["severity"], s["value"], s["detail"], json.dumps(risk["data_points"]))

        spons = await conn.fetch(
            """SELECT r.brand_name, r.relationship_type, r.content_id, c.overlap, r.published_at
               FROM rpi_sponsorship_relationships r
               LEFT JOIN rpi_competitors c ON c.id = r.competitor_id WHERE r.creator_id = $1""", creator_id)
        relevant_spons = sum(1 for s in spons if s["relationship_type"] in ("confirmed_paid", "disclosed_affiliate"))
        recent_deals = recent_competitor_deals(spons, now=datetime.now(timezone.utc))
        saturation = sponsor_saturation(allowed_content, {str(s["content_id"]) for s in spons
                                                          if s["relationship_type"] == "confirmed_paid" and s["content_id"]})
        fee_note = await conn.fetchval(
            "SELECT (data->>'fee_usd')::float FROM rpi_notes WHERE creator_id = $1 AND kind = 'sponsorship_request' "
            "ORDER BY created_at DESC LIMIT 1", creator_id)

        content_provider = allowed_content[0]["provider"] if allowed_content else None
        last_upload = snap["last_upload_at"] if snap else None
        uploads_90d = snap["uploads_last_90d"] if snap else None
        activity_provider = snap["provider"] if snap else None
        dates = [r["published_at"] for r in allowed_content if r["published_at"]]
        if last_upload is None and uploads_90d is None and dates:
            # Search snapshots carry no activity; stored recent content does
            # (a lower bound on uploads, since only recent items are kept).
            cutoff = datetime.now(timezone.utc) - timedelta(days=90)
            last_upload, uploads_90d, activity_provider = max(dates), sum(d >= cutoff for d in dates), content_provider
        inp = QualificationInput(
            niche_relevance=Sourced(analysis.computed_relevance if analysis else None, content_provider,
                                    "content_inferred", f"{analysis.method if analysis else 'none'} over {len(video_labels)} videos"),
            classification_confidence=analysis.confidence if analysis else 0.0,
            best_icp_fit=Sourced(best.fit if best else None, None, "mixed", best.name if best else "no ICP evaluable"),
            icp_completeness=best.completeness if best else 0.0,
            last_upload_at=last_upload,
            uploads_last_90d=uploads_90d,
            activity_provider=activity_provider,
            subscribers=snap["subscribers"] if snap else None,
            subscribers_provider=snap["provider"] if snap else None,
            relevant_sponsorships=relevant_spons,
            recent_competitor_deals=recent_deals,
            sponsored_share=saturation["share"],
            sponsored_sample=saturation["sample"],
            sponsored_view_ratio=saturation["view_ratio"],
            expected_fee_usd=fee_note,
            risk_level=risk["level"],
            scoring_allowed={p: self.allowed(p) for p in {content_provider, snap["provider"] if snap else None} if p},
            preferred_min_subs=self.config.preferred_min_subs,
            preferred_max_subs=self.config.preferred_max_subs,
            preferred_max_fee=self.config.preferred_max_fee,
        )
        result = qualification_score(inp)
        await conn.execute(
            """INSERT INTO rpi_qualification_scores
               (creator_id, scoring_version, stage, niche_relevance, activity, audience_fit, risk_level, commercial_fit,
                overall, confidence, recommendation, components, evidence, missing_data, excluded_sources)
               VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,$14::jsonb,$15::jsonb)""",
            creator_id, result.scoring_version, stage, result.components["niche_relevance"]["value"],
            result.components["activity"]["value"],
            json.dumps([{"icp": f.icp, "name": f.name, "fit": f.fit, "completeness": f.completeness,
                         "components": [c.__dict__ for c in f.components], "missing": f.missing} for f in fits]),
            risk["level"], result.components["commercial_fit"]["value"], result.overall, result.confidence,
            result.recommendation, json.dumps(result.components, default=str),
            json.dumps(analysis.evidence if analysis else []), json.dumps(result.missing_data),
            json.dumps(result.excluded_sources),
        )
        if result.recommendation == "reject_niche_mismatch":
            await conn.execute("UPDATE rpi_creators SET disqualified_reason = 'niche_mismatch' WHERE id = $1", creator_id)
        return {
            "overall": result.overall, "confidence": result.confidence, "recommendation": result.recommendation,
            "missing_data": result.missing_data, "excluded_sources": result.excluded_sources, "reasons": result.reasons,
            "risk_level": risk["level"], "best_icp": best.name if best else None,
        }

    async def _advance_after_screen(self, conn, creator_id, qual: dict, actor: str) -> None:
        if qual.get("recommendation") in ("recommend", "investigate"):
            status = await conn.fetchval("SELECT pipeline_status FROM rpi_creators WHERE id = $1", creator_id)
            if status == Status.DISCOVERED.value:
                try:
                    await transition(conn, creator_id, Status.PRELIMINARY_QUALIFIED, actor=actor,
                                     reason=f"screen: {qual['recommendation']} ({qual['overall']})")
                except TransitionError:
                    pass
            await conn.execute("UPDATE rpi_creators SET funnel_stage = GREATEST(funnel_stage, 2) WHERE id = $1", creator_id)


def recent_competitor_deals(spons, *, now: datetime, days: int = 90) -> list[str]:
    """Paid promotions of direct competitors in the last `days`, newest per brand."""
    latest: dict[str, datetime] = {}
    for s in spons:
        at = s["published_at"]
        if s["overlap"] == "direct" and s["relationship_type"] == "confirmed_paid" and at and now - at <= timedelta(days=days):
            latest[s["brand_name"]] = max(at, latest.get(s["brand_name"], at))
    return [f"{brand} (paid, {at.date().isoformat()})" for brand, at in sorted(latest.items(), key=lambda kv: kv[1], reverse=True)]


def sponsor_saturation(content: list, paid_content_ids: set[str]) -> dict:
    """Share of recent long-form videos that are paid integrations, and how
    their median views compare with the creator's other recent videos."""
    videos = [r for r in content if r["is_short"] is not True]
    if not videos:
        return {"share": None, "sample": 0, "view_ratio": None}
    paid = [r for r in videos if r["sponsored_flag"] or str(r["id"]) in paid_content_ids]
    organic = [r for r in videos if r not in paid]
    ratio = None
    paid_views = [r["views"] for r in paid if r["views"] is not None]
    organic_views = [r["views"] for r in organic if r["views"] is not None]
    if len(paid_views) >= 2 and len(organic_views) >= 2 and median(organic_views) > 0:
        ratio = round(median(paid_views) / median(organic_views), 3)
    return {"share": round(len(paid) / len(videos), 3), "sample": len(videos), "view_ratio": ratio}


def _uuid(v):
    import uuid

    return uuid.UUID(str(v)) if v else None
