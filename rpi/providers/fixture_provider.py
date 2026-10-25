"""
Synthetic fixture provider for local development and end-to-end tests.

Produces deterministic, obviously synthetic creators ("[SYNTHETIC] ...",
channel ids starting with UCFIXTURE) so the whole pipeline can run without
paid API keys. It is refused when ENVIRONMENT=production and all its data is
labeled provider="fixture", which the UI shows as synthetic.
"""

from __future__ import annotations

import hashlib
import random
from datetime import timedelta
from decimal import Decimal
from typing import Any

from rpi.core.provenance import CommercialUse, MeasurementType, Provenance, utcnow
from rpi.providers.base import (
    AudienceBreakdown,
    AudienceBucket,
    Capability,
    CostActual,
    CostEstimate,
    CreatorSearchCriteria,
    NormalizedContact,
    NormalizedContent,
    NormalizedCreator,
    NormalizedSponsorship,
    ProviderAdapter,
    ProviderResult,
    ResultStatus,
)

PROVIDER = "fixture"

_ARCHETYPES = [
    ("faceless", ["How I Run 5 Faceless YouTube Channels With AI", "Faceless Channel Ideas That Still Work",
                  "AI YouTube Shorts: Full Automation Workflow", "Cash Cow Channel Monetization Update",
                  "Text to Video AI Tools Compared", "AI Voiceover for Faceless Videos"]),
    ("ai_video_tools", ["Best AI Video Generator in 2026 (Tested)", "InVideo AI vs Pictory Review",
                        "Opus Clip Tutorial for Beginners", "AI Video Editing Tools Comparison",
                        "Make Viral Shorts With AI in 10 Minutes", "Text to Video AI: Honest Review"]),
    ("smb_marketing", ["Social Media Content Automation for Small Business", "Schedule a Month of Posts in an Hour",
                       "Repurpose One Video Into 20 Shorts", "AI Video Tools for Local Businesses",
                       "Auto Post to TikTok and Reels", "Content Automation Stack 2026"]),
    ("ai_news", ["This Week in AI News", "ChatGPT New Features Explained", "Gemini vs GPT Benchmarks",
                 "AI News: Big Announcements", "OpenAI Update Breakdown", "AI Tools You Missed"]),
    ("gaming", ["Minecraft Hardcore Day 100", "Fortnite Gameplay Highlights", "Let's Play: Episode 12",
                "Reaction to New Trailer", "Funny Gameplay Moments", "Vlog: Gaming Setup Tour"]),
]


def _rng(seed: str) -> random.Random:
    return random.Random(int(hashlib.sha256(seed.encode()).hexdigest()[:12], 16))


def _prov(operation: str, ref: str, data_type: str, measurement: MeasurementType) -> Provenance:
    return Provenance(
        provider=PROVIDER, operation=operation, source_record=ref, retrieved_at=utcnow(), data_type=data_type,
        measurement=measurement, retention_policy="fixture:7d", commercial_use=CommercialUse.INTERNAL_ONLY,
        raw_ref=f"fixture:{ref}", response_version="synthetic-1",
    )


class FixtureProvider(ProviderAdapter):
    name = PROVIDER
    policy_key = PROVIDER
    capabilities = frozenset({
        Capability.SEARCH_CREATORS, Capability.CREATOR_PROFILE, Capability.AUDIENCE_DEMOGRAPHICS,
        Capability.CREATOR_CONTENT, Capability.SPONSORSHIP_HISTORY, Capability.CREATOR_CONTACTS,
        Capability.TRANSCRIPT,
    })

    def __init__(self, population: int = 60, usd_per_call: str = "0.01"):
        self.population = population
        self.usd_per_call = Decimal(usd_per_call)
        self.calls: list[str] = []

    def is_configured(self) -> bool:
        return True

    def estimate_cost(self, operation: Capability, **params: Any) -> CostEstimate:
        return CostEstimate(PROVIDER, operation.value, Decimal("1"), self.usd_per_call, basis="synthetic")

    def _cost(self) -> CostActual:
        return CostActual(Decimal("1"), self.usd_per_call, reported_by_provider=True)

    def _archetype(self, idx: int) -> tuple[str, list[str]]:
        return _ARCHETYPES[idx % len(_ARCHETYPES)]

    def _creator(self, idx: int, operation: str) -> NormalizedCreator:
        arche, _ = self._archetype(idx)
        r = _rng(f"creator{idx}")
        cid = f"UCFIXTURE{idx:06d}"
        subs = int(r.choice([4_000, 12_000, 25_000, 48_000, 90_000, 140_000, 400_000]))
        return NormalizedCreator(
            platform="youtube", platform_account_id=cid, handle=f"@synthetic{idx}",
            display_name=f"[SYNTHETIC] {arche.replace('_', ' ').title()} Creator {idx}",
            url=f"https://www.youtube.com/channel/{cid}", description="Synthetic fixture creator for development.",
            country=r.choice(["US", "US", "GB", "CA", "AU", "IN"]), language="en", subscribers=subs,
            avg_views=subs * r.uniform(0.03, 0.25), median_views=subs * r.uniform(0.02, 0.15),
            engagement_rate=r.uniform(0.01, 0.06), total_videos=r.randint(40, 400),
            last_upload_at=utcnow() - timedelta(days=r.randint(1, 90)), uploads_last_90d=r.randint(0, 30),
            categories=[arche], provider_creator_id=cid,
            provenance=_prov(operation, cid, "creator_profile", MeasurementType.OBSERVED),
        )

    def _index(self, account_id: str) -> int:
        return int(account_id.removeprefix("UCFIXTURE"))

    async def search_creators(self, criteria: CreatorSearchCriteria) -> ProviderResult[list[NormalizedCreator]]:
        self.calls.append("search")
        offset = int(criteria.page_token or 0)
        terms = " ".join(criteria.keywords + criteria.topics + criteria.sponsored_brands).lower()
        wanted = []
        for i in range(self.population):
            arche, titles = self._archetype(i)
            blob = (arche + " " + " ".join(titles)).lower()
            if not terms or any(t in blob for t in terms.split()):
                wanted.append(i)
        page = wanted[offset: offset + criteria.limit]
        creators = [self._creator(i, "search_creators") for i in page]
        if criteria.min_subscribers is not None:
            creators = [c for c in creators if (c.subscribers or 0) >= criteria.min_subscribers]
        if criteria.max_subscribers is not None:
            creators = [c for c in creators if (c.subscribers or 0) <= criteria.max_subscribers]
        nxt = offset + criteria.limit if offset + criteria.limit < len(wanted) else None
        return ProviderResult(PROVIDER, "search_creators", ResultStatus.OK, creators, self._cost(),
                              next_page_token=str(nxt) if nxt is not None else None)

    async def get_creator_profile(self, platform_account_id: str) -> ProviderResult[NormalizedCreator]:
        self.calls.append("profile")
        if not platform_account_id.startswith("UCFIXTURE"):
            return ProviderResult(PROVIDER, "get_creator_profile", ResultStatus.NOT_FOUND)
        return ProviderResult(PROVIDER, "get_creator_profile", ResultStatus.OK,
                              self._creator(self._index(platform_account_id), "get_creator_profile"), self._cost())

    async def get_creator_content(self, platform_account_id: str, limit: int = 30) -> ProviderResult[list[NormalizedContent]]:
        self.calls.append("content")
        idx = self._index(platform_account_id)
        arche, titles = self._archetype(idx)
        r = _rng(f"content{idx}")
        items = []
        for n in range(min(limit, 12)):
            title = titles[n % len(titles)] + ("" if n < len(titles) else f" (Part {n // len(titles) + 1})")
            desc = "Synthetic description."
            if arche in ("faceless", "ai_video_tools") and n == 2:
                desc = "Thanks to OpusClip for sponsoring this video! Try it: https://opus.pro/?via=synthetic"
            if arche == "faceless" and n == 3:
                desc = "Affiliate link (I may earn a commission): https://crayo.ai/?ref=synthetic use code SYNTH20"
            vid = f"FX{idx:04d}V{n:03d}"
            views = int(r.uniform(2_000, 60_000))
            items.append(NormalizedContent(
                platform="youtube", platform_content_id=vid, platform_account_id=platform_account_id, title=title,
                description=desc, url=f"https://www.youtube.com/watch?v={vid}",
                published_at=utcnow() - timedelta(days=7 * n + r.randint(0, 5)), duration_seconds=r.choice([45, 58, 480, 720]),
                views=views, likes=int(views * r.uniform(0.02, 0.06)), comments=int(views * r.uniform(0.001, 0.01)),
                sponsored_flag=(n == 2 and arche in ("faceless", "ai_video_tools")),
                provenance=_prov("get_creator_content", vid, "content", MeasurementType.OBSERVED),
            ))
        return ProviderResult(PROVIDER, "get_creator_content", ResultStatus.OK, items, self._cost())

    async def get_audience_demographics(self, platform_account_id: str) -> ProviderResult[AudienceBreakdown]:
        self.calls.append("audience")
        r = _rng(f"aud{platform_account_id}")
        us = r.uniform(0.2, 0.6)
        buckets = [
            AudienceBucket(dimension="country", bucket="US", share=round(us, 3), measurement=MeasurementType.THIRD_PARTY_MODELED),
            AudienceBucket(dimension="country", bucket="IN", share=round((1 - us) / 2, 3), measurement=MeasurementType.THIRD_PARTY_MODELED),
            AudienceBucket(dimension="age", bucket="25-34", share=round(r.uniform(0.25, 0.45), 3), measurement=MeasurementType.THIRD_PARTY_MODELED),
            AudienceBucket(dimension="age", bucket="18-24", share=round(r.uniform(0.15, 0.35), 3), measurement=MeasurementType.THIRD_PARTY_MODELED),
        ]
        return ProviderResult(PROVIDER, "get_audience_demographics", ResultStatus.PARTIAL,
                              AudienceBreakdown(platform_account_id=platform_account_id, buckets=buckets,
                                                provenance=_prov("get_audience_demographics", platform_account_id,
                                                                 "audience_demographics", MeasurementType.THIRD_PARTY_MODELED)),
                              self._cost(), missing_fields=["gender", "language"])

    async def get_sponsorship_history(self, platform_account_id: str) -> ProviderResult[list[NormalizedSponsorship]]:
        self.calls.append("sponsorship")
        return ProviderResult(PROVIDER, "get_sponsorship_history", ResultStatus.OK, [], self._cost())

    async def get_creator_contacts(self, platform_account_id: str) -> ProviderResult[list[NormalizedContact]]:
        self.calls.append("contacts")
        idx = self._index(platform_account_id)
        email = f"synthetic{idx}@example.invalid"
        return ProviderResult(PROVIDER, "get_creator_contacts", ResultStatus.OK, [
            NormalizedContact(platform_account_id=platform_account_id, contact_type="email", value=email,
                              is_business_contact=True,
                              provenance=_prov("get_creator_contacts", platform_account_id, "contact", MeasurementType.OBSERVED))
        ], self._cost())
