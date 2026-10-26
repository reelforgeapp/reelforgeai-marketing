"""
YouTube Data API adapter — verification and the spoken-language gate.

Used to confirm a channel exists, read its public title/handle and link to
source content, and to read the spoken language each recent upload declares
(rpi.qualification.language_gate). Never used for scoring: every record is
marked `derived_metrics_prohibited` (Developer Policies III.E.4(h)) and expires
after 30 days (III.E.4(c)/(d)). One API project, one key; no key rotation
(III.D.1(c)). Calls are low-cost reads (channels.list, playlistItems.list and
videos.list, 1 quota unit each), never the 100-unit search.list.
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any

import httpx

from rpi.core.provenance import CommercialUse, MeasurementType, Provenance, utcnow
from rpi.providers.base import (
    Capability,
    CostActual,
    CostEstimate,
    NormalizedCreator,
    ProviderAdapter,
    ProviderResult,
    ResultStatus,
    VideoLanguages,
)
from rpi.providers.http import CircuitBreaker, ProviderError, ProviderNotFound, ResilientHTTP, TokenBucket

PROVIDER = "youtube_data_api"


class YouTubeDataAdapter(ProviderAdapter):
    name = PROVIDER
    policy_key = PROVIDER
    capabilities = frozenset({Capability.CREATOR_PROFILE, Capability.VIDEO_LANGUAGES})

    def __init__(self, api_key: str, *, transport: httpx.AsyncBaseTransport | None = None):
        self.api_key = api_key
        self.http = ResilientHTTP(
            PROVIDER, "https://www.googleapis.com/youtube/v3", timeout=15.0, max_retries=2,
            rate_limiter=TokenBucket(2.0, burst=2), breaker=CircuitBreaker(5, 120), transport=transport,
            secrets=(api_key,),
        )

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def estimate_cost(self, operation: Capability, **params: Any) -> CostEstimate:
        # Quota units, not dollars: 1 per list call.
        units = Decimal("2") if operation == Capability.VIDEO_LANGUAGES else Decimal("1")
        return CostEstimate(PROVIDER, operation.value, units, Decimal("0"), basis="quota_units")

    async def get_creator_profile(self, platform_account_id: str) -> ProviderResult[NormalizedCreator]:
        op = Capability.CREATOR_PROFILE.value
        try:
            resp = await self.http.request(
                "GET", "/channels", params={"part": "snippet", "id": platform_account_id, "key": self.api_key}
            )
        except ProviderError as exc:
            return ProviderResult(PROVIDER, op, ResultStatus.ERROR, error=exc.message, error_code=exc.code)
        items = resp.json().get("items") or []
        if not items:
            return ProviderResult(PROVIDER, op, ResultStatus.NOT_FOUND, cost=CostActual(Decimal("1")))
        snippet = items[0].get("snippet", {})
        now = utcnow()
        prov = Provenance(
            provider=PROVIDER, operation=op, source_record=platform_account_id, retrieved_at=now,
            data_type="channel_verification", measurement=MeasurementType.OBSERVED,
            retention_policy=f"{PROVIDER}:30d", commercial_use=CommercialUse.INTERNAL_DISPLAY_ONLY,
            raw_ref=f"youtube:channel:{platform_account_id}", derived_metrics_prohibited=True,
        )
        return ProviderResult(
            PROVIDER, op, ResultStatus.OK,
            NormalizedCreator(
                platform="youtube", platform_account_id=platform_account_id, handle=snippet.get("customUrl"),
                display_name=snippet.get("title"), url=f"https://www.youtube.com/channel/{platform_account_id}",
                country=snippet.get("country"), language=snippet.get("defaultLanguage"), provenance=prov,
            ),
            CostActual(Decimal("1")),
        )


    async def get_video_languages(self, platform_account_id: str, limit: int = 10) -> ProviderResult[VideoLanguages]:
        """The `defaultAudioLanguage` of the channel's `limit` newest uploads, from
        its uploads playlist (the channel id with UC replaced by UU)."""
        op = Capability.VIDEO_LANGUAGES.value
        if not platform_account_id.startswith("UC"):
            return ProviderResult(PROVIDER, op, ResultStatus.NOT_FOUND, error="not a YouTube channel id")
        try:
            uploads = await self.http.request("GET", "/playlistItems", params={
                "part": "contentDetails", "playlistId": "UU" + platform_account_id[2:], "maxResults": limit,
                "key": self.api_key})
            ids = [i["contentDetails"]["videoId"] for i in uploads.json().get("items") or []]
            declared: dict[str, str] = {}
            if ids:
                videos = await self.http.request("GET", "/videos", params={
                    "part": "snippet", "id": ",".join(ids), "key": self.api_key})
                declared = {v["id"]: v.get("snippet", {}).get("defaultAudioLanguage")
                            for v in videos.json().get("items") or []}
        except ProviderNotFound:
            return ProviderResult(PROVIDER, op, ResultStatus.NOT_FOUND, cost=CostActual(Decimal("1")))
        except ProviderError as exc:
            return ProviderResult(PROVIDER, op, ResultStatus.ERROR, error=exc.message, error_code=exc.code)
        prov = Provenance(
            provider=PROVIDER, operation=op, source_record=platform_account_id, retrieved_at=utcnow(),
            data_type="video_languages", measurement=MeasurementType.OBSERVED,
            retention_policy=f"{PROVIDER}:30d", commercial_use=CommercialUse.INTERNAL_DISPLAY_ONLY,
            raw_ref=f"youtube:channel:{platform_account_id}", derived_metrics_prohibited=True,
        )
        return ProviderResult(
            PROVIDER, op, ResultStatus.OK,
            VideoLanguages(platform_account_id=platform_account_id, languages=[declared.get(i) for i in ids],
                           provenance=prov),
            CostActual(Decimal("2") if ids else Decimal("1")),
        )

def youtube_expiry(retrieved_at) -> Any:
    return retrieved_at + timedelta(days=30)
