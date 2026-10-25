"""
YouTube Data API adapter — verification only.

Used to confirm a channel exists, read its public title/handle and link to
source content. Never used for scoring: every record is marked
`derived_metrics_prohibited` (Developer Policies III.E.4(h)) and expires after
30 days (III.E.4(c)/(d)). One API project, one key; no key rotation (III.D.1(c)).
channels.list is the only call made, so the existing Partner Scout search.list
quota usage is not increased.
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
)
from rpi.providers.http import CircuitBreaker, ProviderError, ResilientHTTP, TokenBucket

PROVIDER = "youtube_data_api"


class YouTubeDataAdapter(ProviderAdapter):
    name = PROVIDER
    policy_key = PROVIDER
    capabilities = frozenset({Capability.CREATOR_PROFILE})

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
        # Quota units, not dollars. channels.list is a low-cost read.
        return CostEstimate(PROVIDER, operation.value, Decimal("1"), Decimal("0"), basis="quota_units")

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


def youtube_expiry(retrieved_at) -> Any:
    return retrieved_at + timedelta(days=30)
