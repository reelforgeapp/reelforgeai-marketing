"""
Supadata transcript adapter (optional fallback when CreatorDB has no
subtitles). Retrieval is selective: the enrichment value gate only requests a
transcript for stage-3+ creators whose classification is uncertain.

Verified (https://supadata.ai/, 2026-10-25): free plan of 100 requests; docs
at https://docs.supadata.ai. NOT verified: endpoint path, auth header,
credits per transcript, rate limits and downstream-use terms. The path and
header below are provisional and the adapter is disabled unless
SUPADATA_API_KEY is set. Transcripts are machine output and are never treated
as perfectly accurate.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx

from rpi.compliance.policies import get_policy
from rpi.core.provenance import CommercialUse, MeasurementType, Provenance, utcnow
from rpi.providers.base import (
    Capability,
    CostActual,
    CostEstimate,
    ProviderAdapter,
    ProviderResult,
    ResultStatus,
    Transcript,
)
from rpi.providers.http import CircuitBreaker, ProviderError, ProviderNotFound, ResilientHTTP, TokenBucket

PROVIDER = "supadata"
TRANSCRIPT_PATH = "/v1/youtube/transcript"  # provisional


class SupadataAdapter(ProviderAdapter):
    name = PROVIDER
    policy_key = PROVIDER
    capabilities = frozenset({Capability.TRANSCRIPT})

    def __init__(self, api_key: str, *, usd_per_request: float = 0.0, transport: httpx.AsyncBaseTransport | None = None):
        self.api_key = api_key
        self.usd_per_request = Decimal(str(usd_per_request))
        self.http = ResilientHTTP(
            PROVIDER, "https://api.supadata.ai", headers={"x-api-key": api_key}, timeout=40.0, max_retries=2,
            rate_limiter=TokenBucket(1.0, burst=1), breaker=CircuitBreaker(4, 300), transport=transport, secrets=(api_key,),
        )

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def estimate_cost(self, operation: Capability, **params: Any) -> CostEstimate:
        if operation != Capability.TRANSCRIPT:
            return CostEstimate(PROVIDER, operation.value, Decimal("0"), Decimal("0"), basis="unsupported")
        return CostEstimate(PROVIDER, operation.value, Decimal("1"), self.usd_per_request, basis="configured_rate")

    async def get_transcript(self, platform_content_id: str) -> ProviderResult[Transcript]:
        op = Capability.TRANSCRIPT.value
        try:
            resp = await self.http.request("GET", TRANSCRIPT_PATH, params={"videoId": platform_content_id, "text": "true"})
        except ProviderNotFound:
            return ProviderResult(PROVIDER, op, ResultStatus.NOT_FOUND, cost=self._cost())
        except ProviderError as exc:
            return ProviderResult(PROVIDER, op, ResultStatus.ERROR, error=exc.message, error_code=exc.code)
        payload = resp.json()
        content = payload.get("content") if isinstance(payload, dict) else None
        if isinstance(content, list):
            text = " ".join(str(seg.get("text", "")) for seg in content if isinstance(seg, dict))
        else:
            text = content if isinstance(content, str) else ""
        if not text.strip():
            return ProviderResult(PROVIDER, op, ResultStatus.NOT_FOUND, cost=self._cost(), missing_fields=["content"])
        policy = get_policy(PROVIDER)
        prov = Provenance(
            provider=PROVIDER, operation=op, source_record=platform_content_id, retrieved_at=utcnow(),
            data_type="transcript", measurement=MeasurementType.OBSERVED, confidence=None,
            retention_policy=f"{PROVIDER}:{policy.max_storage_days}d", commercial_use=CommercialUse.INTERNAL_ONLY,
        )
        return ProviderResult(PROVIDER, op, ResultStatus.OK,
                              Transcript(platform_content_id=platform_content_id, language=payload.get("lang"),
                                         text=text, is_auto_generated=None, provenance=prov),
                              self._cost())

    def _cost(self) -> CostActual:
        return CostActual(units=Decimal("1"), usd=self.usd_per_request)
