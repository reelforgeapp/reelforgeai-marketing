"""
SparkToro adapter — selective audience-first research.

Verified from https://sparktoro.com/api/docs on 2026-10-25:
  * base https://api.sparktoro.com, `Authorization: Bearer <key>`
  * POST /v3/describe/create — 10 credits, builds an audience report from a
    prompt (location us/ca/uk)
  * GET /v3/youtube — 1 credit, top YouTube channels for a report
  * GET /v3/account/credits — free
  * each (endpoint, report_id) is charged once per billing period
  * 202 = section still building; 402 = insufficient credits (not charged,
    body has credits_required / credits_remaining); 429 with Retry-After;
    5xx refunded
  * 10 req/min on trial, 60 req/min paid

Not verified: response field names (report id, channel list fields). They
are isolated in SPARKTORO_FIELDS below.

SparkToro results describe where an *audience* spends attention. RPI uses
them only to discover candidate channels, which are then cross-referenced
against the creator index. They are never used as a measurement of a
specific channel's viewers.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Optional

import httpx

from rpi.compliance.policies import get_policy
from rpi.core.provenance import CommercialUse, MeasurementType, Provenance, utcnow
from rpi.providers.base import (
    AudienceResearchResult,
    Capability,
    CostActual,
    CostEstimate,
    ProviderAdapter,
    ProviderHealth,
    ProviderResult,
    ResultStatus,
)
from rpi.providers.http import (
    CircuitBreaker,
    ProviderCreditsExhausted,
    ProviderError,
    ResilientHTTP,
    TokenBucket,
)
from rpi.providers.mapping import first_list, get_path

PROVIDER = "sparktoro"
CREDITS = {"describe_create": Decimal("10"), "youtube": Decimal("1"), "credits": Decimal("0")}

SPARKTORO_FIELDS = {
    "report_id": ("report_id", "data.report_id", "id"),  # unverified
    "channel_list": ("data", "results", "channels", "data.channels"),  # unverified
    "channel_id": ("channel_id", "id", "youtube_id"),  # unverified
    "channel_name": ("name", "title", "channel_name"),  # unverified
    "channel_url": ("url", "channel_url"),  # unverified
}


def _first(record: Any, paths: tuple[str, ...]) -> Any:
    for p in paths:
        v = get_path(record, p)
        if v not in (None, ""):
            return v
    return None


class SparkToroAdapter(ProviderAdapter):
    name = PROVIDER
    policy_key = PROVIDER
    capabilities = frozenset({Capability.AUDIENCE_RESEARCH, Capability.USAGE_COSTS})

    def __init__(self, api_key: str, *, usd_per_credit: float = 0.0, rate_per_second: float = 0.9,
                 location: str = "us", transport: httpx.AsyncBaseTransport | None = None):
        self.api_key = api_key
        self.location = location
        self.usd_per_credit = Decimal(str(usd_per_credit))
        self.http = ResilientHTTP(
            PROVIDER, "https://api.sparktoro.com",
            headers={"Authorization": f"Bearer {api_key}", "accept": "application/json"},
            timeout=30.0, max_retries=3, rate_limiter=TokenBucket(rate_per_second, burst=1),
            breaker=CircuitBreaker(failure_threshold=4, cooldown_seconds=300),
            transport=transport, secrets=(api_key,),
        )

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def estimate_cost(self, operation: Capability, **params: Any) -> CostEstimate:
        if operation == Capability.AUDIENCE_RESEARCH:
            units = CREDITS["describe_create"] + CREDITS["youtube"]
            return CostEstimate(PROVIDER, operation.value, units, units * self.usd_per_credit, basis="documented_credits")
        return CostEstimate(PROVIDER, operation.value, Decimal("0"), Decimal("0"), basis="documented_free")

    def _provenance(self, report_id: Optional[str]) -> Provenance:
        policy = get_policy(PROVIDER)
        return Provenance(
            provider=PROVIDER, operation="research_audience", source_record=report_id, retrieved_at=utcnow(),
            data_type="audience_attention_sources", measurement=MeasurementType.THIRD_PARTY_MODELED,
            retention_policy=f"{PROVIDER}:{policy.max_storage_days}d", commercial_use=CommercialUse.INTERNAL_ONLY,
            raw_ref=f"sparktoro:report:{report_id}" if report_id else None, response_version="v3",
        )

    async def research_audience(self, query: str) -> ProviderResult[AudienceResearchResult]:
        op = Capability.AUDIENCE_RESEARCH.value
        spent = Decimal("0")
        try:
            resp = await self.http.request("POST", "/v3/describe/create", json={"prompt": query, "location": self.location})
            spent += CREDITS["describe_create"]
            report_id = _first(resp.json(), SPARKTORO_FIELDS["report_id"])
            if not report_id:
                return ProviderResult(PROVIDER, op, ResultStatus.ERROR, cost=self._cost(spent),
                                      error="report id not found in describe/create response", error_code="schema_mismatch")
            yt = await self.http.request("GET", "/v3/youtube", params={"report_id": report_id})
            if yt.status_code == 202:
                return ProviderResult(PROVIDER, op, ResultStatus.PARTIAL,
                                      AudienceResearchResult(query=query, sources=[], provenance=self._provenance(str(report_id))),
                                      self._cost(spent), error="report section still building; retry later",
                                      next_page_token=str(report_id))
            spent += CREDITS["youtube"]
            payload = yt.json()
        except ProviderCreditsExhausted as exc:
            # Documented: 402 responses are not charged.
            return ProviderResult(PROVIDER, op, ResultStatus.ERROR, cost=self._cost(spent), error=exc.message, error_code=exc.code)
        except ProviderError as exc:
            return ProviderResult(PROVIDER, op, ResultStatus.ERROR, cost=self._cost(spent), error=exc.message, error_code=exc.code)

        rows = _first_list(payload)
        sources = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            sources.append({
                "platform": "youtube",
                "platform_account_id": _first(row, SPARKTORO_FIELDS["channel_id"]),
                "name": _first(row, SPARKTORO_FIELDS["channel_name"]),
                "url": _first(row, SPARKTORO_FIELDS["channel_url"]),
                "raw_keys": sorted(row.keys())[:20],
            })
        return ProviderResult(PROVIDER, op, ResultStatus.OK if sources else ResultStatus.PARTIAL,
                              AudienceResearchResult(query=query, sources=sources, provenance=self._provenance(str(report_id))),
                              self._cost(spent), [] if sources else ["channel_list"])

    async def get_usage_costs(self) -> ProviderResult[dict[str, Any]]:
        try:
            resp = await self.http.request("GET", "/v3/account/credits")
        except ProviderError as exc:
            return ProviderResult(PROVIDER, Capability.USAGE_COSTS.value, ResultStatus.ERROR, error=exc.message, error_code=exc.code)
        return ProviderResult(PROVIDER, Capability.USAGE_COSTS.value, ResultStatus.OK, resp.json())

    async def health_check(self) -> ProviderHealth:
        if not self.is_configured():
            return ProviderHealth(PROVIDER, False, None, self.http.breaker.state, "not configured")
        result = await self.get_usage_costs()
        return ProviderHealth(PROVIDER, True, result.ok, self.http.breaker.state,
                              "credits endpoint (free)" if result.ok else (result.error or ""), utcnow())

    def _cost(self, credits: Decimal) -> CostActual:
        return CostActual(units=credits, usd=credits * self.usd_per_credit, reported_by_provider=False)


def _first_list(payload: Any) -> list:
    return first_list(payload, *SPARKTORO_FIELDS["channel_list"]) or (payload if isinstance(payload, list) else [])
