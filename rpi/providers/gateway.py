"""
Provider gateway: the only way business code calls a provider.

For each call it
  1. refuses unsupported / unconfigured capabilities explicitly,
  2. returns a cached result when the provider's licensing policy allows
     caching and the entry is fresh (duplicate-request prevention),
  3. reserves the estimated cost with the budget controller,
  4. calls the adapter, then settles the actual cost (or releases the
     reservation when nothing was charged),
  5. records the request (provider, operation, status, cost, duration),
  6. stores the result in the cache with a policy-bounded TTL.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional, Protocol

import structlog

from rpi.administration.budget import BudgetBackend, Priority, ReservationRequest
from rpi.compliance.policies import get_policy
from rpi.providers.base import (
    AudienceBreakdown,
    AudienceResearchResult,
    Capability,
    NormalizedContact,
    NormalizedContent,
    NormalizedCreator,
    NormalizedSponsorship,
    ProviderAdapter,
    ProviderResult,
    ResultStatus,
    Transcript,
)

logger = structlog.get_logger()

RESULT_MODELS = {
    Capability.SEARCH_CREATORS: (list, NormalizedCreator),
    Capability.CREATOR_PROFILE: (None, NormalizedCreator),
    Capability.AUDIENCE_DEMOGRAPHICS: (None, AudienceBreakdown),
    Capability.AUDIENCE_INTERESTS: (None, AudienceBreakdown),
    Capability.CREATOR_CONTENT: (list, NormalizedContent),
    Capability.SPONSORSHIP_HISTORY: (list, NormalizedSponsorship),
    Capability.CREATOR_CONTACTS: (list, NormalizedContact),
    Capability.CREATOR_PERFORMANCE: (None, NormalizedCreator),
    Capability.TRANSCRIPT: (None, Transcript),
    Capability.AUTHORIZED_ANALYTICS: (None, AudienceBreakdown),
    Capability.AUDIENCE_RESEARCH: (None, AudienceResearchResult),
}

# Contact data is never cached; it must be re-read from the canonical store.
NEVER_CACHE = {Capability.CREATOR_CONTACTS, Capability.USAGE_COSTS}


class Cache(Protocol):
    async def get(self, key: str) -> Optional[dict]: ...

    async def put(self, key: str, provider: str, operation: str, payload: dict, ttl: timedelta) -> None: ...


class RequestLog(Protocol):
    async def record(self, entry: dict) -> None: ...


class MemoryCache:
    def __init__(self):
        self.items: dict[str, tuple[datetime, dict]] = {}

    async def get(self, key: str) -> Optional[dict]:
        hit = self.items.get(key)
        if not hit or hit[0] < datetime.now(timezone.utc):
            return None
        return hit[1]

    async def put(self, key, provider, operation, payload, ttl) -> None:
        self.items[key] = (datetime.now(timezone.utc) + ttl, payload)


class MemoryRequestLog:
    def __init__(self):
        self.entries: list[dict] = []

    async def record(self, entry: dict) -> None:
        self.entries.append(entry)


class PgCache:
    def __init__(self, pool):
        self.pool = pool

    async def get(self, key: str) -> Optional[dict]:
        row = await self.pool.fetchrow(
            "SELECT payload FROM rpi_provider_cache WHERE cache_key = $1 AND expires_at > NOW()", key
        )
        if not row:
            return None
        payload = row["payload"]
        return json.loads(payload) if isinstance(payload, str) else payload

    async def put(self, key, provider, operation, payload, ttl) -> None:
        await self.pool.execute(
            """INSERT INTO rpi_provider_cache (cache_key, provider, operation, payload, expires_at)
               VALUES ($1, $2, $3, $4::jsonb, NOW() + $5::interval)
               ON CONFLICT (cache_key) DO UPDATE SET payload = EXCLUDED.payload, expires_at = EXCLUDED.expires_at,
                                                     created_at = NOW()""",
            key, provider, operation, json.dumps(payload, default=str), ttl,
        )


class PgRequestLog:
    def __init__(self, pool):
        self.pool = pool

    async def record(self, entry: dict) -> None:
        await self.pool.execute(
            """INSERT INTO rpi_provider_requests
               (provider, operation, request_hash, job_id, creator_id, status, cost_units, cost_usd,
                cost_reported_by_provider, duration_ms, error_code, error, provider_request_id, attempts)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)""",
            entry["provider"], entry["operation"], entry["request_hash"], entry.get("job_id"), entry.get("creator_id"),
            entry["status"], entry["cost_units"], entry["cost_usd"], entry["cost_reported"], entry["duration_ms"],
            entry.get("error_code"), (entry.get("error") or "")[:500] or None, entry.get("provider_request_id"),
            entry.get("attempts", 1),
        )


@dataclass
class CallContext:
    job_id: Optional[str] = None
    creator_id: Optional[str] = None
    priority: Priority = Priority.NORMAL
    job_budget_usd: Optional[Decimal] = None
    use_cache: bool = True


def request_key(provider: str, operation: Capability, args: tuple, kwargs: dict) -> str:
    def norm(v):
        if hasattr(v, "model_dump"):
            return v.model_dump(mode="json")
        return v

    raw = json.dumps({"p": provider, "o": operation.value, "a": [norm(a) for a in args],
                      "k": {k: norm(v) for k, v in sorted(kwargs.items())}}, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _serialize(data: Any) -> Any:
    if isinstance(data, list):
        return [d.model_dump(mode="json") if hasattr(d, "model_dump") else d for d in data]
    if hasattr(data, "model_dump"):
        return data.model_dump(mode="json")
    return data


def _deserialize(operation: Capability, payload: Any) -> Any:
    container, model = RESULT_MODELS.get(operation, (None, None))
    if model is None:
        return payload
    if container is list:
        return [model.model_validate(p) for p in payload or []]
    return model.model_validate(payload) if payload is not None else None


@dataclass
class ProviderGateway:
    budget: BudgetBackend
    cache: Cache = field(default_factory=MemoryCache)
    request_log: RequestLog = field(default_factory=MemoryRequestLog)

    async def call(
        self,
        adapter: ProviderAdapter,
        operation: Capability,
        *args: Any,
        ctx: Optional[CallContext] = None,
        **kwargs: Any,
    ) -> ProviderResult:
        ctx = ctx or CallContext()
        if not adapter.is_configured():
            return ProviderResult(adapter.name, operation.value, ResultStatus.NOT_CONFIGURED,
                                  error=f"{adapter.name} is not configured")
        if not adapter.supports(operation):
            return ProviderResult.unsupported(adapter.name, operation.value)

        key = request_key(adapter.name, operation, args, kwargs)
        policy = get_policy(adapter.policy_key)
        cacheable = operation not in NEVER_CACHE and (policy.cache_ttl_days or 0) > 0
        if cacheable and ctx.use_cache:
            hit = await self.cache.get(key)
            if hit is not None:
                await self._log(adapter, operation, key, ctx, "cached", Decimal("0"), Decimal("0"), False, 0)
                return ProviderResult(
                    adapter.name, operation.value, ResultStatus.CACHED, _deserialize(operation, hit.get("data")),
                    missing_fields=hit.get("missing_fields", []), next_page_token=hit.get("next_page_token"),
                    unapplied_criteria=hit.get("unapplied_criteria", []), applied_criteria=hit.get("applied_criteria", []),
                    total_available=hit.get("total_available"),
                )

        estimate = adapter.estimate_cost(operation, **kwargs)
        reservation = await self.budget.reserve(ReservationRequest(
            adapter.name, operation.value, estimate.usd, ctx.priority, ctx.job_id, ctx.creator_id, ctx.job_budget_usd,
        ))
        if not reservation.allowed:
            await self._log(adapter, operation, key, ctx, "budget_denied", Decimal("0"), Decimal("0"), False, 0,
                            error=reservation.decision.reason)
            return ProviderResult(adapter.name, operation.value, ResultStatus.BUDGET_DENIED,
                                  error=reservation.decision.reason, error_code="budget_denied")

        started = time.monotonic()
        try:
            result: ProviderResult = await getattr(adapter, operation.value)(*args, **kwargs)
        except Exception as exc:  # adapters should not raise, but never leak a reservation
            await self.budget.release(reservation.id)
            await self._log(adapter, operation, key, ctx, "error", Decimal("0"), Decimal("0"), False,
                            int((time.monotonic() - started) * 1000), error=str(exc), error_code="adapter_exception")
            logger.error("rpi_adapter_exception", provider=adapter.name, operation=operation.value, error=str(exc))
            return ProviderResult(adapter.name, operation.value, ResultStatus.ERROR, error=str(exc),
                                  error_code="adapter_exception")

        duration = int((time.monotonic() - started) * 1000)
        if result.cost.usd > 0 or result.cost.units > 0:
            await self.budget.settle(reservation.id, result.cost.usd, result.cost.units)
        else:
            await self.budget.release(reservation.id)

        http = getattr(adapter, "http", None)
        last = getattr(http, "last_call", None)
        await self._log(adapter, operation, key, ctx, result.status.value, result.cost.units, result.cost.usd,
                        result.cost.reported_by_provider, duration, error=result.error, error_code=result.error_code,
                        provider_request_id=result.request_id or (last.request_id if last else None),
                        attempts=last.attempts if last else 1)

        if cacheable and result.ok and result.status != ResultStatus.CACHED:
            ttl = timedelta(days=policy.cache_ttl_days or 0)
            await self.cache.put(key, adapter.name, operation.value, {
                "data": _serialize(result.data), "missing_fields": result.missing_fields,
                "next_page_token": result.next_page_token, "unapplied_criteria": result.unapplied_criteria,
                "applied_criteria": result.applied_criteria, "total_available": result.total_available,
            }, ttl)
        return result

    async def _log(self, adapter, operation, key, ctx, status, units, usd, reported, duration, **extra):
        try:
            await self.request_log.record({
                "provider": adapter.name, "operation": operation.value, "request_hash": key,
                "job_id": ctx.job_id, "creator_id": ctx.creator_id, "status": status, "cost_units": units,
                "cost_usd": usd, "cost_reported": reported, "duration_ms": duration, **extra,
            })
        except Exception as exc:  # metering failure must be visible but must not hide the result
            logger.error("rpi_request_log_failed", provider=adapter.name, error=str(exc))
