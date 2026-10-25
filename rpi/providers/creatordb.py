"""
CreatorDB API v3 adapter (primary discovery and creator-profile provider).

Verified against CreatorDB's official OpenAPI 3.1 spec ("CreatorDB Headless
API V3", docs.creatordb.app/api-v3.yaml) and its credit pages
(api-v3/api-credit-usage, api-v3/field-and-item-costs,
api-v3/configuring_advanced_search_filters) on 2026-10-25:

  * base URL https://apiv3.creatordb.app, `api-key` request header
  * every endpoint path, request body and response field used below
  * credits per call (CREDITS_PER_CALL); responses report `creditsUsed`,
    which is what gets settled against the budget
  * a direct query that returns nothing costs 0 credits; a search with no
    matches is still charged
  * errors: 400 VALIDATION_ERROR, 403 FRACTIONAL_CALLS_NOT_ENABLED,
    429 RATE_LIMIT_EXCEEDED (retry) or QUOTA_EXCEEDED (out of credits)

Checked live on 2026-10-25 with scripts/rpi_verify_creatordb.py --probe:
every endpoint below returned the mapped fields; responses also carry
fields the spec omits (profile: hasEmail, isAgencyRepresented, isPodcast,
joinedDate, totalViews; performance: videoPrice/shortsPrice estimates;
sponsorship: detectedBrandNamesByVideo), which are not used yet. CreatorDB
publishes no API terms of service, so scoring on CreatorDB fields stays off
until the customer agreement is reviewed (RPI_CREATORDB_SCORING_PERMITTED).

Not used yet: fractional calls (per-field billing and search enrichment
need the feature enabled on the key), /youtube/performance-history,
/youtube/subtitles/*, and /sponsor/creators for competitor discovery.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from decimal import Decimal
from typing import Any, Optional

import httpx

from rpi.compliance.policies import get_policy
from rpi.core.provenance import CommercialUse, MeasurementType, Provenance, utcnow
from rpi.core.redaction import redact_text
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
    ProviderHealth,
    ProviderResult,
    ResultStatus,
)
from rpi.providers.http import (
    CircuitBreaker,
    ProviderBadRequest,
    ProviderCreditsExhausted,
    ProviderError,
    ProviderNotFound,
    ProviderRateLimited,
    ResilientHTTP,
    TokenBucket,
    default_classify_error,
)
from rpi.providers.iso_codes import country_to_a2, country_to_a3, language_to_639_1, language_to_639_3
from rpi.providers.mapping import EndpointSpec, FieldSpec, coerce, critical_missing, extract, get_path

PROVIDER = "creatordb"
RESPONSE_VERSION = "v3"
SPEC_SOURCE = "docs.creatordb.app/api-v3.yaml (2026-10-25)"

ENDPOINTS: dict[str, EndpointSpec] = {
    "usage": EndpointSpec("GET", "/usage", verified=True, cost_units=0, cost_verified=True, notes="free"),
    "search": EndpointSpec("POST", "/youtube/search", verified=True, cost_units=1, cost_verified=True,
                           notes="1 credit per 10 filters per page; charged even with no matches"),
    "profile": EndpointSpec("POST", "/youtube/profile", verified=True, cost_units=2, cost_verified=True),
    "performance": EndpointSpec("POST", "/youtube/performance", verified=True, cost_units=3, cost_verified=True),
    "audience": EndpointSpec("POST", "/youtube/audience", verified=True, cost_units=10, cost_verified=True),
    "content": EndpointSpec("POST", "/youtube/content-detail", verified=True, cost_units=3, cost_verified=True),
    "sponsorship": EndpointSpec("POST", "/youtube/sponsorship", verified=True, cost_units=5, cost_verified=True),
    "contact": EndpointSpec("POST", "/youtube/contact", verified=True, cost_units=15, cost_verified=True,
                            notes="charged only when at least one email is returned"),
}

ENDPOINT_CAPABILITY = {
    "usage": Capability.USAGE_COSTS,
    "search": Capability.SEARCH_CREATORS,
    "profile": Capability.CREATOR_PROFILE,
    "performance": Capability.CREATOR_PERFORMANCE,
    "audience": Capability.AUDIENCE_DEMOGRAPHICS,
    "content": Capability.CREATOR_CONTENT,
    "sponsorship": Capability.SPONSORSHIP_HISTORY,
    "contact": Capability.CREATOR_CONTACTS,
}

# Paths into a /youtube/profile `data` object. A /youtube/search result has
# channelId, uniqueId, displayName and totalSubscribers at the top level.
CREATORDB_FIELDS: dict[str, FieldSpec] = {
    "platform_account_id": FieldSpec("channelId", "str", verified=True, critical=True),
    "handle": FieldSpec("uniqueId", "str", verified=True),
    "display_name": FieldSpec("displayName", "str", verified=True),
    "description": FieldSpec("bio", "str", verified=True),
    "country": FieldSpec("country", "str", verified=True),  # ISO 3166-1 alpha-3, "" when unknown
    "language": FieldSpec("mainLanguage", "str", verified=True),  # ISO 639-3
    "subscribers": FieldSpec("totalSubscribers", "int", verified=True),
    "total_videos": FieldSpec("totalContents", "int", verified=True),  # videos + shorts
    "last_upload_at": FieldSpec("lastPublishTime", "datetime", verified=True),  # epoch ms
}

# Paths into a /youtube/performance `data` object (last 20 long-form videos).
PERFORMANCE_FIELDS: dict[str, FieldSpec] = {
    "avg_views": FieldSpec("videosPerformanceRecent.views.avg", "float", verified=True),
    "median_views": FieldSpec("videosPerformanceRecent.views.median", "float", verified=True),
    "engagement_rate": FieldSpec("videosPerformanceRecent.engagement.avgEngagementRate", "ratio", verified=True),
    "uploads_last_90d": FieldSpec("contentCountByDays.90d", "int", verified=True),
}

# Items of /youtube/content-detail `recentVideos` and `recentShorts`.
CONTENT_FIELDS: dict[str, FieldSpec] = {
    "platform_content_id": FieldSpec("contentId", "str", verified=True, critical=True),
    "title": FieldSpec("title", "str", verified=True),
    "description": FieldSpec("description", "str", verified=True),
    "published_at": FieldSpec("publishTime", "datetime", verified=True),
    "duration_seconds": FieldSpec("length", "int", verified=True),
    "views": FieldSpec("views", "int", verified=True),
    "likes": FieldSpec("likes", "int", verified=True),
    "comments": FieldSpec("comments", "int", verified=True),
    "sponsored_flag": FieldSpec("isSponsored", "bool", verified=True),  # recentVideos only
}

# /youtube/audience: dimension -> (array path, bucket key)
AUDIENCE_DIMENSIONS = {"country": ("audienceLocations", "country"), "age": ("audienceAgeBreakdown", "ageRange"),
                       "gender": ("audienceGender", None)}

# Median long-form views per video (robust to one viral upload); CreatorDB's
# default sort is alphabetical by display name.
DEFAULT_SORT = "medianRecentVideosViews"
FILTERS_PER_CREDIT = 10
CREDIT_ERROR_MARKERS = ("credit", "quota", "insufficient", "balance", "spend limit")


class FractionalCallsNotEnabled(ProviderError):
    code = "fractional_calls_not_enabled"


def _error_body(response: httpx.Response) -> tuple[str, str]:
    try:
        body = response.json()
    except (json.JSONDecodeError, ValueError):
        return "", response.text or ""
    if not isinstance(body, dict):
        return "", response.text or ""
    code = str(body.get("error") or body.get("errorCode") or "")
    message = str(body.get("message") or body.get("errorDescription") or "")
    return code, message


def classify_creatordb_error(provider: str, response: httpx.Response) -> Optional[ProviderError]:
    base = default_classify_error(provider, response)
    if base is None:
        return None
    status = response.status_code
    code, message = _error_body(response)
    detail = redact_text(f"{code}: {message}"[:300]) if code or message else ""
    if code == "QUOTA_EXCEEDED":
        return ProviderCreditsExhausted(provider, detail or "credit quota exceeded", status)
    if code == "RATE_LIMIT_EXCEEDED":
        return ProviderRateLimited(provider, "rate limited", status)
    if code == "FRACTIONAL_CALLS_NOT_ENABLED":
        return FractionalCallsNotEnabled(provider, "fractional calls are not enabled on this API key", status)
    if status in (400, 402, 403, 429) and any(m in f"{code} {message} {response.text or ''}".lower()
                                              for m in CREDIT_ERROR_MARKERS):
        return ProviderCreditsExhausted(provider, detail or "credits exhausted or insufficient", status)
    if status == 400 and detail:
        return ProviderBadRequest(provider, detail, status)
    return base


def hashtag(keyword: str) -> Optional[str]:
    """'Faceless YouTube channel' -> '#facelessyoutubechannel' (CreatorDB stores hashtags lowercase with '#')."""
    tag = re.sub(r"[^0-9a-z_]", "", keyword.lower())
    return f"#{tag}" if tag else None


def topic_label(topic_id: str) -> str:
    """'id_youtubeautomation_ScienceTechnology' -> 'youtubeautomation'."""
    parts = str(topic_id).split("_")
    return parts[1] if len(parts) >= 3 and parts[0] == "id" else str(topic_id)


AUDIENCE_AGE_BANDS = ("13-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+")
SORT_FIELDS = {"median_views": "medianRecentVideosViews", "avg_views": "avgRecentVideosViews",
               "engagement": "avgRecentVideosEngagementRate", "subscribers": "totalSubscribers"}


def build_search_body(criteria: CreatorSearchCriteria, *, now_ms: Optional[int] = None) -> tuple[dict, list[str], list[str]]:
    """Translate provider-neutral criteria into CreatorDB filters (all ANDed;
    values inside one `in` filter are ORed). Returns (body, unapplied, applied)."""
    filters: list[dict[str, Any]] = []
    unapplied: list[str] = []
    applied: list[str] = []

    def add(criterion: str, name: str, op: str, value: Any) -> None:
        filters.append({"filterName": name, "op": op, "value": value})
        if criterion not in applied:
            applied.append(criterion)

    def codes(criterion: str, values: list[str], convert) -> list[str]:
        out = []
        for v in values:
            code = convert(v)
            if code:
                out.append(code)
            else:
                unapplied.append(f"{criterion}:{v}")
        return sorted(set(out))

    tags = sorted({t for t in (hashtag(k) for k in criteria.keywords) if t})
    if tags:
        add("keywords", "hashtags", "in", tags)
    if criteria.topics:
        add("topics", "topics", "in", list(criteria.topics))
    if criteria.niches:
        add("niches", "niches", "in", list(criteria.niches))
    if languages := codes("languages", criteria.languages, language_to_639_3):
        add("languages", "mainLanguage", "in", languages)
    if countries := codes("countries", criteria.countries, country_to_a3):
        add("countries", "country", "in", countries)
    if audience := codes("audience_countries", criteria.audience_countries, country_to_a3):
        add("audience_countries", "mainAudienceLocation", "in", audience)
    if ages := codes("audience_ages", criteria.audience_ages, lambda a: a if a in AUDIENCE_AGE_BANDS else None):
        add("audience_ages", "mainAudienceAge", "in", ages)
    # CreatorDB number filters are strict (> and <); criteria bounds are inclusive.
    if criteria.min_subscribers is not None:
        add("min_subscribers", "totalSubscribers", ">", criteria.min_subscribers - 1)
    if criteria.max_subscribers is not None:
        add("max_subscribers", "totalSubscribers", "<", criteria.max_subscribers + 1)
    if criteria.min_avg_views is not None:
        add("min_avg_views", "avgRecentVideosViews", ">", criteria.min_avg_views - 1)
    if criteria.min_median_views is not None:
        add("min_median_views", "medianRecentVideosViews", ">", criteria.min_median_views - 1)
    if criteria.min_engagement_rate is not None:
        # Rates are rounded to 4 decimals; subtract half a step to keep the bound inclusive.
        add("min_engagement_rate", "avgRecentVideosEngagementRate", ">", round(criteria.min_engagement_rate - 0.00005, 5))
    if criteria.min_uploads_30d is not None:
        add("min_uploads_30d", "contentsIn30Days", ">", criteria.min_uploads_30d - 1)
    if criteria.active_within_days is not None:
        now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        add("active_within_days", "lastPublishTime", ">", now_ms - criteria.active_within_days * 86_400_000)
    if criteria.require_email:
        add("require_email", "hasEmail", "=", True)
    if criteria.require_sponsorship_history:
        add("require_sponsorship_history", "hasSponsors", "=", True)
    if criteria.sort_by and criteria.sort_by not in SORT_FIELDS:
        unapplied.append(f"sort_by:{criteria.sort_by}")
    body = {"filters": filters, "pageSize": max(1, min(criteria.limit, 100)),
            "offset": int(criteria.page_token or 0),
            "sortBy": SORT_FIELDS.get(criteria.sort_by or "", DEFAULT_SORT), "desc": True}
    return body, unapplied, applied


def search_credits(filter_count: int) -> int:
    return max(1, math.ceil(filter_count / FILTERS_PER_CREDIT))


class CreatorDBAdapter(ProviderAdapter):
    name = PROVIDER
    policy_key = PROVIDER

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://apiv3.creatordb.app",
        usd_per_credit: float = 0.0,
        rate_per_second: float = 5.0,
        verified_endpoints: Optional[set[str]] = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self.api_key = api_key
        self.usd_per_credit = Decimal(str(usd_per_credit))
        # Endpoints not in the official spec stay off unless listed in
        # CREATORDB_VERIFIED_ENDPOINTS after a live check.
        enabled = {k for k, e in ENDPOINTS.items() if e.verified} | (verified_endpoints or set())
        self.enabled_endpoints = {k for k in enabled if k in ENDPOINTS}
        self.capabilities = frozenset(ENDPOINT_CAPABILITY[k] for k in self.enabled_endpoints)
        self.credits_available: Optional[Decimal] = None
        self.http = ResilientHTTP(
            PROVIDER,
            base_url,
            headers={"api-key": api_key, "content-type": "application/json", "accept": "application/json"},
            timeout=25.0,
            max_retries=3,
            rate_limiter=TokenBucket(rate_per_second, burst=max(1, int(rate_per_second))),
            breaker=CircuitBreaker(failure_threshold=5, cooldown_seconds=120),
            transport=transport,
            classify_error=classify_creatordb_error,
            secrets=(api_key,),
        )

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def _endpoint_for(self, operation: Capability) -> Optional[str]:
        return next((k for k, c in ENDPOINT_CAPABILITY.items() if c == operation), None)

    def estimate_cost(self, operation: Capability, **params: Any) -> CostEstimate:
        key = self._endpoint_for(operation)
        if key is None or key not in self.enabled_endpoints:
            return CostEstimate(PROVIDER, operation.value, Decimal("0"), Decimal("0"), basis="unsupported")
        spec = ENDPOINTS[key]
        units = Decimal(str(spec.cost_units))
        if operation == Capability.SEARCH_CREATORS:
            criteria = params.get("criteria")
            filters = len(build_search_body(criteria)[0]["filters"]) if criteria is not None else FILTERS_PER_CREDIT
            limit = int(params.get("limit", criteria.limit if criteria is not None else 50))
            units *= max(1, -(-limit // 100)) * search_credits(filters)
        basis = "documented_credits" if spec.cost_verified else "assumed_1_credit_per_call"
        if operation == Capability.CREATOR_CONTACTS:
            basis = "documented_credits_max (0 when no email is returned)"
        return CostEstimate(PROVIDER, operation.value, units, units * self.usd_per_credit, basis=basis)

    def _provenance(self, operation: str, record_ref: Optional[str], data_type: str,
                    measurement: MeasurementType, missing: list[str], total_fields: int,
                    field_measurements: Optional[dict] = None) -> Provenance:
        policy = get_policy(PROVIDER)
        return Provenance(
            provider=PROVIDER,
            operation=operation,
            source_record=record_ref,
            retrieved_at=utcnow(),
            data_type=data_type,
            measurement=measurement,
            completeness=round(1 - len(missing) / total_fields, 3) if total_fields else None,
            retention_policy=f"{PROVIDER}:{policy.max_storage_days}d",
            commercial_use=CommercialUse.INTERNAL_ONLY,
            raw_ref=f"creatordb:{record_ref}" if record_ref else None,
            response_version=RESPONSE_VERSION,
            field_measurements=field_measurements or {},
        )

    def _disabled(self, operation: Capability, endpoint: str) -> ProviderResult:
        return ProviderResult(
            PROVIDER, operation.value, ResultStatus.UNSUPPORTED,
            error=f"CreatorDB endpoint '{endpoint}' is not verified; run scripts/rpi_verify_creatordb.py and add it "
                  f"to CREATORDB_VERIFIED_ENDPOINTS",
        )

    def _error(self, operation: Capability, exc: ProviderError) -> ProviderResult:
        return ProviderResult(PROVIDER, operation.value, ResultStatus.ERROR, error=exc.message, error_code=exc.code,
                              request_id=self._request_id())

    def _request_id(self) -> Optional[str]:
        return self.http.last_call.request_id if self.http.last_call else None

    async def _call(self, endpoint: str, body: Optional[dict] = None, params: Optional[dict] = None) -> dict:
        spec = ENDPOINTS[endpoint]
        response = await self.http.request(spec.method, spec.path, json=body, params=params)
        try:
            payload = response.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise ProviderError(PROVIDER, f"non-JSON response from {spec.path}") from exc
        if not isinstance(payload, dict):
            raise ProviderError(PROVIDER, f"unexpected response type from {spec.path}")
        if payload.get("success") is False:
            code = str(payload.get("errorCode") or payload.get("error") or "")
            message = redact_text(f"{code}: {payload.get('errorDescription') or payload.get('message') or ''}",
                                  (self.api_key,))[:300]
            if code == "QUOTA_EXCEEDED" or any(m in message.lower() for m in CREDIT_ERROR_MARKERS):
                raise ProviderCreditsExhausted(PROVIDER, message, response.status_code)
            raise ProviderError(PROVIDER, message or f"unsuccessful response from {spec.path}", response.status_code)
        available = coerce(payload.get("creditsAvailable"), "float")
        if available is not None:
            self.credits_available = Decimal(str(available))
        return payload

    def _cost(self, endpoint: str, payload: Optional[dict] = None, *, fallback_units: Optional[float] = None) -> CostActual:
        """Prefer the provider-reported `creditsUsed`; otherwise the documented rate."""
        reported = coerce((payload or {}).get("creditsUsed"), "float")
        if reported is not None and reported >= 0:
            units = Decimal(str(reported))
            return CostActual(units=units, usd=units * self.usd_per_credit, reported_by_provider=True)
        units = Decimal(str(ENDPOINTS[endpoint].cost_units if fallback_units is None else fallback_units))
        return CostActual(units=units, usd=units * self.usd_per_credit, reported_by_provider=False)

    @staticmethod
    def _data(payload: dict) -> dict:
        data = payload.get("data")
        return data if isinstance(data, dict) else {}

    def _creator_from_record(self, record: dict, operation: str,
                             performance: Optional[dict] = None) -> tuple[Optional[NormalizedCreator], list[str]]:
        values, missing = extract(record, CREATORDB_FIELDS)
        if critical_missing(CREATORDB_FIELDS, missing):
            return None, missing
        perf: dict[str, Any] = {}
        total = len(CREATORDB_FIELDS)
        if performance is not None:
            perf, perf_missing = extract(performance, PERFORMANCE_FIELDS)
            missing += perf_missing
            total += len(PERFORMANCE_FIELDS)
        field_meas = {"engagement_rate": MeasurementType.THIRD_PARTY_MODELED,
                      "avg_views": MeasurementType.THIRD_PARTY_MODELED,
                      "median_views": MeasurementType.THIRD_PARTY_MODELED,
                      "categories": MeasurementType.THIRD_PARTY_MODELED}
        account_id = values["platform_account_id"]
        prov = self._provenance(operation, account_id, "creator_profile", MeasurementType.OBSERVED, missing, total,
                                field_meas)
        return NormalizedCreator(
            platform="youtube",
            platform_account_id=account_id,
            handle=values.get("handle"),
            display_name=values.get("display_name"),
            url=f"https://www.youtube.com/channel/{account_id}",
            description=values.get("description"),
            country=country_to_a2(values.get("country")),
            language=language_to_639_1(values.get("language")),
            subscribers=values.get("subscribers"),
            avg_views=perf.get("avg_views"),
            median_views=perf.get("median_views"),
            engagement_rate=perf.get("engagement_rate"),
            total_videos=values.get("total_videos"),
            last_upload_at=values.get("last_upload_at"),
            uploads_last_90d=perf.get("uploads_last_90d"),
            categories=self._categories(record),
            provider_creator_id=account_id,
            provenance=prov,
        ), missing

    @staticmethod
    def _categories(record: dict) -> list[str]:
        """YouTube categories by descending share, then CreatorDB topic and niche labels."""
        out: list[str] = []
        breakdown = record.get("categoryBreakdown")
        if isinstance(breakdown, list):
            rows = [b for b in breakdown if isinstance(b, dict) and b.get("category")]
            rows.sort(key=lambda b: coerce(b.get("share"), "float") or 0, reverse=True)
            out += [str(b["category"]) for b in rows]
        for key in ("topics", "niches"):
            items = record.get(key)
            if isinstance(items, list):
                out += [topic_label(t) for t in items if isinstance(t, str) and t]
        return list(dict.fromkeys(out))

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------

    async def get_creator_profile(self, platform_account_id: str) -> ProviderResult[NormalizedCreator]:
        op = Capability.CREATOR_PROFILE
        try:
            payload = await self._call("profile", {"channelId": platform_account_id})
        except ProviderNotFound:
            # A direct query that returns nothing costs 0 credits.
            return ProviderResult(PROVIDER, op.value, ResultStatus.NOT_FOUND, request_id=self._request_id())
        except ProviderError as exc:
            return self._error(op, exc)
        creator, missing = self._creator_from_record(self._data(payload), op.value)
        if creator is None:
            return ProviderResult(PROVIDER, op.value, ResultStatus.ERROR, cost=self._cost("profile", payload),
                                  missing_fields=missing, error="response missing channel id", error_code="schema_mismatch")
        status = ResultStatus.PARTIAL if missing else ResultStatus.OK
        return ProviderResult(PROVIDER, op.value, status, creator, self._cost("profile", payload), missing,
                              request_id=self._request_id())

    async def get_creator_performance(self, platform_account_id: str) -> ProviderResult[NormalizedCreator]:
        """Views, engagement and upload counts only; merge with the profile snapshot."""
        op = Capability.CREATOR_PERFORMANCE
        try:
            payload = await self._call("performance", {"channelId": platform_account_id})
        except ProviderNotFound:
            return ProviderResult(PROVIDER, op.value, ResultStatus.NOT_FOUND, request_id=self._request_id())
        except ProviderError as exc:
            return self._error(op, exc)
        values, missing = extract(self._data(payload), PERFORMANCE_FIELDS)
        prov = self._provenance(op.value, platform_account_id, "creator_performance",
                                MeasurementType.THIRD_PARTY_MODELED, missing, len(PERFORMANCE_FIELDS))
        creator = NormalizedCreator(platform="youtube", platform_account_id=platform_account_id,
                                    provider_creator_id=platform_account_id, provenance=prov, **values)
        status = ResultStatus.PARTIAL if missing else ResultStatus.OK
        return ProviderResult(PROVIDER, op.value, status, creator, self._cost("performance", payload), missing,
                              request_id=self._request_id())

    async def search_creators(self, criteria: CreatorSearchCriteria) -> ProviderResult[list[NormalizedCreator]]:
        op = Capability.SEARCH_CREATORS
        if "search" not in self.enabled_endpoints:
            return self._disabled(op, "search")
        if criteria.sponsored_brands:
            # /youtube/search only filters on hasSponsors; running it would
            # return creators unrelated to the requested brands.
            return ProviderResult(PROVIDER, op.value, ResultStatus.UNSUPPORTED,
                                  error="brand-sponsorship search needs /sponsor/creators, which this adapter "
                                        "does not call yet")
        body, unapplied, applied = build_search_body(criteria)
        if not body["filters"]:
            return ProviderResult(PROVIDER, op.value, ResultStatus.ERROR, error="CreatorDB search needs at least one filter",
                                  error_code="bad_request")
        try:
            payload = await self._call("search", body)
        except ProviderError as exc:
            return self._error(op, exc)
        data = self._data(payload)
        records = data.get("creatorList") if isinstance(data.get("creatorList"), list) else []
        creators, missing_all = [], set()
        for rec in records:
            if not isinstance(rec, dict):
                continue
            # Enriched results nest `profile`/`performance`; plain results
            # carry channelId, uniqueId, displayName and totalSubscribers.
            profile = rec.get("profile") if isinstance(rec.get("profile"), dict) else {}
            merged = {**profile, **{k: v for k, v in rec.items() if k not in ("profile", "performance")}}
            perf = rec.get("performance") if isinstance(rec.get("performance"), dict) else None
            c, missing = self._creator_from_record(merged, op.value, perf)
            missing_all.update(missing)
            if c:
                creators.append(c)
        next_offset = coerce(data.get("nextOffset"), "int") if data.get("hasNextPage") is True else None
        return ProviderResult(
            PROVIDER, op.value, ResultStatus.PARTIAL if missing_all else ResultStatus.OK, creators,
            self._cost("search", payload, fallback_units=search_credits(len(body["filters"]))),
            sorted(missing_all), unapplied, applied,
            next_page_token=str(next_offset) if next_offset is not None else None,
            total_available=coerce(data.get("totalResults"), "int"),
            request_id=self._request_id(),
        )

    async def get_audience_demographics(self, platform_account_id: str) -> ProviderResult[AudienceBreakdown]:
        op = Capability.AUDIENCE_DEMOGRAPHICS
        if "audience" not in self.enabled_endpoints:
            return self._disabled(op, "audience")
        try:
            payload = await self._call("audience", {"channelId": platform_account_id})
        except ProviderNotFound:
            return ProviderResult(PROVIDER, op.value, ResultStatus.NOT_FOUND, request_id=self._request_id())
        except ProviderError as exc:
            return self._error(op, exc)
        record = self._data(payload)
        buckets, missing = [], []

        def bucket(dim: str, name: Any, share: Any) -> bool:
            s = coerce(share, "ratio")
            if name in (None, "") or s is None:
                return False
            label = country_to_a2(str(name)) if dim == "country" else str(name)
            buckets.append(AudienceBucket(dimension=dim, bucket=label, share=s,
                                          measurement=MeasurementType.THIRD_PARTY_MODELED))
            return True

        for dim, (path, key) in AUDIENCE_DIMENSIONS.items():
            raw = get_path(record, path)
            added = False
            if dim == "gender" and isinstance(raw, dict):
                added = bucket(dim, "male", raw.get("maleRatio")) | bucket(dim, "female", raw.get("femaleRatio"))
            elif isinstance(raw, list):
                for item in raw:
                    if isinstance(item, dict):
                        added = bucket(dim, item.get(key), item.get("share")) or added
            if not added:
                missing.append(dim)
        prov = self._provenance(op.value, platform_account_id, "audience_demographics",
                                MeasurementType.THIRD_PARTY_MODELED, missing, len(AUDIENCE_DIMENSIONS))
        status = ResultStatus.PARTIAL if missing else ResultStatus.OK
        return ProviderResult(PROVIDER, op.value, status,
                              AudienceBreakdown(platform_account_id=platform_account_id, buckets=buckets, provenance=prov),
                              self._cost("audience", payload), missing, request_id=self._request_id())

    async def get_creator_content(self, platform_account_id: str, limit: int = 30) -> ProviderResult[list[NormalizedContent]]:
        op = Capability.CREATOR_CONTENT
        if "content" not in self.enabled_endpoints:
            return self._disabled(op, "content")
        try:
            payload = await self._call("content", {"channelId": platform_account_id})
        except ProviderNotFound:
            return ProviderResult(PROVIDER, op.value, ResultStatus.NOT_FOUND, request_id=self._request_id())
        except ProviderError as exc:
            return self._error(op, exc)
        data = self._data(payload)
        items, missing_all = [], set()
        for key, is_short in (("recentVideos", False), ("recentShorts", True)):
            records = data.get(key) if isinstance(data.get(key), list) else []
            for rec in records:
                if not isinstance(rec, dict):
                    continue
                values, missing = extract(rec, CONTENT_FIELDS)
                if is_short:
                    missing = [m for m in missing if m != "sponsored_flag"]  # not reported for shorts
                missing_all.update(missing)
                if critical_missing(CONTENT_FIELDS, missing):
                    continue
                vid = values["platform_content_id"]
                url = f"https://www.youtube.com/shorts/{vid}" if is_short else f"https://www.youtube.com/watch?v={vid}"
                prov = self._provenance(op.value, vid, "content", MeasurementType.OBSERVED, missing, len(CONTENT_FIELDS))
                items.append(NormalizedContent(
                    platform="youtube", platform_content_id=vid, platform_account_id=platform_account_id,
                    title=values.get("title"), description=values.get("description"), url=url,
                    published_at=values.get("published_at"), duration_seconds=values.get("duration_seconds"),
                    is_short=is_short, views=values.get("views"), likes=values.get("likes"),
                    comments=values.get("comments"), sponsored_flag=values.get("sponsored_flag"), provenance=prov,
                ))
        return ProviderResult(PROVIDER, op.value, ResultStatus.PARTIAL if missing_all else ResultStatus.OK,
                              items[: max(0, limit)], self._cost("content", payload), sorted(missing_all),
                              request_id=self._request_id())

    async def get_sponsorship_history(self, platform_account_id: str) -> ProviderResult[list[NormalizedSponsorship]]:
        op = Capability.SPONSORSHIP_HISTORY
        if "sponsorship" not in self.enabled_endpoints:
            return self._disabled(op, "sponsorship")
        try:
            payload = await self._call("sponsorship", {"channelId": platform_account_id})
        except ProviderNotFound:
            return ProviderResult(PROVIDER, op.value, ResultStatus.NOT_FOUND, request_id=self._request_id())
        except ProviderError as exc:
            return self._error(op, exc)
        brands = self._data(payload).get("sponsorList")
        out = []
        for brand in brands if isinstance(brands, list) else []:
            if not isinstance(brand, dict) or not brand.get("brandName"):
                continue
            domain = _domain(brand.get("brandId"))
            videos = [v for v in brand.get("sponsoredVideos") or [] if isinstance(v, dict) and v.get("contentId")]
            for video in videos or [None]:
                vid = str(video["contentId"]) if video else None
                prov = self._provenance(op.value, vid or platform_account_id, "sponsorship",
                                        MeasurementType.THIRD_PARTY_MODELED, [], 1)
                out.append(NormalizedSponsorship(
                    platform_account_id=platform_account_id, brand_name=str(brand["brandName"]), brand_domain=domain,
                    content_id=vid, content_url=f"https://www.youtube.com/watch?v={vid}" if vid else None,
                    published_at=coerce(video.get("publishTime"), "datetime") if video else None,
                    provider_label="creatordb_detected_sponsorship", provenance=prov,
                ))
        return ProviderResult(PROVIDER, op.value, ResultStatus.OK, out, self._cost("sponsorship", payload),
                              request_id=self._request_id())

    async def get_creator_contacts(self, platform_account_id: str) -> ProviderResult[list[NormalizedContact]]:
        op = Capability.CREATOR_CONTACTS
        if "contact" not in self.enabled_endpoints:
            return self._disabled(op, "contact")
        try:
            payload = await self._call("contact", {"channelId": platform_account_id})
        except ProviderNotFound:
            return ProviderResult(PROVIDER, op.value, ResultStatus.NOT_FOUND, request_id=self._request_id())
        except ProviderError as exc:
            return self._error(op, exc)
        emails = self._data(payload).get("emails")
        out, seen = [], set()
        for e in emails if isinstance(emails, list) else []:
            value = e.get("email") if isinstance(e, dict) else e
            if not isinstance(value, str) or "@" not in value or value.strip().lower() in seen:
                continue
            seen.add(value.strip().lower())
            prov = self._provenance(op.value, platform_account_id, "contact", MeasurementType.OBSERVED, [], 1)
            out.append(NormalizedContact(platform_account_id=platform_account_id, contact_type="email",
                                         value=value.strip(), is_business_contact=None, provenance=prov))
        cost = self._cost("contact", payload, fallback_units=ENDPOINTS["contact"].cost_units if out else 0)
        return ProviderResult(PROVIDER, op.value, ResultStatus.OK if out else ResultStatus.NOT_FOUND, out, cost,
                              request_id=self._request_id())

    async def get_usage_costs(self) -> ProviderResult[dict[str, Any]]:
        """GET /usage (free): credits remaining and the key's spend limit."""
        op = Capability.USAGE_COSTS
        try:
            payload = await self._call("usage", params=None)
        except ProviderError as exc:
            return self._error(op, exc)
        data = self._data(payload)
        key = data.get("key") if isinstance(data.get("key"), dict) else {}
        return ProviderResult(PROVIDER, op.value, ResultStatus.OK, {
            "credits_available": payload.get("creditsAvailable"),
            "plan_credits_available": payload.get("planCreditsAvailable"),
            "purchased_credits_available": payload.get("purchasedCreditsAvailable"),
            "key_credit_limit": key.get("creditLimit"),
            "key_credits_remaining": key.get("creditsRemaining"),
            "key_active": key.get("active"),
            "daily_records": data.get("records") if isinstance(data.get("records"), list) else [],
        }, CostActual(), request_id=self._request_id())

    async def health_check(self) -> ProviderHealth:
        result = await self.get_usage_costs() if self.is_configured() else None
        healthy = None if result is None else result.ok
        detail = "enabled endpoints: " + ", ".join(sorted(self.enabled_endpoints))
        if result is not None and result.ok and result.data:
            detail += f"; credits available: {result.data.get('credits_available')}"
        elif result is not None:
            detail += f"; /usage failed: {result.error_code}"
        return ProviderHealth(PROVIDER, self.is_configured(), healthy, self.http.breaker.state, detail=detail)


def _domain(brand_id: Any) -> Optional[str]:
    """CreatorDB brandId is the brand's website URL or an opaque id."""
    if not isinstance(brand_id, str) or "." not in brand_id or " " in brand_id:
        return None
    host = re.sub(r"^[a-z]+://", "", brand_id.strip().lower()).split("/")[0]
    return host.removeprefix("www.") or None


def request_hash(operation: str, params: dict) -> str:
    raw = json.dumps({"op": operation, "params": params}, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()
