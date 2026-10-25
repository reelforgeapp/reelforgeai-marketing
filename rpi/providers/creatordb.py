"""
CreatorDB API v3 adapter (primary discovery and creator-profile provider).

Verified from https://docs.creatordb.app/ on 2026-10-25:
  * base URL https://apiv3.creatordb.app
  * `api-key` request header (the docs page is inconsistent and also says
    "authorization header"; `scripts/rpi_verify_creatordb.py` checks both)
  * POST /youtube/profile with JSON body {"channelId": "UC..."}
  * GET and POST methods; POST needs a JSON body
  * at most 100 results per call, `offset` pagination in the request body
  * up to 6,000 calls/minute, allowance depends on credits

NOT verified (documentation pages could not be fetched during the build):
  * every other endpoint path (search, audience, sponsorship, content, contact)
  * response field names
  * credit cost per endpoint

Unverified endpoints are disabled unless listed in
CREATORDB_VERIFIED_ENDPOINTS after running scripts/rpi_verify_creatordb.py.
Field mappings are in CREATORDB_FIELDS. Anything absent from a response
is reported as missing and never filled in.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any, Optional

import httpx

from rpi.compliance.policies import get_policy
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
    ProviderHealth,
    ProviderResult,
    ResultStatus,
)
from rpi.providers.http import (
    CircuitBreaker,
    ProviderCreditsExhausted,
    ProviderError,
    ProviderNotFound,
    ResilientHTTP,
    TokenBucket,
    default_classify_error,
)
from rpi.providers.mapping import EndpointSpec, FieldSpec, critical_missing, extract, first_list, get_path

PROVIDER = "creatordb"
RESPONSE_VERSION = "v3"

ENDPOINTS: dict[str, EndpointSpec] = {
    "profile": EndpointSpec("POST", "/youtube/profile", verified=True, cost_units=1, cost_verified=False,
                            notes="Documented example: body {channelId}"),
    "search": EndpointSpec("POST", "/youtube/search", verified=False, cost_units=1,
                           notes="Provisional path; confirm in docs (llms.txt index)"),
    "audience": EndpointSpec("POST", "/youtube/audience", verified=False, cost_units=1, notes="Provisional path"),
    "sponsorship": EndpointSpec("POST", "/youtube/sponsorship", verified=False, cost_units=1, notes="Provisional path"),
    "content": EndpointSpec("POST", "/youtube/videos", verified=False, cost_units=1, notes="Provisional path"),
    "contact": EndpointSpec("POST", "/youtube/contact", verified=False, cost_units=1, notes="Provisional path"),
}

ENDPOINT_CAPABILITY = {
    "profile": Capability.CREATOR_PROFILE,
    "search": Capability.SEARCH_CREATORS,
    "audience": Capability.AUDIENCE_DEMOGRAPHICS,
    "sponsorship": Capability.SPONSORSHIP_HISTORY,
    "content": Capability.CREATOR_CONTENT,
    "contact": Capability.CREATOR_CONTACTS,
}

# Field mapping for profile/search records. verified=False until checked
# against a live response with scripts/rpi_verify_creatordb.py.
CREATORDB_FIELDS: dict[str, FieldSpec] = {
    "platform_account_id": FieldSpec("channelId", "str", verified=True, critical=True),
    "display_name": FieldSpec("channelName", "str"),
    "handle": FieldSpec("customUrl", "str"),
    "description": FieldSpec("description", "str"),
    "country": FieldSpec("country", "str"),
    "language": FieldSpec("language", "str"),
    "subscribers": FieldSpec("subscribers", "int"),
    "avg_views": FieldSpec("avgViews", "float"),
    "median_views": FieldSpec("medianViews", "float"),
    "engagement_rate": FieldSpec("engagementRate", "ratio"),
    "total_videos": FieldSpec("totalVideos", "int"),
    "last_upload_at": FieldSpec("lastPublishTime", "datetime"),
    "uploads_last_90d": FieldSpec("videosLast90Days", "int"),
    "categories": FieldSpec("categories", "list"),
}

CONTENT_FIELDS: dict[str, FieldSpec] = {
    "platform_content_id": FieldSpec("videoId", "str", critical=True),
    "title": FieldSpec("title", "str"),
    "description": FieldSpec("description", "str"),
    "published_at": FieldSpec("publishTime", "datetime"),
    "duration_seconds": FieldSpec("duration", "int"),
    "views": FieldSpec("views", "int"),
    "likes": FieldSpec("likes", "int"),
    "comments": FieldSpec("comments", "int"),
    "sponsored_flag": FieldSpec("isSponsored", "bool"),
}

AUDIENCE_DIMENSIONS = {"country": "audienceCountries", "age": "audienceAges", "gender": "audienceGenders",
                       "language": "audienceLanguages"}

CREDIT_ERROR_MARKERS = ("credit", "quota", "insufficient", "balance")


def classify_creatordb_error(provider: str, response: httpx.Response) -> Optional[ProviderError]:
    base = default_classify_error(provider, response)
    if base is None:
        return None
    text = (response.text or "").lower()
    if response.status_code in (400, 403, 429) and any(m in text for m in CREDIT_ERROR_MARKERS):
        return ProviderCreditsExhausted(provider, "credits exhausted or insufficient", response.status_code)
    return base


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
        enabled = {k for k, e in ENDPOINTS.items() if e.verified} | (verified_endpoints or set())
        self.enabled_endpoints = {k for k in enabled if k in ENDPOINTS}
        self.capabilities = frozenset(ENDPOINT_CAPABILITY[k] for k in self.enabled_endpoints)
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
        pages = 1
        if operation == Capability.SEARCH_CREATORS:
            limit = int(params.get("limit", 50))
            pages = max(1, -(-limit // 100))
        units = Decimal(str(spec.cost_units)) * pages
        basis = "documented_credits" if spec.cost_verified else "assumed_1_credit_per_call"
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

    async def _post(self, endpoint: str, body: dict) -> dict:
        spec = ENDPOINTS[endpoint]
        response = await self.http.request(spec.method, spec.path, json=body)
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise ProviderError(PROVIDER, f"non-JSON response from {spec.path}") from exc
        return payload

    def _cost(self, endpoint: str, pages: int = 1) -> CostActual:
        units = Decimal(str(ENDPOINTS[endpoint].cost_units)) * pages
        return CostActual(units=units, usd=units * self.usd_per_credit, reported_by_provider=False)

    def _creator_from_record(self, record: dict, operation: str) -> tuple[Optional[NormalizedCreator], list[str]]:
        values, missing = extract(record, CREATORDB_FIELDS)
        if critical_missing(CREATORDB_FIELDS, missing):
            return None, missing
        field_meas = {"engagement_rate": MeasurementType.THIRD_PARTY_MODELED,
                      "avg_views": MeasurementType.THIRD_PARTY_MODELED,
                      "median_views": MeasurementType.THIRD_PARTY_MODELED,
                      "categories": MeasurementType.THIRD_PARTY_MODELED}
        prov = self._provenance(operation, values["platform_account_id"], "creator_profile",
                                MeasurementType.OBSERVED, missing, len(CREATORDB_FIELDS), field_meas)
        account_id = values["platform_account_id"]
        return NormalizedCreator(
            platform="youtube",
            platform_account_id=account_id,
            handle=values.get("handle"),
            display_name=values.get("display_name"),
            url=f"https://www.youtube.com/channel/{account_id}",
            description=values.get("description"),
            country=values.get("country"),
            language=values.get("language"),
            subscribers=values.get("subscribers"),
            avg_views=values.get("avg_views"),
            median_views=values.get("median_views"),
            engagement_rate=values.get("engagement_rate"),
            total_videos=values.get("total_videos"),
            last_upload_at=values.get("last_upload_at"),
            uploads_last_90d=values.get("uploads_last_90d"),
            categories=[str(c) for c in values.get("categories", [])],
            provider_creator_id=account_id,
            provenance=prov,
        ), missing

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------

    async def get_creator_profile(self, platform_account_id: str) -> ProviderResult[NormalizedCreator]:
        op = Capability.CREATOR_PROFILE
        try:
            payload = await self._post("profile", {"channelId": platform_account_id})
        except ProviderNotFound:
            return ProviderResult(PROVIDER, op.value, ResultStatus.NOT_FOUND, cost=self._cost("profile"))
        except ProviderError as exc:
            return ProviderResult(PROVIDER, op.value, ResultStatus.ERROR, error=exc.message, error_code=exc.code)
        record = payload.get("data", payload) if isinstance(payload, dict) else {}
        creator, missing = self._creator_from_record(record if isinstance(record, dict) else {}, op.value)
        if creator is None:
            return ProviderResult(PROVIDER, op.value, ResultStatus.ERROR, cost=self._cost("profile"),
                                  missing_fields=missing, error="response missing channel id", error_code="schema_mismatch")
        status = ResultStatus.PARTIAL if missing else ResultStatus.OK
        return ProviderResult(PROVIDER, op.value, status, creator, self._cost("profile"), missing,
                              request_id=self.http.last_call.request_id if self.http.last_call else None)

    async def search_creators(self, criteria: CreatorSearchCriteria) -> ProviderResult[list[NormalizedCreator]]:
        op = Capability.SEARCH_CREATORS
        if "search" not in self.enabled_endpoints:
            return self._disabled(op, "search")
        body: dict[str, Any] = {"offset": int(criteria.page_token or 0), "limit": min(criteria.limit, 100)}
        unapplied: list[str] = []
        if criteria.keywords:
            body["keywords"] = criteria.keywords
        if criteria.topics:
            body["topics"] = criteria.topics
        if criteria.languages:
            body["languages"] = criteria.languages
        if criteria.countries:
            body["countries"] = criteria.countries
        if criteria.min_subscribers is not None:
            body["minSubscribers"] = criteria.min_subscribers
        if criteria.max_subscribers is not None:
            body["maxSubscribers"] = criteria.max_subscribers
        if criteria.sponsored_brands:
            body["sponsoredBrands"] = criteria.sponsored_brands
        if criteria.min_avg_views is not None:
            unapplied.append("min_avg_views")
        if criteria.active_within_days is not None:
            unapplied.append("active_within_days")
        try:
            payload = await self._post("search", body)
        except ProviderError as exc:
            return ProviderResult(PROVIDER, op.value, ResultStatus.ERROR, error=exc.message, error_code=exc.code)
        records = first_list(payload, "data", "data.results", "results", "") or []
        creators, missing_all = [], set()
        for rec in records:
            if not isinstance(rec, dict):
                continue
            c, missing = self._creator_from_record(rec, op.value)
            missing_all.update(missing)
            if c:
                creators.append(c)
        next_offset = body["offset"] + len(records) if len(records) >= body["limit"] else None
        return ProviderResult(
            PROVIDER, op.value, ResultStatus.PARTIAL if missing_all else ResultStatus.OK, creators,
            self._cost("search"), sorted(missing_all), unapplied,
            next_page_token=str(next_offset) if next_offset is not None else None,
        )

    async def get_audience_demographics(self, platform_account_id: str) -> ProviderResult[AudienceBreakdown]:
        op = Capability.AUDIENCE_DEMOGRAPHICS
        if "audience" not in self.enabled_endpoints:
            return self._disabled(op, "audience")
        try:
            payload = await self._post("audience", {"channelId": platform_account_id})
        except ProviderError as exc:
            return ProviderResult(PROVIDER, op.value, ResultStatus.ERROR, error=exc.message, error_code=exc.code)
        record = payload.get("data", payload) if isinstance(payload, dict) else {}
        buckets, missing = [], []
        for dim, path in AUDIENCE_DIMENSIONS.items():
            raw = get_path(record, path)
            if not isinstance(raw, dict) or not raw:
                missing.append(dim)
                continue
            for bucket, share in raw.items():
                try:
                    s = float(share)
                except (TypeError, ValueError):
                    continue
                if 0 <= s <= 1:
                    buckets.append(AudienceBucket(dimension=dim, bucket=str(bucket), share=s,
                                                  measurement=MeasurementType.THIRD_PARTY_MODELED))
        prov = self._provenance(op.value, platform_account_id, "audience_demographics",
                                MeasurementType.THIRD_PARTY_MODELED, missing, len(AUDIENCE_DIMENSIONS))
        status = ResultStatus.PARTIAL if missing else ResultStatus.OK
        return ProviderResult(PROVIDER, op.value, status,
                              AudienceBreakdown(platform_account_id=platform_account_id, buckets=buckets, provenance=prov),
                              self._cost("audience"), missing)

    async def get_creator_content(self, platform_account_id: str, limit: int = 30) -> ProviderResult[list[NormalizedContent]]:
        op = Capability.CREATOR_CONTENT
        if "content" not in self.enabled_endpoints:
            return self._disabled(op, "content")
        try:
            payload = await self._post("content", {"channelId": platform_account_id, "limit": min(limit, 100), "offset": 0})
        except ProviderError as exc:
            return ProviderResult(PROVIDER, op.value, ResultStatus.ERROR, error=exc.message, error_code=exc.code)
        records = first_list(payload, "data", "data.videos", "videos", "") or []
        items, missing_all = [], set()
        for rec in records:
            if not isinstance(rec, dict):
                continue
            values, missing = extract(rec, CONTENT_FIELDS)
            missing_all.update(missing)
            if critical_missing(CONTENT_FIELDS, missing):
                continue
            vid = values["platform_content_id"]
            prov = self._provenance(op.value, vid, "content", MeasurementType.OBSERVED, missing, len(CONTENT_FIELDS))
            duration = values.get("duration_seconds")
            items.append(NormalizedContent(
                platform="youtube", platform_content_id=vid, platform_account_id=platform_account_id,
                title=values.get("title"), description=values.get("description"),
                url=f"https://www.youtube.com/watch?v={vid}", published_at=values.get("published_at"),
                duration_seconds=duration, is_short=(duration is not None and duration <= 60),
                views=values.get("views"), likes=values.get("likes"), comments=values.get("comments"),
                sponsored_flag=values.get("sponsored_flag"), provenance=prov,
            ))
        return ProviderResult(PROVIDER, op.value, ResultStatus.PARTIAL if missing_all else ResultStatus.OK,
                              items, self._cost("content"), sorted(missing_all))

    async def get_sponsorship_history(self, platform_account_id: str) -> ProviderResult[list[NormalizedSponsorship]]:
        op = Capability.SPONSORSHIP_HISTORY
        if "sponsorship" not in self.enabled_endpoints:
            return self._disabled(op, "sponsorship")
        try:
            payload = await self._post("sponsorship", {"channelId": platform_account_id})
        except ProviderError as exc:
            return ProviderResult(PROVIDER, op.value, ResultStatus.ERROR, error=exc.message, error_code=exc.code)
        records = first_list(payload, "data", "data.sponsorships", "sponsorships", "") or []
        out = []
        for rec in records:
            if not isinstance(rec, dict):
                continue
            brand = rec.get("brandName") or rec.get("brand")
            if not brand:
                continue
            prov = self._provenance(op.value, platform_account_id, "sponsorship", MeasurementType.THIRD_PARTY_MODELED, [], 1)
            out.append(NormalizedSponsorship(
                platform_account_id=platform_account_id, brand_name=str(brand),
                brand_domain=rec.get("brandDomain"), content_id=rec.get("videoId"),
                content_url=f"https://www.youtube.com/watch?v={rec['videoId']}" if rec.get("videoId") else None,
                provider_label=rec.get("type") or "provider_reported_sponsorship", provenance=prov,
            ))
        return ProviderResult(PROVIDER, op.value, ResultStatus.OK, out, self._cost("sponsorship"))

    async def get_creator_contacts(self, platform_account_id: str) -> ProviderResult[list[NormalizedContact]]:
        op = Capability.CREATOR_CONTACTS
        if "contact" not in self.enabled_endpoints:
            return self._disabled(op, "contact")
        try:
            payload = await self._post("contact", {"channelId": platform_account_id})
        except ProviderError as exc:
            return ProviderResult(PROVIDER, op.value, ResultStatus.ERROR, error=exc.message, error_code=exc.code)
        record = payload.get("data", payload) if isinstance(payload, dict) else {}
        emails = get_path(record, "emails") or ([record["email"]] if isinstance(record, dict) and record.get("email") else [])
        out = []
        for e in emails if isinstance(emails, list) else []:
            if isinstance(e, str) and "@" in e:
                prov = self._provenance(op.value, platform_account_id, "contact", MeasurementType.OBSERVED, [], 1)
                out.append(NormalizedContact(platform_account_id=platform_account_id, contact_type="email",
                                             value=e.strip(), is_business_contact=None, provenance=prov))
        return ProviderResult(PROVIDER, op.value, ResultStatus.OK if out else ResultStatus.NOT_FOUND, out, self._cost("contact"))

    async def health_check(self) -> ProviderHealth:
        return ProviderHealth(
            PROVIDER, self.is_configured(), None, self.http.breaker.state,
            detail="enabled endpoints: " + ", ".join(sorted(self.enabled_endpoints))
            + ". No free health endpoint is documented; health is inferred from recent request outcomes.",
        )


def request_hash(operation: str, params: dict) -> str:
    raw = json.dumps({"op": operation, "params": params}, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()
