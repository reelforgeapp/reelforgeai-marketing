"""
Normalized provider contract.

Business logic, scoring and the database only ever see the normalized models
defined here, never a provider's raw response schema. Each adapter advertises
the capabilities it actually supports; calling anything else returns an
explicit UNSUPPORTED result rather than guessed data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Generic, Optional, TypeVar

from pydantic import BaseModel, Field

from rpi.core.provenance import MeasurementType, Provenance


class Capability(str, Enum):
    SEARCH_CREATORS = "search_creators"
    CREATOR_PROFILE = "get_creator_profile"
    AUDIENCE_DEMOGRAPHICS = "get_audience_demographics"
    AUDIENCE_INTERESTS = "get_audience_interests"
    CREATOR_CONTENT = "get_creator_content"
    SPONSORSHIP_HISTORY = "get_sponsorship_history"
    CREATOR_CONTACTS = "get_creator_contacts"
    CREATOR_PERFORMANCE = "get_creator_performance"
    TRANSCRIPT = "get_transcript"
    AUTHORIZED_ANALYTICS = "get_authorized_analytics"
    USAGE_COSTS = "get_usage_costs"
    AUDIENCE_RESEARCH = "research_audience"


class ResultStatus(str, Enum):
    OK = "ok"
    PARTIAL = "partial"
    UNSUPPORTED = "unsupported"
    NOT_FOUND = "not_found"
    NOT_CONFIGURED = "not_configured"
    ERROR = "error"
    BUDGET_DENIED = "budget_denied"
    POLICY_DENIED = "policy_denied"
    CACHED = "cached"


# --------------------------------------------------------------------------
# Normalized data models
# --------------------------------------------------------------------------


class CreatorSearchCriteria(BaseModel):
    """Provider-neutral discovery criteria. Adapters translate what they can
    and report the rest in `ProviderResult.unapplied_criteria`."""

    platform: str = "youtube"
    keywords: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)
    languages: list[str] = Field(default_factory=lambda: ["en"])
    countries: list[str] = Field(default_factory=list)
    min_subscribers: Optional[int] = None
    max_subscribers: Optional[int] = None
    min_avg_views: Optional[int] = None
    active_within_days: Optional[int] = None
    sponsored_brands: list[str] = Field(default_factory=list)
    limit: int = 50
    page_token: Optional[str] = None


class NormalizedCreator(BaseModel):
    platform: str
    platform_account_id: str
    handle: Optional[str] = None
    display_name: Optional[str] = None
    url: Optional[str] = None
    description: Optional[str] = None
    country: Optional[str] = None
    language: Optional[str] = None
    subscribers: Optional[int] = None
    avg_views: Optional[float] = None
    median_views: Optional[float] = None
    engagement_rate: Optional[float] = None
    total_videos: Optional[int] = None
    last_upload_at: Optional[datetime] = None
    uploads_last_90d: Optional[int] = None
    categories: list[str] = Field(default_factory=list)
    linked_accounts: list["LinkedAccount"] = Field(default_factory=list)
    provider_creator_id: Optional[str] = None
    provenance: Provenance


class LinkedAccount(BaseModel):
    platform: str
    platform_account_id: Optional[str] = None
    handle: Optional[str] = None
    url: Optional[str] = None
    # Where the link was asserted: "profile_link" (creator links it from their
    # profile) or "provider_mapping" (provider claims both are the same person)
    asserted_by: str = "profile_link"


NormalizedCreator.model_rebuild()


class AudienceBucket(BaseModel):
    dimension: str  # country | age | gender | language | interest
    bucket: str  # "US", "18-24", "female", "en", "video editing"
    share: float  # 0..1, exactly as supplied; never imputed
    measurement: MeasurementType


class AudienceBreakdown(BaseModel):
    platform_account_id: str
    buckets: list[AudienceBucket] = Field(default_factory=list)
    credibility_estimate: Optional[float] = None
    provenance: Provenance


class NormalizedContent(BaseModel):
    platform: str
    platform_content_id: str
    platform_account_id: str
    title: Optional[str] = None
    description: Optional[str] = None
    url: Optional[str] = None
    published_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    is_short: Optional[bool] = None
    views: Optional[int] = None
    likes: Optional[int] = None
    comments: Optional[int] = None
    has_captions: Optional[bool] = None
    sponsored_flag: Optional[bool] = None  # provider/platform paid-promotion flag
    provenance: Provenance


class NormalizedSponsorship(BaseModel):
    platform_account_id: str
    brand_name: str
    brand_domain: Optional[str] = None
    content_id: Optional[str] = None
    content_url: Optional[str] = None
    published_at: Optional[datetime] = None
    # What the provider actually asserts, not our classification of it.
    provider_label: Optional[str] = None
    provenance: Provenance


class NormalizedContact(BaseModel):
    platform_account_id: str
    contact_type: str  # email | form | agency | social_dm
    value: str
    is_business_contact: Optional[bool] = None
    provenance: Provenance


class Transcript(BaseModel):
    platform_content_id: str
    language: Optional[str] = None
    text: str
    segments: list[dict[str, Any]] = Field(default_factory=list)
    is_auto_generated: Optional[bool] = None
    provenance: Provenance


class AudienceResearchResult(BaseModel):
    """SparkToro-style audience-first result: sources an audience pays
    attention to. Describes an audience, never a specific channel's viewers."""

    query: str
    sources: list[dict[str, Any]] = Field(default_factory=list)
    provenance: Provenance


# --------------------------------------------------------------------------
# Results and costs
# --------------------------------------------------------------------------


@dataclass
class CostEstimate:
    provider: str
    operation: str
    units: Decimal  # provider credits / quota units
    usd: Decimal
    basis: str = "configured_rate"  # how the estimate was produced


@dataclass
class CostActual:
    units: Decimal = Decimal("0")
    usd: Decimal = Decimal("0")
    # True when the provider reported the charge; False when we derived it
    # from the documented rate.
    reported_by_provider: bool = False


T = TypeVar("T")


@dataclass
class ProviderResult(Generic[T]):
    provider: str
    operation: str
    status: ResultStatus
    data: Optional[T] = None
    cost: CostActual = field(default_factory=CostActual)
    missing_fields: list[str] = field(default_factory=list)
    unapplied_criteria: list[str] = field(default_factory=list)
    next_page_token: Optional[str] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    request_id: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.status in (ResultStatus.OK, ResultStatus.PARTIAL, ResultStatus.CACHED)

    @classmethod
    def unsupported(cls, provider: str, operation: str, reason: str = "") -> "ProviderResult[T]":
        return cls(
            provider=provider,
            operation=operation,
            status=ResultStatus.UNSUPPORTED,
            error=reason or f"{provider} does not support {operation}",
        )


@dataclass
class ProviderHealth:
    provider: str
    configured: bool
    healthy: Optional[bool]
    circuit_state: str
    detail: str = ""
    checked_at: Optional[datetime] = None


class ProviderAdapter:
    """Base adapter. Subclasses override only what they support and list it
    in `capabilities`. Every unsupported call returns UNSUPPORTED."""

    name: str = "base"
    capabilities: frozenset[Capability] = frozenset()
    # Data-licensing policy key in rpi.compliance.policies.
    policy_key: str = "unknown"

    def is_configured(self) -> bool:
        return False

    def supports(self, capability: Capability) -> bool:
        return capability in self.capabilities

    def estimate_cost(self, operation: Capability, **params: Any) -> CostEstimate:
        return CostEstimate(self.name, operation.value, Decimal("0"), Decimal("0"), basis="unsupported")

    async def health_check(self) -> ProviderHealth:
        return ProviderHealth(self.name, self.is_configured(), None, "closed", "no health check implemented")

    def _unsupported(self, operation: Capability) -> ProviderResult:
        return ProviderResult.unsupported(self.name, operation.value)

    async def search_creators(self, criteria: CreatorSearchCriteria) -> ProviderResult[list[NormalizedCreator]]:
        return self._unsupported(Capability.SEARCH_CREATORS)

    async def get_creator_profile(self, platform_account_id: str) -> ProviderResult[NormalizedCreator]:
        return self._unsupported(Capability.CREATOR_PROFILE)

    async def get_audience_demographics(self, platform_account_id: str) -> ProviderResult[AudienceBreakdown]:
        return self._unsupported(Capability.AUDIENCE_DEMOGRAPHICS)

    async def get_audience_interests(self, platform_account_id: str) -> ProviderResult[AudienceBreakdown]:
        return self._unsupported(Capability.AUDIENCE_INTERESTS)

    async def get_creator_content(
        self, platform_account_id: str, limit: int = 30
    ) -> ProviderResult[list[NormalizedContent]]:
        return self._unsupported(Capability.CREATOR_CONTENT)

    async def get_sponsorship_history(
        self, platform_account_id: str
    ) -> ProviderResult[list[NormalizedSponsorship]]:
        return self._unsupported(Capability.SPONSORSHIP_HISTORY)

    async def get_creator_contacts(self, platform_account_id: str) -> ProviderResult[list[NormalizedContact]]:
        return self._unsupported(Capability.CREATOR_CONTACTS)

    async def get_creator_performance(self, platform_account_id: str) -> ProviderResult[NormalizedCreator]:
        return self._unsupported(Capability.CREATOR_PERFORMANCE)

    async def get_transcript(self, platform_content_id: str) -> ProviderResult[Transcript]:
        return self._unsupported(Capability.TRANSCRIPT)

    async def get_authorized_analytics(self, platform_account_id: str) -> ProviderResult[AudienceBreakdown]:
        return self._unsupported(Capability.AUTHORIZED_ANALYTICS)

    async def get_usage_costs(self) -> ProviderResult[dict[str, Any]]:
        return self._unsupported(Capability.USAGE_COSTS)

    async def research_audience(self, query: str) -> ProviderResult[AudienceResearchResult]:
        return self._unsupported(Capability.AUDIENCE_RESEARCH)
