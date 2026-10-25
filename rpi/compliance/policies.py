"""
Provider data-licensing policy registry.

Each provider's permissions are recorded here, in code, so changes are
reviewed in pull requests and enforced by the application rather than by
prompts. `verified` marks which statements were confirmed against the
provider's official documentation (with the source and date) and which are
conservative defaults pending review. Unverified permissions always default to
the most restrictive reasonable interpretation.

See docs/rpi/DATA_LICENSING_AND_COMPLIANCE.md for the narrative version.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Permission(str, Enum):
    ALLOWED = "allowed"
    PROHIBITED = "prohibited"
    RESTRICTED = "restricted"  # allowed with conditions listed in `notes`
    UNKNOWN = "unknown"  # treated as PROHIBITED by enforcement code


@dataclass(frozen=True)
class ProviderPolicy:
    provider: str
    internal_use: Permission
    external_display: Permission
    redistribution: Permission
    ai_analysis: Permission
    model_training: Permission
    derived_metrics: Permission
    # Max days a record may be stored before it must be refreshed or deleted.
    max_storage_days: Optional[int]
    # Max days a cached API response may be reused instead of refetched.
    cache_ttl_days: Optional[int]
    deletion_on_request_days: Optional[int]
    attribution_required: bool
    sources: tuple[str, ...]
    verified: bool
    verified_on: Optional[str]
    notes: tuple[str, ...] = field(default_factory=tuple)

    def permits(self, use: str) -> bool:
        value: Permission = getattr(self, use)
        return value in (Permission.ALLOWED, Permission.RESTRICTED)


YOUTUBE_DEV_POLICIES = "https://developers.google.com/youtube/terms/developer-policies"

POLICIES: dict[str, ProviderPolicy] = {
    "youtube_data_api": ProviderPolicy(
        provider="youtube_data_api",
        internal_use=Permission.RESTRICTED,
        external_display=Permission.RESTRICTED,
        redistribution=Permission.PROHIBITED,
        ai_analysis=Permission.UNKNOWN,
        model_training=Permission.UNKNOWN,
        derived_metrics=Permission.PROHIBITED,
        max_storage_days=30,
        cache_ttl_days=30,
        deletion_on_request_days=7,
        attribution_required=True,
        sources=(YOUTUBE_DEV_POLICIES,),
        verified=True,
        verified_on="2026-10-25",
        notes=(
            "III.E.4(c)/(d): store API data no longer than 30 calendar days, then refresh or delete.",
            "III.E.4(h): do not create new or derived data or metrics from API data (e.g. a score "
            "that factors in likes or views). RPI scoring therefore never reads YouTube-API-sourced fields.",
            "III.D.1(c): exactly one API project per API client; no key rotation to exceed quota.",
            "III.E.6: no scraping.",
            "III.G.1 / III.I.15: no selling, redistributing or reselling API data.",
            "AI analysis / training of API data is not explicitly addressed in the sections reviewed; "
            "treated as not permitted for training, and AI analysis is limited to transient processing "
            "of titles/descriptions for the operator, without storing derived scores.",
        ),
    ),
    "youtube_analytics_api": ProviderPolicy(
        provider="youtube_analytics_api",
        internal_use=Permission.RESTRICTED,
        external_display=Permission.PROHIBITED,
        redistribution=Permission.PROHIBITED,
        ai_analysis=Permission.UNKNOWN,
        model_training=Permission.PROHIBITED,
        derived_metrics=Permission.PROHIBITED,
        max_storage_days=30,
        cache_ttl_days=30,
        deletion_on_request_days=7,
        attribution_required=True,
        sources=(YOUTUBE_DEV_POLICIES,),
        verified=True,
        verified_on="2026-10-25",
        notes=(
            "III.E.3(b): authorized data may be shown only to the authorizing user. Showing a creator's "
            "OAuth analytics to the ReelForge team needs legal review (and possibly III.L audited-developer "
            "approval). Until cleared, the integration stays disabled behind rpi_youtube_analytics_enabled.",
            "III.E.4(b): re-confirm authorization every 30 days.",
            "III.D.2: on revocation, revoke the token immediately and delete authorized data within 7 days "
            "(30 days if revoked through Google security settings).",
        ),
    ),
    "creatordb": ProviderPolicy(
        provider="creatordb",
        internal_use=Permission.ALLOWED,
        external_display=Permission.UNKNOWN,
        redistribution=Permission.UNKNOWN,
        ai_analysis=Permission.UNKNOWN,
        model_training=Permission.UNKNOWN,
        derived_metrics=Permission.UNKNOWN,
        max_storage_days=90,
        cache_ttl_days=14,
        deletion_on_request_days=30,
        attribution_required=False,
        sources=("https://docs.creatordb.app/", "https://docs.creatordb.app/api-v3.yaml",
                 "https://creatordb.app/privacy-policy/"),
        verified=False,
        verified_on=None,
        notes=(
            "CreatorDB publishes no API terms of service (checked 2026-10-25; its privacy policy refers to "
            "Terms of Service that are not online), so the customer agreement governs. Storage limit, AI "
            "analysis and derived scoring permissions are conservative placeholders. Scoring uses CreatorDB "
            "fields only when rpi_creatordb_scoring_permitted is set after that agreement is reviewed.",
        ),
    ),
    "sparktoro": ProviderPolicy(
        provider="sparktoro",
        internal_use=Permission.ALLOWED,
        external_display=Permission.UNKNOWN,
        redistribution=Permission.PROHIBITED,
        ai_analysis=Permission.UNKNOWN,
        model_training=Permission.PROHIBITED,
        derived_metrics=Permission.UNKNOWN,
        max_storage_days=30,
        cache_ttl_days=30,
        deletion_on_request_days=30,
        attribution_required=True,
        sources=("https://sparktoro.com/api/docs",),
        verified=False,
        verified_on=None,
        notes=(
            "API docs reviewed 2026-10-25 for endpoints, credits and limits. The terms covering storage, "
            "training, competing products and redistribution were not readable at review time; "
            "redistribution and training are treated as prohibited.",
            "Docs: TAM figures are modeled estimates and their rationale must be surfaced to end users.",
            "Audience-level data. Never presented as a measurement of one channel's viewers.",
        ),
    ),
    "supadata": ProviderPolicy(
        provider="supadata",
        internal_use=Permission.ALLOWED,
        external_display=Permission.UNKNOWN,
        redistribution=Permission.UNKNOWN,
        ai_analysis=Permission.UNKNOWN,
        model_training=Permission.UNKNOWN,
        derived_metrics=Permission.UNKNOWN,
        max_storage_days=30,
        cache_ttl_days=30,
        deletion_on_request_days=30,
        attribution_required=False,
        sources=("https://supadata.ai/",),
        verified=False,
        verified_on=None,
        notes=(
            "Transcript source content belongs to the creator and YouTube; Supadata terms not yet reviewed. "
            "Transcripts are stored only as short evidence excerpts and are purged with the 30-day cycle.",
        ),
    ),
    "anthropic": ProviderPolicy(
        provider="anthropic",
        internal_use=Permission.ALLOWED,
        external_display=Permission.ALLOWED,
        redistribution=Permission.ALLOWED,
        ai_analysis=Permission.ALLOWED,
        model_training=Permission.ALLOWED,
        derived_metrics=Permission.ALLOWED,
        max_storage_days=None,
        cache_ttl_days=None,
        deletion_on_request_days=None,
        attribution_required=False,
        sources=(),
        verified=False,
        verified_on=None,
        notes=("Inference provider. Its outputs are RPI's own AI judgments, labeled content_inferred.",),
    ),
    "rpi_internal": ProviderPolicy(
        provider="rpi_internal",
        internal_use=Permission.ALLOWED,
        external_display=Permission.ALLOWED,
        redistribution=Permission.RESTRICTED,
        ai_analysis=Permission.ALLOWED,
        model_training=Permission.ALLOWED,
        derived_metrics=Permission.ALLOWED,
        max_storage_days=None,
        cache_ttl_days=None,
        deletion_on_request_days=30,
        attribution_required=False,
        sources=(),
        verified=True,
        verified_on="2026-10-25",
        notes=("First-party records: CRM notes, billing ledger, attribution, manual operator input.",),
    ),
    "manual_submission": ProviderPolicy(
        provider="manual_submission",
        internal_use=Permission.ALLOWED,
        external_display=Permission.PROHIBITED,
        redistribution=Permission.PROHIBITED,
        ai_analysis=Permission.ALLOWED,
        model_training=Permission.PROHIBITED,
        derived_metrics=Permission.ALLOWED,
        max_storage_days=365,
        cache_ttl_days=None,
        deletion_on_request_days=30,
        attribution_required=False,
        sources=(),
        verified=True,
        verified_on="2026-10-25",
        notes=("Creator-submitted analytics. Marked manual_submitted and never shown as API-verified.",),
    ),
    "fixture": ProviderPolicy(
        provider="fixture",
        internal_use=Permission.ALLOWED,
        external_display=Permission.PROHIBITED,
        redistribution=Permission.PROHIBITED,
        ai_analysis=Permission.ALLOWED,
        model_training=Permission.PROHIBITED,
        derived_metrics=Permission.ALLOWED,
        max_storage_days=7,
        cache_ttl_days=7,
        deletion_on_request_days=1,
        attribution_required=False,
        sources=(),
        verified=True,
        verified_on="2026-10-25",
        notes=("Synthetic development data. Disabled when ENVIRONMENT=production.",),
    ),
}


def get_policy(provider: str) -> ProviderPolicy:
    try:
        return POLICIES[provider]
    except KeyError:
        # Unknown providers get the most restrictive treatment.
        return ProviderPolicy(
            provider=provider,
            internal_use=Permission.UNKNOWN,
            external_display=Permission.PROHIBITED,
            redistribution=Permission.PROHIBITED,
            ai_analysis=Permission.UNKNOWN,
            model_training=Permission.PROHIBITED,
            derived_metrics=Permission.PROHIBITED,
            max_storage_days=30,
            cache_ttl_days=0,
            deletion_on_request_days=7,
            attribution_required=True,
            sources=(),
            verified=False,
            verified_on=None,
            notes=("Unregistered provider: most restrictive defaults.",),
        )


def scoring_allowed(provider: str, *, creatordb_scoring_permitted: bool = False) -> bool:
    """Whether a provider's fields may feed RPI-derived scores.

    YouTube API data never may (III.E.4(h)). CreatorDB data may only after an
    operator records that its terms permit derived scoring. First-party and
    content-inferred data may.
    """
    if provider == "creatordb":
        return creatordb_scoring_permitted
    policy = get_policy(provider)
    return policy.derived_metrics in (Permission.ALLOWED, Permission.RESTRICTED)


def training_allowed(provider: str) -> bool:
    return get_policy(provider).model_training == Permission.ALLOWED
