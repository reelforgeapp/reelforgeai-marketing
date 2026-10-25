"""
Audience fit engine.

Scores a creator separately against each ideal-customer profile (ICP). Each
component records the measurement basis of the data behind it:

  first_party_measured  creator-authorized analytics
  third_party_modeled   provider demographic estimates
  manual_submitted      creator-sent screenshots / media kits (unverified)
  content_inferred      RPI's own content classification
  unavailable           no data — the component is excluded, never imputed

The overall fit is a weighted mean over *available* components only, and
`completeness` reports how much of the profile could be evaluated.
SparkToro audience research is never used here: it describes an audience,
not a particular channel's viewers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from rpi.core.provenance import MeasurementType


@dataclass(frozen=True)
class ICP:
    key: str
    name: str
    description: str
    content_labels: tuple[str, ...]
    audience_problems: tuple[str, ...]
    countries: tuple[str, ...] = ("US", "CA", "GB", "AU")
    languages: tuple[str, ...] = ("en",)
    age_buckets: tuple[str, ...] = ()
    weights: dict = field(
        default_factory=lambda: {"content": 0.45, "problems": 0.15, "geography": 0.2, "language": 0.1, "age": 0.1}
    )


DEFAULT_ICPS: tuple[ICP, ...] = (
    ICP(
        "icp_a_faceless_entrepreneurs",
        "ICP A — Faceless content entrepreneurs",
        "People seeking scalable, automated video production for faceless channels.",
        ("faceless_youtube_automation", "ai_shorts_creation", "creator_monetization", "ai_video_tools", "ai_voice_generation"),
        ("difficulty_creating_videos_consistently", "high_production_time", "operating_multiple_channels",
         "limited_technical_skills", "voiceover_problems"),
        age_buckets=("18-24", "25-34", "35-44"),
    ),
    ICP(
        "icp_b_small_business_marketers",
        "ICP B — Small-business marketers",
        "Businesses that need consistent short-form content.",
        ("social_media_content_automation", "ai_video_tools", "ai_shorts_creation"),
        ("expensive_video_editing", "high_production_time", "difficulty_producing_vertical_content",
         "need_automated_publishing", "limited_technical_skills"),
        age_buckets=("25-34", "35-44", "45-54"),
    ),
    ICP(
        "icp_c_content_agencies",
        "ICP C — Content agencies",
        "Organizations managing many clients or publishing workflows.",
        ("social_media_content_automation", "ai_video_tools", "faceless_youtube_automation"),
        ("operating_multiple_channels", "need_automated_publishing", "high_production_time", "need_original_content"),
        age_buckets=("25-34", "35-44", "45-54"),
    ),
)

# Preference order when several sources supply the same dimension.
BASIS_RANK = {
    MeasurementType.FIRST_PARTY_MEASURED: 4,
    MeasurementType.THIRD_PARTY_MODELED: 3,
    MeasurementType.MANUAL_SUBMITTED: 2,
    MeasurementType.CONTENT_INFERRED: 1,
}


@dataclass
class AudienceEvidence:
    """Audience shares for one dimension from one source."""

    dimension: str
    shares: dict[str, float]
    measurement: MeasurementType
    provider: str


@dataclass
class ComponentScore:
    name: str
    score: Optional[float]
    basis: str
    detail: str


@dataclass
class ICPFit:
    icp: str
    name: str
    fit: Optional[float]
    completeness: float
    components: list[ComponentScore]
    missing: list[str]


def _best_source(evidence: list[AudienceEvidence], dimension: str) -> Optional[AudienceEvidence]:
    candidates = [e for e in evidence if e.dimension == dimension and e.shares]
    if not candidates:
        return None
    return max(candidates, key=lambda e: BASIS_RANK.get(e.measurement, 0))


def score_icp(
    icp: ICP,
    *,
    video_labels: list[list[str]],
    audience_problems: list[str],
    audience: list[AudienceEvidence],
    creator_language: Optional[str],
    creator_language_basis: MeasurementType = MeasurementType.OBSERVED,
) -> ICPFit:
    components: list[ComponentScore] = []
    missing: list[str] = []

    if video_labels:
        hits = sum(1 for labels in video_labels if set(labels) & set(icp.content_labels))
        components.append(ComponentScore("content", hits / len(video_labels), MeasurementType.CONTENT_INFERRED.value,
                                         f"{hits} of {len(video_labels)} classified videos match ICP topics"))
    else:
        components.append(ComponentScore("content", None, MeasurementType.UNAVAILABLE.value, "no classified content"))
        missing.append("content classification")

    if audience_problems:
        matched = set(audience_problems) & set(icp.audience_problems)
        components.append(ComponentScore("problems", min(1.0, len(matched) / 2), MeasurementType.CONTENT_INFERRED.value,
                                         "problems discussed: " + (", ".join(sorted(matched)) or "none matching")))
    else:
        components.append(ComponentScore("problems", None, MeasurementType.UNAVAILABLE.value, "no audience problems identified"))

    geo = _best_source(audience, "country")
    if geo:
        share = sum(v for k, v in geo.shares.items() if k.upper() in icp.countries)
        components.append(ComponentScore("geography", min(share, 1.0), geo.measurement.value,
                                         f"{share:.0%} of audience in {', '.join(icp.countries)} ({geo.provider})"))
    else:
        components.append(ComponentScore("geography", None, MeasurementType.UNAVAILABLE.value, "no audience country data"))
        missing.append("audience geography")

    lang = _best_source(audience, "language")
    if lang:
        share = sum(v for k, v in lang.shares.items() if k.lower() in icp.languages)
        components.append(ComponentScore("language", min(share, 1.0), lang.measurement.value,
                                         f"{share:.0%} audience language match ({lang.provider})"))
    elif creator_language:
        ok = creator_language.lower()[:2] in icp.languages
        components.append(ComponentScore("language", 1.0 if ok else 0.0, creator_language_basis.value,
                                         f"creator content language: {creator_language}"))
    else:
        components.append(ComponentScore("language", None, MeasurementType.UNAVAILABLE.value, "language unknown"))
        missing.append("language")

    age = _best_source(audience, "age")
    if age and icp.age_buckets:
        share = sum(v for k, v in age.shares.items() if k in icp.age_buckets)
        components.append(ComponentScore("age", min(share, 1.0), age.measurement.value,
                                         f"{share:.0%} of audience aged {', '.join(icp.age_buckets)} ({age.provider})"))
    else:
        components.append(ComponentScore("age", None, MeasurementType.UNAVAILABLE.value, "no audience age data"))
        missing.append("audience age")

    total_weight = sum(icp.weights.values())
    available = [(c, icp.weights.get(c.name, 0)) for c in components if c.score is not None]
    avail_weight = sum(w for _, w in available)
    fit = round(sum(c.score * w for c, w in available) / avail_weight, 3) if avail_weight else None
    return ICPFit(icp.key, icp.name, fit, round(avail_weight / total_weight, 3), components, missing)


def score_all(icps: tuple[ICP, ...] = DEFAULT_ICPS, **kwargs) -> list[ICPFit]:
    return [score_icp(icp, **kwargs) for icp in icps]
