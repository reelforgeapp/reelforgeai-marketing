"""
Transparent, deterministic qualification scoring.

Version-stamped weights, no black-box model. Each component reports its
value, weight, data basis and source provider. Inputs whose provider policy
forbids derived metrics (YouTube API data; CreatorDB until its terms are
reviewed) are excluded and listed in `excluded_sources`; the score is then
computed over the remaining components and its confidence drops accordingly.

Size targets (10k-150k subscribers, $300-$1,000 fee) are soft preferences:
a highly relevant creator outside the band is penalized slightly, never
excluded.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

SCORING_VERSION = "qual-2026-10-25.2"
SATURATION_MIN_VIDEOS = 5

WEIGHTS = {
    "niche_relevance": 0.40,
    "audience_fit": 0.20,
    "activity": 0.15,
    "commercial_fit": 0.15,
    "size_fit": 0.10,
}


@dataclass
class Sourced:
    value: Optional[float]
    provider: Optional[str]
    basis: str
    detail: str = ""


@dataclass
class QualificationInput:
    niche_relevance: Sourced  # deterministic computed_relevance from classification
    classification_confidence: float
    best_icp_fit: Sourced
    icp_completeness: float
    last_upload_at: Optional[datetime]
    uploads_last_90d: Optional[int]
    activity_provider: Optional[str]
    subscribers: Optional[int]
    subscribers_provider: Optional[str]
    relevant_sponsorships: int  # disclosed affiliate / confirmed paid for registry brands
    expected_fee_usd: Optional[float]
    risk_level: str  # no_flags | review_recommended | elevated_review
    scoring_allowed: dict[str, bool] = field(default_factory=dict)
    # Paid direct-competitor deals in the last 90 days, e.g. "InVideo (paid, 2026-10-17)".
    # Flagged for an exclusivity check; they do not lower the score.
    recent_competitor_deals: list[str] = field(default_factory=list)
    # Sponsor saturation over recent long-form videos (platform flag or confirmed paid).
    sponsored_share: Optional[float] = None
    sponsored_sample: int = 0
    # Median views of sponsored videos / median views of the other recent videos.
    sponsored_view_ratio: Optional[float] = None
    preferred_min_subs: int = 10_000
    preferred_max_subs: int = 150_000
    preferred_max_fee: float = 1_000.0


@dataclass
class QualificationResult:
    scoring_version: str
    overall: Optional[float]
    confidence: float
    recommendation: str
    components: dict[str, dict]
    missing_data: list[str]
    excluded_sources: list[str]
    reasons: list[str]


def _allowed(inp: QualificationInput, provider: Optional[str]) -> bool:
    if provider is None:
        return True
    return inp.scoring_allowed.get(provider, False)


def activity_score(last_upload_at: Optional[datetime], uploads_90d: Optional[int], now: datetime) -> Optional[float]:
    if last_upload_at is None and uploads_90d is None:
        return None
    score = 0.0
    parts = 0
    if last_upload_at is not None:
        days = (now - last_upload_at).days
        score += 1.0 if days <= 14 else 0.8 if days <= 30 else 0.4 if days <= 60 else 0.0
        parts += 1
    if uploads_90d is not None:
        score += min(uploads_90d / 12, 1.0)
        parts += 1
    return round(score / parts, 3)


def size_score(subs: Optional[int], lo: int, hi: int) -> Optional[float]:
    if subs is None:
        return None
    if lo <= subs <= hi:
        return 1.0
    if subs < lo:
        return round(max(0.4, subs / lo), 3)
    return round(max(0.4, hi / subs), 3)


def score(inp: QualificationInput, now: Optional[datetime] = None) -> QualificationResult:
    now = now or datetime.now(timezone.utc)
    components: dict[str, dict] = {}
    missing: list[str] = []
    excluded: list[str] = []
    reasons: list[str] = []

    def add(name: str, value: Optional[float], provider: Optional[str], basis: str, detail: str):
        if value is not None and not _allowed(inp, provider):
            excluded.append(f"{name} from {provider} (provider policy does not permit derived scoring)")
            value = None
        elif value is None:
            missing.append(name)
        components[name] = {"value": value, "weight": WEIGHTS[name], "basis": basis, "provider": provider, "detail": detail}

    nr = inp.niche_relevance
    add("niche_relevance", nr.value, nr.provider, nr.basis, nr.detail)
    fit = inp.best_icp_fit
    add("audience_fit", fit.value, fit.provider, fit.basis, f"{fit.detail} (completeness {inp.icp_completeness:.0%})")
    add("activity", activity_score(inp.last_upload_at, inp.uploads_last_90d, now), inp.activity_provider,
        "observed", "recency of uploads and uploads in the last 90 days")
    size = size_score(inp.subscribers, inp.preferred_min_subs, inp.preferred_max_subs)
    add("size_fit", size, inp.subscribers_provider, "observed",
        f"soft preference {inp.preferred_min_subs:,}-{inp.preferred_max_subs:,} subscribers")

    commercial = 0.5
    detail = []
    if inp.relevant_sponsorships:
        commercial += min(0.3, 0.15 * inp.relevant_sponsorships)
        detail.append(f"{inp.relevant_sponsorships} relevant software promotion(s)")
    # Saturation thresholds are judgment calls, not calibrated: revisit them
    # once campaign outcomes exist (see AI_SCORING_AND_EVALUATION.md).
    if inp.sponsored_share is not None and inp.sponsored_sample >= SATURATION_MIN_VIDEOS:
        share = f"{inp.sponsored_share:.0%} of {inp.sponsored_sample} recent videos sponsored"
        if inp.sponsored_share > 0.5:
            commercial -= 0.2
            detail.append(share + " (saturated)")
        elif inp.sponsored_share > 0.3:
            commercial -= 0.1
            detail.append(share + " (heavy)")
        else:
            detail.append(share)
    if inp.sponsored_view_ratio is not None:
        ratio = f"sponsored videos get {inp.sponsored_view_ratio:.0%} of their usual views"
        if inp.sponsored_view_ratio < 0.6:
            commercial -= 0.1
            detail.append(ratio + " (audience tunes out)")
        elif inp.sponsored_view_ratio >= 0.9:
            commercial += 0.05
            detail.append(ratio)
        else:
            detail.append(ratio)
    if inp.expected_fee_usd is not None:
        if inp.expected_fee_usd > inp.preferred_max_fee * 2:
            commercial -= 0.2
            detail.append(f"expected fee ${inp.expected_fee_usd:,.0f} well above preferred range")
        elif inp.expected_fee_usd <= inp.preferred_max_fee:
            commercial += 0.1
            detail.append("expected fee within preferred range")
    add("commercial_fit", round(max(0.0, min(1.0, commercial)), 3), None, "computed", "; ".join(detail) or "no sponsorship evidence")
    flags = [f"check exclusivity before outreach: {d}" for d in inp.recent_competitor_deals]
    components["commercial_fit"]["flags"] = flags
    reasons.extend(flags)

    available = {k: c for k, c in components.items() if c["value"] is not None}
    weight = sum(WEIGHTS[k] for k in available)
    overall = round(sum(c["value"] * WEIGHTS[k] for k, c in available.items()) / weight, 3) if weight else None

    risk_penalty = {"no_flags": 0.0, "review_recommended": 0.05, "elevated_review": 0.15}.get(inp.risk_level, 0.0)
    if overall is not None and risk_penalty:
        overall = round(max(0.0, overall - risk_penalty), 3)
        reasons.append(f"risk screening '{inp.risk_level}' subtracts {risk_penalty}")

    completeness = weight / sum(WEIGHTS.values())
    confidence = round(completeness * (0.5 + 0.5 * inp.classification_confidence) * (0.6 + 0.4 * inp.icp_completeness), 3)

    niche = components["niche_relevance"]["value"]
    if niche is None:
        recommendation = "needs_data"
        reasons.append("niche relevance unavailable")
    elif niche < 0.25 and inp.classification_confidence >= 0.6:
        recommendation = "reject_niche_mismatch"
        reasons.append(f"niche relevance {niche:.2f} below 0.25")
    elif overall is not None and overall >= 0.7 and niche >= 0.6 and inp.risk_level != "elevated_review":
        recommendation = "recommend"
    elif overall is not None and overall >= 0.5:
        recommendation = "investigate"
    else:
        recommendation = "hold"
    if excluded:
        reasons.append("some inputs excluded by data-licensing policy")

    return QualificationResult(SCORING_VERSION, overall, confidence, recommendation, components, missing, excluded, reasons)
