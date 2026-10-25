"""
Audience quality and risk screening.

Outputs are RISK-SCREENING RESULTS, not forensic determinations. A flag means
"look at this before paying", never "this creator bought followers". Only data
whose provider policy permits derived analysis is used; the caller filters
inputs through rpi.compliance.policies.scoring_allowed.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

SCREENING_LABEL = "risk screening result (not a forensic determination)"


@dataclass
class VideoStats:
    published_at: Optional[datetime]
    views: Optional[int]
    likes: Optional[int]
    comments: Optional[int]


@dataclass
class SnapshotStat:
    observed_at: datetime
    subscribers: Optional[int]


@dataclass
class RiskSignal:
    signal_type: str
    severity: str  # info | review | elevated
    value: Optional[float]
    detail: str


def screen(
    videos: list[VideoStats],
    snapshots: list[SnapshotStat],
    *,
    subscribers: Optional[int],
    now: Optional[datetime] = None,
    relevant_share_recent: Optional[float] = None,
    relevant_share_older: Optional[float] = None,
) -> dict:
    now = now or datetime.now(timezone.utc)
    signals: list[RiskSignal] = []

    views = [v.views for v in videos if v.views is not None]
    if len(views) >= 6:
        median = statistics.median(views)
        top = max(views)
        if median > 0 and top / median >= 20:
            signals.append(RiskSignal("view_concentration", "review", round(top / median, 1),
                                      f"best video has {top / median:.0f}x the median views; typical results may be much lower"))
        if subscribers and subscribers > 0:
            ratio = median / subscribers
            if ratio < 0.005:
                signals.append(RiskSignal("low_views_per_subscriber", "review", round(ratio, 4),
                                          f"median views are {ratio:.2%} of subscribers; subscriber base may be inactive"))

    rates = [v.likes / v.views for v in videos if v.views and v.likes is not None and v.views > 0]
    if len(rates) >= 6:
        mean = statistics.mean(rates)
        if mean > 0:
            cv = statistics.pstdev(rates) / mean
            if cv > 1.2:
                signals.append(RiskSignal("engagement_inconsistency", "review", round(cv, 2),
                                          "like rate varies unusually between videos"))
        if mean > 0.25:
            signals.append(RiskSignal("engagement_unusually_high", "review", round(mean, 3),
                                      f"average like rate {mean:.1%} is far above typical ranges"))
        elif mean < 0.002:
            signals.append(RiskSignal("engagement_unusually_low", "info", round(mean, 4),
                                      f"average like rate {mean:.2%} is low"))

    dated = sorted(v.published_at for v in videos if v.published_at)
    if dated:
        days_since = (now - dated[-1]).days
        if days_since > 60:
            signals.append(RiskSignal("inactive", "review", days_since, f"no upload in {days_since} days"))
        if len(dated) >= 6:
            gaps = [(b - a).days for a, b in zip(dated, dated[1:])]
            mean_gap = statistics.mean(gaps)
            if mean_gap > 0 and statistics.pstdev(gaps) / mean_gap > 1.5:
                signals.append(RiskSignal("irregular_posting", "info", round(mean_gap, 1),
                                          "posting cadence is highly irregular"))

    snaps = sorted((s for s in snapshots if s.subscribers is not None), key=lambda s: s.observed_at)
    if len(snaps) >= 4:
        deltas = []
        for a, b in zip(snaps, snaps[1:]):
            days = max((b.observed_at - a.observed_at).total_seconds() / 86400, 1)
            deltas.append((b.subscribers - a.subscribers) / days)
        typical = statistics.median([abs(d) for d in deltas]) or 1
        spike = max(deltas)
        if spike > 10 * typical and spike > 200:
            signals.append(RiskSignal("growth_spike", "review", round(spike, 1),
                                      f"subscriber growth spike of ~{spike:.0f}/day vs typical {typical:.0f}/day"))
        if any(d < -5 * typical and d < -200 for d in deltas):
            signals.append(RiskSignal("subscriber_drop", "review", None, "sharp subscriber decline between snapshots"))

    if relevant_share_recent is not None and relevant_share_older is not None:
        if relevant_share_older - relevant_share_recent > 0.4:
            signals.append(RiskSignal("niche_drift", "review", round(relevant_share_recent, 2),
                                      "recent videos are much less ReelForge-relevant than older ones"))

    review_count = sum(1 for s in signals if s.severity == "review")
    level = "elevated_review" if review_count >= 3 else "review_recommended" if review_count else "no_flags"
    return {
        "label": SCREENING_LABEL,
        "level": level,
        "signals": [s.__dict__ for s in signals],
        "data_points": {"videos": len(videos), "snapshots": len(snapshots)},
        "limitations": "Anomalies have innocent explanations (a viral video, a channel rename, seasonality). "
        "Use flags to decide what to check, not to judge the creator.",
    }
