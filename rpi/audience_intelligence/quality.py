"""
Audience quality and risk screening.

Outputs are RISK-SCREENING RESULTS, not forensic determinations. A flag means
"look at this before paying", never "this creator bought followers". An
`elevated` signal (flat engagement) is strong enough that qualification rejects
the creator, so the funnel and the autopilot skip them; a person can still
review and draft for them. Only data whose provider policy permits derived
analysis is used; the caller filters inputs through
rpi.compliance.policies.scoring_allowed.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

SCREENING_LABEL = "risk screening result (not a forensic determination)"

# Flat engagement: likes that stay the same whatever a video's views. On real
# channels likes rise with views (log-log slope 0.59-1.19 over the 15 creators
# scored in production on 2026-10-26); @BeerMoneyForum's videos all had ~2,500
# likes at 33k-160k views (slope 0.02), the pattern of a fixed package of
# engagement bought per video. Videos younger than FLAT_MIN_AGE are still
# collecting likes (or a package is still being delivered) and are left out.
FLAT_MIN_VIDEOS = 6
FLAT_MIN_AGE = timedelta(days=3)
FLAT_MIN_VIEW_SPREAD = 2.0  # 90th / 10th percentile views; below it the slope means little
FLAT_MAX_SLOPE = 0.3

# Uniform engagement: the case flat engagement cannot judge, where views are
# bought too, so every video lands on about the same views and likes.
# NyesWorldTV and Tech Explorer AI (2026-10-26) had views within 1.2-1.8x and
# likes within 1.2-2.2x (90th / 10th percentile) across 30-49 videos, views that
# stopped growing after a few days, and comments from the same comment farm; of
# 55 scored creators no other had both views under 2x and likes under 3x.
UNIFORM_MIN_VIDEOS = 10
UNIFORM_MAX_LIKE_SPREAD = 2.5

# Both engagement checks also run on the newest RECENT_VIDEOS judged videos, for
# channels that started buying partway through their history: Ari Thinks
# (2026-10-26) got 7-17k varied views until mid-August, then 36-48k on every
# video. Of the 55 scored creators this also flagged APEX_001, NexcopeAI and
# Trainer1300, each confirmed by hand (fixed view levels, farmed comments).
RECENT_VIDEOS = 15


@dataclass
class VideoStats:
    published_at: Optional[datetime]
    views: Optional[int]
    likes: Optional[int]
    comments: Optional[int]
    is_short: Optional[bool] = None


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
    total_views: Optional[int] = None,
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
    recent = sorted(_judged(videos, now), key=lambda v: v.published_at or _NEVER, reverse=True)[:RECENT_VIDEOS]
    for check in (flat_engagement, uniform_engagement):
        signal = check(videos, now)
        if signal is None and len(recent) < len(_judged(videos, now)):
            signal = check(recent, now)
            if signal:
                signal.detail = f"newest {len(recent)} videos: {signal.detail}"
        if signal:
            signals.append(signal)
    if total_views is not None and subscribers and total_views < subscribers:
        # Real channels have tens to hundreds of lifetime views per subscriber (11-634 over the
        # 55 creators scored by 2026-10-26); Ari Thinks had 737,764 views for 763,000 subscribers.
        signals.append(RiskSignal("views_below_subscribers", "elevated", round(total_views / subscribers, 2),
                                  f"{total_views:,} lifetime views for {subscribers:,} subscribers: fewer views than "
                                  "subscribers, where real channels have tens to hundreds per subscriber; typical of "
                                  "bought subscribers"))

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
    elevated = any(s.severity == "elevated" for s in signals)
    level = ("elevated_review" if elevated or review_count >= 3 else "review_recommended" if review_count
             else "no_flags")
    return {
        "label": SCREENING_LABEL,
        "level": level,
        "signals": [s.__dict__ for s in signals],
        "data_points": {"videos": len(videos), "snapshots": len(snapshots)},
        "limitations": "Anomalies have innocent explanations (a viral video, a channel rename, seasonality). "
        "Use flags to decide what to check, not to judge the creator.",
    }


_NEVER = datetime.min.replace(tzinfo=timezone.utc)


def _judged(videos: list[VideoStats], now: datetime) -> list[VideoStats]:
    """Long-form videos old enough to have collected their likes, with views and likes."""
    return [v for v in videos if v.is_short is not True and v.views and v.likes
            and (v.published_at is None or now - v.published_at >= FLAT_MIN_AGE)]


def _deciles(values: list[int]) -> tuple[int, int]:
    """10th and 90th percentile values."""
    ordered = sorted(values)
    return ordered[int(0.1 * (len(ordered) - 1))], ordered[int(0.9 * (len(ordered) - 1))]


def flat_engagement(videos: list[VideoStats], now: datetime) -> Optional[RiskSignal]:
    """An `elevated` signal when likes do not rise with views across recent
    long-form videos: the slope of log(likes) on log(views) is below
    FLAT_MAX_SLOPE although views vary at least FLAT_MIN_VIEW_SPREAD-fold."""
    vids = _judged(videos, now)
    if len(vids) < FLAT_MIN_VIDEOS:
        return None
    low, high = _deciles([v.views for v in vids])
    spread = high / low
    if spread < FLAT_MIN_VIEW_SPREAD:
        return None
    x = [math.log(v.views) for v in vids]
    y = [math.log(v.likes) for v in vids]
    mx, my = statistics.mean(x), statistics.mean(y)
    slope = sum((a - mx) * (b - my) for a, b in zip(x, y)) / sum((a - mx) ** 2 for a in x)
    if slope >= FLAT_MAX_SLOPE:
        return None
    likes = [v.likes for v in vids]
    return RiskSignal("flat_engagement", "elevated", round(slope, 3),
                      f"likes stay at {min(likes):,}-{max(likes):,} per video while views vary {spread:.1f}x across "
                      f"{len(vids)} videos: 10x the views brings {10 ** slope:.2f}x the likes, where real channels "
                      "get about 4-15x; typical of engagement bought per video")


def uniform_engagement(videos: list[VideoStats], now: datetime) -> Optional[RiskSignal]:
    """An `elevated` signal when recent long-form videos all land on about the
    same views (90th / 10th percentile under FLAT_MIN_VIEW_SPREAD) and the same
    likes (under UNIFORM_MAX_LIKE_SPREAD): views and likes bought per video."""
    vids = _judged(videos, now)
    if len(vids) < UNIFORM_MIN_VIDEOS:
        return None
    views_low, views_high = _deciles([v.views for v in vids])
    likes_low, likes_high = _deciles([v.likes for v in vids])
    view_spread, like_spread = views_high / views_low, likes_high / likes_low
    if view_spread >= FLAT_MIN_VIEW_SPREAD or like_spread >= UNIFORM_MAX_LIKE_SPREAD:
        return None
    return RiskSignal("uniform_engagement", "elevated", round(view_spread, 2),
                      f"most videos land on {views_low:,}-{views_high:,} views and {likes_low:,}-{likes_high:,} likes "
                      f"across {len(vids)} videos ({view_spread:.1f}x and {like_spread:.1f}x apart), where views and "
                      "likes usually vary 3x or more; typical of views and likes bought per video")
