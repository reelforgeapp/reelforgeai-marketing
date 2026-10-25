"""
Free, deterministic keyword prefilter.

Runs on every candidate before any paid call. It decides three things:
  * which taxonomy labels a video's title/description plausibly match,
  * a creator-level relevance estimate from those matches,
  * whether the result is clear enough to skip Claude entirely.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Optional

from rpi.content_intelligence.taxonomy import COMPILED, LABEL_BY_KEY, RELEVANT_LABELS


@dataclass
class VideoMatch:
    content_ref: str
    labels: dict[str, list[str]]  # label -> matched terms

    @property
    def top_label(self) -> Optional[str]:
        if not self.labels:
            return None
        return max(self.labels, key=lambda k: (LABEL_BY_KEY[k].reelforge_relevance, len(self.labels[k])))


@dataclass
class PrefilterResult:
    videos: list[VideoMatch]
    label_counts: Counter
    relevance: float
    relevant_video_share: float
    decision: str  # "clear_relevant" | "clear_irrelevant" | "needs_llm"
    reasons: list[str] = field(default_factory=list)


def match_text(content_ref: str, title: Optional[str], description: Optional[str]) -> VideoMatch:
    # Titles carry most of the signal; only the first 600 characters of a
    # description are used so link dumps and boilerplate don't dominate.
    text = f"{title or ''}\n{(description or '')[:600]}"
    hits: dict[str, list[str]] = {}
    for key, patterns in COMPILED.items():
        terms = []
        for p in patterns:
            m = p.search(text)
            if m:
                terms.append(m.group(0).lower())
        if terms:
            hits[key] = terms
    return VideoMatch(content_ref, hits)


def prefilter_creator(
    videos: list[tuple[str, Optional[str], Optional[str]]],
    *,
    relevant_threshold: float = 0.6,
    irrelevant_threshold: float = 0.1,
) -> PrefilterResult:
    matches = [match_text(ref, title, desc) for ref, title, desc in videos]
    counts: Counter = Counter()
    relevant_videos = 0
    weighted = 0.0
    for m in matches:
        top = m.top_label
        if top:
            counts[top] += 1
            weighted += LABEL_BY_KEY[top].reelforge_relevance
            if top in RELEVANT_LABELS:
                relevant_videos += 1
    n = len(matches)
    share = relevant_videos / n if n else 0.0
    # Videos with no matches count as relevance 0.
    relevance = weighted / n if n else 0.0

    reasons = [f"{k}: {v} of {n} videos" for k, v in counts.most_common(4)]
    if n == 0:
        decision = "needs_llm"
        reasons.append("no content available")
    elif share >= relevant_threshold and n >= 5:
        decision = "clear_relevant"
    elif relevance <= irrelevant_threshold and n >= 8:
        decision = "clear_irrelevant"
    else:
        decision = "needs_llm"
    return PrefilterResult(matches, counts, round(relevance, 3), round(share, 3), decision, reasons)
