"""
Enrichment value gate: "could this paid result change the decision?"

Evaluated before every paid enrichment request, independently of the budget
check. Rules mirror the directive's spending policies:
  * no demographics for creators rejected for niche mismatch (or with low
    niche relevance)
  * transcripts only for stage-3+ creators whose classification is
    uncertain, at most a few per creator
  * no contact lookup (15 CreatorDB credits) unless the creator is recommended
  * no SparkToro outside explicit audience-first runs (enforced in discovery)
  * no re-enrichment of a record fetched within its freshness window
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from rpi.providers.base import Capability

BLOCKED_STATUSES = {"rejected", "do_not_contact"}


@dataclass
class CreatorState:
    pipeline_status: str
    funnel_stage: int
    niche_relevance: Optional[float]
    classification_confidence: Optional[float]
    recommendation: Optional[str]
    last_fetched: dict[str, datetime]  # operation -> last successful fetch
    transcripts_fetched: int = 0


@dataclass
class GateDecision:
    allowed: bool
    reason: str


FRESHNESS = {
    Capability.CREATOR_PROFILE: timedelta(days=7),
    Capability.CREATOR_CONTENT: timedelta(days=7),
    Capability.AUDIENCE_DEMOGRAPHICS: timedelta(days=30),
    Capability.SPONSORSHIP_HISTORY: timedelta(days=30),
    Capability.CREATOR_CONTACTS: timedelta(days=60),
    Capability.TRANSCRIPT: timedelta(days=30),
}
MAX_TRANSCRIPTS_PER_CREATOR = 3


def evaluate(operation: Capability, state: CreatorState, *, force: bool = False,
             now: Optional[datetime] = None) -> GateDecision:
    now = now or datetime.now(timezone.utc)
    if state.pipeline_status in BLOCKED_STATUSES:
        return GateDecision(False, f"creator is {state.pipeline_status}; no further paid enrichment")

    last = state.last_fetched.get(operation.value)
    if not force and last and operation in FRESHNESS and now - last < FRESHNESS[operation]:
        return GateDecision(False, f"{operation.value} fetched {(now - last).days}d ago; still fresh")

    if operation == Capability.AUDIENCE_DEMOGRAPHICS:
        if state.niche_relevance is None:
            return GateDecision(False, "classify content before paying for demographics")
        if state.niche_relevance < 0.4:
            return GateDecision(False, f"niche relevance {state.niche_relevance:.2f} too low for demographics to matter")
        if state.funnel_stage < 3:
            return GateDecision(False, "demographics are retrieved from stage 3 (analysis) onward")

    if operation == Capability.TRANSCRIPT:
        if state.funnel_stage < 3:
            return GateDecision(False, "transcripts only for stage-3+ creators")
        if state.classification_confidence is not None and state.classification_confidence >= 0.6:
            return GateDecision(False, "classification already confident; transcript would not change the decision")
        if state.transcripts_fetched >= MAX_TRANSCRIPTS_PER_CREATOR:
            return GateDecision(False, f"transcript limit ({MAX_TRANSCRIPTS_PER_CREATOR}) reached for this creator")

    if operation == Capability.CREATOR_CONTACTS:
        if state.recommendation != "recommend":
            return GateDecision(False, "contacts are retrieved only for recommended creators")
        if state.funnel_stage < 4:
            return GateDecision(False, "contacts are retrieved from stage 4 (deep qualification) onward")

    if operation == Capability.SPONSORSHIP_HISTORY and state.funnel_stage < 3:
        return GateDecision(False, "provider sponsorship history is retrieved from stage 3 onward")

    return GateDecision(True, "may change the decision")
