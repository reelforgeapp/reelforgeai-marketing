"""
Partnership CRM pipeline.

Statuses and allowed transitions are explicit. Every transition is written to
rpi_pipeline_events (actor, reason, timestamp). `do_not_contact` is terminal
except for an admin reversal, and `rejected` / `do_not_contact` are reachable
from anywhere.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from rpi.core.audit import audit


class Status(str, Enum):
    DISCOVERED = "discovered"
    PRELIMINARY_QUALIFIED = "preliminary_qualified"
    DEEP_ANALYSIS = "deep_analysis"
    RECOMMENDED = "recommended"
    CONTACT_IDENTIFIED = "contact_identified"
    OUTREACH_APPROVED = "outreach_approved"
    CONTACTED = "contacted"
    REPLIED = "replied"
    NEGOTIATING = "negotiating"
    AGREEMENT_PENDING = "agreement_pending"
    ACTIVE_PARTNERSHIP = "active_partnership"
    CAMPAIGN_COMPLETED = "campaign_completed"
    UNDER_PERFORMANCE_REVIEW = "under_performance_review"
    RE_ENGAGEMENT_CANDIDATE = "re_engagement_candidate"
    REJECTED = "rejected"
    DO_NOT_CONTACT = "do_not_contact"


S = Status
TRANSITIONS: dict[Status, set[Status]] = {
    S.DISCOVERED: {S.PRELIMINARY_QUALIFIED, S.DEEP_ANALYSIS},
    S.PRELIMINARY_QUALIFIED: {S.DEEP_ANALYSIS, S.DISCOVERED},
    S.DEEP_ANALYSIS: {S.RECOMMENDED, S.PRELIMINARY_QUALIFIED},
    S.RECOMMENDED: {S.CONTACT_IDENTIFIED, S.DEEP_ANALYSIS},
    S.CONTACT_IDENTIFIED: {S.OUTREACH_APPROVED, S.RECOMMENDED},
    S.OUTREACH_APPROVED: {S.CONTACTED, S.CONTACT_IDENTIFIED},
    S.CONTACTED: {S.REPLIED, S.RE_ENGAGEMENT_CANDIDATE},
    S.REPLIED: {S.NEGOTIATING, S.RE_ENGAGEMENT_CANDIDATE},
    S.NEGOTIATING: {S.AGREEMENT_PENDING, S.RE_ENGAGEMENT_CANDIDATE},
    S.AGREEMENT_PENDING: {S.ACTIVE_PARTNERSHIP, S.NEGOTIATING},
    S.ACTIVE_PARTNERSHIP: {S.CAMPAIGN_COMPLETED},
    S.CAMPAIGN_COMPLETED: {S.UNDER_PERFORMANCE_REVIEW},
    S.UNDER_PERFORMANCE_REVIEW: {S.RE_ENGAGEMENT_CANDIDATE, S.NEGOTIATING, S.ACTIVE_PARTNERSHIP},
    S.RE_ENGAGEMENT_CANDIDATE: {S.RECOMMENDED, S.CONTACT_IDENTIFIED, S.NEGOTIATING},
    S.REJECTED: {S.DISCOVERED, S.RE_ENGAGEMENT_CANDIDATE},
    S.DO_NOT_CONTACT: set(),
}
ALWAYS_ALLOWED = {S.REJECTED, S.DO_NOT_CONTACT}

# Statuses in which a new outreach message may be prepared.
OUTREACH_ELIGIBLE = {S.RECOMMENDED, S.CONTACT_IDENTIFIED, S.OUTREACH_APPROVED, S.RE_ENGAGEMENT_CANDIDATE}

FUNNEL_STAGE = {
    S.DISCOVERED: 1, S.PRELIMINARY_QUALIFIED: 2, S.DEEP_ANALYSIS: 3, S.RECOMMENDED: 5, S.CONTACT_IDENTIFIED: 5,
}


class TransitionError(ValueError):
    pass


def check_transition(current: Status, target: Status, *, is_admin: bool = False) -> None:
    if current == target:
        raise TransitionError(f"already {current.value}")
    if current == S.DO_NOT_CONTACT:
        if is_admin and target == S.REJECTED:
            return
        raise TransitionError("do_not_contact can only be reversed to rejected by an admin")
    if target in ALWAYS_ALLOWED:
        return
    if target not in TRANSITIONS[current]:
        allowed = ", ".join(sorted(s.value for s in TRANSITIONS[current] | ALWAYS_ALLOWED))
        raise TransitionError(f"cannot move from {current.value} to {target.value}; allowed: {allowed}")


async def transition(conn, creator_id, target: Status, *, actor: str, reason: Optional[str] = None,
                     is_admin: bool = False) -> dict:
    row = await conn.fetchrow("SELECT pipeline_status FROM rpi_creators WHERE id = $1 FOR UPDATE", creator_id)
    if not row:
        raise TransitionError("creator not found")
    current = Status(row["pipeline_status"])
    check_transition(current, target, is_admin=is_admin)
    stage = FUNNEL_STAGE.get(target)
    await conn.execute(
        """UPDATE rpi_creators SET pipeline_status = $2, updated_at = NOW(),
               funnel_stage = COALESCE($3, funnel_stage),
               disqualified_reason = CASE WHEN $2 IN ('rejected', 'do_not_contact') THEN $4 ELSE disqualified_reason END
           WHERE id = $1""",
        creator_id, target.value, stage, reason,
    )
    await conn.execute(
        "INSERT INTO rpi_pipeline_events (creator_id, from_status, to_status, actor, reason) VALUES ($1, $2, $3, $4, $5)",
        creator_id, current.value, target.value, actor, reason,
    )
    if target == S.DO_NOT_CONTACT:
        from rpi.crm.suppression import suppress_creator

        await suppress_creator(conn, creator_id, reason or "marked do_not_contact", source=actor)
        # Withdraw any pending drafts.
        await conn.execute(
            "UPDATE rpi_outreach_drafts SET status = 'blocked', failure_reason = 'do_not_contact' "
            "WHERE creator_id = $1 AND status IN ('draft', 'approved')",
            creator_id,
        )
    await audit(conn, actor, "crm.transition", "creator", creator_id,
                {"from": current.value, "to": target.value, "reason": reason})
    return {"creator_id": str(creator_id), "from": current.value, "to": target.value}
