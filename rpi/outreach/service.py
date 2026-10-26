"""
Draft creation and approval, shared by the API and the autopilot.

Both paths run the same checks: a draft rests only on verified research and a
stored contact, and approval requires a draft that passes validation, a contact
and no other approved or sending message to the creator.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import asyncpg

from rpi.api.serialize import jsonb
from rpi.core.audit import audit
from rpi.crm.contact_history import partner_scout_history
from rpi.crm.pipeline import Status, transition
from rpi.enrichment.service import creator_competitor_deals
from rpi.outreach.drafts import DraftContext, generate, save_draft


# The contact a draft goes to when none is chosen: business email first,
# never one already known to be invalid or undeliverable.
USABLE_EMAIL_SQL = """
    SELECT id, value FROM rpi_creator_contacts WHERE creator_id = $1 AND contact_type = 'email'
      AND COALESCE(verification_status, '') NOT IN ('invalid', 'undeliverable')
    ORDER BY is_business_contact DESC NULLS LAST, created_at LIMIT 1"""


class OutreachRefused(Exception):
    """The request cannot be carried out; `status` is the HTTP status the API returns."""

    def __init__(self, status: int, detail: str):
        super().__init__(detail)
        self.status = status
        self.detail = detail


async def create_draft(conn, creator_id, *, program, llm, price, actor: str, proposition: str = "paid sponsorship",
                       request_audience_data: bool = True, contact_id: Optional[str] = None) -> dict:
    """Generate, validate and store a draft (Claude when `llm` is given, else the template). Never sends."""
    creator = await conn.fetchrow("SELECT display_name, pipeline_status FROM rpi_creators WHERE id = $1", creator_id)
    if not creator:
        raise OutreachRefused(404, "creator not found")
    if creator["pipeline_status"] in ("rejected", "do_not_contact"):
        raise OutreachRefused(409, f"creator is {creator['pipeline_status']}")
    settings_row = await conn.fetchrow("SELECT sender_name, sender_email FROM rpi_outreach_settings LIMIT 1")
    cls = await conn.fetchrow(
        "SELECT labels, product_capabilities, evidence FROM rpi_content_classifications WHERE creator_id = $1 "
        "AND content_id IS NULL ORDER BY created_at DESC LIMIT 1", creator_id)
    if not cls:
        raise OutreachRefused(409, "research the creator before drafting outreach (no classification yet)")
    evidence = jsonb(cls["evidence"]) or []
    refs = [e.get("ref") for e in evidence if e.get("ref")]
    videos = await conn.fetch(
        "SELECT id, title, url FROM rpi_creator_content WHERE creator_id = $1 AND id::text = ANY($2::text[])", creator_id, refs)
    if not videos:
        raise OutreachRefused(409, "no verified relevant videos to ground the message")
    if contact_id:
        contact = await conn.fetchrow("SELECT id FROM rpi_creator_contacts WHERE id = $1 AND creator_id = $2",
                                      contact_id, creator_id)
        if not contact:
            raise OutreachRefused(422, "contact does not belong to this creator")
    else:
        contact = await conn.fetchrow(USABLE_EMAIL_SQL, creator_id)
    labels = jsonb(cls["labels"]) or {}
    deals = await creator_competitor_deals(conn, creator_id, now=datetime.now(timezone.utc))
    contact_email = await conn.fetchval("SELECT value FROM rpi_creator_contacts WHERE id = $1", contact["id"]) if contact else None
    prior = await partner_scout_history(conn, creator_id, email=contact_email)
    months = program.duration_months
    dctx = DraftContext(
        creator_id=str(creator_id), creator_name=creator["display_name"],
        videos=[{"ref": str(v["id"]), "title": v["title"], "url": v["url"]} for v in videos],
        primary_labels=labels.get("primary", []), capabilities=jsonb(cls["product_capabilities"]) or [],
        commission_rate_pct=str(int(program.rate * 100)),
        commission_duration_text=f"for {months} months" if months else "for the lifetime of referred customers",
        proposition=proposition, request_audience_data=request_audience_data,
        sender_name=(settings_row["sender_name"] if settings_row and settings_row["sender_name"] else "The ReelForge AI team"),
        competitor_deals=tuple(brand for brand, _ in deals),
        avoid_title_terms=tuple(n for row in await conn.fetch("SELECT name, aliases FROM rpi_competitors WHERE active")
                                for n in (row["name"], *row["aliases"]) if n),
        prior_contact_month=prior.last_contacted_at.strftime("%B %Y") if prior and prior.last_contacted_at else None,
    )
    draft = await generate(llm, price, dctx, sender_email=settings_row["sender_email"] if settings_row else None)
    if prior:
        draft["grounding"]["prior_contact"] = prior.as_dict()
    async with conn.transaction():
        draft_id = await save_draft(conn, creator_id, contact["id"] if contact else None, draft, actor)
    return {"id": draft_id, **draft, "contact_id": str(contact["id"]) if contact else None}


async def approve(conn, draft_id, actor: str) -> None:
    """Approve a draft for sending and move the creator to outreach_approved."""
    async with conn.transaction():
        row = await conn.fetchrow("SELECT * FROM rpi_outreach_drafts WHERE id = $1 FOR UPDATE", draft_id)
        if not row or row["status"] != "draft":
            raise OutreachRefused(409, "draft not in 'draft' status")
        validation = jsonb(row["validation"]) or {}
        if not validation.get("ok"):
            raise OutreachRefused(422, "draft fails validation; edit it first: " + "; ".join(validation.get("errors", [])[:3]))
        if not row["contact_id"]:
            raise OutreachRefused(422, "draft has no contact; add a verified contact first")
        try:
            await conn.execute(
                "UPDATE rpi_outreach_drafts SET status = 'approved', approved_by = $2, approved_at = NOW() WHERE id = $1",
                draft_id, actor)
        except asyncpg.UniqueViolationError:
            raise OutreachRefused(409, "another message to this creator is already approved or sending")
        status = await conn.fetchval("SELECT pipeline_status FROM rpi_creators WHERE id = $1", row["creator_id"])
        if status == Status.RECOMMENDED.value:
            await transition(conn, row["creator_id"], Status.CONTACT_IDENTIFIED, actor=actor, reason="contact on file")
            status = Status.CONTACT_IDENTIFIED.value
        if status == Status.CONTACT_IDENTIFIED.value:
            await transition(conn, row["creator_id"], Status.OUTREACH_APPROVED, actor=actor, reason="draft approved")
        await audit(conn, actor, "outreach.approved", "outreach_draft", draft_id, {})
