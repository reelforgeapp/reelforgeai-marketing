"""
Controlled sending of approved outreach drafts.

Initial operating mode is research + drafts only: rpi_outreach_settings.
sending_enabled defaults to FALSE and only an admin can turn it on. A send is
refused unless every gate passes; the reasons are returned and stored.

Gates (all enforced in code, independent of any model output):
  1. sending enabled for the workspace
  2. draft status is 'approved' by a principal with the approver role
  3. draft re-validates (familiarity / claims / numbers / URLs)
  4. recipient comes from a stored contact record for this creator
  5. recipient and creator are not suppressed (RPI + Partner Scout opt-outs)
  6. creator in an outreach-eligible CRM status
  7. no message to this creator or address within min_days_between_contacts
  8. daily send limit not reached
  9. CAN-SPAM: sender identity and postal address configured
 10. exactly-once: status moves approved → sending via compare-and-swap
"""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

import structlog

from app.config import get_settings
from rpi.core.audit import audit
from rpi.crm.pipeline import OUTREACH_ELIGIBLE, Status, transition
from rpi.crm.suppression import is_suppressed
from rpi.outreach.drafts import regrounding_from_row
from rpi.outreach.validator import EMAIL_RE, validate_draft

logger = structlog.get_logger()

SendFn = Callable[..., Awaitable[dict]]


@dataclass
class SendDecision:
    allowed: bool
    reasons: list[str]


def unsubscribe_token(draft_id: str) -> str:
    secret = (get_settings().rpi_attribution_hash_salt or "") + ":unsubscribe"
    return hmac.new(secret.encode(), draft_id.encode(), hashlib.sha256).hexdigest()[:32]


def verify_unsubscribe_token(draft_id: str, token: str) -> bool:
    return hmac.compare_digest(unsubscribe_token(draft_id), token or "")


def compliance_footer(settings_row: dict, draft_id: str, public_base_url: str) -> str:
    link = f"{public_base_url.rstrip('/')}/rpi/public/unsubscribe/{draft_id}?t={unsubscribe_token(draft_id)}"
    return (
        "\n\n--\n"
        f"{settings_row['sender_name']} · ReelForge AI\n{settings_row['postal_address']}\n"
        f"Not interested? Reply \"unsubscribe\" or opt out here and we won't contact you again: {link}"
    )


async def check_gates(conn, draft: dict, contact: Optional[dict], settings_row: dict) -> SendDecision:
    reasons: list[str] = []
    if not settings_row["sending_enabled"]:
        reasons.append("sending is disabled for this workspace (research and drafts only)")
    if draft["status"] != "approved" or not draft["approved_by"]:
        reasons.append(f"draft status is {draft['status']}, not approved")
    if not settings_row.get("postal_address") or not settings_row.get("sender_email") or not settings_row.get("sender_name"):
        reasons.append("sender name, sender email and postal address must be configured (CAN-SPAM)")
    if len(get_settings().rpi_attribution_hash_salt or "") < 16:
        reasons.append("RPI_ATTRIBUTION_HASH_SALT (16+ chars) must be set; it signs unsubscribe links")

    grounding = regrounding_from_row(draft, settings_row.get("sender_email"))
    check = validate_draft(draft["subject"], draft["body_text"], grounding)
    if not check.ok:
        reasons.append("draft fails validation: " + "; ".join(check.errors[:3]))

    if contact is None:
        reasons.append("no stored contact for this draft")
    elif contact["creator_id"] != draft["creator_id"]:
        reasons.append("contact belongs to a different creator")
    elif contact["contact_type"] != "email" or not EMAIL_RE.fullmatch(contact["value"].strip()):
        reasons.append("contact is not a valid email address")
    elif contact["verification_status"] in ("invalid", "undeliverable"):
        reasons.append(f"contact verification status is {contact['verification_status']}")

    email = contact["value"] if contact else None
    suppressed = await is_suppressed(conn, email=email, creator_id=draft["creator_id"])
    if suppressed:
        reasons.append(suppressed)

    status = await conn.fetchval("SELECT pipeline_status FROM rpi_creators WHERE id = $1", draft["creator_id"])
    if status is None or Status(status) not in OUTREACH_ELIGIBLE:
        reasons.append(f"creator CRM status '{status}' is not eligible for outreach")

    days = settings_row["min_days_between_contacts"]
    recent = await conn.fetchval(
        """SELECT COUNT(*) FROM rpi_outreach_drafts d LEFT JOIN rpi_creator_contacts c ON c.id = d.contact_id
           WHERE d.status IN ('sent', 'sending') AND d.id <> $1
             AND (d.creator_id = $2 OR ($3::text IS NOT NULL AND c.value_normalized = lower($3)))
             AND COALESCE(d.sent_at, NOW()) > NOW() - make_interval(days => $4)""",
        draft["id"], draft["creator_id"], email, days,
    )
    if recent:
        reasons.append(f"already contacted within {days} days")

    sent_today = await conn.fetchval(
        "SELECT COUNT(*) FROM rpi_outreach_drafts WHERE status IN ('sent', 'sending') AND sent_at >= date_trunc('day', NOW())"
    )
    if sent_today >= settings_row["daily_send_limit"]:
        reasons.append(f"daily send limit {settings_row['daily_send_limit']} reached")
    return SendDecision(not reasons, reasons)


async def send_draft(pool, draft_id: str, actor: str, send_fn: SendFn, *, public_base_url: str) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            draft = await conn.fetchrow("SELECT * FROM rpi_outreach_drafts WHERE id = $1 FOR UPDATE", draft_id)
            if not draft:
                return {"sent": False, "reasons": ["draft not found"]}
            draft = dict(draft)
            settings_row = dict(await conn.fetchrow("SELECT * FROM rpi_outreach_settings LIMIT 1"))
            contact = None
            if draft["contact_id"]:
                c = await conn.fetchrow("SELECT * FROM rpi_creator_contacts WHERE id = $1", draft["contact_id"])
                contact = dict(c) if c else None
            decision = await check_gates(conn, draft, contact, settings_row)
            if not decision.allowed:
                await audit(conn, actor, "outreach.send_refused", "outreach_draft", draft_id, {"reasons": decision.reasons})
                return {"sent": False, "reasons": decision.reasons}
            claimed = await conn.fetchval(
                "UPDATE rpi_outreach_drafts SET status = 'sending', sent_at = NOW() WHERE id = $1 AND status = 'approved' RETURNING id",
                draft_id,
            )
            if not claimed:
                return {"sent": False, "reasons": ["draft is already being sent or was sent"]}

    body = draft["body_text"] + compliance_footer(settings_row, str(draft_id), public_base_url)
    try:
        result = await send_fn(
            to_email=contact["value"], to_name="", subject=draft["subject"],
            html_content="<p>" + body.replace("\n\n", "</p><p>").replace("\n", "<br>") + "</p>",
            text_content=body, tags=["rpi", f"rpi_draft:{draft_id}"],
        )
    except Exception as exc:  # network failure after claim
        result = {"success": False, "error": str(exc)}

    async with pool.acquire() as conn:
        async with conn.transaction():
            if result.get("success"):
                await conn.execute(
                    "UPDATE rpi_outreach_drafts SET status = 'sent', provider_message_id = $2 WHERE id = $1",
                    draft_id, result.get("message_id"),
                )
                await conn.execute(
                    "INSERT INTO rpi_outreach_events (draft_id, creator_id, event_type, detail) VALUES ($1, $2, 'sent', $3::jsonb)",
                    draft_id, draft["creator_id"], json.dumps({"message_id": result.get("message_id")}),
                )
                status = await conn.fetchval("SELECT pipeline_status FROM rpi_creators WHERE id = $1", draft["creator_id"])
                if status == Status.OUTREACH_APPROVED.value:
                    await transition(conn, draft["creator_id"], Status.CONTACTED, actor=actor, reason="outreach sent")
                await audit(conn, actor, "outreach.sent", "outreach_draft", draft_id, {"message_id": result.get("message_id")})
                return {"sent": True, "message_id": result.get("message_id")}
            await conn.execute(
                "UPDATE rpi_outreach_drafts SET status = 'failed', failure_reason = $2 WHERE id = $1",
                draft_id, str(result.get("error"))[:500],
            )
            await audit(conn, actor, "outreach.send_failed", "outreach_draft", draft_id, {"error": str(result.get("error"))[:200]})
            return {"sent": False, "reasons": [f"provider error: {str(result.get('error'))[:200]}"]}
