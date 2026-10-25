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
  7. no message to this creator or address within min_days_between_contacts,
     counting Partner Scout's emails (marketing_prospects.last_contacted_at)
  8. daily send limit not reached
  9. CAN-SPAM: sender identity and postal address configured
 10. exactly-once: status moves approved → sending via compare-and-swap

Messages go out under the configured sender name/email (reply-to the same
address) with RFC 8058 one-click unsubscribe headers. test_send() delivers a
draft only to RPI_TEST_RECIPIENTS, with an inactive unsubscribe link, and
never changes the draft, the CRM record or contact history.
"""

from __future__ import annotations

import hashlib
import html
import hmac
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Optional

import structlog

from app.config import get_settings
from rpi.core.audit import audit
from rpi.crm.contact_history import partner_scout_history
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


def unsubscribe_link(draft_id: str, public_base_url: str) -> str:
    return f"{public_base_url.rstrip('/')}/rpi/public/unsubscribe/{draft_id}?t={unsubscribe_token(draft_id)}"


# Test sends get a link that does nothing: the real one would unsubscribe the creator.
TEST_LINK_ID = "TEST-PREVIEW"


def compliance_footer(settings_row: dict, draft_id: str, public_base_url: str, *, test: bool = False) -> str:
    link = (f"{public_base_url.rstrip('/')}/rpi/public/unsubscribe/{TEST_LINK_ID}" if test
            else unsubscribe_link(draft_id, public_base_url))
    return (
        "\n\n--\n"
        f"{settings_row['sender_name']} · ReelForge AI\n{settings_row['postal_address']}\n"
        f"Not interested? Reply \"unsubscribe\" or opt out here and we won't contact you again: {link}"
        + ("\n(Test send: this link is inactive. The creator's email carries a working one.)" if test else "")
    )


FONT_STACK = "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif"


def _linkify(escaped: str) -> str:
    """Turn URLs in already-escaped text into short, readable links."""
    def link(m: re.Match) -> str:
        url = m.group(0).rstrip(".,")
        tail = m.group(0)[len(url):]
        label = re.sub(r"^https?://(www\.)?", "", url).rstrip("/")
        return f'<a href="{url}" style="color:#2563eb;text-decoration:underline">{label}</a>{tail}'
    return re.sub(r"https?://[^\s<>\"]+", link, escaped)


BRAND_TEAL = "#5EEAD4"  # RF monogram border (reelforge-ai frontend/public/icon.svg)
LINK_TEAL = "#0F766E"  # readable teal for links on white


def _signature_parts(settings_row: dict) -> dict:
    s = get_settings()
    company = "ReelForge AI"
    title = (s.rpi_signature_title or "").strip()
    website = (s.rpi_signature_website or "").strip()
    return {"name": settings_row["sender_name"], "role": f"{title}, {company}" if title else company,
            "website": website, "website_label": re.sub(r"^https?://(www\.)?", "", website).rstrip("/"),
            "email": settings_row["sender_email"], "logo": (s.rpi_signature_logo_url or "").strip()}


def signature_text(settings_row: dict) -> str:
    sig = _signature_parts(settings_row)
    contact = " · ".join(x for x in (sig["website_label"], sig["email"]) if x)
    return "\n".join(x for x in (sig["name"], sig["role"], contact) if x)


def signature_html(settings_row: dict) -> str:
    """Logo, name, role and contact line; a table so every email client lays it
    out the same. The logo has alt text and the rest is text, so it still reads
    when images are blocked."""
    sig = {k: html.escape(v) for k, v in _signature_parts(settings_row).items()}
    link = f'color:{LINK_TEAL};text-decoration:none'
    logo = (f'<td style="padding:0 14px 0 0;vertical-align:middle">'
            f'<a href="{sig["website"] or "#"}"><img src="{sig["logo"]}" width="48" height="48" alt="ReelForge AI" '
            f'style="display:block;border:0;outline:none;border-radius:10px"></a></td>') if sig["logo"] else ""
    contact = " <span style=\"color:#9ca3af\">&middot;</span> ".join(
        x for x in ((f'<a href="{sig["website"]}" style="{link}">{sig["website_label"]}</a>' if sig["website"] else ""),
                    (f'<a href="mailto:{sig["email"]}" style="{link}">{sig["email"]}</a>' if sig["email"] else "")) if x)
    return (
        '<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;margin:4px 0 0">'
        f'<tr>{logo}<td style="padding:2px 0 2px 14px;border-left:2px solid {BRAND_TEAL};vertical-align:middle;'
        f'font-family:{FONT_STACK};font-size:14px;line-height:1.45;color:#1f2328">'
        f'<div style="font-weight:600;font-size:15px">{sig["name"]}</div>'
        f'<div style="color:#4b5563">{sig["role"]}</div><div>{contact}</div></td></tr></table>'
    )


def _without_signed_name(body: str, sender_name: str) -> list[str]:
    """Body paragraphs with a trailing sign-off name removed (the signature
    block replaces it); a closing such as "Best," above the name is kept."""
    paragraphs = [p.strip() for p in body.replace("\r\n", "\n").split("\n\n") if p.strip()]
    if paragraphs and sender_name:
        lines = paragraphs[-1].split("\n")
        if lines[-1].strip() == sender_name.strip():
            closing = "\n".join(lines[:-1]).strip()
            return paragraphs[:-1] + ([closing] if closing else [])
    return paragraphs


def with_text_signature(body: str, settings_row: dict) -> str:
    paragraphs = _without_signed_name(body, settings_row["sender_name"])
    return "\n\n".join(paragraphs + [signature_text(settings_row)])


def render_html(body: str, settings_row: dict, link: str, *, test: bool = False) -> str:
    """Email-safe HTML with the same paragraphs as the plain-text body: one
    column, system font, the signature block, footer in small grey type. All
    text is escaped; video titles in drafts come from creators."""
    paragraphs = _without_signed_name(body, settings_row["sender_name"])
    content = "".join(f'<p style="margin:0 0 16px">{_linkify(html.escape(p)).replace(chr(10), "<br>")}</p>'
                      for p in paragraphs) + signature_html(settings_row)
    note = ("<br>(Test send: this link is inactive. The creator&#39;s email carries a working one.)" if test else "")
    footer = (
        '<hr style="border:none;border-top:1px solid #e5e7eb;margin:28px 0 14px">'
        '<p style="margin:0;font-size:12px;line-height:1.5;color:#6b7280">'
        f"{html.escape(settings_row['sender_name'])} &middot; ReelForge AI<br>{html.escape(settings_row['postal_address'])}<br>"
        f'Not interested? Reply &quot;unsubscribe&quot; or <a href="{html.escape(link)}" style="color:#6b7280;'
        f'text-decoration:underline">opt out here</a> and we won&#39;t contact you again.{note}</p>'
    )
    return (
        '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">'
        '</head><body style="margin:0;padding:0;background:#ffffff">'
        f'<div style="max-width:600px;margin:0 auto;padding:24px 16px;font-family:{FONT_STACK};font-size:15px;'
        f'line-height:1.6;color:#1f2328">{content}{footer}</div></body></html>'
    )


def message_fields(settings_row: dict, body: str, draft_id: str, public_base_url: str, *, test: bool = False) -> dict:
    """Sender identity, HTML/text bodies with the compliance footer, and
    one-click unsubscribe headers (RFC 8058)."""
    link = (f"{public_base_url.rstrip('/')}/rpi/public/unsubscribe/{TEST_LINK_ID}" if test
            else unsubscribe_link(draft_id, public_base_url))
    return {
        "sender_name": settings_row["sender_name"], "sender_email": settings_row["sender_email"],
        "reply_to": settings_row["sender_email"],
        "html_content": render_html(body, settings_row, link, test=test),
        "text_content": with_text_signature(body, settings_row)
                        + compliance_footer(settings_row, draft_id, public_base_url, test=test),
        "headers": {"List-Unsubscribe": f"<{link}>", "List-Unsubscribe-Post": "List-Unsubscribe=One-Click"},
    }


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
    prior = await partner_scout_history(conn, draft["creator_id"], email=email)
    if prior and prior.last_contacted_at and prior.last_contacted_at > datetime.now(timezone.utc) - timedelta(days=days):
        reasons.append(f"Partner Scout emailed this creator on {prior.last_contacted_at.date().isoformat()}, "
                       f"within {days} days")

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

    try:
        result = await send_fn(
            to_email=contact["value"], to_name="", subject=draft["subject"], tags=["rpi", f"rpi_draft:{draft_id}"],
            **message_fields(settings_row, draft["body_text"], str(draft_id), public_base_url),
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


async def test_send(pool, draft_id: str, to_email: str, actor: str, send_fn: SendFn, *, public_base_url: str,
                    allowed_recipients: set[str]) -> dict:
    """Send a draft exactly as the creator would get it, but only to an approved
    internal address. Works while sending is disabled and before approval; it
    never touches the draft's status, the creator's CRM record or contact
    history, and its unsubscribe link is inactive."""
    to = (to_email or "").strip().lower()
    reasons = []
    if not allowed_recipients:
        reasons.append("no test recipients configured (RPI_TEST_RECIPIENTS)")
    elif to not in allowed_recipients:
        reasons.append("recipient is not in RPI_TEST_RECIPIENTS")
    async with pool.acquire() as conn:
        draft = await conn.fetchrow("SELECT * FROM rpi_outreach_drafts WHERE id = $1", draft_id)
        if not draft:
            return {"sent": False, "reasons": ["draft not found"]}
        draft = dict(draft)
        settings_row = dict(await conn.fetchrow("SELECT * FROM rpi_outreach_settings LIMIT 1"))
        if not settings_row.get("postal_address") or not settings_row.get("sender_email") or not settings_row.get("sender_name"):
            reasons.append("sender name, sender email and postal address must be configured (CAN-SPAM)")
        check = validate_draft(draft["subject"], draft["body_text"],
                               regrounding_from_row(draft, settings_row.get("sender_email")))
        if not check.ok:
            reasons.append("draft fails validation: " + "; ".join(check.errors[:3]))
        if reasons:
            await audit(conn, actor, "outreach.test_send_refused", "outreach_draft", draft_id, {"reasons": reasons})
            return {"sent": False, "reasons": reasons}
    try:
        result = await send_fn(to_email=to, to_name="", subject="[TEST] " + draft["subject"], tags=["rpi", "rpi_test"],
                               **message_fields(settings_row, draft["body_text"], str(draft_id), public_base_url, test=True))
    except Exception as exc:
        result = {"success": False, "error": str(exc)}
    async with pool.acquire() as conn:
        await audit(conn, actor, "outreach.test_sent" if result.get("success") else "outreach.test_send_failed",
                    "outreach_draft", draft_id, {"message_id": result.get("message_id"), "error": str(result.get("error") or "")[:200]})
    if not result.get("success"):
        return {"sent": False, "reasons": [f"provider error: {str(result.get('error'))[:200]}"]}
    return {"sent": True, "message_id": result.get("message_id"), "to": to}
