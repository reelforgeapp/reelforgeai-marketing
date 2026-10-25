"""
Delivery events for RPI messages, fed from the existing Brevo webhook.

Bounces, unsubscribes and spam complaints suppress the address and creator;
replies are surfaced in the CRM. Events are matched by Brevo message id and
deduplicated by (message id, event, timestamp).
"""

from __future__ import annotations

import json

from rpi.core.audit import audit
from rpi.crm import suppression

SUPPRESSING = {"hardBounce": "hard bounce", "unsubscribed": "unsubscribed", "spam": "spam complaint", "complaint": "spam complaint"}
TRACKED = SUPPRESSING.keys() | {"delivered", "softBounce", "opened", "uniqueOpened", "clicked", "uniqueClicked"}


async def handle_brevo_event(pool, payload: dict) -> bool:
    event = payload.get("event")
    message_id = payload.get("message-id")
    if not message_id or event not in TRACKED:
        return False
    async with pool.acquire() as conn:
        draft = await conn.fetchrow(
            """SELECT d.id, d.creator_id, c.value_normalized FROM rpi_outreach_drafts d
               LEFT JOIN rpi_creator_contacts c ON c.id = d.contact_id WHERE d.provider_message_id = $1""", message_id)
        if not draft:
            return False
        async with conn.transaction():
            key = f"brevo:{message_id}:{event}:{payload.get('ts_event') or payload.get('date')}"
            inserted = await conn.fetchval(
                """INSERT INTO rpi_outreach_events (draft_id, creator_id, event_type, provider_event_id, detail)
                   VALUES ($1, $2, $3, $4, $5::jsonb) ON CONFLICT (provider_event_id) DO NOTHING RETURNING id""",
                draft["id"], draft["creator_id"], event, key, json.dumps({"date": payload.get("date")}))
            if inserted and event in SUPPRESSING:
                reason = SUPPRESSING[event]
                if draft["value_normalized"]:
                    await suppression.add(conn, "email", draft["value_normalized"], reason, "brevo")
                if event != "hardBounce":
                    await suppression.suppress_creator(conn, draft["creator_id"], reason, "brevo")
                if event == "hardBounce":
                    await conn.execute(
                        "UPDATE rpi_creator_contacts SET verification_status = 'undeliverable' WHERE value_normalized = $1",
                        draft["value_normalized"])
                await audit(conn, "brevo", f"outreach.{event}", "creator", draft["creator_id"], {"draft_id": str(draft["id"])})
    return True
