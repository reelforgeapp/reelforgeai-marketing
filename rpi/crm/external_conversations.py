"""
Conversations other ReelForge systems are having with creators.

The partnership bot (reelforge-backend `scripts/partnership-bot`) reads and
answers the marketing@reelforgeai.io inbox, where replies to RPI emails also
land. After every run it reports each sender: address, name, channel
identifiers and message dates, never message content. RPI uses that to

* never cold-email a creator the bot is in conversation with (send gate and
  autopilot drafting), and
* mark a creator `replied` when they write in after an RPI email, so the CRM
  and response rate stay current while a person answers from the inbox.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from rpi.core.audit import audit
from rpi.crm.pipeline import Status, transition

SOURCE_BOT = "partnership_bot"
CHANNEL_ID_RE = re.compile(r"\b(UC[A-Za-z0-9_-]{22})\b")
HANDLE_URL_RE = re.compile(r"youtube\.com/@([A-Za-z0-9_.\-]+)", re.IGNORECASE)
HANDLE_RE = re.compile(r"^@?([A-Za-z0-9_.\-]{2,100})$")


def identifiers(handles: list[str], channel_urls: list[str]) -> tuple[list[str], list[str]]:
    """Normalized handles (lowercase, no '@') and YouTube channel ids from what the bot reported."""
    found_handles, channel_ids = set(), set()
    for h in handles:
        m = HANDLE_RE.match((h or "").strip())
        if m:
            found_handles.add(m.group(1).lower())
    for url in channel_urls:
        found_handles.update(m.group(1).lower() for m in HANDLE_URL_RE.finditer(url or ""))
        channel_ids.update(CHANNEL_ID_RE.findall(url or ""))
    return sorted(found_handles), sorted(channel_ids)


def _utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


@dataclass
class ExternalConversation:
    source: str
    email: str
    name: Optional[str]
    last_inbound_at: Optional[datetime]
    last_outbound_at: Optional[datetime]

    def summary(self) -> str:
        who = self.name or self.email
        parts = []
        if self.last_inbound_at:
            parts.append(f"they last wrote on {self.last_inbound_at.date().isoformat()}")
        if self.last_outbound_at:
            parts.append(f"the bot last wrote on {self.last_outbound_at.date().isoformat()}")
        return f"The partnership bot is in conversation with {who} ({self.email}): " + ", ".join(parts or ["no dates reported"])

    def as_dict(self) -> dict:
        return {"source": self.source, "email": self.email, "name": self.name, "last_inbound_at": self.last_inbound_at,
                "last_outbound_at": self.last_outbound_at, "summary": self.summary()}


def _match(creator: str, email: str) -> str:
    """SQL condition: external contact x belongs to the creator (by any stored
    email, the given email, a YouTube handle or a channel id)."""
    return f"""(
        x.email_normalized IN (SELECT k.value_normalized FROM rpi_creator_contacts k
                               WHERE k.creator_id = {creator} AND k.contact_type = 'email')
        OR x.email_normalized = lower({email})
        OR x.handles && COALESCE((SELECT array_agg(a.handle_normalized) FROM rpi_creator_social_accounts a
                                  WHERE a.creator_id = {creator} AND a.handle_normalized IS NOT NULL), '{{}}')
        OR x.channel_ids && COALESCE((SELECT array_agg(a.platform_account_id) FROM rpi_creator_social_accounts a
                                      WHERE a.creator_id = {creator} AND a.platform = 'youtube'), '{{}}'))"""


async def _table_exists(conn) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass('public.rpi_external_contacts') IS NOT NULL"))


async def conversation(conn, creator_id, *, email: Optional[str] = None, now: Optional[datetime] = None,
                       days: Optional[int] = None) -> Optional[ExternalConversation]:
    """The most recent conversation the partnership bot has with this creator,
    limited to the last `days` days when given."""
    if not await _table_exists(conn):
        return None
    since = (now or datetime.now(timezone.utc)) - timedelta(days=days) if days is not None else None
    row = await conn.fetchrow(
        f"""SELECT x.source, x.email_normalized, x.name, x.last_inbound_at, x.last_outbound_at
            FROM rpi_external_contacts x
            WHERE x.is_partner AND {_match('$1', '$2::text')}
              AND ($3::timestamptz IS NULL OR GREATEST(x.last_inbound_at, x.last_outbound_at) > $3)
            ORDER BY GREATEST(x.last_inbound_at, x.last_outbound_at) DESC NULLS LAST LIMIT 1""",
        creator_id, email, since)
    if not row:
        return None
    return ExternalConversation(row["source"], row["email_normalized"], row["name"], row["last_inbound_at"],
                                row["last_outbound_at"])


async def upsert(conn, source: str, contacts: list[dict]) -> int:
    """Store what the bot reported. Dates only move forward (first contact only back)."""
    n = 0
    for c in contacts:
        email = (c.get("email") or "").strip().lower()
        if "@" not in email:
            continue
        handles, channel_ids = identifiers(c.get("handles") or [], c.get("channel_urls") or [])
        await conn.execute(
            """INSERT INTO rpi_external_contacts (source, email_normalized, name, is_partner, handles, channel_ids,
                   first_inbound_at, last_inbound_at, last_outbound_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
               ON CONFLICT (source, email_normalized) DO UPDATE SET
                   name = COALESCE(EXCLUDED.name, rpi_external_contacts.name),
                   is_partner = EXCLUDED.is_partner, handles = EXCLUDED.handles, channel_ids = EXCLUDED.channel_ids,
                   first_inbound_at = LEAST(rpi_external_contacts.first_inbound_at, EXCLUDED.first_inbound_at),
                   last_inbound_at = GREATEST(rpi_external_contacts.last_inbound_at, EXCLUDED.last_inbound_at),
                   last_outbound_at = GREATEST(rpi_external_contacts.last_outbound_at, EXCLUDED.last_outbound_at),
                   updated_at = NOW()""",
            source, email, str(c["name"])[:200] if c.get("name") else None, bool(c.get("is_partner", True)),
            handles, channel_ids, _utc(c.get("first_inbound_at")), _utc(c.get("last_inbound_at")),
            _utc(c.get("last_outbound_at")))
        n += 1
    return n


async def mark_replies(conn, source: str, actor: str) -> list[dict]:
    """Creators who wrote to the inbox after an RPI email was sent to them:
    record a `replied` event on the email and move a `contacted` creator to `replied`."""
    rows = await conn.fetch(
        f"""SELECT d.id, d.creator_id, c.display_name, c.pipeline_status, x.email_normalized, x.last_inbound_at
            FROM rpi_outreach_drafts d
            JOIN rpi_creators c ON c.id = d.creator_id
            LEFT JOIN rpi_creator_contacts dk ON dk.id = d.contact_id
            JOIN LATERAL (
                SELECT x.email_normalized, x.last_inbound_at FROM rpi_external_contacts x
                WHERE x.source = $1 AND x.last_inbound_at > d.sent_at AND {_match('d.creator_id', 'dk.value')}
                ORDER BY x.last_inbound_at DESC LIMIT 1) x ON TRUE
            WHERE d.status = 'sent' AND d.sent_at IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM rpi_outreach_events e WHERE e.draft_id = d.id AND e.event_type = 'replied')""",
        source)
    marked = []
    for r in rows:
        await conn.execute(
            """INSERT INTO rpi_outreach_events (draft_id, creator_id, event_type, provider_event_id, detail, occurred_at)
               VALUES ($1, $2, 'replied', $3, $4::jsonb, $5) ON CONFLICT (provider_event_id) DO NOTHING""",
            r["id"], r["creator_id"], f"{source}:replied:{r['id']}",
            json.dumps({"source": source, "from": r["email_normalized"]}), r["last_inbound_at"])
        if r["pipeline_status"] == Status.CONTACTED.value:
            await transition(conn, r["creator_id"], Status.REPLIED, actor=actor,
                             reason=f"wrote to the inbox on {r['last_inbound_at'].date().isoformat()} (seen by the partnership bot)")
        await audit(conn, actor, "outreach.replied", "outreach_draft", r["id"], {"source": source})
        marked.append({"creator": r["display_name"], "replied_at": r["last_inbound_at"].isoformat()})
    return marked
