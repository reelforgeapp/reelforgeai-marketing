"""
Conversations other ReelForge systems are having with creators.

The partnership bot (reelforge-backend `scripts/partnership-bot`) reads and
answers the marketing@reelforgeai.io inbox, where replies to RPI emails also
land. After every run it reports each sender: address, name, channel
identifiers and message dates, never message content. RPI uses that to

* never cold-email a creator the bot is in conversation with (send gate and
  autopilot drafting),
* mark a creator `replied` when they write in after an RPI email, so the CRM
  and response rate stay current while a person answers from the inbox, and
* check everyone who wrote in for bought views: creators not yet in RPI are
  added from their YouTube links, and the autopilot screens the ones RPI has
  never checked (rpi.administration.autopilot). Inbound pitches come mostly
  from paid-review channels, where bought views are common.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Optional

from rpi.core.audit import audit
from rpi.core.provenance import MeasurementType, Provenance
from rpi.crm.pipeline import Status, transition
from rpi.providers.base import Capability, NormalizedCreator
from rpi.providers.gateway import CallContext

SOURCE_BOT = "partnership_bot"
CHANNEL_ID_RE = re.compile(r"\b(UC[A-Za-z0-9_-]{22})\b")
HANDLE_URL_RE = re.compile(r"youtube\.com/@([A-Za-z0-9_.\-]+)", re.IGNORECASE)
HANDLE_RE = re.compile(r"^@?([A-Za-z0-9_.\-]{2,100})$")
EMAIL_IN_NAME_RE = re.compile(r"\s*\([^)]*@[^)]*\)")
# Who counts as an inbound partner to check: wrote in within this many days.
INBOUND_DAYS = 90
# The last of the bought-engagement checks (RPI 0.2.10) went live on
# 2026-10-26; a creator screened before then was never checked.
ENGAGEMENT_CHECKS_SINCE = datetime(2026, 10, 27, tzinfo=timezone.utc)


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


def youtube_identifiers(channel_urls: list[str]) -> tuple[list[str], list[str]]:
    """Handles and channel ids from youtube.com links only. The bot's bare
    handles can name a TikTok or Instagram account, and looking one up on
    YouTube could find someone else's channel."""
    handles, channel_ids = set(), set()
    for url in channel_urls:
        if "youtube.com/" not in (url or "").lower():
            continue
        handles.update(m.group(1).lower() for m in HANDLE_URL_RE.finditer(url))
        channel_ids.update(CHANNEL_ID_RE.findall(url))
    return sorted(handles), sorted(channel_ids)


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
        if self.email not in who:
            who = f"{who} ({self.email})"
        parts = []
        if self.last_inbound_at:
            parts.append(f"they last wrote on {self.last_inbound_at.date().isoformat()}")
        if self.last_outbound_at:
            parts.append(f"the bot last wrote on {self.last_outbound_at.date().isoformat()}")
        return f"The partnership bot is in conversation with {who}: " + ", ".join(parts or ["no dates reported"])

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


async def _known(conn, email: str, handles: list[str], channel_ids: list[str]) -> bool:
    return bool(await conn.fetchval(
        """SELECT EXISTS (SELECT 1 FROM rpi_creator_social_accounts WHERE platform = 'youtube'
                          AND (platform_account_id = ANY($1::text[]) OR handle_normalized = ANY($2::text[])))
               OR EXISTS (SELECT 1 FROM rpi_creator_contacts WHERE contact_type = 'email' AND value_normalized = $3)""",
        channel_ids, handles, email))


HANDLE_LOOKUP = ("youtube_data_api", "fixture")


def handle_resolver(registry, gateway) -> Callable[[str], Awaitable[Optional[str]]]:
    """`resolve` for add_inbound_partners: a handle's channel id from the YouTube
    Data API, never a paid provider; None when it is not configured or the
    handle is unknown."""
    adapter = next((a for a in registry.supporting(Capability.CREATOR_PROFILE, HANDLE_LOOKUP)
                    if a.name in HANDLE_LOOKUP), None)

    async def resolve(handle: str) -> Optional[str]:
        if adapter is None:
            return None
        result = await gateway.call(adapter, Capability.CREATOR_PROFILE, f"@{handle}", ctx=CallContext())
        return result.data.platform_account_id if result.ok and result.data else None

    return resolve


async def add_inbound_partners(conn, contacts: list[dict], resolve: Callable[[str], Awaitable[Optional[str]]], *,
                               now: Optional[datetime] = None, max_lookups: int = 10) -> list[str]:
    """Add creators who wrote in within INBOUND_DAYS and are not in RPI yet,
    identified from their YouTube links only, so the autopilot can check them
    for bought views. `resolve` turns a handle into a channel id (one free
    YouTube Data API quota unit), at most `max_lookups` times per call; a
    channel id in a link needs no lookup. Stores the identity only: the bot's
    name for them, the handle they gave and the channel id. Returns the names added."""
    from rpi.storage.creators import find_account, upsert_creator  # storage imports this module

    since = (now or datetime.now(timezone.utc)) - timedelta(days=INBOUND_DAYS)
    added: list[str] = []
    lookups = 0
    for c in contacts:
        last = _utc(c.get("last_inbound_at"))
        if not c.get("is_partner", True) or last is None or last < since:
            continue
        email = (c.get("email") or "").strip().lower()
        handles, channel_ids = youtube_identifiers(c.get("channel_urls") or [])
        if not (handles or channel_ids) or await _known(conn, email, handles, channel_ids):
            continue
        channel_id = channel_ids[0] if channel_ids else None
        if channel_id is None:
            if lookups >= max_lookups:
                continue
            lookups += 1
            channel_id = await resolve(handles[0])
            if not channel_id or await find_account(conn, "youtube", channel_id):
                continue
        name = EMAIL_IN_NAME_RE.sub("", str(c.get("name") or "")).strip()[:200] or (handles[0] if handles else email)
        creator = NormalizedCreator(
            platform="youtube", platform_account_id=channel_id, handle=f"@{handles[0]}" if handles else None,
            display_name=name, url=f"https://www.youtube.com/channel/{channel_id}",
            provenance=Provenance(provider=SOURCE_BOT, operation="external_contact", source_record=email,
                                  data_type="creator_identity", measurement=MeasurementType.MANUAL_SUBMITTED,
                                  retention_policy=f"{SOURCE_BOT}:contact"))
        async with conn.transaction():
            await upsert_creator(conn, creator)
        added.append(name)
    return added


async def partners_to_check(conn, *, now: Optional[datetime] = None, limit: int = 10) -> list[dict]:
    """Creators who wrote in within INBOUND_DAYS that RPI has never checked for
    bought views (no score since ENGAGEMENT_CHECKS_SINCE), most recent first."""
    if not await _table_exists(conn):
        return []
    since = (now or datetime.now(timezone.utc)) - timedelta(days=INBOUND_DAYS)
    rows = await conn.fetch(
        """WITH partners AS (SELECT email_normalized, handles, channel_ids, last_inbound_at FROM rpi_external_contacts
                             WHERE is_partner AND last_inbound_at > $1),
                matched AS (
                    SELECT a.creator_id, p.last_inbound_at FROM partners p JOIN rpi_creator_social_accounts a
                      ON a.platform = 'youtube'
                     AND (a.handle_normalized = ANY(p.handles) OR a.platform_account_id = ANY(p.channel_ids))
                    UNION ALL
                    SELECT k.creator_id, p.last_inbound_at FROM partners p JOIN rpi_creator_contacts k
                      ON k.contact_type = 'email' AND k.value_normalized = p.email_normalized)
           SELECT c.id, c.display_name, MAX(m.last_inbound_at) AS last_inbound_at
           FROM matched m JOIN rpi_creators c ON c.id = m.creator_id
           WHERE c.merged_into_id IS NULL AND c.disqualified_reason IS NULL
             AND c.pipeline_status NOT IN ('rejected', 'do_not_contact')
             AND EXISTS (SELECT 1 FROM rpi_creator_social_accounts a WHERE a.creator_id = c.id AND a.platform = 'youtube')
             AND NOT EXISTS (SELECT 1 FROM rpi_qualification_scores q WHERE q.creator_id = c.id AND q.computed_at >= $3)
           GROUP BY c.id, c.display_name ORDER BY MAX(m.last_inbound_at) DESC LIMIT $2""",
        since, limit, ENGAGEMENT_CHECKS_SINCE)
    return [dict(r) for r in rows]
