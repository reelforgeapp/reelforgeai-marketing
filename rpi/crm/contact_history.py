"""
Earlier contact by Partner Scout.

Partner Scout, the older outreach system in this app, emails prospects from
`marketing_prospects`. Many RPI creators are the same channels, so RPI has to
know about those emails: the contact-frequency gate counts them, the creator
page shows them, and drafts acknowledge the earlier contact instead of reading
like a first email.

A prospect matches a creator by the creator's `legacy_prospect_id`, by YouTube
channel id, by any stored contact email, or by the address RPI would send to.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from rpi.crm.suppression import _legacy_table_exists


def _utc(value: Optional[datetime]) -> Optional[datetime]:
    """Partner Scout stores naive UTC timestamps (TIMESTAMP without time zone)."""
    return value.replace(tzinfo=timezone.utc) if value and value.tzinfo is None else value


@dataclass
class PriorContact:
    emails_sent: int
    first_contacted_at: Optional[datetime]
    last_contacted_at: Optional[datetime]
    # Partner Scout prospect status, e.g. "contacted". Partner Scout does not
    # record replies, so "contacted" does not mean the creator never answered.
    status: Optional[str]

    def summary(self) -> str:
        last = self.last_contacted_at.date().isoformat() if self.last_contacted_at else "unknown date"
        first = self.first_contacted_at.date().isoformat() if self.first_contacted_at else None
        span = f"first {first}, last {last}" if first and first != last else f"on {last}"
        return f"Partner Scout emailed this creator {self.emails_sent} time(s), {span} (status: {self.status or 'unknown'})"

    def as_dict(self) -> dict:
        return {"emails_sent": self.emails_sent, "first_contacted_at": self.first_contacted_at,
                "last_contacted_at": self.last_contacted_at, "status": self.status, "summary": self.summary()}


async def partner_scout_history(conn, creator_id, *, email: Optional[str] = None) -> Optional[PriorContact]:
    """Partner Scout emails already sent to this creator, or None if there were none."""
    if not await _legacy_table_exists(conn):
        return None
    rows = await conn.fetch(
        """SELECT p.status, COALESCE(p.total_emails_sent, 0) AS sent, p.first_contacted_at, p.last_contacted_at
           FROM marketing_prospects p
           WHERE p.id = (SELECT legacy_prospect_id FROM rpi_creators WHERE id = $1)
              OR p.youtube_channel_id IN (SELECT platform_account_id FROM rpi_creator_social_accounts
                                          WHERE creator_id = $1 AND platform = 'youtube')
              OR lower(p.email) IN (SELECT value_normalized FROM rpi_creator_contacts
                                    WHERE creator_id = $1 AND contact_type = 'email')
              OR ($2::text IS NOT NULL AND lower(p.email) = lower($2))""",
        creator_id, email,
    )
    rows = [dict(r, first_contacted_at=_utc(r["first_contacted_at"]), last_contacted_at=_utc(r["last_contacted_at"]))
            for r in rows]
    contacted = [r for r in rows if r["sent"] > 0 or r["last_contacted_at"]]
    if not contacted:
        return None
    dated = [r for r in contacted if r["last_contacted_at"]]
    latest = max(dated, key=lambda r: r["last_contacted_at"]) if dated else contacted[0]
    firsts = [r["first_contacted_at"] for r in contacted if r["first_contacted_at"]]
    lasts = [r["last_contacted_at"] for r in dated]
    return PriorContact(
        emails_sent=sum(r["sent"] for r in contacted),
        first_contacted_at=min(firsts) if firsts else None,
        last_contacted_at=max(lasts) if lasts else None,
        status=latest["status"],
    )
