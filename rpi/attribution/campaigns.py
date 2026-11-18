"""
Campaign creation, shared by `POST /campaigns` and the Reditus sync.

A campaign's referral code is the creator's Reditus slug (the `?red=` value of
their affiliate link), which the product stores as the signup's referral code.
Creating a campaign also attributes signups that used the code earlier.
"""

from __future__ import annotations

import secrets
import uuid
from datetime import date
from typing import Optional

import asyncpg

from rpi.attribution.touches import attribute_existing_signups
from rpi.core.audit import audit
from rpi.financial_ledger.ledger import accrue_commissions


class CampaignConflict(Exception):
    """The referral code or utm_campaign is already used by another campaign."""


async def create_campaign(conn, *, creator_id, name: str, actor: str, window_days: int,
                          referral_code: Optional[str] = None, deal_id=None, utm_source: Optional[str] = None,
                          utm_medium: Optional[str] = "influencer", utm_campaign: Optional[str] = None,
                          landing_page: Optional[str] = None, start_date: Optional[date] = None,
                          end_date: Optional[date] = None, audit_detail: Optional[dict] = None) -> dict:
    code = referral_code or ("rf" + secrets.token_hex(4))
    utm = utm_campaign or code
    # Codes are matched case-insensitively (Reditus slugs are not case-sensitive).
    if await conn.fetchval("SELECT 1 FROM rpi_campaigns WHERE lower(referral_code) = lower($1) OR utm_campaign = $2",
                           code, utm):
        raise CampaignConflict("referral code or utm_campaign already in use")
    creator = uuid.UUID(str(creator_id))
    try:
        cid = await conn.fetchval(
            """INSERT INTO rpi_campaigns (creator_id, deal_id, name, referral_code, utm_source, utm_medium, utm_campaign,
                                          landing_page, start_date, end_date, attribution_window_days)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id""",
            creator, deal_id, name, code, utm_source, utm_medium, utm, landing_page, start_date, end_date, window_days)
    except asyncpg.UniqueViolationError as exc:
        raise CampaignConflict("referral code or utm_campaign already in use") from exc
    async with conn.transaction():
        earlier = await attribute_existing_signups(conn, cid, code)
    await accrue_commissions(conn)
    await audit(conn, actor, "campaign.created", "campaign", cid,
                {"referral_code": code, "earlier_signups": earlier, **(audit_detail or {})})
    return {"id": str(cid), "referral_code": code, "utm_campaign": utm, "attribution_window_days": window_days,
            "earlier_signups_attributed": earlier}
