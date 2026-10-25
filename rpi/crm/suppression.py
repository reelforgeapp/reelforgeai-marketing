"""
Suppression list and contact eligibility.

Values are stored only as salted SHA-256 hashes, so the list itself does not
retain email addresses after a deletion request. Checks cover the exact
email, its domain (for domain-level opt-outs) and the creator's platform
accounts. The existing Partner Scout tables are also checked so a creator who
unsubscribed or bounced there is never contacted from RPI.
"""

from __future__ import annotations

import hashlib
from typing import Optional

from app.config import get_settings


def hash_value(value: str) -> str:
    salt = get_settings().rpi_attribution_hash_salt or "rpi-suppression"
    return hashlib.sha256(f"{salt}:{value.strip().lower()}".encode()).hexdigest()


async def add(conn, kind: str, value: str, reason: str, source: str) -> None:
    await conn.execute(
        """INSERT INTO rpi_suppression_list (kind, value_hash, reason, source) VALUES ($1, $2, $3, $4)
           ON CONFLICT (kind, value_hash) DO NOTHING""",
        kind, hash_value(value), reason, source,
    )


async def suppress_creator(conn, creator_id, reason: str, source: str) -> None:
    accounts = await conn.fetch(
        "SELECT platform, platform_account_id FROM rpi_creator_social_accounts WHERE creator_id = $1", creator_id)
    for a in accounts:
        await add(conn, "creator", f"{a['platform']}:{a['platform_account_id']}", reason, source)
    emails = await conn.fetch(
        "SELECT value_normalized FROM rpi_creator_contacts WHERE creator_id = $1 AND contact_type = 'email'", creator_id)
    for e in emails:
        await add(conn, "email", e["value_normalized"], reason, source)


async def is_suppressed(conn, *, email: Optional[str] = None, creator_id=None) -> Optional[str]:
    """Return the reason a contact is suppressed, or None."""
    hashes: list[tuple[str, str]] = []
    if email:
        e = email.strip().lower()
        hashes.append(("email", hash_value(e)))
        if "@" in e:
            hashes.append(("domain", hash_value(e.split("@", 1)[1])))
    if creator_id:
        accounts = await conn.fetch(
            "SELECT platform, platform_account_id FROM rpi_creator_social_accounts WHERE creator_id = $1", creator_id)
        hashes.extend(("creator", hash_value(f"{a['platform']}:{a['platform_account_id']}")) for a in accounts)
    for kind, h in hashes:
        reason = await conn.fetchval("SELECT reason FROM rpi_suppression_list WHERE kind = $1 AND value_hash = $2", kind, h)
        if reason:
            return f"{kind} suppressed: {reason}"
    if email:
        # Partner Scout (legacy) opt-outs and bounces.
        legacy = await conn.fetchval(
            """SELECT status FROM marketing_prospects WHERE lower(email) = lower($1)
               AND status IN ('unsubscribed', 'bounced') LIMIT 1""",
            email,
        ) if await _legacy_table_exists(conn) else None
        if legacy:
            return f"legacy Partner Scout status: {legacy}"
    return None


async def _legacy_table_exists(conn) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass('public.marketing_prospects') IS NOT NULL"))
