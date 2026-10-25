"""Persist identity-resolution decisions after a creator upsert."""

from __future__ import annotations

import json

from rpi.identity.resolver import AccountRef, IdentityRecord, MatchDecision, compare, normalize_handle
from rpi.providers.base import NormalizedCreator


async def _record_for(conn, creator_id) -> IdentityRecord:
    accounts = await conn.fetch(
        "SELECT platform, platform_account_id, handle, url FROM rpi_creator_social_accounts WHERE creator_id = $1", creator_id)
    name = await conn.fetchval("SELECT display_name FROM rpi_creators WHERE id = $1", creator_id)
    emails = await conn.fetch(
        "SELECT value_normalized FROM rpi_creator_contacts WHERE creator_id = $1 AND contact_type = 'email' AND is_business_contact",
        creator_id)
    return IdentityRecord(
        creator_id=str(creator_id),
        accounts=[AccountRef(a["platform"], a["platform_account_id"], a["handle"], a["url"]) for a in accounts],
        display_name=name,
        business_emails={e["value_normalized"] for e in emails},
    )


async def resolve_after_upsert(conn, creator_id, creator: NormalizedCreator) -> list[dict]:
    """Check the new/updated creator against existing records that share a
    handle or a linked account. Strong evidence merges automatically; weaker
    evidence creates a pending_review match. Returns decisions taken."""
    decisions = []
    me = await _record_for(conn, creator_id)
    me.outbound_links = [AccountRef(l.platform, l.platform_account_id, l.handle, l.url)
                         for l in creator.linked_accounts if l.asserted_by == "profile_link"]
    me.provider_mappings = [AccountRef(l.platform, l.platform_account_id, l.handle, l.url)
                            for l in creator.linked_accounts if l.asserted_by == "provider_mapping"]

    candidates = set()
    handle = normalize_handle(creator.handle)
    if handle:
        rows = await conn.fetch(
            """SELECT DISTINCT creator_id FROM rpi_creator_social_accounts
               WHERE platform = $1 AND handle_normalized = $2 AND creator_id <> $3""",
            creator.platform, handle, creator_id)
        candidates.update(r["creator_id"] for r in rows)
    for link in creator.linked_accounts:
        if link.platform_account_id:
            row = await conn.fetchval(
                "SELECT creator_id FROM rpi_creator_social_accounts WHERE platform = $1 AND platform_account_id = $2 AND creator_id <> $3",
                link.platform, link.platform_account_id, creator_id)
        else:
            row = await conn.fetchval(
                "SELECT creator_id FROM rpi_creator_social_accounts WHERE platform = $1 AND handle_normalized = $2 AND creator_id <> $3",
                link.platform, normalize_handle(link.handle), creator_id)
        if row:
            candidates.add(row)

    for other_id in candidates:
        other = await _record_for(conn, other_id)
        result = compare(me, other)
        if result.decision == MatchDecision.NO_MATCH:
            continue
        a, b = sorted([str(creator_id), str(other_id)])
        decision = "auto_merged" if result.decision == MatchDecision.AUTO_MERGE else "pending_review"
        await conn.execute(
            """INSERT INTO rpi_creator_identity_matches (creator_a, creator_b, decision, confidence, signals)
               VALUES ($1, $2, $3, $4, $5::jsonb) ON CONFLICT (creator_a, creator_b) DO NOTHING""",
            a, b, decision, result.confidence, json.dumps([s.__dict__ for s in result.signals], default=str))
        if decision == "auto_merged":
            keep, drop = a, b
            await conn.execute("UPDATE rpi_creator_social_accounts SET creator_id = $1 WHERE creator_id = $2", keep, drop)
            await conn.execute("UPDATE rpi_creators SET merged_into_id = $1 WHERE id = $2", keep, drop)
        decisions.append({"other": str(other_id), "decision": decision, "reason": result.reason})
    return decisions
