"""
Creator repository: persists normalized provider data with provenance.

The schema is independent of any provider's response format. Every stored
fact links to an rpi_data_provenance row whose expires_at implements the
provider's retention rule.
"""

from __future__ import annotations

import json
import uuid
from datetime import timedelta
from typing import Any, Optional

from rpi.compliance.policies import get_policy
from rpi.crm.contact_history import partner_scout_history
from rpi.crm.external_conversations import conversation as external_conversation
from rpi.core.provenance import Provenance
from rpi.identity.resolver import normalize_email, normalize_handle
from rpi.providers.base import (
    AudienceBreakdown,
    NormalizedContact,
    NormalizedContent,
    NormalizedCreator,
)


async def insert_provenance(conn, prov: Provenance) -> uuid.UUID:
    policy = get_policy(prov.provider)
    expires = prov.retrieved_at + timedelta(days=policy.max_storage_days) if policy.max_storage_days else None
    return await conn.fetchval(
        """INSERT INTO rpi_data_provenance
           (provider, operation, source_record, retrieved_at, observed_at, data_type, measurement, confidence,
            completeness, retention_policy, commercial_use, raw_ref, response_version, derived_metrics_prohibited,
            field_measurements, expires_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16) RETURNING id""",
        prov.provider, prov.operation, prov.source_record, prov.retrieved_at, prov.observed_at, prov.data_type,
        prov.measurement.value, prov.confidence, prov.completeness, prov.retention_policy, prov.commercial_use.value,
        prov.raw_ref, prov.response_version, prov.derived_metrics_prohibited,
        json.dumps({k: v.value for k, v in prov.field_measurements.items()}), expires,
    )


async def find_account(conn, platform: str, platform_account_id: str) -> Optional[dict]:
    row = await conn.fetchrow(
        """SELECT a.id AS account_id, a.creator_id, c.merged_into_id
           FROM rpi_creator_social_accounts a JOIN rpi_creators c ON c.id = a.creator_id
           WHERE a.platform = $1 AND a.platform_account_id = $2""",
        platform, platform_account_id,
    )
    return dict(row) if row else None


async def upsert_creator(conn, creator: NormalizedCreator) -> tuple[uuid.UUID, uuid.UUID, bool]:
    """Returns (creator_id, social_account_id, created). Exact match on the
    stable platform account id; never merges by name."""
    existing = await find_account(conn, creator.platform, creator.platform_account_id)
    if existing:
        creator_id = existing["merged_into_id"] or existing["creator_id"]
        await conn.execute(
            """UPDATE rpi_creator_social_accounts
               SET handle = COALESCE($2, handle), handle_normalized = COALESCE($3, handle_normalized),
                   url = COALESCE($4, url), last_seen_at = NOW(),
                   provider_creator_ids = provider_creator_ids || $5::jsonb
               WHERE id = $1""",
            existing["account_id"], creator.handle, normalize_handle(creator.handle), creator.url,
            json.dumps({creator.provenance.provider: creator.provider_creator_id} if creator.provider_creator_id else {}),
        )
        await conn.execute(
            """UPDATE rpi_creators SET display_name = COALESCE(display_name, $2),
                   primary_language = COALESCE(primary_language, $3), country = COALESCE(country, $4), updated_at = NOW()
               WHERE id = $1""",
            creator_id, creator.display_name, creator.language, creator.country,
        )
        return creator_id, existing["account_id"], False

    creator_id = await conn.fetchval(
        """INSERT INTO rpi_creators (display_name, primary_platform, primary_language, country)
           VALUES ($1, $2, $3, $4) RETURNING id""",
        creator.display_name, creator.platform, creator.language, creator.country,
    )
    account_id = await conn.fetchval(
        """INSERT INTO rpi_creator_social_accounts
           (creator_id, platform, platform_account_id, handle, handle_normalized, url, provider_creator_ids)
           VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb) RETURNING id""",
        creator_id, creator.platform, creator.platform_account_id, creator.handle, normalize_handle(creator.handle),
        creator.url,
        json.dumps({creator.provenance.provider: creator.provider_creator_id} if creator.provider_creator_id else {}),
    )
    await conn.execute(
        "INSERT INTO rpi_pipeline_events (creator_id, from_status, to_status, actor, reason) VALUES ($1, NULL, 'discovered', 'system', $2)",
        creator_id, f"discovered via {creator.provenance.provider}",
    )
    return creator_id, account_id, True


async def record_snapshot(conn, creator_id, account_id, creator: NormalizedCreator) -> uuid.UUID:
    prov_id = await insert_provenance(conn, creator.provenance)
    await conn.execute(
        """INSERT INTO rpi_creator_profile_snapshots
           (creator_id, social_account_id, provider, subscribers, avg_views, median_views, engagement_rate,
            total_videos, uploads_last_90d, last_upload_at, country, language, categories, description, provenance_id)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)""",
        creator_id, account_id, creator.provenance.provider, creator.subscribers, creator.avg_views,
        creator.median_views, creator.engagement_rate, creator.total_videos, creator.uploads_last_90d,
        creator.last_upload_at, creator.country, creator.language, creator.categories,
        (creator.description or "")[:2000] or None, prov_id,
    )
    return prov_id


async def record_content(conn, creator_id, items: list[NormalizedContent]) -> dict[str, uuid.UUID]:
    """Upsert content; returns platform_content_id -> rpi content id."""
    ids: dict[str, uuid.UUID] = {}
    for item in items:
        prov_id = await insert_provenance(conn, item.provenance)
        cid = await conn.fetchval(
            """INSERT INTO rpi_creator_content
               (creator_id, platform, platform_content_id, title, description, url, published_at, duration_seconds,
                is_short, views, likes, comments, sponsored_flag, provenance_id)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
               ON CONFLICT (platform, platform_content_id) DO UPDATE SET
                 title = EXCLUDED.title, description = EXCLUDED.description, views = EXCLUDED.views,
                 likes = EXCLUDED.likes, comments = EXCLUDED.comments, sponsored_flag = EXCLUDED.sponsored_flag,
                 provenance_id = EXCLUDED.provenance_id
               RETURNING id""",
            creator_id, item.platform, item.platform_content_id, item.title, (item.description or "")[:5000] or None,
            item.url, item.published_at, item.duration_seconds, item.is_short, item.views, item.likes, item.comments,
            item.sponsored_flag, prov_id,
        )
        ids[item.platform_content_id] = cid
    return ids


async def record_audience(conn, creator_id, breakdown: AudienceBreakdown) -> int:
    prov_id = await insert_provenance(conn, breakdown.provenance)
    # Replace earlier estimates from the same provider so stale shares never mix with new ones.
    await conn.execute("DELETE FROM rpi_audience_estimates WHERE creator_id = $1 AND provider = $2",
                       creator_id, breakdown.provenance.provider)
    for b in breakdown.buckets:
        await conn.execute(
            """INSERT INTO rpi_audience_estimates (creator_id, provider, dimension, bucket, share, measurement, provenance_id)
               VALUES ($1,$2,$3,$4,$5,$6,$7)""",
            creator_id, breakdown.provenance.provider, b.dimension, b.bucket, b.share, b.measurement.value, prov_id,
        )
    return len(breakdown.buckets)


async def record_contacts(conn, creator_id, contacts: list[NormalizedContact]) -> int:
    n = 0
    for c in contacts:
        prov_id = await insert_provenance(conn, c.provenance)
        value_norm = normalize_email(c.value) if c.contact_type == "email" else c.value.strip().lower()
        res = await conn.execute(
            """INSERT INTO rpi_creator_contacts
               (creator_id, contact_type, value, value_normalized, source, is_business_contact, provenance_id)
               VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (creator_id, contact_type, value_normalized) DO NOTHING""",
            creator_id, c.contact_type, c.value.strip(), value_norm, c.provenance.provider, c.is_business_contact, prov_id,
        )
        n += res.endswith("1")
    return n


async def latest_snapshot(conn, creator_id) -> Optional[dict]:
    row = await conn.fetchrow(
        """SELECT s.*, p.measurement, p.derived_metrics_prohibited, p.field_measurements, p.retrieved_at
           FROM rpi_creator_profile_snapshots s JOIN rpi_data_provenance p ON p.id = s.provenance_id
           WHERE s.creator_id = $1 ORDER BY s.observed_at DESC LIMIT 1""",
        creator_id,
    )
    return dict(row) if row else None


async def creator_profile(conn, creator_id) -> Optional[dict[str, Any]]:
    creator = await conn.fetchrow("SELECT * FROM rpi_creators WHERE id = $1", creator_id)
    if not creator:
        return None
    accounts = await conn.fetch("SELECT * FROM rpi_creator_social_accounts WHERE creator_id = $1", creator_id)
    snapshots = await conn.fetch(
        """SELECT s.*, p.measurement, p.retrieved_at, p.expires_at, p.derived_metrics_prohibited, p.field_measurements
           FROM rpi_creator_profile_snapshots s JOIN rpi_data_provenance p ON p.id = s.provenance_id
           WHERE s.creator_id = $1 ORDER BY s.observed_at DESC LIMIT 10""", creator_id)
    audience = await conn.fetch(
        """SELECT e.dimension, e.bucket, e.share, e.measurement, e.provider, p.retrieved_at
           FROM rpi_audience_estimates e JOIN rpi_data_provenance p ON p.id = e.provenance_id
           WHERE e.creator_id = $1 ORDER BY e.dimension, e.share DESC""", creator_id)
    content = await conn.fetch(
        """SELECT c.id, c.platform_content_id, c.title, c.url, c.published_at, c.views, c.likes, c.comments,
                  c.is_short, c.sponsored_flag, p.provider, p.retrieved_at
           FROM rpi_creator_content c JOIN rpi_data_provenance p ON p.id = c.provenance_id
           WHERE c.creator_id = $1 ORDER BY c.published_at DESC NULLS LAST LIMIT 30""", creator_id)
    classification = await conn.fetchrow(
        "SELECT * FROM rpi_content_classifications WHERE creator_id = $1 AND content_id IS NULL ORDER BY created_at DESC LIMIT 1",
        creator_id)
    score = await conn.fetchrow(
        "SELECT * FROM rpi_qualification_scores WHERE creator_id = $1 ORDER BY computed_at DESC LIMIT 1", creator_id)
    sponsorships = await conn.fetch(
        "SELECT * FROM rpi_sponsorship_relationships WHERE creator_id = $1 ORDER BY published_at DESC NULLS LAST", creator_id)
    risk = await conn.fetch(
        "SELECT * FROM rpi_audience_quality_signals WHERE creator_id = $1 ORDER BY computed_at DESC", creator_id)
    contacts = await conn.fetch(
        """SELECT id, contact_type, value, source, is_business_contact, verification_status, created_at
           FROM rpi_creator_contacts WHERE creator_id = $1""", creator_id)
    events = await conn.fetch(
        "SELECT * FROM rpi_pipeline_events WHERE creator_id = $1 ORDER BY created_at DESC LIMIT 50", creator_id)
    notes = await conn.fetch("SELECT * FROM rpi_notes WHERE creator_id = $1 ORDER BY created_at DESC LIMIT 50", creator_id)
    identity = await conn.fetch(
        """SELECT * FROM rpi_creator_identity_matches WHERE creator_a = $1 OR creator_b = $1 ORDER BY created_at DESC""",
        creator_id)
    prior = await partner_scout_history(conn, creator_id)
    talking = await external_conversation(conn, creator_id)
    return {
        "partner_scout_history": prior.as_dict() if prior else None,
        "external_conversation": talking.as_dict() if talking else None,
        "creator": dict(creator),
        "accounts": [dict(a) for a in accounts],
        "snapshots": [dict(s) for s in snapshots],
        "audience": [dict(a) for a in audience],
        "content": [dict(c) for c in content],
        "classification": dict(classification) if classification else None,
        "qualification": dict(score) if score else None,
        "sponsorships": [dict(s) for s in sponsorships],
        "risk_signals": [dict(r) for r in risk],
        "contacts": [dict(c) for c in contacts],
        "pipeline_events": [dict(e) for e in events],
        "notes": [dict(n) for n in notes],
        "identity_matches": [dict(i) for i in identity],
    }


async def delete_creator(conn, creator_id, actor: str, reason: str) -> dict:
    """Compliant deletion: removes the creator, all provider-derived records
    (cascade) and orphaned provenance. Adds the creator's emails to the
    suppression list (hashed) so the deletion is not undone by re-discovery
    followed by outreach."""
    from rpi.crm.suppression import hash_value

    # Campaign/ledger history references the creator. Financial records are
    # retained, so deletion is refused while campaigns exist (anonymize instead).
    if await conn.fetchval("SELECT EXISTS(SELECT 1 FROM rpi_campaigns WHERE creator_id = $1)", creator_id):
        raise ValueError("creator has campaigns with financial history; anonymize instead of deleting")
    emails = await conn.fetch(
        "SELECT value_normalized FROM rpi_creator_contacts WHERE creator_id = $1 AND contact_type = 'email'", creator_id)
    accounts = await conn.fetch(
        "SELECT platform, platform_account_id FROM rpi_creator_social_accounts WHERE creator_id = $1", creator_id)
    for e in emails:
        await conn.execute(
            """INSERT INTO rpi_suppression_list (kind, value_hash, reason, source) VALUES ('email', $1, $2, 'deletion')
               ON CONFLICT (kind, value_hash) DO NOTHING""", hash_value(e["value_normalized"]), reason)
    for a in accounts:
        await conn.execute(
            """INSERT INTO rpi_suppression_list (kind, value_hash, reason, source) VALUES ('creator', $1, $2, 'deletion')
               ON CONFLICT (kind, value_hash) DO NOTHING""", hash_value(f"{a['platform']}:{a['platform_account_id']}"), reason)
    await conn.execute("UPDATE rpi_creators SET merged_into_id = NULL WHERE merged_into_id = $1", creator_id)
    await conn.execute("DELETE FROM rpi_creators WHERE id = $1", creator_id)
    orphans = await conn.execute(
        """DELETE FROM rpi_data_provenance p WHERE NOT EXISTS (SELECT 1 FROM rpi_creator_profile_snapshots s WHERE s.provenance_id = p.id)
             AND NOT EXISTS (SELECT 1 FROM rpi_audience_estimates e WHERE e.provenance_id = p.id)
             AND NOT EXISTS (SELECT 1 FROM rpi_creator_content c WHERE c.provenance_id = p.id OR c.transcript_provenance_id = p.id)
             AND NOT EXISTS (SELECT 1 FROM rpi_sponsorship_relationships r WHERE r.provenance_id = p.id)
             AND NOT EXISTS (SELECT 1 FROM rpi_creator_contacts k WHERE k.provenance_id = p.id)
             AND NOT EXISTS (SELECT 1 FROM rpi_provider_cache pc WHERE pc.provenance_id = p.id)""")
    return {"deleted": str(creator_id), "suppressed_emails": len(emails), "orphan_provenance_deleted": orphans}
