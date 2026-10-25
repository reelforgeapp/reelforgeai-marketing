"""
Retention and deletion enforcement (runs daily via Celery beat).

  * provenance rows past expires_at are deleted; facts referencing them
    (snapshots, content, audience estimates, sponsorships from provider data)
    cascade. RPI does not auto-refresh expired records, because refreshing
    costs money; the next research run fetches fresh data if still needed.
  * expired cache entries are deleted.
  * revoked OAuth grants have their authorized data deleted by data_delete_by
    (7 days; YouTube Developer Policies III.D.2).
  * grants not re-confirmed within 30 days are revoked (III.E.4(b)).
"""

from __future__ import annotations

import structlog

from rpi.core.audit import audit

logger = structlog.get_logger()


async def enforce(conn, *, dry_run: bool = False) -> dict:
    expired = await conn.fetch(
        "SELECT provider, COUNT(*) AS n FROM rpi_data_provenance WHERE expires_at <= NOW() GROUP BY provider")
    report = {"expired_provenance": {r["provider"]: r["n"] for r in expired}}
    if dry_run:
        report["dry_run"] = True
        return report

    async with conn.transaction():
        report["provenance_deleted"] = _count(await conn.execute("DELETE FROM rpi_data_provenance WHERE expires_at <= NOW()"))
        report["cache_deleted"] = _count(await conn.execute("DELETE FROM rpi_provider_cache WHERE expires_at <= NOW()"))
        report["oauth_states_deleted"] = _count(await conn.execute("DELETE FROM rpi_oauth_states WHERE expires_at <= NOW()"))
        stale = await conn.fetch(
            """UPDATE rpi_oauth_grants SET revoked_at = NOW(), revocation_source = 'reconfirmation_expired',
                   data_delete_by = NOW() + INTERVAL '7 days'
               WHERE revoked_at IS NULL AND reconfirm_by <= NOW() RETURNING creator_id""")
        report["grants_expired"] = len(stale)
        due = await conn.fetch(
            "SELECT creator_id FROM rpi_oauth_grants WHERE revoked_at IS NOT NULL AND data_delete_by <= NOW()")
        deleted = 0
        for g in due:
            deleted += _count(await conn.execute(
                """DELETE FROM rpi_audience_estimates WHERE creator_id = $1 AND provider = 'youtube_analytics_api'""",
                g["creator_id"]))
            await conn.execute(
                "UPDATE rpi_oauth_grants SET encrypted_refresh_token = '', data_delete_by = NULL WHERE creator_id = $1 "
                "AND provider = 'youtube_analytics_api'", g["creator_id"])
        report["authorized_rows_deleted"] = deleted
        await audit(conn, "system", "retention.enforce", None, None, report)
    logger.info("rpi_retention_enforced", **{k: v for k, v in report.items() if not isinstance(v, dict)})
    return report


def _count(status: str) -> int:
    try:
        return int(status.split()[-1])
    except (ValueError, IndexError, AttributeError):
        return 0
