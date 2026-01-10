"""
ReelForge Marketing Engine - Maintenance Tasks (Fixed)
"""

import asyncio
import hashlib
import json
from datetime import datetime, timedelta
import structlog

from celery_config import celery_app, BaseTaskWithRetry
from app.config import get_settings
from app.database import get_db_connection, DatabaseTransaction
from services.idempotency import IdempotencyService

logger = structlog.get_logger()


def _parse_delete_count(result):
    if not result:
        return 0
    try:
        parts = result.split()
        if len(parts) >= 2:
            return int(parts[-1])
    except:
        pass
    return 0


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def purge_expired_data(self):
    """GDPR-compliant data purge."""
    return asyncio.run(_purge_data_async())


async def _purge_data_async() -> dict:
    settings = get_settings()
    retention_days = settings.data_retention_days or 180
    results = {"prospects_anonymized": 0, "sequences_deleted": 0, "sends_deleted": 0, "consent_logs_deleted": 0, "errors": 0}
    
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
        
        async with get_db_connection() as db:
            expired_prospects = await db.fetch("""
                SELECT id, email, full_name FROM marketing_prospects
                WHERE discovered_at < $1 AND status NOT IN ('converted', 'active_affiliate') AND email IS NOT NULL
                LIMIT 1000
            """, cutoff_date)
        
        logger.info(f"Found {len(expired_prospects)} prospects to purge")
        
        for prospect in expired_prospects:
            try:
                async with DatabaseTransaction() as conn:
                    prospect_id = prospect["id"]
                    
                    seq_result = await conn.execute("DELETE FROM outreach_sequences WHERE prospect_id = $1", prospect_id)
                    results["sequences_deleted"] += _parse_delete_count(seq_result)
                    
                    send_result = await conn.execute("DELETE FROM email_sends WHERE prospect_id = $1", prospect_id)
                    results["sends_deleted"] += _parse_delete_count(send_result)
                    
                    consent_result = await conn.execute("DELETE FROM consent_log WHERE prospect_id = $1", prospect_id)
                    results["consent_logs_deleted"] += _parse_delete_count(consent_result)
                    
                    await conn.execute("""
                        UPDATE marketing_prospects SET
                            email = NULL, full_name = 'REDACTED', youtube_handle = NULL, youtube_channel_id = NULL,
                            instagram_handle = NULL, tiktok_handle = NULL, website_url = NULL, bio_link_url = NULL,
                            raw_data = '{}', status = 'purged', updated_at = NOW()
                        WHERE id = $1
                    """, prospect_id)
                    
                    results["prospects_anonymized"] += 1
                    
            except Exception as e:
                logger.error("Failed to purge prospect", prospect_id=str(prospect["id"]), error=str(e))
                results["errors"] += 1
        
        async with get_db_connection() as db:
            audit_details = json.dumps({
                "prospects_anonymized": results["prospects_anonymized"],
                "sequences_deleted": results["sequences_deleted"],
                "sends_deleted": results["sends_deleted"],
                "retention_days": retention_days,
                "cutoff_date": cutoff_date.isoformat()
            })
            
            await db.execute(
                "INSERT INTO audit_log (action, details, performed_by, created_at) VALUES ('data_purge', $1::jsonb, 'system', NOW())",
                audit_details
            )
        
        logger.info("Data purge complete", **results)
        
    except Exception as e:
        logger.error(f"Data purge failed: {e}")
        results["errors"] += 1
    
    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def cleanup_idempotency_keys(self):
    """Clean up expired idempotency keys."""
    return asyncio.run(_cleanup_keys_async())


async def _cleanup_keys_async() -> dict:
    idempotency = IdempotencyService()
    results = {"keys_deleted": 0, "errors": 0}
    
    try:
        deleted = await idempotency.cleanup_expired()
        results["keys_deleted"] = deleted
        
        async with get_db_connection() as db:
            old_keys = await db.execute("DELETE FROM idempotency_keys WHERE created_at < NOW() - INTERVAL '30 days'")
            if old_keys:
                results["keys_deleted"] += _parse_delete_count(old_keys)
        
        logger.info("Idempotency cleanup complete", **results)
        
    except Exception as e:
        logger.error(f"Idempotency cleanup failed: {e}")
        results["errors"] += 1
    
    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def cleanup_old_audit_logs(self):
    """Clean up audit logs older than 1 year."""
    return asyncio.run(_cleanup_audit_async())


async def _cleanup_audit_async() -> dict:
    results = {"logs_deleted": 0, "errors": 0}
    
    try:
        async with get_db_connection() as db:
            result = await db.execute("DELETE FROM audit_log WHERE created_at < NOW() - INTERVAL '1 year'")
            if result:
                results["logs_deleted"] = _parse_delete_count(result)
        logger.info("Audit log cleanup complete", **results)
    except Exception as e:
        logger.error(f"Audit cleanup failed: {e}")
        results["errors"] += 1
    
    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def update_daily_stats(self):
    """Update daily statistics."""
    return asyncio.run(_update_stats_async())


async def _update_stats_async() -> dict:
    today = datetime.utcnow().date()
    
    try:
        async with get_db_connection() as db:
            stats = await db.fetchrow("""
                SELECT
                    (SELECT COUNT(*) FROM marketing_prospects WHERE DATE(discovered_at) = $1) as discovered,
                    (SELECT COUNT(*) FROM marketing_prospects WHERE DATE(discovered_at) = $1 AND primary_platform = 'youtube') as youtube,
                    (SELECT COUNT(*) FROM marketing_prospects WHERE DATE(discovered_at) = $1 AND primary_platform = 'instagram') as instagram,
                    (SELECT COUNT(*) FROM marketing_prospects WHERE DATE(discovered_at) = $1 AND primary_platform = 'tiktok') as tiktok,
                    (SELECT COUNT(*) FROM marketing_prospects WHERE DATE(verified_at) = $1 AND email_verified = TRUE) as verified,
                    (SELECT COUNT(*) FROM email_sends WHERE DATE(sent_at) = $1) as emails_sent,
                    (SELECT COUNT(*) FROM email_sends WHERE DATE(first_opened_at) = $1) as emails_opened,
                    (SELECT COUNT(*) FROM email_sends WHERE DATE(first_clicked_at) = $1) as emails_clicked,
                    (SELECT COUNT(*) FROM affiliates WHERE DATE(created_at) = $1) as affiliates_signed
            """, today)
            
            await db.execute("""
                INSERT INTO marketing_daily_stats (stat_date, prospects_discovered, prospects_youtube, prospects_instagram, prospects_tiktok, emails_verified, emails_sent, emails_opened, emails_clicked, affiliates_signed_up)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (stat_date) DO UPDATE SET
                    prospects_discovered = $2, prospects_youtube = $3, prospects_instagram = $4, prospects_tiktok = $5,
                    emails_verified = $6, emails_sent = $7, emails_opened = $8, emails_clicked = $9, affiliates_signed_up = $10
            """, today, stats["discovered"] or 0, stats["youtube"] or 0, stats["instagram"] or 0, stats["tiktok"] or 0,
                stats["verified"] or 0, stats["emails_sent"] or 0, stats["emails_opened"] or 0, stats["emails_clicked"] or 0, stats["affiliates_signed"] or 0)
        
        return {"status": "success", "date": str(today)}
    except Exception as e:
        logger.error(f"Stats update failed: {e}")
        return {"status": "error", "error": str(e)}


@celery_app.task(queue='default')
def handle_gdpr_deletion_request(email: str):
    """Handle GDPR right-to-deletion request."""
    return asyncio.run(_handle_deletion_async(email))


async def _handle_deletion_async(email: str) -> dict:
    email_hash = hashlib.sha256(email.encode()).hexdigest()[:16]
    results = {"email_hash": email_hash, "prospect_deleted": False, "sequences_deleted": 0, "sends_deleted": 0, "consent_logs_deleted": 0}
    
    try:
        async with get_db_connection() as db:
            prospect = await db.fetchrow("SELECT id FROM marketing_prospects WHERE email = $1", email)
        
        if not prospect:
            logger.warning("GDPR deletion: No prospect found")
            return results
        
        prospect_id = prospect["id"]
        
        async with DatabaseTransaction() as conn:
            seq_result = await conn.execute("DELETE FROM outreach_sequences WHERE prospect_id = $1", prospect_id)
            results["sequences_deleted"] = _parse_delete_count(seq_result)
            
            send_result = await conn.execute("DELETE FROM email_sends WHERE prospect_id = $1", prospect_id)
            results["sends_deleted"] = _parse_delete_count(send_result)
            
            consent_result = await conn.execute("DELETE FROM consent_log WHERE prospect_id = $1", prospect_id)
            results["consent_logs_deleted"] = _parse_delete_count(consent_result)
            
            await conn.execute("DELETE FROM marketing_prospects WHERE id = $1", prospect_id)
            results["prospect_deleted"] = True
            
            audit_details = json.dumps({"email_hash": email_hash, "results": results})
            await conn.execute(
                "INSERT INTO audit_log (action, details, performed_by, created_at) VALUES ('gdpr_deletion', $1::jsonb, 'user_request', NOW())",
                audit_details
            )
        
        logger.info("GDPR deletion complete", **results)
        
    except Exception as e:
        logger.error(f"GDPR deletion failed: {e}")
        results["error"] = str(e)
    
    return results
