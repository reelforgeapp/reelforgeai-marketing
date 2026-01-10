"""
ReelForge Marketing Engine - Maintenance Tasks
Celery tasks for GDPR data purge and system cleanup
"""

import asyncio
from datetime import datetime, timedelta
import structlog

from celery_config import celery_app, BaseTaskWithRetry
from app.config import get_settings
from app.database import get_database
from services.idempotency import get_idempotency_service

logger = structlog.get_logger()


@celery_app.task(
    bind=True,
    base=BaseTaskWithRetry,
    max_retries=3,
    queue='default'
)
def purge_expired_data(self):
    """
    GDPR-compliant data purge for prospects not converted after 6 months.
    
    Runs daily at 4 AM EST via Celery Beat.
    
    Process:
    1. Find prospects discovered > 6 months ago that haven't converted
    2. Anonymize PII (email, name, handles)
    3. Keep anonymized record for analytics
    4. Log purge action for audit trail
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_purge_data_async())


async def _purge_data_async() -> dict:
    """Async implementation of data purge."""
    settings = get_settings()
    db = get_database()
    
    retention_days = settings.data_retention_days or 180  # 6 months default
    
    results = {
        "prospects_anonymized": 0,
        "sequences_deleted": 0,
        "sends_deleted": 0,
        "consent_logs_deleted": 0,
        "errors": 0
    }
    
    try:
        # Find prospects to purge
        cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
        
        expired_prospects = await db.fetch(
            """
            SELECT id, email, full_name
            FROM marketing_prospects
            WHERE discovered_at < $1
              AND status NOT IN ('converted', 'active_affiliate')
              AND email IS NOT NULL
            LIMIT 1000
            """,
            cutoff_date
        )
        
        logger.info(
            f"Found {len(expired_prospects)} prospects to purge",
            cutoff_date=cutoff_date.isoformat()
        )
        
        for prospect in expired_prospects:
            try:
                async with db.transaction():
                    prospect_id = prospect["id"]
                    
                    # Delete related sequences
                    seq_result = await db.execute(
                        """
                        DELETE FROM outreach_sequences
                        WHERE prospect_id = $1
                        """,
                        prospect_id
                    )
                    seq_count = int(seq_result.split()[-1]) if seq_result else 0
                    results["sequences_deleted"] += seq_count
                    
                    # Delete email sends
                    send_result = await db.execute(
                        """
                        DELETE FROM email_sends
                        WHERE prospect_id = $1
                        """,
                        prospect_id
                    )
                    send_count = int(send_result.split()[-1]) if send_result else 0
                    results["sends_deleted"] += send_count
                    
                    # Delete consent logs
                    consent_result = await db.execute(
                        """
                        DELETE FROM consent_log
                        WHERE prospect_id = $1
                        """,
                        prospect_id
                    )
                    consent_count = int(consent_result.split()[-1]) if consent_result else 0
                    results["consent_logs_deleted"] += consent_count
                    
                    # Anonymize prospect record (keep for analytics)
                    await db.execute(
                        """
                        UPDATE marketing_prospects SET
                            email = NULL,
                            full_name = 'REDACTED',
                            youtube_handle = NULL,
                            youtube_channel_id = NULL,
                            instagram_handle = NULL,
                            tiktok_handle = NULL,
                            website_url = NULL,
                            bio_link_url = NULL,
                            raw_data = '{}',
                            status = 'purged',
                            updated_at = NOW()
                        WHERE id = $1
                        """,
                        prospect_id
                    )
                    
                    results["prospects_anonymized"] += 1
                    
            except Exception as e:
                logger.error(
                    "Failed to purge prospect",
                    prospect_id=str(prospect["id"]),
                    error=str(e)
                )
                results["errors"] += 1
        
        # Log purge action for audit
        await db.execute(
            """
            INSERT INTO audit_log (action, details, performed_by, created_at)
            VALUES ('data_purge', $1, 'system', NOW())
            """,
            {
                "prospects_anonymized": results["prospects_anonymized"],
                "sequences_deleted": results["sequences_deleted"],
                "sends_deleted": results["sends_deleted"],
                "retention_days": retention_days,
                "cutoff_date": cutoff_date.isoformat()
            }
        )
        
        logger.info("Data purge complete", **results)
        
    except Exception as e:
        logger.error(f"Data purge failed: {e}")
        results["errors"] += 1
    
    return results


@celery_app.task(
    bind=True,
    base=BaseTaskWithRetry,
    max_retries=3,
    queue='default'
)
def cleanup_idempotency_keys(self):
    """
    Clean up expired idempotency keys from database.
    
    Runs daily at 5 AM EST via Celery Beat.
    
    Redis keys auto-expire, but database records need manual cleanup.
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_cleanup_keys_async())


async def _cleanup_keys_async() -> dict:
    """Async implementation of idempotency key cleanup."""
    db = get_database()
    idempotency = get_idempotency_service()
    
    results = {
        "keys_deleted": 0,
        "errors": 0
    }
    
    try:
        # Use the service method
        deleted = await idempotency.cleanup_expired()
        results["keys_deleted"] = deleted
        
        # Also clean up any orphaned keys older than 30 days
        old_keys = await db.execute(
            """
            DELETE FROM idempotency_keys
            WHERE created_at < NOW() - INTERVAL '30 days'
            """
        )
        
        if old_keys:
            count = int(old_keys.split()[-1])
            results["keys_deleted"] += count
        
        logger.info("Idempotency cleanup complete", **results)
        
    except Exception as e:
        logger.error(f"Idempotency cleanup failed: {e}")
        results["errors"] += 1
    
    return results


@celery_app.task(
    bind=True,
    base=BaseTaskWithRetry,
    max_retries=3,
    queue='default'
)
def cleanup_old_audit_logs(self):
    """
    Clean up audit logs older than 1 year.
    
    Runs weekly.
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_cleanup_audit_async())


async def _cleanup_audit_async() -> dict:
    """Async implementation of audit log cleanup."""
    db = get_database()
    
    results = {"logs_deleted": 0, "errors": 0}
    
    try:
        result = await db.execute(
            """
            DELETE FROM audit_log
            WHERE created_at < NOW() - INTERVAL '1 year'
            """
        )
        
        if result:
            results["logs_deleted"] = int(result.split()[-1])
        
        logger.info("Audit log cleanup complete", **results)
        
    except Exception as e:
        logger.error(f"Audit cleanup failed: {e}")
        results["errors"] += 1
    
    return results


@celery_app.task(
    bind=True,
    base=BaseTaskWithRetry,
    max_retries=3,
    queue='default'
)
def update_daily_stats(self):
    """
    Update daily statistics aggregation.
    
    Runs at end of day to ensure accurate counts.
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_update_stats_async())


async def _update_stats_async() -> dict:
    """Async implementation of stats update."""
    db = get_database()
    
    today = datetime.utcnow().date()
    
    try:
        # Get today's counts
        stats = await db.fetchrow(
            """
            SELECT
                (SELECT COUNT(*) FROM marketing_prospects 
                 WHERE DATE(discovered_at) = $1) as discovered,
                (SELECT COUNT(*) FROM marketing_prospects 
                 WHERE DATE(discovered_at) = $1 AND primary_platform = 'youtube') as youtube,
                (SELECT COUNT(*) FROM marketing_prospects 
                 WHERE DATE(discovered_at) = $1 AND primary_platform = 'instagram') as instagram,
                (SELECT COUNT(*) FROM marketing_prospects 
                 WHERE DATE(discovered_at) = $1 AND primary_platform = 'tiktok') as tiktok,
                (SELECT COUNT(*) FROM marketing_prospects 
                 WHERE DATE(verified_at) = $1 AND email_verified = TRUE) as verified,
                (SELECT COUNT(*) FROM email_sends 
                 WHERE DATE(sent_at) = $1) as emails_sent,
                (SELECT COUNT(*) FROM email_sends 
                 WHERE DATE(first_opened_at) = $1) as emails_opened,
                (SELECT COUNT(*) FROM email_sends 
                 WHERE DATE(first_clicked_at) = $1) as emails_clicked,
                (SELECT COUNT(*) FROM affiliates 
                 WHERE DATE(created_at) = $1) as affiliates_signed
            """,
            today
        )
        
        # Upsert daily stats
        await db.execute(
            """
            INSERT INTO marketing_daily_stats (
                stat_date, 
                prospects_discovered, prospects_youtube, prospects_instagram, prospects_tiktok,
                emails_verified, emails_sent, emails_opened, emails_clicked,
                affiliates_signed_up
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
            )
            ON CONFLICT (stat_date) DO UPDATE SET
                prospects_discovered = $2,
                prospects_youtube = $3,
                prospects_instagram = $4,
                prospects_tiktok = $5,
                emails_verified = $6,
                emails_sent = $7,
                emails_opened = $8,
                emails_clicked = $9,
                affiliates_signed_up = $10
            """,
            today,
            stats["discovered"] or 0,
            stats["youtube"] or 0,
            stats["instagram"] or 0,
            stats["tiktok"] or 0,
            stats["verified"] or 0,
            stats["emails_sent"] or 0,
            stats["emails_opened"] or 0,
            stats["emails_clicked"] or 0,
            stats["affiliates_signed"] or 0
        )
        
        logger.info("Daily stats updated", date=str(today))
        return {"status": "success", "date": str(today)}
        
    except Exception as e:
        logger.error(f"Stats update failed: {e}")
        return {"status": "error", "error": str(e)}


@celery_app.task(queue='default')
def handle_gdpr_deletion_request(email: str):
    """
    Handle GDPR right-to-deletion request.
    
    Triggered manually via admin endpoint.
    Completely removes all data for a specific email.
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_handle_deletion_async(email))


async def _handle_deletion_async(email: str) -> dict:
    """Async implementation of GDPR deletion."""
    db = get_database()
    
    results = {
        "email": email[:5] + "***",
        "prospect_deleted": False,
        "sequences_deleted": 0,
        "sends_deleted": 0,
        "consent_logs_deleted": 0
    }
    
    try:
        # Find prospect by email
        prospect = await db.fetchrow(
            """
            SELECT id FROM marketing_prospects WHERE email = $1
            """,
            email
        )
        
        if not prospect:
            logger.warning(f"GDPR deletion: No prospect found for email")
            return results
        
        prospect_id = prospect["id"]
        
        async with db.transaction():
            # Delete sequences
            seq_result = await db.execute(
                "DELETE FROM outreach_sequences WHERE prospect_id = $1",
                prospect_id
            )
            results["sequences_deleted"] = int(seq_result.split()[-1]) if seq_result else 0
            
            # Delete sends
            send_result = await db.execute(
                "DELETE FROM email_sends WHERE prospect_id = $1",
                prospect_id
            )
            results["sends_deleted"] = int(send_result.split()[-1]) if send_result else 0
            
            # Delete consent logs
            consent_result = await db.execute(
                "DELETE FROM consent_log WHERE prospect_id = $1",
                prospect_id
            )
            results["consent_logs_deleted"] = int(consent_result.split()[-1]) if consent_result else 0
            
            # Delete prospect completely (not just anonymize)
            await db.execute(
                "DELETE FROM marketing_prospects WHERE id = $1",
                prospect_id
            )
            results["prospect_deleted"] = True
            
            # Log for audit
            await db.execute(
                """
                INSERT INTO audit_log (action, details, performed_by, created_at)
                VALUES ('gdpr_deletion', $1, 'user_request', NOW())
                """,
                {"email_hash": hash(email), "results": results}
            )
        
        logger.info("GDPR deletion complete", **results)
        
    except Exception as e:
        logger.error(f"GDPR deletion failed: {e}")
        results["error"] = str(e)
    
    return results
