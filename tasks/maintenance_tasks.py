"""
ReelForge Marketing Engine - Maintenance Tasks
"""
import sys
sys.path.insert(0, '/app')

import asyncio
from datetime import datetime, timedelta
import structlog

from celery_config import celery_app, BaseTaskWithRetry
from app.config import get_settings
from app.database import get_database_async, DatabaseTransaction

logger = structlog.get_logger()


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def purge_expired_data(self):
    return asyncio.run(_purge_data_async())


async def _purge_data_async() -> dict:
    settings = get_settings()
    db = await get_database_async()
    
    results = {"prospects_purged": 0, "errors": 0}
    
    try:
        cutoff = datetime.utcnow() - timedelta(days=settings.data_retention_days)
        
        expired = await db.fetch("""
            SELECT id FROM marketing_prospects
            WHERE discovered_at < $1
              AND status NOT IN ('converted', 'active_affiliate')
            LIMIT 100
        """, cutoff)
        
        for prospect in expired:
            try:
                async with DatabaseTransaction() as conn:
                    await conn.execute("DELETE FROM outreach_sequences WHERE prospect_id = $1", prospect["id"])
                    await conn.execute("DELETE FROM email_sends WHERE prospect_id = $1", prospect["id"])
                    await conn.execute("DELETE FROM marketing_prospects WHERE id = $1", prospect["id"])
                results["prospects_purged"] += 1
            except Exception as e:
                results["errors"] += 1
        
        logger.info("Data purge complete", **results)
        
    except Exception as e:
        logger.error("Data purge failed", error=str(e))
        results["error"] = str(e)
    
    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def cleanup_old_data(self):
    return asyncio.run(_cleanup_async())


async def _cleanup_async() -> dict:
    db = await get_database_async()
    results = {"cleaned": 0}
    
    try:
        await db.execute("DELETE FROM idempotency_keys WHERE expires_at < NOW()")
        logger.info("Cleanup complete")
    except Exception as e:
        logger.error("Cleanup failed", error=str(e))
        results["error"] = str(e)
    
    return results
