"""
ReelForge Marketing Engine - Enrichment Tasks
"""

import asyncio
import structlog

from celery_config import celery_app, BaseTaskWithRetry
from app.config import get_settings
from app.database import get_database_async

logger = structlog.get_logger()


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def run_email_extraction(self):
    """Extract emails from prospect profiles."""
    return asyncio.run(_email_extraction_async())


async def _email_extraction_async() -> dict:
    db = await get_database_async()
    
    results = {"processed": 0, "emails_found": 0, "http_method": 0, "playwright_method": 0, "failed": 0}
    
    try:
        from discovery.hybrid_email_extractor import HybridEmailExtractor
        
        extractor = HybridEmailExtractor()
        extraction_results = await extractor.extract_for_prospects(limit=30, only_missing=True)
        results.update(extraction_results)
        logger.info("Email extraction complete", **results)
        
    except ImportError as e:
        logger.warning(f"Hybrid email extractor not available: {e}")
        
        prospects = await db.fetch("""
            SELECT id, youtube_channel_id, youtube_handle
            FROM marketing_prospects
            WHERE youtube_channel_id IS NOT NULL AND email IS NULL AND status = 'discovered'
            ORDER BY relevance_score DESC LIMIT 30
        """)
        
        results["processed"] = len(prospects)
        results["status"] = "fallback_mode"
        
    except Exception as e:
        logger.error(f"Email extraction failed: {e}")
        results["error"] = str(e)
    
    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def run_email_verification(self):
    """Verify extracted emails."""
    return asyncio.run(_email_verification_async())


async def _email_verification_async() -> dict:
    settings = get_settings()
    db = await get_database_async()
    
    results = {"processed": 0, "valid": 0, "invalid": 0, "catch_all": 0, "errors": 0}
    
    # Check if ANY verification service is configured (including Bouncer!)
    if not settings.bouncer_api_key and not settings.clearout_api_key and not settings.hunter_api_key:
        logger.warning("Email verification not configured")
        return {"status": "skipped", "reason": "No verification service configured"}
    
    try:
        from services.email_verification import get_verification_client
        
        client = get_verification_client()
        verification_results = await client.verify_batch(limit=100, only_unverified=True)
        results.update(verification_results)
        logger.info("Email verification complete", **results)
        
    except ImportError as e:
        logger.warning(f"Email verification module not available: {e}")
        await db.execute("""
            UPDATE marketing_prospects
            SET email_verified = FALSE, verification_status = 'pending'
            WHERE email IS NOT NULL AND email_verified IS NULL
        """)
        results["status"] = "marked_pending"
        
    except ValueError as e:
        logger.warning(f"Email verification skipped: {e}")
        results["status"] = "skipped"
        results["reason"] = str(e)
        
    except Exception as e:
        logger.error(f"Email verification failed: {e}")
        results["error"] = str(e)
    
    return results
