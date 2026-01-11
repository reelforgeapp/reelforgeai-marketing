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
    return asyncio.run(_email_extraction_async())


async def _email_extraction_async() -> dict:
    db = await get_database_async()
    
    results = {
        "processed": 0,
        "emails_found": 0,
        "http_method": 0,
        "playwright_method": 0,
        "failed": 0
    }
    
    try:
        from discovery.hybrid_email_extractor import HybridEmailExtractor
        
        extractor = HybridEmailExtractor()
        extraction_results = await extractor.extract_for_prospects(limit=30, only_missing=True)
        results.update(extraction_results)
        
        logger.info("Email extraction complete", **results)
        
    except ImportError as e:
        logger.warning("Email extractor not available", error=str(e))
        results["status"] = "import_error"
    except Exception as e:
        logger.error("Email extraction failed", error=str(e))
        results["error"] = str(e)
    
    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def run_email_verification(self):
    return asyncio.run(_email_verification_async())


async def _email_verification_async() -> dict:
    settings = get_settings()
    db = await get_database_async()
    
    results = {
        "processed": 0,
        "valid": 0,
        "invalid": 0,
        "catch_all": 0,
        "errors": 0
    }
    
    if not settings.bouncer_api_key and not settings.clearout_api_key and not settings.hunter_api_key:
        return {"status": "skipped", "reason": "No verification service configured"}
    
    try:
        from services.email_verification import get_verification_client
        
        client = get_verification_client()
        verification_results = await client.verify_batch(limit=100, only_unverified=True)
        results.update(verification_results)
        
        logger.info("Email verification complete", **results)
        
    except ImportError as e:
        logger.warning("Verification module not available", error=str(e))
        results["status"] = "import_error"
    except ValueError as e:
        results["status"] = "skipped"
        results["reason"] = str(e)
    except Exception as e:
        logger.error("Email verification failed", error=str(e))
        results["error"] = str(e)
    
    return results
