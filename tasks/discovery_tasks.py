"""
ReelForge Marketing Engine - Discovery Tasks
"""

import asyncio
import structlog

from celery_config import celery_app, BaseTaskWithRetry
from app.config import get_settings
from app.database import get_database_async

logger = structlog.get_logger()


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='discovery')
def run_youtube_discovery(self):
    """Discover new prospects via YouTube Data API."""
    return asyncio.run(_youtube_discovery_async())


async def _youtube_discovery_async() -> dict:
    settings = get_settings()
    db = await get_database_async()
    
    if not settings.youtube_api_key:
        return {"status": "error", "error": "API key missing"}
    
    results = {"videos_searched": 0, "channels_found": 0, "prospects_created": 0, "duplicates_skipped": 0, "errors": 0}
    
    try:
        from discovery.youtube_discovery import YouTubeDiscovery
        
        discovery = YouTubeDiscovery(api_key=settings.youtube_api_key, db=db)
        
        keywords = await db.fetch(
            "SELECT keyword FROM competitor_keywords WHERE platform = 'youtube' AND is_active = TRUE LIMIT 10"
        )
        
        if not keywords:
            return {"status": "warning", "message": "No keywords configured"}
        
        for kw in keywords:
            try:
                kw_results = await discovery.search_and_store(keyword=kw['keyword'], max_results=50)
                results["videos_searched"] += kw_results.get("videos_searched", 0)
                results["channels_found"] += kw_results.get("channels_found", 0)
                results["prospects_created"] += kw_results.get("prospects_created", 0)
                results["duplicates_skipped"] += kw_results.get("duplicates_skipped", 0)
            except Exception as e:
                logger.error(f"Keyword search failed: {kw['keyword']}", error=str(e))
                results["errors"] += 1
        
        logger.info("YouTube discovery complete", **results)
        
    except ImportError as e:
        results["status"] = "module_missing"
    except Exception as e:
        logger.error(f"YouTube discovery failed: {e}")
        results["errors"] += 1
    
    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='discovery')
def run_apify_discovery(self):
    """Discover new prospects via Apify."""
    return asyncio.run(_apify_discovery_async())


async def _apify_discovery_async() -> dict:
    settings = get_settings()
    db = await get_database_async()
    
    if not settings.apify_api_token:
        return {"status": "error", "error": "API token missing"}
    
    results = {"instagram_profiles": 0, "tiktok_profiles": 0, "prospects_created": 0, "errors": 0}
    
    try:
        from discovery.apify_client import ApifyDiscovery
        
        discovery = ApifyDiscovery(api_token=settings.apify_api_token, db=db)
        
        try:
            ig_results = await discovery.discover_instagram_creators(limit=50)
            results["instagram_profiles"] = ig_results.get("profiles_found", 0)
            results["prospects_created"] += ig_results.get("prospects_created", 0)
        except Exception as e:
            logger.error(f"Instagram discovery failed: {e}")
            results["errors"] += 1
        
        try:
            tt_results = await discovery.discover_tiktok_creators(limit=50)
            results["tiktok_profiles"] = tt_results.get("profiles_found", 0)
            results["prospects_created"] += tt_results.get("prospects_created", 0)
        except Exception as e:
            logger.error(f"TikTok discovery failed: {e}")
            results["errors"] += 1
        
        logger.info("Apify discovery complete", **results)
        
    except ImportError:
        results["status"] = "module_missing"
    except Exception as e:
        logger.error(f"Apify discovery failed: {e}")
        results["errors"] += 1
    
    return results
