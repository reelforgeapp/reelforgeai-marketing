"""
ReelForge Marketing Engine - Discovery Tasks (Fixed)
"""

import asyncio
import structlog

from celery_config import celery_app, BaseTaskWithRetry
from app.config import get_settings
from app.database import get_db_connection

logger = structlog.get_logger()


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='discovery')
def run_youtube_discovery(self):
    """Discover YouTube creators reviewing AI video tools."""
    return asyncio.run(_youtube_discovery_async())


async def _youtube_discovery_async() -> dict:
    settings = get_settings()
    results = {"processed": 0, "new_prospects": 0, "errors": 0, "keywords_searched": 0}
    
    async with get_db_connection() as db:
        try:
            keywords = await db.fetch(
                "SELECT keyword FROM competitor_keywords WHERE platform = 'youtube' AND is_active = TRUE ORDER BY priority ASC, last_searched_at ASC NULLS FIRST LIMIT 5"
            )
            
            if not keywords:
                logger.warning("No active YouTube keywords found")
                return {"status": "skipped", "reason": "no_keywords"}
            
            from discovery.youtube_api import YouTubeDiscoveryClient
            
            client = YouTubeDiscoveryClient(api_key=settings.youtube_api_key)
            
            for row in keywords:
                keyword = row["keyword"]
                results["keywords_searched"] += 1
                
                try:
                    channels = await client.search_channels(keyword, max_results=10)
                    
                    for channel in channels:
                        results["processed"] += 1
                        
                        existing = await db.fetchval(
                            "SELECT id FROM marketing_prospects WHERE youtube_channel_id = $1",
                            channel.get("channel_id")
                        )
                        
                        if existing:
                            continue
                        
                        await db.execute("""
                            INSERT INTO marketing_prospects (youtube_channel_id, youtube_handle, full_name, primary_platform, youtube_subscribers, youtube_views, relevance_score, competitor_mentions, status, discovered_at, raw_data)
                            VALUES ($1, $2, $3, 'youtube', $4, $5, $6, $7, 'discovered', NOW(), $8)
                            ON CONFLICT (youtube_channel_id) DO NOTHING
                        """,
                            channel.get("channel_id"),
                            channel.get("handle"),
                            channel.get("title"),
                            channel.get("subscriber_count", 0),
                            channel.get("view_count", 0),
                            channel.get("relevance_score", 50),
                            [keyword],
                            "{}"
                        )
                        results["new_prospects"] += 1
                    
                    await db.execute(
                        "UPDATE competitor_keywords SET last_searched_at = NOW(), search_count = COALESCE(search_count, 0) + 1 WHERE keyword = $1 AND platform = 'youtube'",
                        keyword
                    )
                    
                except Exception as e:
                    logger.error(f"Error searching keyword {keyword}: {e}")
                    results["errors"] += 1
                
                await asyncio.sleep(1)
            
            logger.info("YouTube discovery complete", **results)
            
        except ImportError as e:
            logger.warning(f"YouTube discovery module not available: {e}")
            results["status"] = "module_unavailable"
            
        except Exception as e:
            logger.error(f"YouTube discovery failed: {e}")
            results["error"] = str(e)
    
    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='discovery')
def run_apify_discovery(self):
    """Discover creators using Apify scrapers."""
    return asyncio.run(_apify_discovery_async())


async def _apify_discovery_async() -> dict:
    settings = get_settings()
    results = {"processed": 0, "new_prospects": 0, "errors": 0}
    
    if not settings.apify_api_token:
        logger.warning("Apify API token not configured")
        return {"status": "skipped", "reason": "no_api_token"}
    
    async with get_db_connection() as db:
        try:
            from discovery.apify_client import ApifyDiscoveryClient
            
            client = ApifyDiscoveryClient(api_token=settings.apify_api_token)
            
            keywords = await db.fetch(
                "SELECT keyword FROM competitor_keywords WHERE platform = 'youtube' AND is_active = TRUE ORDER BY priority ASC LIMIT 3"
            )
            
            for row in keywords:
                keyword = row["keyword"]
                
                try:
                    creators = await client.run_youtube_scraper(keyword, max_results=20)
                    
                    for creator in creators:
                        results["processed"] += 1
                        
                        channel_id = creator.get("channelId") or creator.get("channel_id")
                        if not channel_id:
                            continue
                        
                        existing = await db.fetchval(
                            "SELECT id FROM marketing_prospects WHERE youtube_channel_id = $1",
                            channel_id
                        )
                        
                        if existing:
                            continue
                        
                        await db.execute("""
                            INSERT INTO marketing_prospects (youtube_channel_id, youtube_handle, full_name, primary_platform, youtube_subscribers, relevance_score, competitor_mentions, status, discovered_at, raw_data)
                            VALUES ($1, $2, $3, 'youtube', $4, $5, $6, 'discovered', NOW(), $7)
                            ON CONFLICT (youtube_channel_id) DO NOTHING
                        """,
                            channel_id,
                            creator.get("channelHandle") or creator.get("handle"),
                            creator.get("channelName") or creator.get("title"),
                            creator.get("subscriberCount") or creator.get("subscribers", 0),
                            creator.get("relevanceScore", 50),
                            [keyword],
                            "{}"
                        )
                        results["new_prospects"] += 1
                        
                except Exception as e:
                    logger.error(f"Apify error for {keyword}: {e}")
                    results["errors"] += 1
            
            logger.info("Apify discovery complete", **results)
            
        except ImportError as e:
            logger.warning(f"Apify client not available: {e}")
            results["status"] = "module_unavailable"
            
        except Exception as e:
            logger.error(f"Apify discovery failed: {e}")
            results["error"] = str(e)
    
    return results
