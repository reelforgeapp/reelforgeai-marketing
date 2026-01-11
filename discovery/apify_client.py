"""
Apify Discovery Client for Instagram and TikTok
"""

import asyncio
from typing import Optional
from apify_client import ApifyClient
import structlog

from app.config import get_settings

logger = structlog.get_logger()


class ApifyDiscovery:
    # Use hashtag/search scrapers instead of profile scrapers
    INSTAGRAM_HASHTAG_ACTOR = "apify/instagram-hashtag-scraper"
    INSTAGRAM_SEARCH_ACTOR = "apify/instagram-scraper"
    TIKTOK_SCRAPER_ACTOR = "clockworks/tiktok-scraper"
    
    def __init__(self, api_token: str, db):
        self.settings = get_settings()
        self.client = ApifyClient(api_token)
        self.db = db
    
    async def discover_instagram_creators(self, limit: int = 50) -> dict:
        results = {"profiles_found": 0, "prospects_created": 0, "errors": 0}
        
        try:
            keywords = await self.db.fetch(
                "SELECT keyword FROM competitor_keywords WHERE platform = 'instagram' AND is_active = TRUE LIMIT 5"
            )
            
            if not keywords:
                # Use default hashtags if none configured
                keywords = [
                    {"keyword": "aitools"},
                    {"keyword": "aivideoediting"},
                    {"keyword": "contentcreator"}
                ]
            
            for kw in keywords:
                try:
                    hashtag = kw['keyword'].replace('#', '').replace(' ', '')
                    
                    run_input = {
                        "hashtags": [hashtag],
                        "resultsLimit": limit,
                        "searchType": "hashtag"
                    }
                    
                    logger.info("Running Instagram hashtag search", hashtag=hashtag)
                    
                    run = await asyncio.to_thread(
                        lambda ri=run_input: self.client.actor(self.INSTAGRAM_HASHTAG_ACTOR).call(run_input=ri)
                    )
                    
                    items = await asyncio.to_thread(
                        lambda r=run: list(self.client.dataset(r["defaultDatasetId"]).iterate_items())
                    )
                    
                    logger.info("Instagram search results", hashtag=hashtag, count=len(items))
                    
                    for item in items:
                        try:
                            created = await self._process_instagram_profile(item, kw['keyword'])
                            if created:
                                results["prospects_created"] += 1
                            results["profiles_found"] += 1
                        except Exception as e:
                            logger.error("Profile processing error", error=str(e))
                            results["errors"] += 1
                            
                except Exception as e:
                    logger.error("Instagram search failed", keyword=kw['keyword'], error=str(e))
                    results["errors"] += 1
                    
        except Exception as e:
            logger.error("Instagram discovery failed", error=str(e))
            results["errors"] += 1
        
        logger.info("Instagram discovery complete", **results)
        return results
    
    async def _process_instagram_profile(self, item: dict, keyword: str) -> bool:
        # Handle different response formats
        username = (
            item.get('ownerUsername') or 
            item.get('username') or
            item.get('owner', {}).get('username')
        )
        if not username:
            return False
        
        existing = await self.db.fetchval(
            "SELECT id FROM marketing_prospects WHERE instagram_handle = $1",
            username
        )
        if existing:
            return False
        
        # Get follower count from various possible fields
        followers = (
            item.get('followersCount') or 
            item.get('owner', {}).get('followersCount') or
            0
        )
        
        if followers < self.settings.min_instagram_followers:
            return False
        
        full_name = (
            item.get('fullName') or 
            item.get('owner', {}).get('fullName') or
            ''
        )
        
        bio = item.get('biography') or item.get('owner', {}).get('biography') or ''
        
        await self.db.execute("""
            INSERT INTO marketing_prospects (
                instagram_handle, full_name, instagram_followers,
                primary_platform, relevance_score, competitor_mentions,
                raw_data, status, discovered_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        """,
            username,
            full_name,
            followers,
            'instagram',
            0.5,
            [keyword],
            bio[:500] if bio else '{}',
            'discovered'
        )
        
        logger.info("Instagram prospect created", username=username, followers=followers)
        return True
    
    async def discover_tiktok_creators(self, limit: int = 50) -> dict:
        results = {"profiles_found": 0, "prospects_created": 0, "errors": 0}
        
        try:
            keywords = await self.db.fetch(
                "SELECT keyword FROM competitor_keywords WHERE platform = 'tiktok' AND is_active = TRUE LIMIT 5"
            )
            
            if not keywords:
                # Use default keywords if none configured
                keywords = [
                    {"keyword": "ai video editing"},
                    {"keyword": "ai tools review"},
                    {"keyword": "content creation tips"}
                ]
            
            for kw in keywords:
                try:
                    run_input = {
                        "searchQueries": [kw['keyword']],
                        "resultsPerPage": limit,
                        "shouldDownloadVideos": False,
                        "shouldDownloadCovers": False
                    }
                    
                    logger.info("Running TikTok search", keyword=kw['keyword'])
                    
                    run = await asyncio.to_thread(
                        lambda ri=run_input: self.client.actor(self.TIKTOK_SCRAPER_ACTOR).call(run_input=ri)
                    )
                    
                    items = await asyncio.to_thread(
                        lambda r=run: list(self.client.dataset(r["defaultDatasetId"]).iterate_items())
                    )
                    
                    logger.info("TikTok search results", keyword=kw['keyword'], count=len(items))
                    
                    for item in items:
                        try:
                            created = await self._process_tiktok_profile(item, kw['keyword'])
                            if created:
                                results["prospects_created"] += 1
                            results["profiles_found"] += 1
                        except Exception as e:
                            logger.error("TikTok profile processing error", error=str(e))
                            results["errors"] += 1
                            
                except Exception as e:
                    logger.error("TikTok search failed", keyword=kw['keyword'], error=str(e))
                    results["errors"] += 1
                    
        except Exception as e:
            logger.error("TikTok discovery failed", error=str(e))
            results["errors"] += 1
        
        logger.info("TikTok discovery complete", **results)
        return results
    
    async def _process_tiktok_profile(self, item: dict, keyword: str) -> bool:
        # Handle different response formats
        author_meta = item.get('authorMeta', {})
        username = author_meta.get('name') or item.get('author') or item.get('uniqueId')
        
        if not username:
            return False
        
        existing = await self.db.fetchval(
            "SELECT id FROM marketing_prospects WHERE tiktok_handle = $1",
            username
        )
        if existing:
            return False
        
        followers = author_meta.get('fans') or author_meta.get('followers') or 0
        
        if followers < self.settings.min_tiktok_followers:
            return False
        
        full_name = author_meta.get('nickName') or author_meta.get('nickname') or ''
        bio = author_meta.get('signature') or ''
        
        await self.db.execute("""
            INSERT INTO marketing_prospects (
                tiktok_handle, full_name, tiktok_followers,
                primary_platform, relevance_score, competitor_mentions,
                raw_data, status, discovered_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        """,
            username,
            full_name,
            followers,
            'tiktok',
            0.5,
            [keyword],
            bio[:500] if bio else '{}',
            'discovered'
        )
        
        logger.info("TikTok prospect created", username=username, followers=followers)
        return True
