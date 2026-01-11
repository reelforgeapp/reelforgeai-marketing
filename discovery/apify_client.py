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
            
            for kw in keywords:
                try:
                    run_input = {
                        "search": kw['keyword'],
                        "resultsType": "posts",
                        "resultsLimit": limit,
                    }
                    
                    run = await asyncio.to_thread(
                        lambda: self.client.actor(self.settings.apify_instagram_actor).call(run_input=run_input)
                    )
                    
                    items = await asyncio.to_thread(
                        lambda: list(self.client.dataset(run["defaultDatasetId"]).iterate_items())
                    )
                    
                    for item in items:
                        try:
                            created = await self._process_instagram_profile(item, kw['keyword'])
                            if created:
                                results["prospects_created"] += 1
                            results["profiles_found"] += 1
                        except Exception as e:
                            results["errors"] += 1
                            
                except Exception as e:
                    logger.error("Instagram search failed", keyword=kw['keyword'], error=str(e))
                    results["errors"] += 1
                    
        except Exception as e:
            logger.error("Instagram discovery failed", error=str(e))
            results["errors"] += 1
        
        return results
    
    async def _process_instagram_profile(self, item: dict, keyword: str) -> bool:
        username = item.get('ownerUsername') or item.get('username')
        if not username:
            return False
        
        existing = await self.db.fetchval(
            "SELECT id FROM marketing_prospects WHERE instagram_handle = $1",
            username
        )
        if existing:
            return False
        
        followers = item.get('followersCount', 0)
        if followers < self.settings.min_instagram_followers:
            return False
        
        await self.db.execute("""
            INSERT INTO marketing_prospects (
                instagram_handle, full_name, instagram_followers,
                primary_platform, relevance_score, competitor_mentions,
                status, discovered_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        """,
            username,
            item.get('fullName', ''),
            followers,
            'instagram',
            0.5,
            [keyword],
            'discovered'
        )
        
        return True
    
    async def discover_tiktok_creators(self, limit: int = 50) -> dict:
        results = {"profiles_found": 0, "prospects_created": 0, "errors": 0}
        
        try:
            keywords = await self.db.fetch(
                "SELECT keyword FROM competitor_keywords WHERE platform = 'tiktok' AND is_active = TRUE LIMIT 5"
            )
            
            for kw in keywords:
                try:
                    run_input = {
                        "searchQueries": [kw['keyword']],
                        "resultsPerPage": limit,
                    }
                    
                    run = await asyncio.to_thread(
                        lambda: self.client.actor(self.settings.apify_tiktok_actor).call(run_input=run_input)
                    )
                    
                    items = await asyncio.to_thread(
                        lambda: list(self.client.dataset(run["defaultDatasetId"]).iterate_items())
                    )
                    
                    for item in items:
                        try:
                            created = await self._process_tiktok_profile(item, kw['keyword'])
                            if created:
                                results["prospects_created"] += 1
                            results["profiles_found"] += 1
                        except Exception as e:
                            results["errors"] += 1
                            
                except Exception as e:
                    logger.error("TikTok search failed", keyword=kw['keyword'], error=str(e))
                    results["errors"] += 1
                    
        except Exception as e:
            logger.error("TikTok discovery failed", error=str(e))
            results["errors"] += 1
        
        return results
    
    async def _process_tiktok_profile(self, item: dict, keyword: str) -> bool:
        username = item.get('authorMeta', {}).get('name') or item.get('author')
        if not username:
            return False
        
        existing = await self.db.fetchval(
            "SELECT id FROM marketing_prospects WHERE tiktok_handle = $1",
            username
        )
        if existing:
            return False
        
        followers = item.get('authorMeta', {}).get('fans', 0)
        if followers < self.settings.min_tiktok_followers:
            return False
        
        await self.db.execute("""
            INSERT INTO marketing_prospects (
                tiktok_handle, full_name, tiktok_followers,
                primary_platform, relevance_score, competitor_mentions,
                status, discovered_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        """,
            username,
            item.get('authorMeta', {}).get('nickName', ''),
            followers,
            'tiktok',
            0.5,
            [keyword],
            'discovered'
        )
        
        return True
