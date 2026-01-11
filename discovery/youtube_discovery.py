"""
YouTube Discovery Engine
"""

import asyncio
import re
from typing import Optional
from googleapiclient.discovery import build
import structlog

from app.config import get_settings

logger = structlog.get_logger()


class YouTubeDiscovery:
    EMAIL_PATTERN = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    
    def __init__(self, api_key: str, db):
        self.settings = get_settings()
        self.api_key = api_key
        self.db = db
        self.youtube = build('youtube', 'v3', developerKey=api_key)
    
    async def search_and_store(self, keyword: str, max_results: int = 50) -> dict:
        results = {
            "videos_searched": 0,
            "channels_found": 0,
            "prospects_created": 0,
            "duplicates_skipped": 0,
            "errors": 0
        }
        
        try:
            search_response = await asyncio.to_thread(
                lambda: self.youtube.search().list(
                    q=keyword,
                    part='snippet',
                    type='video',
                    maxResults=min(max_results, 50),
                    order='relevance',
                    relevantLanguage='en'
                ).execute()
            )
            
            videos = search_response.get('items', [])
            results["videos_searched"] = len(videos)
            
            channel_ids = list(set(v['snippet']['channelId'] for v in videos))
            results["channels_found"] = len(channel_ids)
            
            for channel_id in channel_ids:
                try:
                    status = await self._process_channel(channel_id, keyword)
                    if status == "new":
                        results["prospects_created"] += 1
                    elif status == "duplicate":
                        results["duplicates_skipped"] += 1
                except Exception as e:
                    logger.error("Channel processing failed", channel_id=channel_id, error=str(e))
                    results["errors"] += 1
                
                await asyncio.sleep(0.1)
                
        except Exception as e:
            logger.error("YouTube search failed", keyword=keyword, error=str(e))
            results["errors"] += 1
        
        return results
    
    async def _process_channel(self, channel_id: str, keyword: str) -> str:
        existing = await self.db.fetchval(
            "SELECT id FROM marketing_prospects WHERE youtube_channel_id = $1",
            channel_id
        )
        if existing:
            return "duplicate"
        
        channel_response = await asyncio.to_thread(
            lambda: self.youtube.channels().list(
                part='snippet,statistics',
                id=channel_id
            ).execute()
        )
        
        channels = channel_response.get('items', [])
        if not channels:
            return "skipped"
        
        channel = channels[0]
        snippet = channel.get('snippet', {})
        statistics = channel.get('statistics', {})
        
        subscriber_count = int(statistics.get('subscriberCount', 0))
        
        if not (self.settings.min_youtube_subscribers <= subscriber_count <= self.settings.max_youtube_subscribers):
            return "skipped"
        
        channel_title = snippet.get('title', '')
        description = snippet.get('description', '')
        custom_url = snippet.get('customUrl', '')
        
        email = self._extract_email(description)
        
        await self.db.execute("""
            INSERT INTO marketing_prospects (
                youtube_channel_id, youtube_handle, full_name,
                youtube_subscribers, youtube_total_views,
                email, primary_platform, relevance_score,
                competitor_mentions, raw_data, status, discovered_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
        """,
            channel_id,
            custom_url or channel_id,
            channel_title,
            subscriber_count,
            int(statistics.get('viewCount', 0)),
            email,
            'youtube',
            0.6,
            [keyword],
            '{}',
            'discovered'
        )
        
        logger.info("Prospect created", channel=channel_title, subscribers=subscriber_count, has_email=bool(email))
        return "new"
    
    def _extract_email(self, text: str) -> Optional[str]:
        if not text:
            return None
        
        excluded = ['example.com', 'email.com', 'domain.com']
        for match in self.EMAIL_PATTERN.findall(text):
            if not any(ex in match.lower() for ex in excluded):
                return match.lower()
        
        return None
