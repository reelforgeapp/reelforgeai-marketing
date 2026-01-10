"""
ReelForge Marketing Engine - YouTube API Discovery
Uses official YouTube Data API v3 to find creators reviewing competitor tools
"""

import asyncio
import re
from typing import Optional
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import structlog

from app.config import get_settings
from app.database import get_database

logger = structlog.get_logger()


class YouTubeDiscovery:
    """
    Discover YouTube creators reviewing AI video tools using the official API.
    
    This is FREE - YouTube Data API provides 10,000 quota units per day.
    - Search: 100 units per request
    - Channel details: 1-3 units per request
    - ~100 searches + 3,000 channel lookups per day
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.youtube = build(
            'youtube', 'v3',
            developerKey=self.settings.youtube_api_key
        )
        self.db = get_database()
        
        # Competitor keywords to search
        self.competitors = [
            'pictory', 'synthesia', 'invideo', 'heygen',
            'lumen5', 'd-id', 'runway', 'kapwing', 'opus clip'
        ]
    
    async def discover_creators(
        self,
        search_query: str,
        max_results: int = 50,
        published_after: Optional[datetime] = None
    ) -> list[dict]:
        """
        Search YouTube for videos matching query and extract channel info.
        
        Args:
            search_query: Search terms (e.g., "pictory review")
            max_results: Maximum videos to return (up to 50 per request)
            published_after: Only videos published after this date
        
        Returns:
            List of prospect dictionaries
        """
        logger.info(
            "Starting YouTube discovery",
            query=search_query,
            max_results=max_results
        )
        
        prospects = []
        seen_channels = set()
        
        try:
            # Build search request
            search_params = {
                'q': search_query,
                'part': 'snippet',
                'type': 'video',
                'maxResults': min(max_results, 50),
                'order': 'relevance',
                'relevanceLanguage': 'en'
            }
            
            if published_after:
                search_params['publishedAfter'] = published_after.isoformat() + 'Z'
            
            # Execute search (100 quota units)
            search_response = await asyncio.to_thread(
                self.youtube.search().list(**search_params).execute
            )
            
            videos = search_response.get('items', [])
            logger.info(f"Found {len(videos)} videos for query: {search_query}")
            
            # Process each video
            for video in videos:
                channel_id = video['snippet']['channelId']
                
                # Skip if we've already processed this channel
                if channel_id in seen_channels:
                    continue
                seen_channels.add(channel_id)
                
                # Get channel details
                channel_info = await self._get_channel_details(channel_id)
                
                if channel_info:
                    # Check if meets criteria
                    subscribers = channel_info.get('subscriber_count', 0)
                    
                    if (self.settings.min_youtube_subscribers <= subscribers <= 
                        self.settings.max_youtube_subscribers):
                        
                        # Extract competitor mentions from video
                        video_title = video['snippet']['title']
                        video_description = video['snippet'].get('description', '')
                        competitor_mentions = self._extract_competitors(
                            f"{video_title} {video_description}"
                        )
                        
                        prospect = {
                            'youtube_channel_id': channel_id,
                            'youtube_handle': channel_info.get('custom_url'),
                            'youtube_url': f"https://youtube.com/channel/{channel_id}",
                            'full_name': channel_info.get('title'),
                            'youtube_subscribers': subscribers,
                            'youtube_total_videos': channel_info.get('video_count', 0),
                            'primary_platform': 'youtube',
                            'source': 'youtube_api',
                            'source_query': search_query,
                            'source_video_id': video['id']['videoId'],
                            'source_video_title': video_title,
                            'competitor_mentions': competitor_mentions,
                            'website_url': channel_info.get('website'),
                            'bio_link_url': self._extract_bio_link(
                                channel_info.get('description', '')
                            ),
                            'location': channel_info.get('country'),
                            'raw_data': {
                                'channel': channel_info,
                                'video': {
                                    'id': video['id']['videoId'],
                                    'title': video_title,
                                    'description': video_description[:500]
                                }
                            }
                        }
                        
                        # Calculate relevance score
                        prospect['relevance_score'] = self._calculate_relevance(
                            prospect, search_query
                        )
                        
                        prospects.append(prospect)
                        
                        logger.debug(
                            "Found prospect",
                            channel=prospect['full_name'],
                            subscribers=subscribers,
                            relevance=prospect['relevance_score']
                        )
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(0.1)
            
            logger.info(
                "YouTube discovery complete",
                query=search_query,
                prospects_found=len(prospects)
            )
            
            return prospects
            
        except HttpError as e:
            logger.error(
                "YouTube API error",
                error=str(e),
                query=search_query
            )
            raise
    
    async def _get_channel_details(self, channel_id: str) -> Optional[dict]:
        """
        Get detailed channel information.
        
        Costs 1-3 quota units depending on parts requested.
        """
        try:
            response = await asyncio.to_thread(
                self.youtube.channels().list(
                    part='snippet,statistics,brandingSettings',
                    id=channel_id
                ).execute
            )
            
            if not response.get('items'):
                return None
            
            channel = response['items'][0]
            snippet = channel.get('snippet', {})
            stats = channel.get('statistics', {})
            branding = channel.get('brandingSettings', {}).get('channel', {})
            
            # Extract custom URL (handle)
            custom_url = snippet.get('customUrl', '')
            if custom_url and not custom_url.startswith('@'):
                custom_url = f"@{custom_url}"
            
            return {
                'id': channel_id,
                'title': snippet.get('title'),
                'description': snippet.get('description', ''),
                'custom_url': custom_url,
                'country': snippet.get('country'),
                'subscriber_count': int(stats.get('subscriberCount', 0)),
                'view_count': int(stats.get('viewCount', 0)),
                'video_count': int(stats.get('videoCount', 0)),
                'website': branding.get('unsubscribedTrailer'),  # Sometimes has website
                'keywords': branding.get('keywords', ''),
            }
            
        except HttpError as e:
            logger.warning(
                "Failed to get channel details",
                channel_id=channel_id,
                error=str(e)
            )
            return None
    
    def _extract_competitors(self, text: str) -> list[str]:
        """Extract competitor mentions from text."""
        text_lower = text.lower()
        mentions = []
        
        for competitor in self.competitors:
            if competitor in text_lower:
                mentions.append(competitor)
        
        return mentions
    
    def _extract_bio_link(self, description: str) -> Optional[str]:
        """Extract bio link service URLs from description."""
        bio_link_patterns = [
            r'linktr\.ee/[\w-]+',
            r'beacons\.ai/[\w-]+',
            r'stan\.store/[\w-]+',
            r'bio\.link/[\w-]+',
            r'linkin\.bio/[\w-]+',
            r'solo\.to/[\w-]+',
        ]
        
        for pattern in bio_link_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                url = match.group(0)
                if not url.startswith('http'):
                    url = f"https://{url}"
                return url
        
        return None
    
    def _calculate_relevance(self, prospect: dict, search_query: str) -> float:
        """
        Calculate relevance score (0.0 - 1.0) based on multiple factors.
        """
        score = 0.0
        
        # Subscriber count (max 0.3)
        subscribers = prospect.get('youtube_subscribers', 0)
        if subscribers >= 100000:
            score += 0.30
        elif subscribers >= 50000:
            score += 0.25
        elif subscribers >= 20000:
            score += 0.20
        elif subscribers >= 10000:
            score += 0.15
        elif subscribers >= 5000:
            score += 0.10
        
        # Competitor mentions (max 0.3)
        mentions = len(prospect.get('competitor_mentions', []))
        score += min(mentions * 0.10, 0.30)
        
        # Has bio link (indicates they monetize) (0.15)
        if prospect.get('bio_link_url'):
            score += 0.15
        
        # Video title relevance (max 0.15)
        video_title = prospect.get('source_video_title', '').lower()
        relevance_keywords = ['review', 'tutorial', 'comparison', 'vs', 'best', 'top']
        for keyword in relevance_keywords:
            if keyword in video_title:
                score += 0.05
                if score >= 0.15:
                    break
        
        # Active channel (has recent videos) (0.10)
        video_count = prospect.get('youtube_total_videos', 0)
        if video_count >= 50:
            score += 0.10
        elif video_count >= 20:
            score += 0.05
        
        return min(score, 1.0)
    
    async def save_prospects(self, prospects: list[dict]) -> dict:
        """
        Save discovered prospects to database.
        
        Returns dict with counts: {new: int, updated: int, skipped: int}
        """
        results = {'new': 0, 'updated': 0, 'skipped': 0}
        
        for prospect in prospects:
            try:
                # Check if prospect already exists
                existing = await self.db.fetchrow(
                    """
                    SELECT id, youtube_channel_id 
                    FROM marketing_prospects 
                    WHERE youtube_channel_id = $1
                    """,
                    prospect['youtube_channel_id']
                )
                
                if existing:
                    # Update existing prospect
                    await self.db.execute(
                        """
                        UPDATE marketing_prospects SET
                            youtube_subscribers = $1,
                            youtube_total_videos = $2,
                            competitor_mentions = $3,
                            relevance_score = GREATEST(relevance_score, $4),
                            raw_data = raw_data || $5::jsonb,
                            updated_at = NOW()
                        WHERE youtube_channel_id = $6
                        """,
                        prospect['youtube_subscribers'],
                        prospect['youtube_total_videos'],
                        prospect.get('competitor_mentions', []),
                        prospect['relevance_score'],
                        prospect.get('raw_data', {}),
                        prospect['youtube_channel_id']
                    )
                    results['updated'] += 1
                else:
                    # Insert new prospect
                    await self.db.execute(
                        """
                        INSERT INTO marketing_prospects (
                            youtube_channel_id, youtube_handle, youtube_url,
                            full_name, youtube_subscribers, youtube_total_videos,
                            primary_platform, source, source_query,
                            source_video_id, source_video_title,
                            competitor_mentions, website_url, bio_link_url,
                            location, relevance_score, raw_data,
                            status, discovered_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15, $16, $17, 'discovered', NOW()
                        )
                        """,
                        prospect['youtube_channel_id'],
                        prospect.get('youtube_handle'),
                        prospect.get('youtube_url'),
                        prospect.get('full_name'),
                        prospect.get('youtube_subscribers', 0),
                        prospect.get('youtube_total_videos', 0),
                        prospect.get('primary_platform', 'youtube'),
                        prospect.get('source', 'youtube_api'),
                        prospect.get('source_query'),
                        prospect.get('source_video_id'),
                        prospect.get('source_video_title'),
                        prospect.get('competitor_mentions', []),
                        prospect.get('website_url'),
                        prospect.get('bio_link_url'),
                        prospect.get('location'),
                        prospect.get('relevance_score', 0.0),
                        prospect.get('raw_data', {})
                    )
                    results['new'] += 1
                    
            except Exception as e:
                logger.error(
                    "Failed to save prospect",
                    channel_id=prospect.get('youtube_channel_id'),
                    error=str(e)
                )
                results['skipped'] += 1
        
        logger.info(
            "Saved prospects to database",
            new=results['new'],
            updated=results['updated'],
            skipped=results['skipped']
        )
        
        return results
    
    async def run_discovery_for_all_keywords(self) -> dict:
        """
        Run discovery for all active keywords in the database.
        
        Returns summary of discovery run.
        """
        # Get active keywords
        keywords = await self.db.fetch(
            """
            SELECT id, competitor_name, keyword, platform
            FROM competitor_keywords
            WHERE is_active = TRUE AND platform = 'youtube'
            ORDER BY last_searched_at ASC NULLS FIRST
            LIMIT 10
            """
        )
        
        total_results = {'new': 0, 'updated': 0, 'skipped': 0}
        
        for kw in keywords:
            try:
                # Discover creators
                prospects = await self.discover_creators(
                    search_query=kw['keyword'],
                    max_results=25,
                    published_after=datetime.utcnow() - timedelta(days=90)
                )
                
                # Save to database
                results = await self.save_prospects(prospects)
                
                # Update keyword stats
                await self.db.execute(
                    """
                    UPDATE competitor_keywords SET
                        last_searched_at = NOW(),
                        total_prospects_found = total_prospects_found + $1
                    WHERE id = $2
                    """,
                    len(prospects),
                    kw['id']
                )
                
                # Aggregate results
                total_results['new'] += results['new']
                total_results['updated'] += results['updated']
                total_results['skipped'] += results['skipped']
                
                # Delay between searches
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(
                    "Discovery failed for keyword",
                    keyword=kw['keyword'],
                    error=str(e)
                )
        
        logger.info(
            "YouTube discovery run complete",
            keywords_processed=len(keywords),
            **total_results
        )
        
        return total_results
