# discovery/youtube_discovery.py
"""
YouTube Discovery Engine
Uses the free YouTube Data API v3 to find creators reviewing AI video tools
Then extracts their email from channel About pages
"""

import asyncio
import re
from typing import Optional
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from playwright.async_api import async_playwright
import structlog
from config import settings

logger = structlog.get_logger()


class YouTubeDiscoveryEngine:
    """
    Discover potential affiliates by searching YouTube for:
    1. Reviews of competitor products (Pictory, Synthesia, etc.)
    2. AI video tool tutorials and comparisons
    3. Content creators in the AI/automation niche
    
    FREE: YouTube Data API has 10,000 units/day quota
    - Search: 100 units
    - Channel details: 1 unit
    - Video details: 1 unit
    
    This allows ~100 searches/day or ~500-1000 prospects/day
    """
    
    EMAIL_PATTERN = re.compile(
        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    )
    
    def __init__(self):
        self.youtube = build(
            'youtube', 'v3',
            developerKey=settings.youtube_api_key
        )
        self.competitors = settings.competitor_products
        self.search_queries = settings.search_keywords
    
    async def discover_creators(
        self,
        max_results_per_query: int = 25,
        min_subscribers: int = 1000,
        max_subscribers: int = 500000
    ) -> list[dict]:
        """
        Main discovery method - searches YouTube for potential affiliates
        
        Args:
            max_results_per_query: Results per search query (max 50)
            min_subscribers: Minimum subscriber count
            max_subscribers: Maximum subscriber count (avoid mega influencers)
        
        Returns:
            List of prospect dictionaries
        """
        all_prospects = []
        seen_channels = set()
        
        logger.info(
            "Starting YouTube discovery",
            queries=len(self.search_queries),
            max_per_query=max_results_per_query
        )
        
        for query in self.search_queries:
            try:
                prospects = await self._search_and_extract(
                    query,
                    max_results_per_query,
                    min_subscribers,
                    max_subscribers
                )
                
                # Deduplicate
                for prospect in prospects:
                    channel_id = prospect.get('youtube_channel_id')
                    if channel_id and channel_id not in seen_channels:
                        seen_channels.add(channel_id)
                        all_prospects.append(prospect)
                
                logger.info(
                    "Query completed",
                    query=query,
                    found=len(prospects),
                    total_unique=len(all_prospects)
                )
                
                # Rate limiting - be nice to the API
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error("Query failed", query=query, error=str(e))
                continue
        
        logger.info(
            "Discovery complete",
            total_prospects=len(all_prospects)
        )
        
        return all_prospects
    
    async def _search_and_extract(
        self,
        query: str,
        max_results: int,
        min_subs: int,
        max_subs: int
    ) -> list[dict]:
        """Search YouTube and extract channel info"""
        
        prospects = []
        
        # Search for videos
        search_response = self.youtube.search().list(
            q=query,
            part='snippet',
            type='video',
            maxResults=min(max_results, 50),
            order='relevance',
            publishedAfter=(datetime.utcnow() - timedelta(days=365)).isoformat() + 'Z',
            relevanceLanguage='en'
        ).execute()
        
        # Collect unique channel IDs
        channel_ids = list(set(
            item['snippet']['channelId']
            for item in search_response.get('items', [])
        ))
        
        if not channel_ids:
            return prospects
        
        # Get channel details in batch (more efficient)
        channels_response = self.youtube.channels().list(
            part='snippet,statistics,brandingSettings',
            id=','.join(channel_ids[:50])  # API limit
        ).execute()
        
        for channel in channels_response.get('items', []):
            try:
                stats = channel.get('statistics', {})
                snippet = channel.get('snippet', {})
                
                sub_count = int(stats.get('subscriberCount', 0))
                
                # Filter by subscriber count
                if not (min_subs <= sub_count <= max_subs):
                    continue
                
                # Check if hidden subscriber count
                if stats.get('hiddenSubscriberCount', False):
                    continue
                
                # Extract competitor mentions from channel description
                description = snippet.get('description', '').lower()
                title = snippet.get('title', '')
                mentions = self._extract_competitors(description + ' ' + title)
                
                prospect = {
                    'youtube_channel_id': channel['id'],
                    'youtube_handle': snippet.get('customUrl', ''),
                    'full_name': title,
                    'youtube_subscribers': sub_count,
                    'youtube_avg_views': self._estimate_avg_views(channel['id']),
                    'source': 'youtube_search',
                    'source_query': query,
                    'competitor_mentions': mentions if mentions else None,
                    'website_url': self._extract_website(channel),
                    'relevance_score': self._calculate_relevance(
                        sub_count, mentions, description
                    )
                }
                
                prospects.append(prospect)
                
            except Exception as e:
                logger.warning(
                    "Failed to process channel",
                    channel_id=channel.get('id'),
                    error=str(e)
                )
                continue
        
        return prospects
    
    def _extract_competitors(self, text: str) -> list[str]:
        """Find competitor product mentions in text"""
        text_lower = text.lower()
        return [
            comp for comp in self.competitors
            if comp.lower() in text_lower
        ]
    
    def _extract_website(self, channel: dict) -> Optional[str]:
        """Extract website from channel branding settings"""
        try:
            branding = channel.get('brandingSettings', {})
            channel_settings = branding.get('channel', {})
            
            # Check unsubscribed trailer description for links
            description = channel_settings.get('description', '')
            
            # Look for URL patterns
            url_pattern = re.compile(
                r'https?://(?:www\.)?([a-zA-Z0-9-]+\.[a-zA-Z]{2,})(?:/[^\s]*)?'
            )
            
            matches = url_pattern.findall(description)
            
            # Filter out social media
            social_domains = [
                'youtube', 'twitter', 'instagram', 'tiktok', 'facebook',
                'linkedin', 'discord', 'twitch', 'patreon', 'ko-fi'
            ]
            
            for match in matches:
                if not any(social in match.lower() for social in social_domains):
                    return f"https://{match}"
            
            return None
            
        except Exception:
            return None
    
    def _estimate_avg_views(self, channel_id: str) -> int:
        """Estimate average views from recent videos"""
        try:
            # Get recent videos
            videos_response = self.youtube.search().list(
                channelId=channel_id,
                part='id',
                order='date',
                maxResults=10,
                type='video'
            ).execute()
            
            video_ids = [
                item['id']['videoId']
                for item in videos_response.get('items', [])
                if item['id'].get('videoId')
            ]
            
            if not video_ids:
                return 0
            
            # Get video statistics
            stats_response = self.youtube.videos().list(
                part='statistics',
                id=','.join(video_ids)
            ).execute()
            
            total_views = sum(
                int(video['statistics'].get('viewCount', 0))
                for video in stats_response.get('items', [])
            )
            
            return total_views // len(video_ids) if video_ids else 0
            
        except Exception:
            return 0
    
    def _calculate_relevance(
        self,
        subscribers: int,
        competitor_mentions: list,
        description: str
    ) -> float:
        """Calculate relevance score 0.0 - 1.0"""
        score = 0.0
        
        # Subscriber score (max 0.3)
        if subscribers >= 100000:
            score += 0.3
        elif subscribers >= 50000:
            score += 0.25
        elif subscribers >= 10000:
            score += 0.2
        elif subscribers >= 5000:
            score += 0.15
        elif subscribers >= 1000:
            score += 0.1
        
        # Competitor mentions (max 0.4)
        if competitor_mentions:
            score += min(len(competitor_mentions) * 0.1, 0.4)
        
        # Keyword relevance (max 0.3)
        keywords = [
            'ai video', 'video generator', 'content creator',
            'tutorial', 'review', 'affiliate', 'make money',
            'side hustle', 'passive income', 'faceless'
        ]
        description_lower = description.lower()
        keyword_matches = sum(1 for kw in keywords if kw in description_lower)
        score += min(keyword_matches * 0.05, 0.3)
        
        return min(score, 1.0)


class YouTubeEmailExtractor:
    """
    Extract business emails from YouTube channel About pages
    Uses Playwright for dynamic content that requires JavaScript
    """
    
    EMAIL_PATTERN = re.compile(
        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    )
    
    async def extract_email(self, channel_handle: str) -> dict:
        """
        Navigate to YouTube channel About page and extract business email
        
        Args:
            channel_handle: YouTube handle (with or without @)
        
        Returns:
            Dict with email, method, and confidence
        """
        result = {
            'email': None,
            'method': None,
            'confidence': 0.0
        }
        
        # Normalize handle
        if not channel_handle:
            return result
        
        if not channel_handle.startswith('@'):
            channel_handle = f'@{channel_handle}'
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            try:
                context = await browser.new_context(
                    user_agent=(
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                        'AppleWebKit/537.36 (KHTML, like Gecko) '
                        'Chrome/120.0.0.0 Safari/537.36'
                    )
                )
                page = await context.new_page()
                
                # Navigate to About page
                url = f"https://www.youtube.com/{channel_handle}/about"
                await page.goto(url, wait_until='networkidle', timeout=15000)
                await page.wait_for_timeout(2000)
                
                # Method 1: Click "View email address" button
                email = await self._click_view_email(page)
                if email:
                    result['email'] = email
                    result['method'] = 'youtube_view_email_button'
                    result['confidence'] = 0.95
                    return result
                
                # Method 2: Scan page for email patterns
                email = await self._scan_page(page)
                if email:
                    result['email'] = email
                    result['method'] = 'youtube_page_scan'
                    result['confidence'] = 0.7
                    return result
                
                # Method 3: Check for mailto links
                email = await self._check_mailto(page)
                if email:
                    result['email'] = email
                    result['method'] = 'youtube_mailto_link'
                    result['confidence'] = 0.8
                    return result
                
            except Exception as e:
                logger.warning(
                    "Email extraction failed",
                    channel=channel_handle,
                    error=str(e)
                )
            finally:
                await browser.close()
        
        return result
    
    async def _click_view_email(self, page) -> Optional[str]:
        """Click YouTube's 'View email address' button"""
        try:
            selectors = [
                'button:has-text("View email address")',
                'yt-button-renderer:has-text("View email")',
                '[aria-label*="View email"]',
            ]
            
            for selector in selectors:
                try:
                    button = page.locator(selector).first
                    if await button.is_visible(timeout=2000):
                        await button.click()
                        await page.wait_for_timeout(1500)
                        
                        content = await page.content()
                        emails = self.EMAIL_PATTERN.findall(content)
                        valid = self._filter_emails(emails)
                        
                        if valid:
                            return valid[0]
                except:
                    continue
                    
        except Exception:
            pass
        
        return None
    
    async def _scan_page(self, page) -> Optional[str]:
        """Scan page content for email patterns"""
        try:
            content = await page.content()
            emails = self.EMAIL_PATTERN.findall(content)
            valid = self._filter_emails(emails)
            return valid[0] if valid else None
        except:
            return None
    
    async def _check_mailto(self, page) -> Optional[str]:
        """Check for mailto: links"""
        try:
            links = await page.locator('a[href^="mailto:"]').all()
            for link in links:
                href = await link.get_attribute('href')
                if href:
                    email = href.replace('mailto:', '').split('?')[0]
                    if self._is_valid_email(email):
                        return email
        except:
            pass
        return None
    
    def _filter_emails(self, emails: list) -> list:
        """Remove false positives"""
        exclude_domains = [
            'youtube.com', 'google.com', 'example.com',
            'sentry.io', 'segment.io', 'gmail.com'
        ]
        exclude_patterns = [
            'noreply', 'no-reply', 'support@', 'help@',
            'admin@', 'notifications@'
        ]
        
        valid = []
        for email in emails:
            email_lower = email.lower()
            
            if any(d in email_lower for d in exclude_domains):
                continue
            if any(p in email_lower for p in exclude_patterns):
                continue
            if self._is_valid_email(email):
                valid.append(email)
        
        return valid
    
    def _is_valid_email(self, email: str) -> bool:
        """Basic email validation"""
        if not email or '@' not in email:
            return False
        
        local, domain = email.rsplit('@', 1)
        if not local or not domain or '.' not in domain:
            return False
        
        return True
