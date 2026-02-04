"""
Hybrid Email Extractor - HTTP + Playwright fallback
"""

import asyncio
import re
from typing import Optional
import httpx
from bs4 import BeautifulSoup
import structlog

from app.config import get_settings
from app.database import get_database_async

logger = structlog.get_logger()


class HybridEmailExtractor:
    EMAIL_PATTERN = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    
    def __init__(self):
        self.settings = get_settings()
    
    async def extract_for_prospects(self, limit: int = 30, only_missing: bool = True) -> dict:
        db = None

        results = {
            "processed": 0,
            "emails_found": 0,
            "http_method": 0,
            "playwright_method": 0,
            "failed": 0
        }

        try:
            db = await get_database_async()

            query = """
                SELECT id, youtube_channel_id, youtube_handle, website_url, bio_link_url
                FROM marketing_prospects
                WHERE email IS NULL AND status = 'discovered'
                ORDER BY relevance_score DESC
                LIMIT $1
            """ if only_missing else """
                SELECT id, youtube_channel_id, youtube_handle, website_url, bio_link_url
                FROM marketing_prospects
                ORDER BY relevance_score DESC
                LIMIT $1
            """

            prospects = await db.fetch(query, limit)

            for prospect in prospects:
                results["processed"] += 1

                email = None
                method = None

                # Try YouTube About page first
                if prospect['youtube_channel_id']:
                    email, method = await self._extract_from_youtube(prospect['youtube_channel_id'])

                # Try website/bio link
                if not email and prospect.get('website_url'):
                    email, method = await self._extract_from_url(prospect['website_url'])

                if not email and prospect.get('bio_link_url'):
                    email, method = await self._extract_from_url(prospect['bio_link_url'])

                if email:
                    await db.execute(
                        "UPDATE marketing_prospects SET email = $1, email_source = 'extracted', status = 'enriched', last_enriched_at = NOW() WHERE id = $2",
                        email, prospect['id']
                    )
                    results["emails_found"] += 1
                    if method == "http":
                        results["http_method"] += 1
                    elif method == "playwright":
                        results["playwright_method"] += 1
                else:
                    results["failed"] += 1

                await asyncio.sleep(0.5)

        except Exception as e:
            logger.error("Email extraction failed", error=str(e))
            results["error"] = str(e)
        finally:
            if db:
                await db.close()

        return results
    
    async def _extract_from_youtube(self, channel_id: str) -> tuple[Optional[str], Optional[str]]:
        url = f"https://www.youtube.com/channel/{channel_id}/about"
        return await self._extract_from_url(url)
    
    async def _extract_from_url(self, url: str) -> tuple[Optional[str], Optional[str]]:
        # Try HTTP first
        try:
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                response = await client.get(url, headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                })
                
                if response.status_code == 200:
                    email = self._extract_email_from_html(response.text)
                    if email:
                        return email, "http"
        except Exception as e:
            logger.debug("HTTP extraction failed", url=url, error=str(e))
        
        # Fallback to Playwright
        try:
            email = await self._extract_with_playwright(url)
            if email:
                return email, "playwright"
        except Exception as e:
            logger.debug("Playwright extraction failed", url=url, error=str(e))
        
        return None, None
    
    async def _extract_with_playwright(self, url: str) -> Optional[str]:
        try:
            from playwright.async_api import async_playwright
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                try:
                    await page.goto(url, timeout=15000, wait_until='domcontentloaded')
                    await asyncio.sleep(2)
                    
                    content = await page.content()
                    return self._extract_email_from_html(content)
                finally:
                    await browser.close()
                    
        except Exception as e:
            logger.debug("Playwright failed", error=str(e))
            return None
    
    def _extract_email_from_html(self, html: str) -> Optional[str]:
        soup = BeautifulSoup(html, 'lxml')
        text = soup.get_text(separator=' ')
        
        excluded = ['example.com', 'email.com', 'domain.com', 'sentry.io', 'google.com', 'youtube.com']
        
        for match in self.EMAIL_PATTERN.findall(text):
            email = match.lower()
            if not any(ex in email for ex in excluded):
                if not email.startswith('noreply') and not email.startswith('no-reply'):
                    return email
        
        return None
