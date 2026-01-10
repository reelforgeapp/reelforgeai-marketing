"""
ReelForge Marketing Engine - Hybrid Email Extractor
HTTP + BeautifulSoup first, Playwright fallback for JS-rendered content
"""

import asyncio
import re
import json
import urllib.parse
from typing import Optional
from bs4 import BeautifulSoup
import httpx
from playwright.async_api import async_playwright, Browser
import structlog

from app.config import get_settings
from app.database import get_database

logger = structlog.get_logger()


class HybridEmailExtractor:
    """
    Hybrid email extraction strategy:
    
    1. Try HTTP + BeautifulSoup first (fast, low resource)
       - Direct HTTP request to About page
       - Parse HTML for email patterns
       - Check JSON-LD structured data
       
    2. Fallback to Playwright (slow, resource-intensive)
       - Only if HTTP method fails
       - Launch headless browser
       - Click "View email address" button
       - Extract revealed email
    
    This reduces Playwright usage by ~60-70%, saving memory and time.
    """
    
    EMAIL_PATTERN = re.compile(
        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    )
    
    # Domains to exclude (false positives)
    EXCLUDE_DOMAINS = {
        'youtube.com', 'google.com', 'gmail.com', 'gstatic.com',
        'example.com', 'email.com', 'test.com', 'youtu.be',
        'facebook.com', 'instagram.com', 'twitter.com', 'tiktok.com'
    }
    
    # System email patterns to exclude
    EXCLUDE_PATTERNS = [
        'noreply', 'no-reply', 'donotreply',
        'support@', 'help@', 'admin@', 'info@',
        'notifications@', 'updates@', 'team@'
    ]
    
    def __init__(self):
        self.settings = get_settings()
        self.db = get_database()
        self.browser: Optional[Browser] = None
    
    async def extract_email(
        self,
        channel_id: str,
        channel_handle: str = None
    ) -> dict:
        """
        Extract email from YouTube channel using hybrid approach.
        
        Args:
            channel_id: YouTube channel ID
            channel_handle: Optional channel handle (@username)
        
        Returns:
            dict with email and extraction method
        """
        result = {
            'channel_id': channel_id,
            'email': None,
            'email_source': None,
            'method': None,
            'links': [],
            'error': None
        }
        
        # Build URL
        if channel_handle:
            handle = channel_handle if channel_handle.startswith('@') else f"@{channel_handle}"
            url = f"https://www.youtube.com/{handle}/about"
        else:
            url = f"https://www.youtube.com/channel/{channel_id}/about"
        
        logger.debug(f"Extracting email from: {url}")
        
        # Method 1: HTTP + BeautifulSoup (fast path)
        email, links = await self._try_http_extraction(url)
        
        if email:
            result['email'] = email
            result['email_source'] = 'youtube_about_http'
            result['method'] = 'http_bs4'
            result['links'] = links
            logger.info(f"Email found via HTTP: {email}")
            return result
        
        # Store links even if no email found
        result['links'] = links
        
        # Method 2: Playwright fallback (slow path)
        logger.debug("HTTP extraction failed, trying Playwright")
        
        try:
            email = await self._try_playwright_extraction(url)
            
            if email:
                result['email'] = email
                result['email_source'] = 'youtube_about_playwright'
                result['method'] = 'playwright'
                logger.info(f"Email found via Playwright: {email}")
        
        except Exception as e:
            logger.warning(f"Playwright extraction failed: {e}")
            result['error'] = str(e)
        
        return result
    
    async def _try_http_extraction(self, url: str) -> tuple[Optional[str], list]:
        """
        Try to extract email using HTTP + BeautifulSoup.
        
        This works when:
        - Email is visible in page HTML (not hidden behind button)
        - Email is in JSON-LD structured data
        - Email is in meta tags
        """
        emails_found = []
        links_found = []
        
        try:
            async with httpx.AsyncClient(
                timeout=15.0,
                follow_redirects=True,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                                  'Chrome/120.0.0.0 Safari/537.36',
                    'Accept-Language': 'en-US,en;q=0.9',
                }
            ) as client:
                response = await client.get(url)
                
                if response.status_code != 200:
                    logger.debug(f"HTTP request failed: {response.status_code}")
                    return None, []
                
                html = response.text
                soup = BeautifulSoup(html, 'lxml')
                
                # Method 1: Search entire page text for emails
                page_text = soup.get_text()
                potential_emails = self.EMAIL_PATTERN.findall(page_text)
                emails_found.extend(potential_emails)
                
                # Method 2: Check JSON-LD structured data
                for script in soup.find_all('script', type='application/ld+json'):
                    try:
                        data = json.loads(script.string)
                        if isinstance(data, dict):
                            # Look for email in structured data
                            email = data.get('email')
                            if email:
                                emails_found.append(email)
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                # Method 3: Check meta tags
                for meta in soup.find_all('meta', property=True):
                    if 'email' in meta.get('property', '').lower():
                        content = meta.get('content')
                        if content and '@' in content:
                            emails_found.append(content)
                
                # Extract useful links for later scraping
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if self._is_useful_link(href):
                        if 'redirect' in href:
                            # YouTube redirect link - extract actual URL
                            parsed = urllib.parse.urlparse(href)
                            params = urllib.parse.parse_qs(parsed.query)
                            if 'q' in params:
                                href = params['q'][0]
                        if href not in links_found:
                            links_found.append(href)
                
                # Filter and return best email
                valid_emails = self._filter_emails(emails_found)
                
                return (valid_emails[0] if valid_emails else None), links_found[:10]
                
        except httpx.TimeoutException:
            logger.debug("HTTP request timeout")
            return None, []
            
        except Exception as e:
            logger.debug(f"HTTP extraction error: {e}")
            return None, []
    
    async def _try_playwright_extraction(self, url: str) -> Optional[str]:
        """
        Fallback extraction using Playwright for JS-rendered content.
        
        Used when:
        - Email is hidden behind "View email address" button
        - Page requires JavaScript to render
        """
        email = None
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-gpu',
                    '--disable-dev-shm-usage',
                    '--disable-setuid-sandbox',
                    '--no-sandbox',
                    '--disable-extensions',
                ]
            )
            
            try:
                context = await browser.new_context(
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                               'AppleWebKit/537.36 (KHTML, like Gecko) '
                               'Chrome/120.0.0.0 Safari/537.36',
                    viewport={'width': 1280, 'height': 720},
                    locale='en-US'
                )
                
                page = await context.new_page()
                
                # Navigate to page
                await page.goto(url, wait_until='networkidle', timeout=30000)
                
                # Wait for content to load
                await asyncio.sleep(2)
                
                # Try to find and click "View email address" button
                email_button_selectors = [
                    'button:has-text("View email address")',
                    'yt-button-renderer:has-text("View email")',
                    '[aria-label*="View email"]',
                    'button:has-text("view email")',
                    'tp-yt-paper-button:has-text("email")',
                ]
                
                for selector in email_button_selectors:
                    try:
                        button = page.locator(selector).first
                        if await button.is_visible(timeout=2000):
                            await button.click()
                            logger.debug(f"Clicked email button: {selector}")
                            await asyncio.sleep(1.5)
                            break
                    except Exception:
                        continue
                
                # Extract email from page content
                page_text = await page.content()
                
                # Look for revealed email
                emails = self.EMAIL_PATTERN.findall(page_text)
                valid_emails = self._filter_emails(emails)
                
                if valid_emails:
                    email = valid_emails[0]
                
                await context.close()
                
            finally:
                await browser.close()
        
        return email
    
    def _filter_emails(self, emails: list) -> list:
        """Filter out invalid and unwanted emails."""
        valid = []
        seen = set()
        
        for email in emails:
            email = email.lower().strip()
            
            # Skip duplicates
            if email in seen:
                continue
            seen.add(email)
            
            # Skip excluded domains
            domain = email.split('@')[-1] if '@' in email else ''
            if domain in self.EXCLUDE_DOMAINS:
                continue
            
            # Skip system emails
            if any(pattern in email for pattern in self.EXCLUDE_PATTERNS):
                continue
            
            # Basic validation
            if len(email) > 5 and '.' in email.split('@')[-1]:
                valid.append(email)
        
        return valid
    
    def _is_useful_link(self, href: str) -> bool:
        """Check if link might contain contact info."""
        useful_domains = [
            'linktr.ee', 'beacons.ai', 'stan.store', 'bio.link',
            'linkin.bio', 'solo.to', 'lnk.bio', 'tap.bio',
            'campsite.bio', 'allmylinks.com', 'carrd.co'
        ]
        
        return any(domain in href.lower() for domain in useful_domains)
    
    async def extract_for_prospects(
        self,
        limit: int = 30,
        only_missing: bool = True
    ) -> dict:
        """
        Extract emails for multiple prospects.
        
        Args:
            limit: Maximum prospects to process
            only_missing: Only process prospects without email
        
        Returns:
            Summary of extraction results
        """
        results = {
            'processed': 0,
            'emails_found': 0,
            'http_method': 0,
            'playwright_method': 0,
            'failed': 0
        }
        
        # Get prospects needing email extraction
        query = """
            SELECT id, youtube_channel_id, youtube_handle
            FROM marketing_prospects
            WHERE youtube_channel_id IS NOT NULL
              AND status IN ('discovered')
        """
        
        if only_missing:
            query += " AND email IS NULL"
        
        query += f"""
            ORDER BY relevance_score DESC
            LIMIT {limit}
        """
        
        prospects = await self.db.fetch(query)
        
        logger.info(f"Extracting emails for {len(prospects)} prospects")
        
        for prospect in prospects:
            results['processed'] += 1
            
            try:
                extraction = await self.extract_email(
                    channel_id=prospect['youtube_channel_id'],
                    channel_handle=prospect['youtube_handle']
                )
                
                if extraction['email']:
                    results['emails_found'] += 1
                    
                    if extraction['method'] == 'http_bs4':
                        results['http_method'] += 1
                    else:
                        results['playwright_method'] += 1
                    
                    # Update prospect
                    await self.db.execute(
                        """
                        UPDATE marketing_prospects SET
                            email = $1,
                            email_source = $2,
                            bio_link_url = $3,
                            status = 'enriched',
                            updated_at = NOW()
                        WHERE id = $4
                        """,
                        extraction['email'],
                        extraction['email_source'],
                        extraction['links'][0] if extraction['links'] else None,
                        prospect['id']
                    )
                else:
                    results['failed'] += 1
                
                # Rate limiting
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Extraction failed for {prospect['id']}: {e}")
                results['failed'] += 1
        
        logger.info("Email extraction batch complete", **results)
        return results
