"""
ReelForge Marketing Engine - YouTube Email Extractor
Extracts business emails from YouTube channel About pages using Playwright
"""

import asyncio
import re
from typing import Optional
from playwright.async_api import async_playwright, Browser, Page
import structlog

from app.database import get_database

logger = structlog.get_logger()


class YouTubeEmailExtractor:
    """
    Extract business emails from YouTube channel About pages.
    
    YouTube creators often list their business email on their About page.
    This requires browser automation because:
    1. The email is hidden behind a "View email address" button
    2. The page content is dynamically loaded
    
    This is FREE and compliant - we're just visiting public pages.
    """
    
    EMAIL_PATTERN = re.compile(
        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    )
    
    # Domains to exclude (false positives)
    EXCLUDE_DOMAINS = [
        'youtube.com', 'google.com', 'gmail.com',
        'example.com', 'email.com', 'test.com',
        'sentry.io', 'segment.io', 'facebook.com',
        'instagram.com', 'twitter.com', 'tiktok.com'
    ]
    
    # Patterns to exclude (system emails)
    EXCLUDE_PATTERNS = [
        'noreply', 'no-reply', 'donotreply',
        'support@', 'help@', 'admin@',
        'notifications@', 'updates@', 'info@'
    ]
    
    def __init__(self):
        self.db = get_database()
        self.browser: Optional[Browser] = None
    
    async def start_browser(self):
        """Initialize Playwright browser."""
        if self.browser is None:
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox']
            )
            logger.info("Browser started for email extraction")
    
    async def stop_browser(self):
        """Close Playwright browser."""
        if self.browser:
            await self.browser.close()
            self.browser = None
            logger.info("Browser closed")
    
    async def extract_email(self, channel_id: str, channel_handle: str = None) -> dict:
        """
        Extract business email from a YouTube channel's About page.
        
        Args:
            channel_id: YouTube channel ID
            channel_handle: Optional channel handle (@username)
        
        Returns:
            dict with email and extraction details
        """
        result = {
            'channel_id': channel_id,
            'email': None,
            'email_source': None,
            'links': [],
            'error': None
        }
        
        await self.start_browser()
        
        context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1280, 'height': 800}
        )
        
        page = await context.new_page()
        
        try:
            # Build URL - prefer handle if available
            if channel_handle:
                if not channel_handle.startswith('@'):
                    channel_handle = f"@{channel_handle}"
                url = f"https://www.youtube.com/{channel_handle}/about"
            else:
                url = f"https://www.youtube.com/channel/{channel_id}/about"
            
            logger.debug(f"Fetching YouTube About page: {url}")
            
            # Navigate to About page
            await page.goto(url, wait_until='networkidle', timeout=30000)
            await page.wait_for_timeout(2000)  # Let dynamic content load
            
            # Method 1: Click "View email address" button
            email = await self._click_view_email_button(page)
            if email:
                result['email'] = email
                result['email_source'] = 'youtube_about_button'
                logger.info(f"Found email via button: {email}")
            
            # Method 2: Scan page content for emails
            if not result['email']:
                email = await self._scan_page_for_email(page)
                if email:
                    result['email'] = email
                    result['email_source'] = 'youtube_about_scan'
                    logger.info(f"Found email via scan: {email}")
            
            # Method 3: Extract external links (for later website scraping)
            result['links'] = await self._extract_external_links(page)
            
        except Exception as e:
            logger.warning(
                "Email extraction failed",
                channel_id=channel_id,
                error=str(e)
            )
            result['error'] = str(e)
        
        finally:
            await context.close()
        
        return result
    
    async def _click_view_email_button(self, page: Page) -> Optional[str]:
        """
        Click the "View email address" button to reveal hidden email.
        
        YouTube hides business emails behind this button to prevent scraping,
        but clicking it reveals the email.
        """
        try:
            # YouTube uses various selectors for this button
            selectors = [
                'button:has-text("View email address")',
                'yt-button-renderer:has-text("View email")',
                '[aria-label*="View email"]',
                'button:has-text("view email")',
                'tp-yt-paper-button:has-text("View email")',
            ]
            
            for selector in selectors:
                try:
                    button = page.locator(selector).first
                    
                    # Check if button is visible
                    if await button.is_visible(timeout=2000):
                        # Click the button
                        await button.click()
                        
                        # Wait for email to appear
                        await page.wait_for_timeout(1500)
                        
                        # Now scan for the revealed email
                        content = await page.content()
                        emails = self.EMAIL_PATTERN.findall(content)
                        
                        # Filter valid emails
                        valid = self._filter_valid_emails(emails)
                        if valid:
                            return valid[0]
                        
                except Exception:
                    continue
                    
        except Exception as e:
            logger.debug(f"View email button not found: {e}")
        
        return None
    
    async def _scan_page_for_email(self, page: Page) -> Optional[str]:
        """Scan the entire page content for email patterns."""
        try:
            content = await page.content()
            emails = self.EMAIL_PATTERN.findall(content)
            valid = self._filter_valid_emails(emails)
            
            return valid[0] if valid else None
            
        except Exception as e:
            logger.debug(f"Page scan failed: {e}")
            return None
    
    async def _extract_external_links(self, page: Page) -> list[str]:
        """Extract external links from the About page."""
        links = []
        
        try:
            # Find all links in the About section
            link_elements = await page.locator('a[href]').all()
            
            for element in link_elements:
                try:
                    href = await element.get_attribute('href')
                    
                    if href and self._is_useful_link(href):
                        # Clean up YouTube redirect URLs
                        if 'youtube.com/redirect' in href:
                            # Extract actual URL from redirect
                            import urllib.parse
                            parsed = urllib.parse.urlparse(href)
                            params = urllib.parse.parse_qs(parsed.query)
                            if 'q' in params:
                                href = params['q'][0]
                        
                        if href not in links:
                            links.append(href)
                            
                except Exception:
                    continue
                    
        except Exception as e:
            logger.debug(f"Link extraction failed: {e}")
        
        return links[:10]  # Limit to 10 links
    
    def _filter_valid_emails(self, emails: list[str]) -> list[str]:
        """Filter out invalid/system emails."""
        valid = []
        
        for email in emails:
            email_lower = email.lower()
            
            # Skip excluded domains
            if any(domain in email_lower for domain in self.EXCLUDE_DOMAINS):
                continue
            
            # Skip system emails
            if any(pattern in email_lower for pattern in self.EXCLUDE_PATTERNS):
                continue
            
            # Basic validation
            if self._is_valid_email(email):
                valid.append(email)
        
        # Prioritize business-looking emails
        priority_keywords = ['contact', 'hello', 'business', 'collab', 'partner', 'pr']
        valid.sort(
            key=lambda e: any(kw in e.lower() for kw in priority_keywords),
            reverse=True
        )
        
        return valid
    
    def _is_valid_email(self, email: str) -> bool:
        """Basic email format validation."""
        if not email or len(email) < 5 or len(email) > 254:
            return False
        
        if email.count('@') != 1:
            return False
        
        local, domain = email.rsplit('@', 1)
        
        if not local or not domain or '.' not in domain:
            return False
        
        return True
    
    def _is_useful_link(self, href: str) -> bool:
        """Check if a link is useful for further scraping."""
        if not href:
            return False
        
        # Skip YouTube internal links
        if 'youtube.com' in href and 'redirect' not in href:
            return False
        
        # Skip social media we handle separately
        skip_domains = [
            'facebook.com', 'twitter.com', 'x.com',
            'instagram.com', 'tiktok.com'
        ]
        if any(domain in href.lower() for domain in skip_domains):
            return False
        
        # Skip common non-useful links
        skip_patterns = [
            'javascript:', 'mailto:', '#',
            'about:blank', 'data:'
        ]
        if any(pattern in href.lower() for pattern in skip_patterns):
            return False
        
        # Accept bio link services
        bio_link_domains = [
            'linktr.ee', 'beacons.ai', 'stan.store',
            'bio.link', 'linkin.bio', 'solo.to'
        ]
        if any(domain in href.lower() for domain in bio_link_domains):
            return True
        
        # Accept http/https links
        if href.startswith('http://') or href.startswith('https://'):
            return True
        
        return False
    
    async def extract_emails_for_prospects(
        self,
        limit: int = 50,
        only_missing: bool = True
    ) -> dict:
        """
        Extract emails for prospects in the database.
        
        Args:
            limit: Maximum prospects to process
            only_missing: Only process prospects without email
        
        Returns:
            Summary of extraction run
        """
        # Get prospects needing email extraction
        query = """
            SELECT id, youtube_channel_id, youtube_handle, full_name, bio_link_url
            FROM marketing_prospects
            WHERE primary_platform = 'youtube'
              AND status = 'discovered'
        """
        
        if only_missing:
            query += " AND email IS NULL"
        
        query += f"""
            ORDER BY relevance_score DESC
            LIMIT {limit}
        """
        
        prospects = await self.db.fetch(query)
        
        logger.info(f"Processing {len(prospects)} prospects for email extraction")
        
        results = {
            'processed': 0,
            'emails_found': 0,
            'links_found': 0,
            'errors': 0
        }
        
        try:
            await self.start_browser()
            
            for prospect in prospects:
                try:
                    # Extract email
                    extraction = await self.extract_email(
                        channel_id=prospect['youtube_channel_id'],
                        channel_handle=prospect['youtube_handle']
                    )
                    
                    results['processed'] += 1
                    
                    # Update prospect if email found
                    if extraction['email']:
                        await self.db.execute(
                            """
                            UPDATE marketing_prospects SET
                                email = $1,
                                email_source = $2,
                                email_verified = FALSE,
                                status = 'enriched',
                                last_enriched_at = NOW()
                            WHERE id = $3
                            """,
                            extraction['email'],
                            extraction['email_source'],
                            prospect['id']
                        )
                        results['emails_found'] += 1
                        
                        logger.info(
                            "Found email for prospect",
                            name=prospect['full_name'],
                            email=extraction['email']
                        )
                    
                    # Update bio link if found and missing
                    if extraction['links'] and not prospect['bio_link_url']:
                        # Find bio link in extracted links
                        for link in extraction['links']:
                            if any(d in link for d in ['linktr.ee', 'beacons.ai', 'stan.store']):
                                await self.db.execute(
                                    """
                                    UPDATE marketing_prospects SET
                                        bio_link_url = $1
                                    WHERE id = $2
                                    """,
                                    link,
                                    prospect['id']
                                )
                                results['links_found'] += 1
                                break
                    
                    # Delay between requests
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(
                        "Email extraction failed for prospect",
                        prospect_id=str(prospect['id']),
                        error=str(e)
                    )
                    results['errors'] += 1
            
        finally:
            await self.stop_browser()
        
        logger.info(
            "Email extraction batch complete",
            **results
        )
        
        return results
