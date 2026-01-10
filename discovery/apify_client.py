"""
ReelForge Marketing Engine - Apify Client
Integration with Apify for Instagram and TikTok scraping
"""

import asyncio
import re
import httpx
from typing import Optional
from datetime import datetime
import structlog

from app.config import get_settings
from app.database import get_database

logger = structlog.get_logger()


class ApifyClient:
    """
    Client for Apify API to run Instagram and TikTok scrapers.
    
    Apify handles:
    - Anti-bot bypass
    - Proxy rotation
    - Rate limiting
    - Data parsing
    
    Cost: ~$5-10/month for typical usage within $49 Starter plan
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.api_token = self.settings.apify_api_token
        self.base_url = "https://api.apify.com/v2"
        self.db = get_database()
    
    async def run_actor(
        self,
        actor_id: str,
        run_input: dict,
        wait_for_finish: bool = True,
        timeout_secs: int = 300
    ) -> dict:
        """
        Run an Apify actor and optionally wait for results.
        
        Args:
            actor_id: Actor identifier (e.g., "apify/instagram-profile-scraper")
            run_input: Input parameters for the actor
            wait_for_finish: Whether to wait for completion
            timeout_secs: Maximum wait time
        
        Returns:
            Actor run result with dataset items
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Start actor run
            logger.info(f"Starting Apify actor: {actor_id}")
            
            response = await client.post(
                f"{self.base_url}/acts/{actor_id}/runs",
                params={"token": self.api_token},
                json=run_input
            )
            response.raise_for_status()
            
            run_data = response.json()["data"]
            run_id = run_data["id"]
            dataset_id = run_data["defaultDatasetId"]
            
            logger.info(
                "Actor run started",
                actor=actor_id,
                run_id=run_id,
                dataset_id=dataset_id
            )
            
            if not wait_for_finish:
                return {
                    "run_id": run_id,
                    "dataset_id": dataset_id,
                    "status": "RUNNING"
                }
            
            # Poll for completion
            start_time = datetime.utcnow()
            while True:
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                if elapsed > timeout_secs:
                    logger.warning(f"Actor run timed out after {timeout_secs}s")
                    return {
                        "run_id": run_id,
                        "dataset_id": dataset_id,
                        "status": "TIMEOUT"
                    }
                
                # Check run status
                status_response = await client.get(
                    f"{self.base_url}/actor-runs/{run_id}",
                    params={"token": self.api_token}
                )
                status_data = status_response.json()["data"]
                status = status_data["status"]
                
                if status == "SUCCEEDED":
                    break
                elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
                    logger.error(f"Actor run failed: {status}")
                    return {
                        "run_id": run_id,
                        "dataset_id": dataset_id,
                        "status": status,
                        "error": status_data.get("exitCode")
                    }
                
                await asyncio.sleep(5)
            
            # Get results from dataset
            items_response = await client.get(
                f"{self.base_url}/datasets/{dataset_id}/items",
                params={
                    "token": self.api_token,
                    "format": "json"
                }
            )
            items = items_response.json()
            
            logger.info(
                "Actor run completed",
                actor=actor_id,
                items_count=len(items)
            )
            
            return {
                "run_id": run_id,
                "dataset_id": dataset_id,
                "status": "SUCCEEDED",
                "items": items
            }
    
    async def get_run_status(self, run_id: str) -> dict:
        """Get status of an actor run."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/actor-runs/{run_id}",
                params={"token": self.api_token}
            )
            return response.json()["data"]
    
    async def get_dataset_items(self, dataset_id: str) -> list:
        """Get items from a dataset."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/datasets/{dataset_id}/items",
                params={
                    "token": self.api_token,
                    "format": "json"
                }
            )
            return response.json()


class InstagramDiscovery:
    """
    Discover Instagram creators using Apify.
    
    Uses: apify/instagram-profile-scraper
    Cost: ~$2-3/month for typical usage
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.client = ApifyClient()
        self.db = get_database()
        self.actor_id = self.settings.apify_instagram_actor
    
    async def discover_by_hashtags(
        self,
        hashtags: list[str],
        results_per_hashtag: int = 30
    ) -> list[dict]:
        """
        Discover Instagram creators by hashtag search.
        
        Args:
            hashtags: List of hashtags to search (without #)
            results_per_hashtag: Max results per hashtag
        
        Returns:
            List of prospect dictionaries
        """
        logger.info(
            "Starting Instagram hashtag discovery",
            hashtags=hashtags,
            results_per_hashtag=results_per_hashtag
        )
        
        # Run Instagram scraper
        result = await self.client.run_actor(
            actor_id=self.actor_id,
            run_input={
                "hashtags": hashtags,
                "resultsLimit": results_per_hashtag * len(hashtags),
                "searchType": "hashtag"
            }
        )
        
        if result["status"] != "SUCCEEDED":
            logger.error("Instagram discovery failed", status=result["status"])
            return []
        
        # Process results into prospects
        prospects = []
        seen_users = set()
        
        for item in result.get("items", []):
            try:
                username = item.get("ownerUsername") or item.get("username")
                
                if not username or username in seen_users:
                    continue
                
                seen_users.add(username)
                
                # Extract follower count
                followers = item.get("followersCount") or item.get("ownerFollowersCount") or 0
                
                # Filter by minimum followers
                if followers < self.settings.min_instagram_followers:
                    continue
                
                # Extract email from bio if present
                bio = item.get("biography") or item.get("ownerBiography") or ""
                email = self._extract_email_from_bio(bio)
                
                # Extract website/bio link
                website = item.get("externalUrl") or item.get("ownerExternalUrl")
                
                prospect = {
                    "instagram_handle": username,
                    "instagram_url": f"https://instagram.com/{username}",
                    "instagram_followers": followers,
                    "full_name": item.get("fullName") or item.get("ownerFullName") or username,
                    "email": email,
                    "email_source": "instagram_bio" if email else None,
                    "website_url": website,
                    "bio_link_url": website if self._is_bio_link(website) else None,
                    "primary_platform": "instagram",
                    "source": "apify_instagram",
                    "source_query": ",".join(hashtags),
                    "raw_data": {
                        "biography": bio[:500] if bio else None,
                        "posts_count": item.get("postsCount"),
                        "is_business": item.get("isBusinessAccount"),
                        "business_email": item.get("businessEmail"),
                        "business_category": item.get("businessCategory")
                    }
                }
                
                # Use business email if available
                if item.get("businessEmail"):
                    prospect["email"] = item["businessEmail"]
                    prospect["email_source"] = "instagram_business"
                
                # Calculate relevance score
                prospect["relevance_score"] = self._calculate_relevance(prospect)
                
                prospects.append(prospect)
                
            except Exception as e:
                logger.warning(f"Failed to process Instagram item: {e}")
                continue
        
        logger.info(
            "Instagram discovery complete",
            hashtags=hashtags,
            prospects_found=len(prospects)
        )
        
        return prospects
    
    def _extract_email_from_bio(self, bio: str) -> Optional[str]:
        """Extract email address from Instagram bio."""
        if not bio:
            return None
        
        email_pattern = re.compile(
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        )
        
        matches = email_pattern.findall(bio)
        
        # Filter out common false positives
        for email in matches:
            email_lower = email.lower()
            if not any(x in email_lower for x in ['example.com', 'email.com', 'test.com']):
                return email
        
        return None
    
    def _is_bio_link(self, url: str) -> bool:
        """Check if URL is a bio link service."""
        if not url:
            return False
        
        bio_link_domains = [
            'linktr.ee', 'beacons.ai', 'stan.store',
            'bio.link', 'linkin.bio', 'solo.to',
            'lnk.bio', 'tap.bio', 'campsite.bio'
        ]
        
        return any(domain in url.lower() for domain in bio_link_domains)
    
    def _calculate_relevance(self, prospect: dict) -> float:
        """Calculate relevance score for Instagram prospect."""
        score = 0.0
        
        # Follower count (max 0.35)
        followers = prospect.get("instagram_followers", 0)
        if followers >= 100000:
            score += 0.35
        elif followers >= 50000:
            score += 0.30
        elif followers >= 25000:
            score += 0.25
        elif followers >= 10000:
            score += 0.20
        elif followers >= 5000:
            score += 0.15
        
        # Has email (0.25)
        if prospect.get("email"):
            score += 0.25
        
        # Has website/bio link (0.15)
        if prospect.get("website_url") or prospect.get("bio_link_url"):
            score += 0.15
        
        # Is business account (0.15)
        raw_data = prospect.get("raw_data", {})
        if raw_data.get("is_business"):
            score += 0.15
        
        # Has business email (0.10 extra)
        if raw_data.get("business_email"):
            score += 0.10
        
        return min(score, 1.0)
    
    async def save_prospects(self, prospects: list[dict]) -> dict:
        """Save Instagram prospects to database."""
        results = {"new": 0, "updated": 0, "skipped": 0}
        
        for prospect in prospects:
            try:
                # Check if exists
                existing = await self.db.fetchrow(
                    """
                    SELECT id FROM marketing_prospects 
                    WHERE instagram_handle = $1
                    """,
                    prospect["instagram_handle"]
                )
                
                if existing:
                    # Update
                    await self.db.execute(
                        """
                        UPDATE marketing_prospects SET
                            instagram_followers = $1,
                            email = COALESCE(email, $2),
                            email_source = COALESCE(email_source, $3),
                            website_url = COALESCE(website_url, $4),
                            relevance_score = GREATEST(relevance_score, $5),
                            updated_at = NOW()
                        WHERE instagram_handle = $6
                        """,
                        prospect["instagram_followers"],
                        prospect.get("email"),
                        prospect.get("email_source"),
                        prospect.get("website_url"),
                        prospect["relevance_score"],
                        prospect["instagram_handle"]
                    )
                    results["updated"] += 1
                else:
                    # Insert
                    await self.db.execute(
                        """
                        INSERT INTO marketing_prospects (
                            instagram_handle, instagram_url, instagram_followers,
                            full_name, email, email_source, website_url, bio_link_url,
                            primary_platform, source, source_query,
                            relevance_score, raw_data, status, discovered_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 
                            'discovered', NOW()
                        )
                        """,
                        prospect["instagram_handle"],
                        prospect.get("instagram_url"),
                        prospect.get("instagram_followers", 0),
                        prospect.get("full_name"),
                        prospect.get("email"),
                        prospect.get("email_source"),
                        prospect.get("website_url"),
                        prospect.get("bio_link_url"),
                        prospect.get("primary_platform", "instagram"),
                        prospect.get("source", "apify_instagram"),
                        prospect.get("source_query"),
                        prospect.get("relevance_score", 0.0),
                        prospect.get("raw_data", {})
                    )
                    results["new"] += 1
                    
            except Exception as e:
                logger.error(f"Failed to save Instagram prospect: {e}")
                results["skipped"] += 1
        
        logger.info("Saved Instagram prospects", **results)
        return results


class TikTokDiscovery:
    """
    Discover TikTok creators using Apify.
    
    Uses: clockworks/tiktok-scraper
    Cost: ~$3-5/month for typical usage
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.client = ApifyClient()
        self.db = get_database()
        self.actor_id = self.settings.apify_tiktok_actor
    
    async def discover_by_hashtags(
        self,
        hashtags: list[str],
        results_per_hashtag: int = 30
    ) -> list[dict]:
        """
        Discover TikTok creators by hashtag search.
        
        Args:
            hashtags: List of hashtags to search (without #)
            results_per_hashtag: Max results per hashtag
        
        Returns:
            List of prospect dictionaries
        """
        logger.info(
            "Starting TikTok hashtag discovery",
            hashtags=hashtags,
            results_per_hashtag=results_per_hashtag
        )
        
        # Run TikTok scraper
        result = await self.client.run_actor(
            actor_id=self.actor_id,
            run_input={
                "hashtags": hashtags,
                "resultsPerPage": results_per_hashtag,
                "shouldDownloadVideos": False,
                "shouldDownloadCovers": False
            }
        )
        
        if result["status"] != "SUCCEEDED":
            logger.error("TikTok discovery failed", status=result["status"])
            return []
        
        # Process results into prospects
        prospects = []
        seen_users = set()
        
        for item in result.get("items", []):
            try:
                author = item.get("authorMeta", {})
                username = author.get("name") or author.get("uniqueId")
                
                if not username or username in seen_users:
                    continue
                
                seen_users.add(username)
                
                # Extract follower count
                followers = author.get("fans") or author.get("followers") or 0
                
                # Filter by minimum followers
                if followers < self.settings.min_tiktok_followers:
                    continue
                
                # Extract email from signature/bio
                signature = author.get("signature", "")
                email = self._extract_email_from_bio(signature)
                
                prospect = {
                    "tiktok_handle": username,
                    "tiktok_url": f"https://tiktok.com/@{username}",
                    "tiktok_followers": followers,
                    "full_name": author.get("nickname") or username,
                    "email": email,
                    "email_source": "tiktok_bio" if email else None,
                    "primary_platform": "tiktok",
                    "source": "apify_tiktok",
                    "source_query": ",".join(hashtags),
                    "raw_data": {
                        "signature": signature[:500] if signature else None,
                        "following": author.get("following"),
                        "hearts": author.get("heart"),
                        "videos": author.get("video"),
                        "verified": author.get("verified")
                    }
                }
                
                # Calculate relevance score
                prospect["relevance_score"] = self._calculate_relevance(prospect)
                
                prospects.append(prospect)
                
            except Exception as e:
                logger.warning(f"Failed to process TikTok item: {e}")
                continue
        
        logger.info(
            "TikTok discovery complete",
            hashtags=hashtags,
            prospects_found=len(prospects)
        )
        
        return prospects
    
    def _extract_email_from_bio(self, bio: str) -> Optional[str]:
        """Extract email address from TikTok bio/signature."""
        if not bio:
            return None
        
        email_pattern = re.compile(
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        )
        
        matches = email_pattern.findall(bio)
        
        for email in matches:
            email_lower = email.lower()
            if not any(x in email_lower for x in ['example.com', 'email.com', 'test.com']):
                return email
        
        return None
    
    def _calculate_relevance(self, prospect: dict) -> float:
        """Calculate relevance score for TikTok prospect."""
        score = 0.0
        
        # Follower count (max 0.40)
        followers = prospect.get("tiktok_followers", 0)
        if followers >= 500000:
            score += 0.40
        elif followers >= 100000:
            score += 0.35
        elif followers >= 50000:
            score += 0.30
        elif followers >= 25000:
            score += 0.25
        elif followers >= 10000:
            score += 0.20
        
        # Has email (0.30)
        if prospect.get("email"):
            score += 0.30
        
        # Engagement (hearts/followers ratio) - from raw_data
        raw_data = prospect.get("raw_data", {})
        hearts = raw_data.get("hearts", 0)
        if followers > 0 and hearts > 0:
            engagement_ratio = hearts / followers
            if engagement_ratio >= 10:
                score += 0.15
            elif engagement_ratio >= 5:
                score += 0.10
            elif engagement_ratio >= 2:
                score += 0.05
        
        # Verified account (0.10)
        if raw_data.get("verified"):
            score += 0.10
        
        return min(score, 1.0)
    
    async def save_prospects(self, prospects: list[dict]) -> dict:
        """Save TikTok prospects to database."""
        results = {"new": 0, "updated": 0, "skipped": 0}
        
        for prospect in prospects:
            try:
                # Check if exists
                existing = await self.db.fetchrow(
                    """
                    SELECT id FROM marketing_prospects 
                    WHERE tiktok_handle = $1
                    """,
                    prospect["tiktok_handle"]
                )
                
                if existing:
                    # Update
                    await self.db.execute(
                        """
                        UPDATE marketing_prospects SET
                            tiktok_followers = $1,
                            email = COALESCE(email, $2),
                            email_source = COALESCE(email_source, $3),
                            relevance_score = GREATEST(relevance_score, $4),
                            updated_at = NOW()
                        WHERE tiktok_handle = $5
                        """,
                        prospect["tiktok_followers"],
                        prospect.get("email"),
                        prospect.get("email_source"),
                        prospect["relevance_score"],
                        prospect["tiktok_handle"]
                    )
                    results["updated"] += 1
                else:
                    # Insert
                    await self.db.execute(
                        """
                        INSERT INTO marketing_prospects (
                            tiktok_handle, tiktok_url, tiktok_followers,
                            full_name, email, email_source,
                            primary_platform, source, source_query,
                            relevance_score, raw_data, status, discovered_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                            'discovered', NOW()
                        )
                        """,
                        prospect["tiktok_handle"],
                        prospect.get("tiktok_url"),
                        prospect.get("tiktok_followers", 0),
                        prospect.get("full_name"),
                        prospect.get("email"),
                        prospect.get("email_source"),
                        prospect.get("primary_platform", "tiktok"),
                        prospect.get("source", "apify_tiktok"),
                        prospect.get("source_query"),
                        prospect.get("relevance_score", 0.0),
                        prospect.get("raw_data", {})
                    )
                    results["new"] += 1
                    
            except Exception as e:
                logger.error(f"Failed to save TikTok prospect: {e}")
                results["skipped"] += 1
        
        logger.info("Saved TikTok prospects", **results)
        return results
