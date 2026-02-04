"""
ReelForge Marketing Engine - Google Trends Analyzer via SerpApi
Analyzes keyword trends to automatically adjust discovery priorities.
"""

import asyncio
import httpx
import structlog
from typing import Optional

from app.config import get_settings
from app.database import get_database_async
from services.http_client import get_serpapi_client

logger = structlog.get_logger()


class TrendsAnalyzer:
    """Analyze Google Trends data via SerpApi to optimize keyword priorities."""

    BASE_URL = "https://serpapi.com/search"

    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.serpapi_api_key
        self.http_client = get_serpapi_client()

    async def get_trend_score(self, keyword: str, timeframe: str = "today 3-m") -> Optional[dict]:
        """
        Get Google Trends interest score for a keyword.

        Args:
            keyword: Search term to analyze
            timeframe: Time range (today 3-m, today 12-m, today 5-y)

        Returns:
            dict with average_score, trend_direction, related_queries
        """
        if not self.api_key:
            logger.warning("SerpApi key not configured")
            return None

        params = {
            "engine": "google_trends",
            "q": keyword,
            "data_type": "TIMESERIES",
            "date": timeframe,
            "api_key": self.api_key
        }

        try:
            response = await self.http_client.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            # Extract interest over time
            timeline = data.get("interest_over_time", {}).get("timeline_data", [])

            if not timeline:
                return {"keyword": keyword, "average_score": 0, "trend_direction": "no_data"}

            # Calculate average score from recent data points
            scores = []
            for point in timeline:
                values = point.get("values", [])
                if values and values[0].get("extracted_value") is not None:
                    scores.append(values[0]["extracted_value"])

            if not scores:
                return {"keyword": keyword, "average_score": 0, "trend_direction": "no_data"}

            avg_score = sum(scores) / len(scores)

            # Determine trend direction (compare last 30% vs first 30%)
            split = len(scores) // 3
            if split > 0:
                early_avg = sum(scores[:split]) / split
                recent_avg = sum(scores[-split:]) / split
                if recent_avg > early_avg * 1.2:
                    trend = "rising"
                elif recent_avg < early_avg * 0.8:
                    trend = "declining"
                else:
                    trend = "stable"
            else:
                trend = "stable"

            return {
                "keyword": keyword,
                "average_score": round(avg_score, 1),
                "trend_direction": trend,
                "data_points": len(scores)
            }

        except httpx.HTTPStatusError as e:
            logger.error("SerpApi HTTP error", keyword=keyword, status=e.response.status_code)
            return None
        except Exception as e:
            logger.error("Trends analysis failed", keyword=keyword, error=str(e))
            return None

    async def get_related_queries(self, keyword: str) -> list[dict]:
        """
        Get rising related queries for keyword discovery.

        Returns list of {query, trend_percentage} for rising searches.
        """
        if not self.api_key:
            return []

        params = {
            "engine": "google_trends",
            "q": keyword,
            "data_type": "RELATED_QUERIES",
            "api_key": self.api_key
        }

        try:
            response = await self.http_client.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            rising = data.get("related_queries", {}).get("rising", [])

            return [
                {
                    "query": item.get("query", ""),
                    "trend_value": item.get("extracted_value", 0),
                    "trend_label": item.get("value", "")
                }
                for item in rising[:10]  # Top 10 rising queries
            ]

        except Exception as e:
            logger.error("Related queries fetch failed", keyword=keyword, error=str(e))
            return []

    async def analyze_all_keywords(self) -> dict:
        """
        Analyze all active keywords and update their priorities.

        Returns summary of changes made.
        """
        db = None
        results = {
            "analyzed": 0,
            "boosted": 0,
            "demoted": 0,
            "deactivated": 0,
            "new_suggestions": 0,
            "errors": 0
        }

        try:
            db = await get_database_async()

            # Get all keywords (active and inactive for re-evaluation)
            keywords = await db.fetch("""
                SELECT id, keyword, competitor_name, priority, is_active
                FROM competitor_keywords
                WHERE platform = 'youtube'
                ORDER BY priority DESC
            """)

            logger.info("Starting trends analysis", keyword_count=len(keywords))

            for kw in keywords:
                try:
                    # Rate limit: SerpApi has limits, be conservative
                    await asyncio.sleep(self.settings.trends_api_rate_limit)

                    trend_data = await self.get_trend_score(kw["keyword"])
                    results["analyzed"] += 1

                    if not trend_data:
                        continue

                    avg_score = trend_data["average_score"]
                    trend_dir = trend_data["trend_direction"]
                    current_priority = kw["priority"] or 0

                    # Determine new priority based on trends
                    new_priority = current_priority
                    new_active = kw["is_active"]

                    if avg_score >= self.settings.trends_rising_threshold and trend_dir == "rising":
                        # Hot keyword: boost priority
                        new_priority = min(10, current_priority + 2)
                        new_active = True
                        if new_priority > current_priority:
                            results["boosted"] += 1

                    elif avg_score >= self.settings.trends_min_interest_score:
                        # Decent interest: slight boost if rising
                        if trend_dir == "rising":
                            new_priority = min(10, current_priority + 1)
                            if new_priority > current_priority:
                                results["boosted"] += 1
                        elif trend_dir == "declining":
                            new_priority = max(0, current_priority - 1)
                            if new_priority < current_priority:
                                results["demoted"] += 1

                    else:
                        # Low interest: demote or deactivate
                        if avg_score < 10:
                            new_active = False
                            results["deactivated"] += 1
                        else:
                            new_priority = max(0, current_priority - 2)
                            results["demoted"] += 1

                    # Update if changed
                    if new_priority != current_priority or new_active != kw["is_active"]:
                        await db.execute("""
                            UPDATE competitor_keywords
                            SET priority = $1, is_active = $2, last_searched_at = NOW()
                            WHERE id = $3
                        """, new_priority, new_active, kw["id"])

                        logger.info("Keyword priority updated",
                                    keyword=kw["keyword"],
                                    old_priority=current_priority,
                                    new_priority=new_priority,
                                    trend_score=avg_score,
                                    trend_direction=trend_dir,
                                    is_active=new_active)

                except Exception as e:
                    logger.error("Keyword analysis failed", keyword=kw["keyword"], error=str(e))
                    results["errors"] += 1

            # Discover new keywords from top performers
            top_keywords = await db.fetch("""
                SELECT keyword FROM competitor_keywords
                WHERE is_active = TRUE AND priority >= 7
                LIMIT 5
            """)

            for kw in top_keywords:
                try:
                    await asyncio.sleep(self.settings.trends_api_rate_limit)
                    related = await self.get_related_queries(kw["keyword"])

                    for query in related[:3]:  # Top 3 related per keyword
                        query_text = query["query"]

                        # Skip if already exists
                        exists = await db.fetchval(
                            "SELECT id FROM competitor_keywords WHERE keyword = $1",
                            query_text
                        )
                        if exists:
                            continue

                        # Add new keyword suggestion
                        await db.execute("""
                            INSERT INTO competitor_keywords (competitor_name, keyword, platform, priority, is_active)
                            VALUES ('Discovered', $1, 'youtube', 5, TRUE)
                        """, query_text)

                        results["new_suggestions"] += 1
                        logger.info("New keyword discovered", keyword=query_text, source=kw["keyword"])

                except Exception as e:
                    logger.error("Related queries analysis failed", keyword=kw["keyword"], error=str(e))

            logger.info("Trends analysis complete", **results)

        except Exception as e:
            logger.error("Trends analysis failed", error=str(e))
            results["error"] = str(e)
        finally:
            if db:
                await db.close()

        return results

    async def get_competitor_comparison(self, keywords: list[str]) -> dict:
        """
        Compare multiple keywords head-to-head.

        Args:
            keywords: List of up to 5 keywords to compare

        Returns:
            Comparison data with relative interest scores
        """
        if not self.api_key or not keywords:
            return {}

        # SerpApi supports up to 5 keywords in comparison
        keywords = keywords[:5]

        params = {
            "engine": "google_trends",
            "q": ",".join(keywords),
            "data_type": "TIMESERIES",
            "date": "today 3-m",
            "api_key": self.api_key
        }

        try:
            response = await self.http_client.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            timeline = data.get("interest_over_time", {}).get("timeline_data", [])
            averages = data.get("interest_over_time", {}).get("averages", [])

            result = {
                "keywords": keywords,
                "averages": [],
                "winner": None
            }

            if averages:
                for i, kw in enumerate(keywords):
                    if i < len(averages):
                        result["averages"].append({
                            "keyword": kw,
                            "average_score": averages[i].get("extracted_value", 0)
                        })

                # Determine winner
                if result["averages"]:
                    winner = max(result["averages"], key=lambda x: x["average_score"])
                    result["winner"] = winner["keyword"]

            return result

        except Exception as e:
            logger.error("Competitor comparison failed", error=str(e))
            return {"error": str(e)}
