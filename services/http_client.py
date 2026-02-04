"""
ReelForge Marketing Engine - HTTP Client with Retry Logic
Provides resilient HTTP client for external API calls.
"""

import asyncio
from typing import Optional, Dict, Any
import httpx
import structlog

logger = structlog.get_logger()

# Default retry configuration
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0  # seconds
DEFAULT_RETRY_BACKOFF = 2.0  # exponential backoff multiplier
DEFAULT_TIMEOUT = 30.0


class RetryableHTTPClient:
    """HTTP client with automatic retry logic for transient failures."""

    # Status codes that should trigger a retry
    RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

    def __init__(
        self,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: float = DEFAULT_RETRY_DELAY,
        retry_backoff: float = DEFAULT_RETRY_BACKOFF,
        timeout: float = DEFAULT_TIMEOUT,
        headers: Optional[Dict[str, str]] = None
    ):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff
        self.timeout = timeout
        self.default_headers = headers or {}

    async def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> httpx.Response:
        """
        Make an HTTP request with automatic retries.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            headers: Request headers (merged with default headers)
            json: JSON body
            data: Form data
            params: Query parameters
            **kwargs: Additional httpx arguments

        Returns:
            httpx.Response object

        Raises:
            httpx.HTTPError: After all retries exhausted
        """
        merged_headers = {**self.default_headers, **(headers or {})}
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=merged_headers,
                        json=json,
                        data=data,
                        params=params,
                        **kwargs
                    )

                    # Check if we should retry based on status code
                    if response.status_code in self.RETRYABLE_STATUS_CODES:
                        if attempt < self.max_retries:
                            delay = self.retry_delay * (self.retry_backoff ** attempt)
                            logger.warning(
                                "Retryable status code received",
                                status=response.status_code,
                                url=url,
                                attempt=attempt + 1,
                                max_retries=self.max_retries,
                                retry_in=delay
                            )
                            await asyncio.sleep(delay)
                            continue

                    return response

            except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout) as e:
                last_exception = e
                if attempt < self.max_retries:
                    delay = self.retry_delay * (self.retry_backoff ** attempt)
                    logger.warning(
                        "HTTP request failed, retrying",
                        error=str(e),
                        url=url,
                        attempt=attempt + 1,
                        max_retries=self.max_retries,
                        retry_in=delay
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        "HTTP request failed after all retries",
                        error=str(e),
                        url=url,
                        attempts=self.max_retries + 1
                    )
                    raise

        # Should not reach here, but just in case
        if last_exception:
            raise last_exception

    async def get(self, url: str, **kwargs) -> httpx.Response:
        """Make a GET request."""
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs) -> httpx.Response:
        """Make a POST request."""
        return await self.request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs) -> httpx.Response:
        """Make a PUT request."""
        return await self.request("PUT", url, **kwargs)

    async def delete(self, url: str, **kwargs) -> httpx.Response:
        """Make a DELETE request."""
        return await self.request("DELETE", url, **kwargs)


def get_brevo_client(api_key: str) -> RetryableHTTPClient:
    """Create a configured HTTP client for Brevo API."""
    return RetryableHTTPClient(
        headers={
            "api-key": api_key,
            "Content-Type": "application/json"
        },
        max_retries=3,
        timeout=30.0
    )


def get_serpapi_client() -> RetryableHTTPClient:
    """Create a configured HTTP client for SerpApi."""
    return RetryableHTTPClient(
        max_retries=2,
        timeout=30.0
    )


def get_anthropic_client(api_key: str) -> RetryableHTTPClient:
    """Create a configured HTTP client for Anthropic API."""
    return RetryableHTTPClient(
        headers={
            "x-api-key": api_key,
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01"
        },
        max_retries=2,
        timeout=60.0  # AI calls can be slow
    )
