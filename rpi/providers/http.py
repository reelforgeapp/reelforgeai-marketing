"""
Resilient HTTP executor for provider adapters.

Adds what the existing `services.http_client.RetryableHTTPClient` lacks for
paid data providers: jittered backoff honoring Retry-After, a circuit breaker,
a token-bucket rate limiter, request timeouts, structured error classes and
secret-safe logging. Adapters translate provider-specific error bodies into
these classes via `classify_error`.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

import httpx
import structlog

from rpi.core.redaction import redact_text

logger = structlog.get_logger()


# --------------------------------------------------------------------------
# Structured errors
# --------------------------------------------------------------------------


class ProviderError(Exception):
    code = "provider_error"
    retryable = False

    def __init__(self, provider: str, message: str, status_code: Optional[int] = None):
        super().__init__(f"{provider}: {message}")
        self.provider = provider
        self.message = message
        self.status_code = status_code


class ProviderAuthError(ProviderError):
    code = "auth_failed"


class ProviderCreditsExhausted(ProviderError):
    code = "credits_exhausted"


class ProviderRateLimited(ProviderError):
    code = "rate_limited"
    retryable = True


class ProviderNotFound(ProviderError):
    code = "not_found"


class ProviderBadRequest(ProviderError):
    code = "bad_request"


class ProviderUnavailable(ProviderError):
    code = "unavailable"
    retryable = True


class ProviderSchemaError(ProviderError):
    code = "schema_mismatch"


class ProviderCircuitOpen(ProviderError):
    code = "circuit_open"


def default_classify_error(provider: str, response: httpx.Response) -> Optional[ProviderError]:
    status = response.status_code
    body = redact_text(response.text[:300]) if response.text else ""
    if status < 400:
        return None
    if status in (401, 403):
        return ProviderAuthError(provider, f"HTTP {status}: {body}", status)
    if status == 402:
        return ProviderCreditsExhausted(provider, f"HTTP 402: {body}", status)
    if status == 404:
        return ProviderNotFound(provider, "not found", status)
    if status == 429:
        return ProviderRateLimited(provider, "rate limited", status)
    if status in (408, 500, 502, 503, 504, 529):
        return ProviderUnavailable(provider, f"HTTP {status}", status)
    return ProviderBadRequest(provider, f"HTTP {status}: {body}", status)


# --------------------------------------------------------------------------
# Circuit breaker and rate limiter
# --------------------------------------------------------------------------


class CircuitBreaker:
    """Consecutive-failure breaker. Open → reject for `cooldown_seconds`,
    then half-open: one trial request decides whether to close again."""

    def __init__(self, failure_threshold: int = 5, cooldown_seconds: float = 60.0, clock: Callable[[], float] = time.monotonic):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self._clock = clock
        self._failures = 0
        self._opened_at: Optional[float] = None
        self._half_open_trial = False

    @property
    def state(self) -> str:
        if self._opened_at is None:
            return "closed"
        if self._clock() - self._opened_at >= self.cooldown_seconds:
            return "half_open"
        return "open"

    def allow(self) -> bool:
        state = self.state
        if state == "closed":
            return True
        if state == "half_open" and not self._half_open_trial:
            self._half_open_trial = True
            return True
        return False

    def record_success(self) -> None:
        self._failures = 0
        self._opened_at = None
        self._half_open_trial = False

    def record_failure(self) -> None:
        self._failures += 1
        if self._half_open_trial or self._failures >= self.failure_threshold:
            self._opened_at = self._clock()
            self._half_open_trial = False


class TokenBucket:
    """In-process async token bucket. Per-process only: with N Celery worker
    processes the effective ceiling is N × rate, so configure rates for the
    worker concurrency (documented in OPERATIONS_RUNBOOK.md)."""

    def __init__(self, rate_per_second: float, burst: int = 1, clock: Callable[[], float] = time.monotonic):
        self.rate = max(rate_per_second, 1e-6)
        self.capacity = max(burst, 1)
        self._tokens = float(self.capacity)
        self._clock = clock
        self._updated = clock()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = self._clock()
                self._tokens = min(self.capacity, self._tokens + (now - self._updated) * self.rate)
                self._updated = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                await asyncio.sleep((1 - self._tokens) / self.rate)


def full_jitter_backoff(attempt: int, base: float = 0.5, cap: float = 20.0, rng: random.Random | None = None) -> float:
    rng = rng or random
    return rng.uniform(0, min(cap, base * (2 ** attempt)))


# --------------------------------------------------------------------------
# Executor
# --------------------------------------------------------------------------


@dataclass
class HTTPCallRecord:
    method: str
    path: str
    status_code: Optional[int]
    duration_ms: int
    attempts: int
    error_code: Optional[str] = None
    request_id: Optional[str] = None


class ResilientHTTP:
    def __init__(
        self,
        provider: str,
        base_url: str,
        *,
        headers: Mapping[str, str] | None = None,
        timeout: float = 20.0,
        max_retries: int = 3,
        rate_limiter: TokenBucket | None = None,
        breaker: CircuitBreaker | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        classify_error: Callable[[str, httpx.Response], Optional[ProviderError]] = default_classify_error,
        secrets: tuple[str, ...] = (),
        sleep: Callable[[float], Any] = asyncio.sleep,
    ):
        self.provider = provider
        self.base_url = base_url.rstrip("/")
        self.headers = dict(headers or {})
        self.timeout = timeout
        self.max_retries = max_retries
        self.rate_limiter = rate_limiter
        self.breaker = breaker or CircuitBreaker()
        self.transport = transport
        self.classify_error = classify_error
        self.secrets = secrets
        self._sleep = sleep
        self.last_call: Optional[HTTPCallRecord] = None

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        if not self.breaker.allow():
            raise ProviderCircuitOpen(self.provider, "circuit open after repeated failures")

        url = f"{self.base_url}/{path.lstrip('/')}" if not path.startswith("http") else path
        merged = {**self.headers, **(headers or {})}
        started = time.monotonic()
        attempt = 0
        last_error: Optional[ProviderError] = None

        while attempt <= self.max_retries:
            if self.rate_limiter:
                await self.rate_limiter.acquire()
            try:
                async with httpx.AsyncClient(timeout=self.timeout, transport=self.transport) as client:
                    response = await client.request(method, url, params=params, json=json, headers=merged)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = ProviderUnavailable(self.provider, redact_text(f"{type(exc).__name__}: {exc}", self.secrets))
            else:
                error = self.classify_error(self.provider, response)
                if error is not None:
                    # Providers sometimes echo credentials in error bodies.
                    error.message = redact_text(error.message, self.secrets)
                    error.args = (f"{error.provider}: {error.message}",)
                if error is None:
                    self.breaker.record_success()
                    self._record(method, path, response.status_code, started, attempt + 1, None, response)
                    return response
                last_error = error
                if not error.retryable:
                    # Auth/credit/4xx problems are not provider outages.
                    if isinstance(error, (ProviderAuthError, ProviderCreditsExhausted)):
                        self.breaker.record_failure()
                    self._record(method, path, response.status_code, started, attempt + 1, error.code, response)
                    raise error
                retry_after = _retry_after_seconds(response)
                if retry_after is not None and attempt < self.max_retries:
                    logger.warning("provider_retry_after", provider=self.provider, seconds=retry_after)
                    await self._sleep(min(retry_after, 60.0))
                    attempt += 1
                    continue

            if attempt < self.max_retries:
                delay = full_jitter_backoff(attempt)
                logger.warning(
                    "provider_retry",
                    provider=self.provider,
                    path=path,
                    attempt=attempt + 1,
                    error=last_error.code if last_error else None,
                    delay=round(delay, 2),
                )
                await self._sleep(delay)
            attempt += 1

        self.breaker.record_failure()
        status = last_error.status_code if last_error else None
        self._record(method, path, status, started, attempt, last_error.code if last_error else None, None)
        assert last_error is not None
        raise last_error

    def _record(self, method, path, status, started, attempts, error_code, response) -> None:
        request_id = None
        if response is not None:
            request_id = response.headers.get("x-request-id") or response.headers.get("request-id")
        self.last_call = HTTPCallRecord(
            method=method,
            path=path,
            status_code=status,
            duration_ms=int((time.monotonic() - started) * 1000),
            attempts=attempts,
            error_code=error_code,
            request_id=request_id,
        )


def _retry_after_seconds(response: httpx.Response) -> Optional[float]:
    value = response.headers.get("retry-after")
    if not value:
        return None
    try:
        return max(float(value), 0.0)
    except ValueError:
        return None
