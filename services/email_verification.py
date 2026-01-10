"""
ReelForge Marketing Engine - Email Verification Service
"""

import asyncio
from enum import Enum
from typing import Optional
from dataclasses import dataclass
import httpx
import structlog

from app.config import get_settings
from app.database import get_database_async

logger = structlog.get_logger()


class VerificationStatus(Enum):
    VALID = "valid"
    INVALID = "invalid"
    CATCH_ALL = "catch_all"
    DISPOSABLE = "disposable"
    ROLE = "role"
    UNKNOWN = "unknown"
    TOXIC = "toxic"


@dataclass
class VerificationResult:
    email: str
    status: VerificationStatus
    is_deliverable: bool
    reason: Optional[str] = None
    toxicity_score: Optional[int] = None
    did_you_mean: Optional[str] = None


class BouncerClient:
    """Bouncer email verification client."""
    
    BASE_URL = "https://api.usebouncer.com/v1.1"
    MAX_RETRIES = 3
    
    def __init__(self, api_key: str = None):
        settings = get_settings()
        self.api_key = api_key or settings.bouncer_api_key
        if not self.api_key:
            raise ValueError("Bouncer API key not configured")
    
    async def verify_email(self, email: str, _retry_count: int = 0) -> VerificationResult:
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    f"{self.BASE_URL}/email/verify",
                    params={"email": email},
                    headers={"x-api-key": self.api_key, "Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    return self._parse_response(email, response.json())
                elif response.status_code == 402:
                    return VerificationResult(email=email, status=VerificationStatus.UNKNOWN, is_deliverable=False, reason="insufficient_credits")
                elif response.status_code == 429:
                    if _retry_count >= self.MAX_RETRIES:
                        return VerificationResult(email=email, status=VerificationStatus.UNKNOWN, is_deliverable=False, reason="rate_limit_exceeded")
                    await asyncio.sleep(2 ** _retry_count)
                    return await self.verify_email(email, _retry_count + 1)
                else:
                    return VerificationResult(email=email, status=VerificationStatus.UNKNOWN, is_deliverable=False, reason=f"api_error_{response.status_code}")
            except httpx.TimeoutException:
                return VerificationResult(email=email, status=VerificationStatus.UNKNOWN, is_deliverable=False, reason="timeout")
            except Exception as e:
                return VerificationResult(email=email, status=VerificationStatus.UNKNOWN, is_deliverable=False, reason=str(e)[:100])
    
    def _parse_response(self, email: str, data: dict) -> VerificationResult:
        status = data.get("status", "unknown")
        reason = data.get("reason", "")
        
        if status == "deliverable":
            verification_status, is_deliverable = VerificationStatus.VALID, True
        elif status == "undeliverable":
            verification_status, is_deliverable = VerificationStatus.INVALID, False
        elif status == "risky":
            if "disposable" in reason.lower():
                verification_status = VerificationStatus.DISPOSABLE
            elif "role" in reason.lower():
                verification_status = VerificationStatus.ROLE
            else:
                verification_status = VerificationStatus.CATCH_ALL
            is_deliverable = False
        else:
            verification_status, is_deliverable = VerificationStatus.UNKNOWN, False
        
        toxicity = data.get("toxicity")
        toxicity_score = toxicity.get("score", 0) if toxicity else None
        if toxicity_score and toxicity_score >= 3:
            verification_status, is_deliverable = VerificationStatus.TOXIC, False
        
        return VerificationResult(
            email=email, status=verification_status, is_deliverable=is_deliverable,
            reason=reason, toxicity_score=toxicity_score, did_you_mean=data.get("didYouMean")
        )
    
    async def verify_batch(self, limit: int = 100, only_unverified: bool = True) -> dict:
        db = await get_database_async()
        results = {"processed": 0, "valid": 0, "invalid": 0, "catch_all": 0, "disposable": 0, "role": 0, "toxic": 0, "unknown": 0, "errors": 0}
        
        if only_unverified:
            prospects = await db.fetch(
                "SELECT id, email FROM marketing_prospects WHERE email IS NOT NULL AND (email_verified IS NULL OR email_verified = FALSE) ORDER BY relevance_score DESC LIMIT $1",
                limit
            )
        else:
            prospects = await db.fetch(
                "SELECT id, email FROM marketing_prospects WHERE email IS NOT NULL ORDER BY relevance_score DESC LIMIT $1",
                limit
            )
        
        logger.info(f"Verifying {len(prospects)} emails with Bouncer")
        
        for prospect in prospects:
            results["processed"] += 1
            try:
                result = await self.verify_email(prospect["email"])
                status_key = result.status.value
                if status_key in results:
                    results[status_key] += 1
                
                is_verified = result.status in [VerificationStatus.VALID, VerificationStatus.CATCH_ALL]
                
                await db.execute(
                    "UPDATE marketing_prospects SET email_verified = $1, verified_at = NOW(), verification_status = $2 WHERE id = $3",
                    is_verified, result.status.value, prospect["id"]
                )
                await asyncio.sleep(0.15)
            except Exception as e:
                logger.error(f"Verification failed: {e}")
                results["errors"] += 1
        
        return results


class ClearoutClient:
    """Clearout email verification client."""
    
    BASE_URL = "https://api.clearout.io/v2"
    MAX_RETRIES = 3
    
    def __init__(self, api_key: str = None):
        settings = get_settings()
        self.api_key = api_key or settings.clearout_api_key
        if not self.api_key:
            raise ValueError("Clearout API key not configured")
    
    async def verify_email(self, email: str, _retry_count: int = 0) -> VerificationResult:
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(
                    f"{self.BASE_URL}/email_verify/instant",
                    headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                    json={"email": email}
                )
                
                if response.status_code == 200:
                    data = response.json().get("data", {})
                    status = data.get("status", "unknown")
                    
                    status_map = {"valid": VerificationStatus.VALID, "invalid": VerificationStatus.INVALID, "catch_all": VerificationStatus.CATCH_ALL, "disposable": VerificationStatus.DISPOSABLE, "role": VerificationStatus.ROLE}
                    verification_status = status_map.get(status, VerificationStatus.UNKNOWN)
                    
                    return VerificationResult(email=email, status=verification_status, is_deliverable=verification_status == VerificationStatus.VALID, reason=status)
                elif response.status_code == 429:
                    if _retry_count >= self.MAX_RETRIES:
                        return VerificationResult(email=email, status=VerificationStatus.UNKNOWN, is_deliverable=False, reason="rate_limit_exceeded")
                    await asyncio.sleep(2 ** _retry_count)
                    return await self.verify_email(email, _retry_count + 1)
                else:
                    return VerificationResult(email=email, status=VerificationStatus.UNKNOWN, is_deliverable=False, reason=f"api_error_{response.status_code}")
            except Exception as e:
                return VerificationResult(email=email, status=VerificationStatus.UNKNOWN, is_deliverable=False, reason=str(e)[:100])
    
    async def verify_batch(self, limit: int = 100, only_unverified: bool = True) -> dict:
        db = await get_database_async()
        results = {"processed": 0, "valid": 0, "invalid": 0, "catch_all": 0, "unknown": 0, "errors": 0}
        
        if only_unverified:
            prospects = await db.fetch(
                "SELECT id, email FROM marketing_prospects WHERE email IS NOT NULL AND (email_verified IS NULL OR email_verified = FALSE) ORDER BY relevance_score DESC LIMIT $1",
                limit
            )
        else:
            prospects = await db.fetch(
                "SELECT id, email FROM marketing_prospects WHERE email IS NOT NULL ORDER BY relevance_score DESC LIMIT $1",
                limit
            )
        
        for prospect in prospects:
            results["processed"] += 1
            try:
                result = await self.verify_email(prospect["email"])
                status_key = result.status.value
                if status_key in results:
                    results[status_key] += 1
                
                is_verified = result.status in [VerificationStatus.VALID, VerificationStatus.CATCH_ALL]
                await db.execute(
                    "UPDATE marketing_prospects SET email_verified = $1, verified_at = NOW(), verification_status = $2 WHERE id = $3",
                    is_verified, result.status.value, prospect["id"]
                )
                await asyncio.sleep(0.2)
            except Exception as e:
                results["errors"] += 1
        
        return results


class HunterClient:
    """Hunter.io email verification client."""
    
    BASE_URL = "https://api.hunter.io/v2"
    MAX_RETRIES = 3
    
    def __init__(self, api_key: str = None):
        settings = get_settings()
        self.api_key = api_key or settings.hunter_api_key
        if not self.api_key:
            raise ValueError("Hunter API key not configured")
    
    async def verify_email(self, email: str, _retry_count: int = 0) -> VerificationResult:
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(f"{self.BASE_URL}/email-verifier", params={"email": email, "api_key": self.api_key})
                
                if response.status_code == 200:
                    data = response.json().get("data", {})
                    result = data.get("result", "unknown")
                    
                    if result == "deliverable":
                        verification_status, is_deliverable = VerificationStatus.VALID, True
                    elif result == "undeliverable":
                        verification_status, is_deliverable = VerificationStatus.INVALID, False
                    elif result == "risky":
                        if data.get("disposable"):
                            verification_status = VerificationStatus.DISPOSABLE
                        elif data.get("role"):
                            verification_status = VerificationStatus.ROLE
                        else:
                            verification_status = VerificationStatus.CATCH_ALL
                        is_deliverable = False
                    else:
                        verification_status, is_deliverable = VerificationStatus.UNKNOWN, False
                    
                    return VerificationResult(email=email, status=verification_status, is_deliverable=is_deliverable, reason=data.get("status", ""))
                elif response.status_code == 429:
                    if _retry_count >= self.MAX_RETRIES:
                        return VerificationResult(email=email, status=VerificationStatus.UNKNOWN, is_deliverable=False, reason="rate_limit_exceeded")
                    await asyncio.sleep(2 ** _retry_count)
                    return await self.verify_email(email, _retry_count + 1)
                else:
                    return VerificationResult(email=email, status=VerificationStatus.UNKNOWN, is_deliverable=False, reason=f"api_error_{response.status_code}")
            except Exception as e:
                return VerificationResult(email=email, status=VerificationStatus.UNKNOWN, is_deliverable=False, reason=str(e)[:100])
    
    async def verify_batch(self, limit: int = 100, only_unverified: bool = True) -> dict:
        db = await get_database_async()
        results = {"processed": 0, "valid": 0, "invalid": 0, "catch_all": 0, "unknown": 0, "errors": 0}
        
        if only_unverified:
            prospects = await db.fetch(
                "SELECT id, email FROM marketing_prospects WHERE email IS NOT NULL AND (email_verified IS NULL OR email_verified = FALSE) ORDER BY relevance_score DESC LIMIT $1",
                limit
            )
        else:
            prospects = await db.fetch(
                "SELECT id, email FROM marketing_prospects WHERE email IS NOT NULL ORDER BY relevance_score DESC LIMIT $1",
                limit
            )
        
        for prospect in prospects:
            results["processed"] += 1
            try:
                result = await self.verify_email(prospect["email"])
                status_key = result.status.value
                if status_key in results:
                    results[status_key] += 1
                
                is_verified = result.status in [VerificationStatus.VALID, VerificationStatus.CATCH_ALL]
                await db.execute(
                    "UPDATE marketing_prospects SET email_verified = $1, verified_at = NOW(), verification_status = $2 WHERE id = $3",
                    is_verified, result.status.value, prospect["id"]
                )
                await asyncio.sleep(0.5)
            except Exception as e:
                results["errors"] += 1
        
        return results


def get_verification_client():
    """Get the configured email verification client. Priority: Bouncer > Clearout > Hunter"""
    settings = get_settings()
    
    if settings.bouncer_api_key:
        logger.info("Using Bouncer for email verification")
        return BouncerClient()
    
    if settings.clearout_api_key:
        logger.info("Using Clearout for email verification")
        return ClearoutClient()
    
    if settings.hunter_api_key:
        logger.info("Using Hunter for email verification")
        return HunterClient()
    
    raise ValueError("No email verification service configured. Set BOUNCER_API_KEY, CLEAROUT_API_KEY, or HUNTER_API_KEY")
