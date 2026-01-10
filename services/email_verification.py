"""
ReelForge Marketing Engine - Email Verification Service
Supports Bouncer (primary), Clearout, and Hunter.io
"""

import asyncio
from enum import Enum
from typing import Optional
from dataclasses import dataclass
import httpx
import structlog

from app.config import get_settings
from app.database import get_database

logger = structlog.get_logger()


class VerificationStatus(Enum):
    """Email verification status."""
    VALID = "valid"
    INVALID = "invalid"
    CATCH_ALL = "catch_all"
    DISPOSABLE = "disposable"
    ROLE = "role"
    UNKNOWN = "unknown"
    TOXIC = "toxic"


@dataclass
class VerificationResult:
    """Result of email verification."""
    email: str
    status: VerificationStatus
    is_deliverable: bool
    reason: Optional[str] = None
    toxicity_score: Optional[int] = None  # Bouncer-specific: 0-5
    did_you_mean: Optional[str] = None  # Suggested correction


# =============================================================================
# Bouncer Client (Primary)
# =============================================================================

class BouncerClient:
    """
    Bouncer email verification client.
    
    API Docs: https://docs.usebouncer.com/
    
    Features:
    - Real-time single verification
    - Batch verification
    - Toxicity check (spam traps, complainers)
    - Bounce prediction
    """
    
    BASE_URL = "https://api.usebouncer.com/v1.1"
    
    def __init__(self, api_key: str = None):
        settings = get_settings()
        self.api_key = api_key or settings.bouncer_api_key
        
        if not self.api_key:
            raise ValueError("Bouncer API key not configured")
    
    async def verify_email(self, email: str) -> VerificationResult:
        """
        Verify a single email address.
        
        Args:
            email: Email address to verify
            
        Returns:
            VerificationResult with status and details
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    f"{self.BASE_URL}/email/verify",
                    params={"email": email},
                    headers={
                        "x-api-key": self.api_key,
                        "Content-Type": "application/json"
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    return self._parse_response(email, data)
                
                elif response.status_code == 402:
                    logger.error("Bouncer: Insufficient credits")
                    return VerificationResult(
                        email=email,
                        status=VerificationStatus.UNKNOWN,
                        is_deliverable=False,
                        reason="insufficient_credits"
                    )
                
                elif response.status_code == 429:
                    logger.warning("Bouncer: Rate limited, waiting...")
                    await asyncio.sleep(2)
                    return await self.verify_email(email)  # Retry once
                
                else:
                    logger.error(f"Bouncer API error: {response.status_code}")
                    return VerificationResult(
                        email=email,
                        status=VerificationStatus.UNKNOWN,
                        is_deliverable=False,
                        reason=f"api_error_{response.status_code}"
                    )
                    
            except httpx.TimeoutException:
                logger.error(f"Bouncer timeout for {email}")
                return VerificationResult(
                    email=email,
                    status=VerificationStatus.UNKNOWN,
                    is_deliverable=False,
                    reason="timeout"
                )
            except Exception as e:
                logger.error(f"Bouncer error: {e}")
                return VerificationResult(
                    email=email,
                    status=VerificationStatus.UNKNOWN,
                    is_deliverable=False,
                    reason=str(e)
                )
    
    def _parse_response(self, email: str, data: dict) -> VerificationResult:
        """Parse Bouncer API response."""
        
        # Bouncer status values:
        # deliverable, undeliverable, risky, unknown
        status = data.get("status", "unknown")
        reason = data.get("reason", "")
        
        # Map Bouncer status to our enum
        if status == "deliverable":
            verification_status = VerificationStatus.VALID
            is_deliverable = True
            
        elif status == "undeliverable":
            verification_status = VerificationStatus.INVALID
            is_deliverable = False
            
        elif status == "risky":
            # Check specific risk type
            if "disposable" in reason.lower():
                verification_status = VerificationStatus.DISPOSABLE
            elif "role" in reason.lower() or "group" in reason.lower():
                verification_status = VerificationStatus.ROLE
            elif "catch" in reason.lower() or "accept_all" in reason.lower():
                verification_status = VerificationStatus.CATCH_ALL
            else:
                verification_status = VerificationStatus.CATCH_ALL
            is_deliverable = False  # Treat risky as not deliverable by default
            
        else:  # unknown
            verification_status = VerificationStatus.UNKNOWN
            is_deliverable = False
        
        # Check toxicity if available
        toxicity = data.get("toxicity")
        toxicity_score = None
        if toxicity:
            toxicity_score = toxicity.get("score", 0)
            # If high toxicity, mark as toxic
            if toxicity_score >= 3:
                verification_status = VerificationStatus.TOXIC
                is_deliverable = False
        
        # Check for suggested correction
        did_you_mean = data.get("didYouMean")
        
        return VerificationResult(
            email=email,
            status=verification_status,
            is_deliverable=is_deliverable,
            reason=reason,
            toxicity_score=toxicity_score,
            did_you_mean=did_you_mean
        )
    
    async def verify_batch(
        self,
        limit: int = 100,
        only_unverified: bool = True
    ) -> dict:
        """
        Verify batch of prospects from database.
        
        Args:
            limit: Max prospects to verify
            only_unverified: Only verify unverified emails
            
        Returns:
            Summary of verification results
        """
        db = get_database()
        
        results = {
            "processed": 0,
            "valid": 0,
            "invalid": 0,
            "catch_all": 0,
            "disposable": 0,
            "role": 0,
            "toxic": 0,
            "unknown": 0,
            "errors": 0
        }
        
        # Get prospects to verify
        query = """
            SELECT id, email
            FROM marketing_prospects
            WHERE email IS NOT NULL
        """
        
        if only_unverified:
            query += " AND (email_verified IS NULL OR email_verified = FALSE)"
        
        query += f"""
            ORDER BY relevance_score DESC
            LIMIT {limit}
        """
        
        prospects = await db.fetch(query)
        
        logger.info(f"Verifying {len(prospects)} emails with Bouncer")
        
        for prospect in prospects:
            results["processed"] += 1
            
            try:
                result = await self.verify_email(prospect["email"])
                
                # Update counts
                status_key = result.status.value
                if status_key in results:
                    results[status_key] += 1
                
                # Map status to database values
                db_status = result.status.value
                is_verified = result.status in [
                    VerificationStatus.VALID,
                    VerificationStatus.CATCH_ALL  # Allow catch-all with flag
                ]
                
                # Update prospect record
                await db.execute(
                    """
                    UPDATE marketing_prospects SET
                        email_verified = $1,
                        verified_at = NOW(),
                        verification_status = $2,
                        updated_at = NOW()
                    WHERE id = $3
                    """,
                    is_verified,
                    db_status,
                    prospect["id"]
                )
                
                # If there's a suggested correction, log it
                if result.did_you_mean:
                    logger.info(
                        f"Email correction suggested",
                        original=prospect["email"][:5] + "***",
                        suggested=result.did_you_mean[:5] + "***"
                    )
                
                # Rate limiting - Bouncer allows ~10 req/sec
                await asyncio.sleep(0.15)
                
            except Exception as e:
                logger.error(f"Verification failed for prospect {prospect['id']}: {e}")
                results["errors"] += 1
        
        logger.info("Bouncer batch verification complete", **results)
        return results


# =============================================================================
# Clearout Client (Alternative)
# =============================================================================

class ClearoutClient:
    """
    Clearout email verification client.
    
    API Docs: https://docs.clearout.io/
    """
    
    BASE_URL = "https://api.clearout.io/v2"
    
    def __init__(self, api_key: str = None):
        settings = get_settings()
        self.api_key = api_key or settings.clearout_api_key
        
        if not self.api_key:
            raise ValueError("Clearout API key not configured")
    
    async def verify_email(self, email: str) -> VerificationResult:
        """Verify a single email address."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(
                    f"{self.BASE_URL}/email_verify/instant",
                    json={"email": email},
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    return self._parse_response(email, data)
                else:
                    return VerificationResult(
                        email=email,
                        status=VerificationStatus.UNKNOWN,
                        is_deliverable=False,
                        reason=f"api_error_{response.status_code}"
                    )
                    
            except Exception as e:
                logger.error(f"Clearout error: {e}")
                return VerificationResult(
                    email=email,
                    status=VerificationStatus.UNKNOWN,
                    is_deliverable=False,
                    reason=str(e)
                )
    
    def _parse_response(self, email: str, data: dict) -> VerificationResult:
        """Parse Clearout API response."""
        status = data.get("status", "unknown")
        
        status_map = {
            "valid": VerificationStatus.VALID,
            "invalid": VerificationStatus.INVALID,
            "catch_all": VerificationStatus.CATCH_ALL,
            "disposable": VerificationStatus.DISPOSABLE,
            "role": VerificationStatus.ROLE,
            "unknown": VerificationStatus.UNKNOWN
        }
        
        verification_status = status_map.get(status, VerificationStatus.UNKNOWN)
        is_deliverable = verification_status == VerificationStatus.VALID
        
        return VerificationResult(
            email=email,
            status=verification_status,
            is_deliverable=is_deliverable,
            reason=data.get("reason")
        )
    
    async def verify_batch(
        self,
        limit: int = 100,
        only_unverified: bool = True
    ) -> dict:
        """Verify batch of prospects from database."""
        db = get_database()
        
        results = {
            "processed": 0,
            "valid": 0,
            "invalid": 0,
            "catch_all": 0,
            "unknown": 0,
            "errors": 0
        }
        
        query = """
            SELECT id, email
            FROM marketing_prospects
            WHERE email IS NOT NULL
        """
        
        if only_unverified:
            query += " AND (email_verified IS NULL OR email_verified = FALSE)"
        
        query += f" ORDER BY relevance_score DESC LIMIT {limit}"
        
        prospects = await db.fetch(query)
        
        for prospect in prospects:
            results["processed"] += 1
            
            try:
                result = await self.verify_email(prospect["email"])
                
                status_key = result.status.value
                if status_key in results:
                    results[status_key] += 1
                
                is_verified = result.status in [
                    VerificationStatus.VALID,
                    VerificationStatus.CATCH_ALL
                ]
                
                await db.execute(
                    """
                    UPDATE marketing_prospects SET
                        email_verified = $1,
                        verified_at = NOW(),
                        verification_status = $2
                    WHERE id = $3
                    """,
                    is_verified,
                    result.status.value,
                    prospect["id"]
                )
                
                await asyncio.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Verification failed: {e}")
                results["errors"] += 1
        
        return results


# =============================================================================
# Hunter Client (Alternative)
# =============================================================================

class HunterClient:
    """
    Hunter.io email verification client.
    
    API Docs: https://hunter.io/api-documentation
    """
    
    BASE_URL = "https://api.hunter.io/v2"
    
    def __init__(self, api_key: str = None):
        settings = get_settings()
        self.api_key = api_key or settings.hunter_api_key
        
        if not self.api_key:
            raise ValueError("Hunter API key not configured")
    
    async def verify_email(self, email: str) -> VerificationResult:
        """Verify a single email address."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    f"{self.BASE_URL}/email-verifier",
                    params={
                        "email": email,
                        "api_key": self.api_key
                    }
                )
                
                if response.status_code == 200:
                    data = response.json().get("data", {})
                    return self._parse_response(email, data)
                else:
                    return VerificationResult(
                        email=email,
                        status=VerificationStatus.UNKNOWN,
                        is_deliverable=False,
                        reason=f"api_error_{response.status_code}"
                    )
                    
            except Exception as e:
                logger.error(f"Hunter error: {e}")
                return VerificationResult(
                    email=email,
                    status=VerificationStatus.UNKNOWN,
                    is_deliverable=False,
                    reason=str(e)
                )
    
    def _parse_response(self, email: str, data: dict) -> VerificationResult:
        """Parse Hunter API response."""
        status = data.get("status", "unknown")
        result = data.get("result", "unknown")
        
        if result == "deliverable":
            verification_status = VerificationStatus.VALID
            is_deliverable = True
        elif result == "undeliverable":
            verification_status = VerificationStatus.INVALID
            is_deliverable = False
        elif result == "risky":
            if data.get("disposable"):
                verification_status = VerificationStatus.DISPOSABLE
            elif data.get("role"):
                verification_status = VerificationStatus.ROLE
            elif data.get("accept_all"):
                verification_status = VerificationStatus.CATCH_ALL
            else:
                verification_status = VerificationStatus.CATCH_ALL
            is_deliverable = False
        else:
            verification_status = VerificationStatus.UNKNOWN
            is_deliverable = False
        
        return VerificationResult(
            email=email,
            status=verification_status,
            is_deliverable=is_deliverable,
            reason=status
        )
    
    async def verify_batch(
        self,
        limit: int = 100,
        only_unverified: bool = True
    ) -> dict:
        """Verify batch of prospects from database."""
        db = get_database()
        
        results = {
            "processed": 0,
            "valid": 0,
            "invalid": 0,
            "catch_all": 0,
            "unknown": 0,
            "errors": 0
        }
        
        query = """
            SELECT id, email
            FROM marketing_prospects
            WHERE email IS NOT NULL
        """
        
        if only_unverified:
            query += " AND (email_verified IS NULL OR email_verified = FALSE)"
        
        query += f" ORDER BY relevance_score DESC LIMIT {limit}"
        
        prospects = await db.fetch(query)
        
        for prospect in prospects:
            results["processed"] += 1
            
            try:
                result = await self.verify_email(prospect["email"])
                
                status_key = result.status.value
                if status_key in results:
                    results[status_key] += 1
                
                is_verified = result.status in [
                    VerificationStatus.VALID,
                    VerificationStatus.CATCH_ALL
                ]
                
                await db.execute(
                    """
                    UPDATE marketing_prospects SET
                        email_verified = $1,
                        verified_at = NOW(),
                        verification_status = $2
                    WHERE id = $3
                    """,
                    is_verified,
                    result.status.value,
                    prospect["id"]
                )
                
                await asyncio.sleep(0.5)  # Hunter has stricter rate limits
                
            except Exception as e:
                logger.error(f"Verification failed: {e}")
                results["errors"] += 1
        
        return results


# =============================================================================
# Factory Function
# =============================================================================

def get_verification_client():
    """
    Get the configured email verification client.
    
    Priority:
    1. Bouncer (if BOUNCER_API_KEY set)
    2. Clearout (if CLEAROUT_API_KEY set)
    3. Hunter (if HUNTER_API_KEY set)
    
    Returns:
        Verification client instance
        
    Raises:
        ValueError if no verification service is configured
    """
    settings = get_settings()
    
    # Priority 1: Bouncer
    if settings.bouncer_api_key:
        logger.info("Using Bouncer for email verification")
        return BouncerClient()
    
    # Priority 2: Clearout
    if settings.clearout_api_key:
        logger.info("Using Clearout for email verification")
        return ClearoutClient()
    
    # Priority 3: Hunter
    if settings.hunter_api_key:
        logger.info("Using Hunter for email verification")
        return HunterClient()
    
    raise ValueError(
        "No email verification service configured. "
        "Set BOUNCER_API_KEY, CLEAROUT_API_KEY, or HUNTER_API_KEY"
    )
