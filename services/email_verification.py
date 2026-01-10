"""
ReelForge Marketing Engine - Email Verification Service
Integration with Clearout for email verification before outreach
"""

import httpx
from typing import Optional
from enum import Enum
from dataclasses import dataclass
import structlog

from app.config import get_settings
from app.database import get_database

logger = structlog.get_logger()


class VerificationStatus(str, Enum):
    """Email verification status codes."""
    VALID = "valid"
    INVALID = "invalid"
    CATCH_ALL = "catch_all"
    UNKNOWN = "unknown"
    DISPOSABLE = "disposable"
    ROLE = "role"
    PENDING = "pending"
    ERROR = "error"


@dataclass
class VerificationResult:
    """Result of email verification."""
    email: str
    status: VerificationStatus
    safe_to_send: bool
    reason: Optional[str] = None
    
    # Detailed flags
    is_disposable: bool = False
    is_role_account: bool = False
    is_catch_all: bool = False
    is_free_email: bool = False
    
    # Raw response for debugging
    raw_response: Optional[dict] = None


class ClearoutClient:
    """
    Client for Clearout email verification API.
    
    Clearout provides:
    - Real-time email verification
    - Syntax validation
    - Domain/MX record checks
    - SMTP mailbox verification
    - Disposable email detection
    - Role account detection (info@, admin@, etc.)
    - Catch-all detection
    
    Pricing: ~$0.005 per verification ($25/mo for 5,000 verifications)
    
    API Docs: https://docs.clearout.io/
    """
    
    BASE_URL = "https://api.clearout.io/v2"
    
    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.clearout_api_key
        self.db = get_database()
    
    async def verify_email(self, email: str) -> VerificationResult:
        """
        Verify a single email address.
        
        Args:
            email: Email address to verify
        
        Returns:
            VerificationResult with status and flags
        """
        if not self.api_key:
            logger.warning("Clearout API key not configured, skipping verification")
            return VerificationResult(
                email=email,
                status=VerificationStatus.UNKNOWN,
                safe_to_send=True,
                reason="Verification not configured"
            )
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.BASE_URL}/email_verify/instant",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json={"email": email}
                )
                
                if response.status_code == 200:
                    data = response.json().get("data", {})
                    return self._parse_response(email, data)
                
                elif response.status_code == 402:
                    logger.error("Clearout credit balance exhausted")
                    return VerificationResult(
                        email=email,
                        status=VerificationStatus.ERROR,
                        safe_to_send=False,
                        reason="Verification credits exhausted"
                    )
                
                else:
                    logger.error(
                        "Clearout API error",
                        status_code=response.status_code,
                        response=response.text
                    )
                    return VerificationResult(
                        email=email,
                        status=VerificationStatus.ERROR,
                        safe_to_send=False,
                        reason=f"API error: {response.status_code}"
                    )
                    
        except httpx.TimeoutException:
            logger.warning(f"Clearout verification timeout for {email}")
            return VerificationResult(
                email=email,
                status=VerificationStatus.UNKNOWN,
                safe_to_send=True,  # Allow send on timeout
                reason="Verification timeout"
            )
            
        except Exception as e:
            logger.error(f"Clearout verification failed: {e}")
            return VerificationResult(
                email=email,
                status=VerificationStatus.ERROR,
                safe_to_send=False,
                reason=str(e)
            )
    
    def _parse_response(self, email: str, data: dict) -> VerificationResult:
        """Parse Clearout API response into VerificationResult."""
        
        # Clearout status values:
        # valid, invalid, catch_all, unknown, disposable, role
        status_map = {
            "valid": VerificationStatus.VALID,
            "invalid": VerificationStatus.INVALID,
            "catch_all": VerificationStatus.CATCH_ALL,
            "unknown": VerificationStatus.UNKNOWN,
            "disposable": VerificationStatus.DISPOSABLE,
            "role": VerificationStatus.ROLE,
        }
        
        raw_status = data.get("status", "unknown").lower()
        status = status_map.get(raw_status, VerificationStatus.UNKNOWN)
        
        # Determine if safe to send
        safe_statuses = {
            VerificationStatus.VALID,
            VerificationStatus.CATCH_ALL,  # Allow with caution
        }
        safe_to_send = status in safe_statuses
        
        # Don't send to disposable or role accounts
        if data.get("disposable") or data.get("role"):
            safe_to_send = False
        
        return VerificationResult(
            email=email,
            status=status,
            safe_to_send=safe_to_send,
            reason=data.get("sub_status"),
            is_disposable=data.get("disposable", False),
            is_role_account=data.get("role", False),
            is_catch_all=(status == VerificationStatus.CATCH_ALL),
            is_free_email=data.get("free", False),
            raw_response=data
        )
    
    async def verify_and_update_prospect(self, prospect_id: str) -> VerificationResult:
        """
        Verify prospect's email and update database.
        
        Args:
            prospect_id: UUID of the prospect
        
        Returns:
            VerificationResult
        """
        # Get prospect email
        prospect = await self.db.fetchrow(
            """
            SELECT id, email FROM marketing_prospects
            WHERE id = $1 AND email IS NOT NULL
            """,
            prospect_id
        )
        
        if not prospect or not prospect['email']:
            logger.warning(f"Prospect {prospect_id} has no email")
            return VerificationResult(
                email="",
                status=VerificationStatus.INVALID,
                safe_to_send=False,
                reason="No email found"
            )
        
        # Verify email
        result = await self.verify_email(prospect['email'])
        
        # Update prospect record
        await self.db.execute(
            """
            UPDATE marketing_prospects SET
                email_verified = $1,
                verified_at = NOW(),
                verification_status = $2
            WHERE id = $3
            """,
            result.safe_to_send,
            result.status.value,
            prospect_id
        )
        
        logger.info(
            "Email verification complete",
            prospect_id=prospect_id,
            email=prospect['email'][:5] + "***",
            status=result.status.value,
            safe_to_send=result.safe_to_send
        )
        
        return result
    
    async def verify_batch(
        self,
        limit: int = 100,
        only_unverified: bool = True
    ) -> dict:
        """
        Verify a batch of prospect emails.
        
        Args:
            limit: Maximum prospects to verify
            only_unverified: Only verify prospects without verification
        
        Returns:
            Summary dict with counts
        """
        # Get prospects needing verification
        query = """
            SELECT id, email FROM marketing_prospects
            WHERE email IS NOT NULL
              AND status IN ('discovered', 'enriched')
        """
        
        if only_unverified:
            query += " AND email_verified IS NOT TRUE"
        
        query += f"""
            ORDER BY relevance_score DESC
            LIMIT {limit}
        """
        
        prospects = await self.db.fetch(query)
        
        logger.info(f"Verifying {len(prospects)} prospect emails")
        
        results = {
            "total": len(prospects),
            "valid": 0,
            "invalid": 0,
            "catch_all": 0,
            "unknown": 0,
            "errors": 0
        }
        
        for prospect in prospects:
            result = await self.verify_and_update_prospect(str(prospect['id']))
            
            if result.status == VerificationStatus.VALID:
                results["valid"] += 1
            elif result.status == VerificationStatus.INVALID:
                results["invalid"] += 1
            elif result.status == VerificationStatus.CATCH_ALL:
                results["catch_all"] += 1
            elif result.status == VerificationStatus.ERROR:
                results["errors"] += 1
            else:
                results["unknown"] += 1
        
        logger.info("Email verification batch complete", **results)
        return results


# Alternative: Hunter.io integration (if preferred over Clearout)
class HunterClient:
    """
    Alternative email verification using Hunter.io
    
    Pricing: $49/mo for 1,000 verifications
    
    Hunter provides similar features to Clearout but also includes
    email finding capabilities if needed later.
    """
    
    BASE_URL = "https://api.hunter.io/v2"
    
    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.hunter_api_key
    
    async def verify_email(self, email: str) -> VerificationResult:
        """Verify email using Hunter.io API."""
        
        if not self.api_key:
            return VerificationResult(
                email=email,
                status=VerificationStatus.UNKNOWN,
                safe_to_send=True,
                reason="Hunter.io not configured"
            )
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    f"{self.BASE_URL}/email-verifier",
                    params={
                        "email": email,
                        "api_key": self.api_key
                    }
                )
                
                if response.status_code == 200:
                    data = response.json().get("data", {})
                    
                    # Hunter status: valid, invalid, accept_all, webmail, disposable, unknown
                    status = data.get("status", "unknown")
                    
                    status_map = {
                        "valid": VerificationStatus.VALID,
                        "invalid": VerificationStatus.INVALID,
                        "accept_all": VerificationStatus.CATCH_ALL,
                        "disposable": VerificationStatus.DISPOSABLE,
                        "unknown": VerificationStatus.UNKNOWN,
                    }
                    
                    return VerificationResult(
                        email=email,
                        status=status_map.get(status, VerificationStatus.UNKNOWN),
                        safe_to_send=status in ["valid", "accept_all"],
                        is_disposable=(status == "disposable"),
                        raw_response=data
                    )
                else:
                    return VerificationResult(
                        email=email,
                        status=VerificationStatus.ERROR,
                        safe_to_send=False,
                        reason=f"API error: {response.status_code}"
                    )
                    
        except Exception as e:
            return VerificationResult(
                email=email,
                status=VerificationStatus.ERROR,
                safe_to_send=False,
                reason=str(e)
            )


# Factory function to get configured verification client
def get_verification_client():
    """Get the configured email verification client."""
    settings = get_settings()
    
    if settings.clearout_api_key:
        return ClearoutClient()
    elif settings.hunter_api_key:
        return HunterClient()
    else:
        logger.warning("No email verification service configured")
        return ClearoutClient()  # Will return unknown status
