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
    UNKNOWN = "unknown"


@dataclass
class VerificationResult:
    email: str
    status: VerificationStatus
    is_deliverable: bool


class BouncerClient:
    BASE_URL = "https://api.usebouncer.com/v1.1"
    
    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.bouncer_api_key
    
    async def verify_email(self, email: str) -> VerificationResult:
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    f"{self.BASE_URL}/email/verify",
                    params={"email": email},
                    headers={"x-api-key": self.api_key}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    status = data.get("status", "unknown")
                    
                    if status == "deliverable":
                        return VerificationResult(email, VerificationStatus.VALID, True)
                    elif status == "undeliverable":
                        return VerificationResult(email, VerificationStatus.INVALID, False)
                    elif status == "risky":
                        return VerificationResult(email, VerificationStatus.CATCH_ALL, False)
                    
                return VerificationResult(email, VerificationStatus.UNKNOWN, False)
                
            except Exception as e:
                logger.error("Bouncer verification failed", error=str(e))
                return VerificationResult(email, VerificationStatus.UNKNOWN, False)
    
    async def verify_batch(self, limit: int = 100, only_unverified: bool = True) -> dict:
        db = await get_database_async()
        results = {"processed": 0, "valid": 0, "invalid": 0, "catch_all": 0, "unknown": 0, "errors": 0}
        
        prospects = await db.fetch(
            "SELECT id, email FROM marketing_prospects WHERE email IS NOT NULL AND email_verified = FALSE ORDER BY relevance_score DESC LIMIT $1",
            limit
        )
        
        for prospect in prospects:
            results["processed"] += 1
            try:
                result = await self.verify_email(prospect["email"])
                results[result.status.value] += 1
                
                is_verified = result.status == VerificationStatus.VALID
                await db.execute(
                    "UPDATE marketing_prospects SET email_verified = $1, verification_status = $2, verified_at = NOW() WHERE id = $3",
                    is_verified, result.status.value, prospect["id"]
                )
                
                await asyncio.sleep(0.15)
            except Exception as e:
                results["errors"] += 1
        
        return results


class ClearoutClient:
    BASE_URL = "https://api.clearout.io/v2"
    
    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.clearout_api_key
    
    async def verify_email(self, email: str) -> VerificationResult:
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(
                    f"{self.BASE_URL}/email_verify/instant",
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    json={"email": email}
                )
                
                if response.status_code == 200:
                    data = response.json().get("data", {})
                    status = data.get("status", "unknown")
                    
                    status_map = {
                        "valid": VerificationStatus.VALID,
                        "invalid": VerificationStatus.INVALID,
                        "catch_all": VerificationStatus.CATCH_ALL
                    }
                    
                    return VerificationResult(
                        email,
                        status_map.get(status, VerificationStatus.UNKNOWN),
                        status == "valid"
                    )
                
                return VerificationResult(email, VerificationStatus.UNKNOWN, False)
                
            except Exception as e:
                return VerificationResult(email, VerificationStatus.UNKNOWN, False)
    
    async def verify_batch(self, limit: int = 100, only_unverified: bool = True) -> dict:
        db = await get_database_async()
        results = {"processed": 0, "valid": 0, "invalid": 0, "catch_all": 0, "unknown": 0, "errors": 0}
        
        prospects = await db.fetch(
            "SELECT id, email FROM marketing_prospects WHERE email IS NOT NULL AND email_verified = FALSE ORDER BY relevance_score DESC LIMIT $1",
            limit
        )
        
        for prospect in prospects:
            results["processed"] += 1
            try:
                result = await self.verify_email(prospect["email"])
                results[result.status.value] += 1
                
                is_verified = result.status == VerificationStatus.VALID
                await db.execute(
                    "UPDATE marketing_prospects SET email_verified = $1, verification_status = $2, verified_at = NOW() WHERE id = $3",
                    is_verified, result.status.value, prospect["id"]
                )
                
                await asyncio.sleep(0.2)
            except Exception as e:
                results["errors"] += 1
        
        return results


class HunterClient:
    BASE_URL = "https://api.hunter.io/v2"
    
    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.hunter_api_key
    
    async def verify_email(self, email: str) -> VerificationResult:
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    f"{self.BASE_URL}/email-verifier",
                    params={"email": email, "api_key": self.api_key}
                )
                
                if response.status_code == 200:
                    data = response.json().get("data", {})
                    result = data.get("result", "unknown")
                    
                    if result == "deliverable":
                        return VerificationResult(email, VerificationStatus.VALID, True)
                    elif result == "undeliverable":
                        return VerificationResult(email, VerificationStatus.INVALID, False)
                    elif result == "risky":
                        return VerificationResult(email, VerificationStatus.CATCH_ALL, False)
                
                return VerificationResult(email, VerificationStatus.UNKNOWN, False)
                
            except Exception as e:
                return VerificationResult(email, VerificationStatus.UNKNOWN, False)
    
    async def verify_batch(self, limit: int = 100, only_unverified: bool = True) -> dict:
        db = await get_database_async()
        results = {"processed": 0, "valid": 0, "invalid": 0, "catch_all": 0, "unknown": 0, "errors": 0}
        
        prospects = await db.fetch(
            "SELECT id, email FROM marketing_prospects WHERE email IS NOT NULL AND email_verified = FALSE ORDER BY relevance_score DESC LIMIT $1",
            limit
        )
        
        for prospect in prospects:
            results["processed"] += 1
            try:
                result = await self.verify_email(prospect["email"])
                results[result.status.value] += 1
                
                is_verified = result.status == VerificationStatus.VALID
                await db.execute(
                    "UPDATE marketing_prospects SET email_verified = $1, verification_status = $2, verified_at = NOW() WHERE id = $3",
                    is_verified, result.status.value, prospect["id"]
                )
                
                await asyncio.sleep(0.5)
            except Exception as e:
                results["errors"] += 1
        
        return results


def get_verification_client():
    settings = get_settings()
    
    if settings.bouncer_api_key:
        return BouncerClient()
    if settings.clearout_api_key:
        return ClearoutClient()
    if settings.hunter_api_key:
        return HunterClient()
    
    raise ValueError("No email verification service configured")
