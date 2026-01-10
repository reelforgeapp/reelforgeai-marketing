"""
ReelForge Marketing Engine - Webhook Security
HMAC signature validation for Brevo webhooks
"""

import hmac
import hashlib
from typing import Optional, Callable
from functools import wraps

from fastapi import Request, HTTPException, Depends
from fastapi.security import APIKeyHeader
import structlog

from app.config import get_settings

logger = structlog.get_logger()


class WebhookValidator:
    """
    HMAC signature validator for webhook endpoints.
    
    Brevo sends webhooks with a signature header that we can validate
    to ensure the request actually came from Brevo.
    
    Signature format: sha256={hex_digest}
    
    To set up in Brevo:
    1. Go to Settings → Webhooks
    2. Add webhook URL: https://your-domain.com/webhooks/brevo
    3. Set a signing secret (copy to BREVO_WEBHOOK_SECRET env var)
    4. Brevo will include X-Brevo-Signature header on all webhooks
    """
    
    SIGNATURE_HEADER = "X-Brevo-Signature"
    SIGNATURE_PREFIX = "sha256="
    
    def __init__(self, secret: str):
        self.secret = secret.encode('utf-8')
    
    def compute_signature(self, payload: bytes) -> str:
        """Compute HMAC-SHA256 signature of payload."""
        digest = hmac.new(
            self.secret,
            payload,
            hashlib.sha256
        ).hexdigest()
        
        return f"{self.SIGNATURE_PREFIX}{digest}"
    
    def validate_signature(
        self,
        payload: bytes,
        signature: str
    ) -> bool:
        """
        Validate that the signature matches the payload.
        
        Uses constant-time comparison to prevent timing attacks.
        """
        if not signature:
            return False
        
        expected = self.compute_signature(payload)
        
        # Constant-time comparison
        return hmac.compare_digest(signature, expected)


# Global validator instance
_webhook_validator: Optional[WebhookValidator] = None


def get_webhook_validator() -> WebhookValidator:
    """Get the webhook validator singleton."""
    global _webhook_validator
    if _webhook_validator is None:
        settings = get_settings()
        secret = settings.brevo_webhook_secret
        
        if not secret:
            logger.warning(
                "BREVO_WEBHOOK_SECRET not set - webhook validation disabled"
            )
            secret = "insecure-development-secret"
        
        _webhook_validator = WebhookValidator(secret)
    
    return _webhook_validator


async def validate_brevo_webhook(request: Request) -> bytes:
    """
    FastAPI dependency that validates Brevo webhook signatures.
    
    Usage:
        @app.post("/webhooks/brevo")
        async def brevo_webhook(
            body: bytes = Depends(validate_brevo_webhook)
        ):
            payload = json.loads(body)
            ...
    
    Raises:
        HTTPException 401 if signature is invalid
        HTTPException 400 if signature header is missing
    """
    settings = get_settings()
    
    # Skip validation in development if secret not set
    if settings.environment == "development" and not settings.brevo_webhook_secret:
        logger.warning("Skipping webhook validation in development")
        return await request.body()
    
    # Get signature from header
    signature = request.headers.get(WebhookValidator.SIGNATURE_HEADER)
    
    if not signature:
        logger.warning(
            "Webhook request missing signature header",
            path=request.url.path,
            ip=request.client.host if request.client else "unknown"
        )
        raise HTTPException(
            status_code=400,
            detail="Missing signature header"
        )
    
    # Get request body
    body = await request.body()
    
    # Validate signature
    validator = get_webhook_validator()
    
    if not validator.validate_signature(body, signature):
        logger.error(
            "Webhook signature validation failed",
            path=request.url.path,
            ip=request.client.host if request.client else "unknown",
            signature_received=signature[:20] + "..."
        )
        raise HTTPException(
            status_code=401,
            detail="Invalid signature"
        )
    
    logger.debug("Webhook signature validated successfully")
    return body


def require_webhook_signature(func: Callable) -> Callable:
    """
    Decorator for webhook endpoints that require signature validation.
    
    Alternative to using Depends() in route definition.
    
    Usage:
        @app.post("/webhooks/brevo")
        @require_webhook_signature
        async def brevo_webhook(request: Request):
            ...
    """
    @wraps(func)
    async def wrapper(request: Request, *args, **kwargs):
        await validate_brevo_webhook(request)
        return await func(request, *args, **kwargs)
    
    return wrapper


# Example usage in FastAPI routes:
"""
from fastapi import FastAPI, Request, Depends
from security.webhook_validator import validate_brevo_webhook
import json

app = FastAPI()

@app.post("/webhooks/brevo")
async def brevo_webhook(
    body: bytes = Depends(validate_brevo_webhook)
):
    '''
    Handle Brevo email events.
    
    The body parameter is already validated by the time we get here.
    If signature validation fails, a 401 response is returned automatically.
    '''
    payload = json.loads(body)
    
    event_type = payload.get("event")
    message_id = payload.get("message-id")
    
    # Process the event...
    
    return {"status": "processed"}
"""
