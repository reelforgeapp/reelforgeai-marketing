"""
ReelForge Marketing Engine - Main Application
FastAPI application with Celery, Sentry, and webhook validation
"""

import json
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import JSONResponse
import uvicorn
import structlog

from app.config import get_settings
from app.database import init_database, close_database, get_database

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()
settings = get_settings()


# Initialize Sentry if configured
if settings.sentry_dsn:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.celery import CeleryIntegration
        
        sentry_sdk.init(
            dsn=settings.sentry_dsn,
            integrations=[
                FastApiIntegration(transaction_style="endpoint"),
                CeleryIntegration(),
            ],
            traces_sample_rate=0.1,
            environment=settings.environment,
            release="reelforge-marketing@2.0.0",
        )
        logger.info("Sentry initialized", environment=settings.environment)
    except ImportError:
        logger.warning("Sentry SDK not installed, error monitoring disabled")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    logger.info("Starting ReelForge Marketing Engine")
    
    await init_database()
    
    logger.info(
        "Application started",
        environment=settings.environment
    )
    
    yield
    
    # Shutdown
    logger.info("Shutting down Marketing Engine")
    await close_database()
    logger.info("Shutdown complete")


app = FastAPI(
    title="ReelForge Marketing Engine",
    description="Automated affiliate recruitment and outreach",
    version="2.0.0",
    lifespan=lifespan
)


# =============================================================================
# Webhook Validation Dependency
# =============================================================================

async def validate_brevo_webhook(request: Request) -> bytes:
    """Validate Brevo webhook HMAC signature."""
    import hmac
    import hashlib
    
    # Skip validation if no secret configured
    if not settings.brevo_webhook_secret:
        logger.warning("Webhook validation disabled - no secret configured")
        return await request.body()
    
    signature = request.headers.get("X-Brevo-Signature", "")
    body = await request.body()
    
    expected = hmac.new(
        settings.brevo_webhook_secret.encode(),
        body,
        hashlib.sha256
    ).hexdigest()
    
    if not hmac.compare_digest(signature, f"sha256={expected}"):
        logger.error("Webhook signature validation failed")
        raise HTTPException(status_code=401, detail="Invalid signature")
    
    return body


# =============================================================================
# Health & Status Endpoints
# =============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint for Render."""
    return {
        "status": "healthy",
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "environment": settings.environment
    }


@app.get("/status")
async def get_status():
    """Get worker status and statistics."""
    db = get_database()
    
    # Get counts
    try:
        counts = await db.fetchrow(
            """
            SELECT
                (SELECT COUNT(*) FROM marketing_prospects) as prospects,
                (SELECT COUNT(*) FROM marketing_prospects WHERE email_verified = TRUE) as verified,
                (SELECT COUNT(*) FROM outreach_sequences WHERE status = 'active') as active_sequences,
                (SELECT COUNT(*) FROM affiliates WHERE status = 'active') as affiliates
            """
        )
    except Exception:
        counts = {"prospects": 0, "verified": 0, "active_sequences": 0, "affiliates": 0}
    
    # Check Redis connectivity
    redis_status = "unknown"
    try:
        import redis.asyncio as redis_lib
        r = redis_lib.from_url(settings.redis_url)
        await r.ping()
        redis_status = "connected"
        await r.close()
    except Exception as e:
        redis_status = f"error: {str(e)[:50]}"
    
    return {
        "status": "running",
        "version": "2.0.0",
        "redis_status": redis_status,
        "totals": dict(counts) if counts else {},
        "features": {
            "email_verification": bool(settings.clearout_api_key or settings.hunter_api_key),
            "webhook_validation": bool(settings.brevo_webhook_secret),
            "error_monitoring": bool(settings.sentry_dsn)
        }
    }


# =============================================================================
# Webhook Endpoints
# =============================================================================

@app.post("/webhooks/brevo")
async def brevo_webhook(
    request: Request,
    body: bytes = Depends(validate_brevo_webhook)
):
    """Handle Brevo email event webhooks."""
    try:
        payload = json.loads(body)
        db = get_database()
        
        event_type = payload.get("event")
        message_id = payload.get("message-id")
        email = payload.get("email")
        
        logger.info(f"Received Brevo webhook: {event_type}", message_id=message_id)
        
        # Update email_sends based on event
        if event_type == "delivered":
            await db.execute(
                "UPDATE email_sends SET status = 'delivered', delivered_at = NOW() WHERE brevo_message_id = $1",
                message_id
            )
        elif event_type == "opened":
            await db.execute(
                """UPDATE email_sends SET status = 'opened', first_opened_at = COALESCE(first_opened_at, NOW()), 
                   open_count = open_count + 1 WHERE brevo_message_id = $1""",
                message_id
            )
        elif event_type == "clicked":
            await db.execute(
                """UPDATE email_sends SET status = 'clicked', first_clicked_at = COALESCE(first_clicked_at, NOW()),
                   click_count = click_count + 1 WHERE brevo_message_id = $1""",
                message_id
            )
        elif event_type in ("hardBounce", "softBounce"):
            await db.execute(
                "UPDATE email_sends SET status = 'bounced', bounced_at = NOW() WHERE brevo_message_id = $1",
                message_id
            )
            # Mark prospect as bounced
            await db.execute(
                "UPDATE marketing_prospects SET status = 'bounced' WHERE email = $1",
                email
            )
        elif event_type == "unsubscribed":
            await db.execute(
                "UPDATE email_sends SET status = 'unsubscribed' WHERE brevo_message_id = $1",
                message_id
            )
            await db.execute(
                "UPDATE marketing_prospects SET status = 'unsubscribed' WHERE email = $1",
                email
            )
        elif event_type == "complaint":
            await db.execute(
                "UPDATE marketing_prospects SET status = 'complained' WHERE email = $1",
                email
            )
        
        return {"status": "processed", "event": event_type}
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    except Exception as e:
        logger.error(f"Webhook processing failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Manual Trigger Endpoints
# =============================================================================

@app.post("/trigger/youtube-discovery")
async def trigger_youtube_discovery():
    """Manually trigger YouTube discovery job."""
    try:
        from tasks.discovery_tasks import run_youtube_discovery
        task = run_youtube_discovery.delay()
        return {"status": "triggered", "task_id": task.id}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/trigger/email-extraction")
async def trigger_email_extraction():
    """Manually trigger email extraction job."""
    try:
        from tasks.enrichment_tasks import run_email_extraction
        task = run_email_extraction.delay()
        return {"status": "triggered", "task_id": task.id}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/trigger/email-verification")
async def trigger_email_verification():
    """Manually trigger email verification job."""
    try:
        from tasks.enrichment_tasks import run_email_verification
        task = run_email_verification.delay()
        return {"status": "triggered", "task_id": task.id}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/trigger/sequence-processing")
async def trigger_sequence_processing():
    """Manually trigger sequence processing job."""
    try:
        from tasks.outreach_tasks import process_pending_sequences
        task = process_pending_sequences.delay()
        return {"status": "triggered", "task_id": task.id}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/trigger/auto-enroll")
async def trigger_auto_enroll():
    """Manually trigger auto-enrollment job."""
    try:
        from tasks.outreach_tasks import auto_enroll_prospects
        task = auto_enroll_prospects.delay()
        return {"status": "triggered", "task_id": task.id}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# =============================================================================
# Task Status Endpoint
# =============================================================================

@app.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    """Get status of a Celery task."""
    try:
        from celery.result import AsyncResult
        from celery_config import celery_app
        
        result = AsyncResult(task_id, app=celery_app)
        
        return {
            "task_id": task_id,
            "status": result.status,
            "result": result.result if result.ready() else None
        }
    except Exception as e:
        return {"task_id": task_id, "status": "error", "error": str(e)}


# =============================================================================
# GDPR Admin Endpoints
# =============================================================================

@app.delete("/admin/gdpr/delete")
async def gdpr_delete(email: str, admin_key: str):
    """Delete all data for a specific email (GDPR right to erasure)."""
    if admin_key != settings.admin_api_key:
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    try:
        from tasks.maintenance_tasks import handle_gdpr_deletion_request
        task = handle_gdpr_deletion_request.delay(email)
        return {"status": "deletion_requested", "task_id": task.id}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Run the marketing engine API server."""
    uvicorn.run(
        "app.main:app",
        host=settings.webhook_host,
        port=settings.webhook_port,
        log_level=settings.log_level.lower(),
        reload=settings.environment == "development"
    )


if __name__ == "__main__":
    main()
