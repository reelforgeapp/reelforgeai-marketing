"""
ReelForge Marketing Engine - Main Application
"""

import json
import re
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import JSONResponse
import uvicorn
import structlog

from app.config import get_settings
from app.database import init_database, close_database, get_database_async

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

if settings.sentry_dsn:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.celery import CeleryIntegration
        
        sentry_sdk.init(
            dsn=settings.sentry_dsn,
            integrations=[FastApiIntegration(), CeleryIntegration()],
            traces_sample_rate=0.1,
            environment=settings.environment,
        )
        logger.info("Sentry initialized")
    except ImportError:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting ReelForge Marketing Engine")
    await init_database()
    yield
    await close_database()
    logger.info("Shutdown complete")


app = FastAPI(
    title="ReelForge Marketing Engine",
    version="2.0.0",
    lifespan=lifespan
)


async def validate_brevo_webhook(request: Request) -> bytes:
    """Validate Brevo webhook HMAC signature."""
    import hmac
    import hashlib
    
    if not settings.brevo_webhook_secret:
        return await request.body()
    
    signature = request.headers.get("X-Sib-Signature", "") or request.headers.get("X-Brevo-Signature", "")
    body = await request.body()
    
    expected = hmac.new(
        settings.brevo_webhook_secret.encode(),
        body,
        hashlib.sha256
    ).hexdigest()
    
    signature_clean = signature.replace("sha256=", "").lower()
    
    if not hmac.compare_digest(signature_clean, expected.lower()):
        raise HTTPException(status_code=401, detail="Invalid signature")
    
    return body


@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": "2.0.0", "timestamp": datetime.utcnow().isoformat()}


@app.get("/status")
async def get_status():
    db = await get_database_async()
    
    try:
        counts = await db.fetchrow("""
            SELECT
                (SELECT COUNT(*) FROM marketing_prospects) as prospects,
                (SELECT COUNT(*) FROM marketing_prospects WHERE email_verified = TRUE) as verified,
                (SELECT COUNT(*) FROM outreach_sequences WHERE status = 'active') as active_sequences,
                (SELECT COUNT(*) FROM affiliates WHERE status = 'active') as affiliates
        """)
    except Exception:
        counts = {"prospects": 0, "verified": 0, "active_sequences": 0, "affiliates": 0}
    
    verification_service = None
    if settings.bouncer_api_key:
        verification_service = "bouncer"
    elif settings.clearout_api_key:
        verification_service = "clearout"
    elif settings.hunter_api_key:
        verification_service = "hunter"
    
    return {
        "status": "running",
        "version": "2.0.0",
        "totals": dict(counts) if counts else {},
        "features": {
            "email_verification": verification_service,
            "webhook_validation": bool(settings.brevo_webhook_secret),
        }
    }


@app.post("/webhooks/brevo")
async def brevo_webhook(request: Request, body: bytes = Depends(validate_brevo_webhook)):
    try:
        payload = json.loads(body)
        db = await get_database_async()
        
        event_type = payload.get("event")
        message_id = payload.get("message-id")
        email = payload.get("email")
        
        timestamp_str = payload.get("date") or payload.get("ts_event")
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')) if timestamp_str else datetime.utcnow()
        except:
            timestamp = datetime.utcnow()
        
        if event_type == "delivered":
            await db.execute("UPDATE email_sends SET status = 'delivered', delivered_at = $1 WHERE brevo_message_id = $2", timestamp, message_id)
        elif event_type in ("opened", "uniqueOpened"):
            await db.execute("UPDATE email_sends SET status = 'opened', first_opened_at = COALESCE(first_opened_at, $1), open_count = COALESCE(open_count, 0) + 1 WHERE brevo_message_id = $2", timestamp, message_id)
        elif event_type in ("clicked", "uniqueClicked"):
            await db.execute("UPDATE email_sends SET status = 'clicked', first_clicked_at = COALESCE(first_clicked_at, $1), click_count = COALESCE(click_count, 0) + 1 WHERE brevo_message_id = $2", timestamp, message_id)
        elif event_type in ("hardBounce", "softBounce"):
            await db.execute("UPDATE email_sends SET status = 'bounced', bounced_at = $1 WHERE brevo_message_id = $2", timestamp, message_id)
            if email:
                await db.execute("UPDATE marketing_prospects SET status = 'bounced' WHERE email = $1", email)
        elif event_type == "unsubscribed":
            await db.execute("UPDATE email_sends SET status = 'unsubscribed' WHERE brevo_message_id = $1", message_id)
            if email:
                await db.execute("UPDATE marketing_prospects SET status = 'unsubscribed' WHERE email = $1", email)
        elif event_type == "complaint" and email:
            await db.execute("UPDATE marketing_prospects SET status = 'complained' WHERE email = $1", email)
        
        return {"status": "processed", "event": event_type}
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")


@app.post("/trigger/youtube-discovery")
async def trigger_youtube_discovery():
    try:
        from tasks.discovery_tasks import run_youtube_discovery
        task = run_youtube_discovery.delay()
        return {"status": "triggered", "task_id": task.id}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/trigger/email-extraction")
async def trigger_email_extraction():
    try:
        from tasks.enrichment_tasks import run_email_extraction
        task = run_email_extraction.delay()
        return {"status": "triggered", "task_id": task.id}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/trigger/email-verification")
async def trigger_email_verification():
    try:
        from tasks.enrichment_tasks import run_email_verification
        task = run_email_verification.delay()
        return {"status": "triggered", "task_id": task.id}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/trigger/sequence-processing")
async def trigger_sequence_processing():
    try:
        from tasks.outreach_tasks import process_pending_sequences
        task = process_pending_sequences.delay()
        return {"status": "triggered", "task_id": task.id}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/trigger/auto-enroll")
async def trigger_auto_enroll():
    try:
        from tasks.outreach_tasks import auto_enroll_prospects
        task = auto_enroll_prospects.delay()
        return {"status": "triggered", "task_id": task.id}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    try:
        from celery.result import AsyncResult
        from celery_config import celery_app
        result = AsyncResult(task_id, app=celery_app)
        return {"task_id": task_id, "status": result.status, "result": result.result if result.ready() else None}
    except Exception as e:
        return {"task_id": task_id, "status": "error", "error": str(e)}


@app.delete("/admin/gdpr/delete")
async def gdpr_delete(email: str, admin_key: str):
    if admin_key != settings.admin_api_key:
        raise HTTPException(status_code=401, detail="Invalid admin key")
    
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
        raise HTTPException(status_code=400, detail="Invalid email format")
    
    try:
        from tasks.maintenance_tasks import handle_gdpr_deletion_request
        task = handle_gdpr_deletion_request.delay(email)
        return {"status": "deletion_requested", "task_id": task.id}
    except Exception as e:
        return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080)
