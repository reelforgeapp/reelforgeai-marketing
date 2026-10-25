"""
ReelForge Marketing Engine - Main Application
"""

import base64
import binascii
import json
import uuid
import hmac
import hashlib
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request, HTTPException, Depends, Header
from fastapi.responses import HTMLResponse, RedirectResponse
import structlog

from app.config import get_settings
from app.database import init_database, close_database, get_database_async

from rpi.core.redaction import structlog_redactor

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog_redactor,
        structlog.processors.JSONRenderer()
    ]
)

logger = structlog.get_logger()
settings = get_settings()

if settings.sentry_dsn:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        sentry_sdk.init(dsn=settings.sentry_dsn, integrations=[FastApiIntegration()], traces_sample_rate=0.1)
    except ImportError:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_database()
    yield
    await close_database()


APP_VERSION = "4.13.0"

app = FastAPI(title="ReelForge Marketing Engine", version=APP_VERSION, lifespan=lifespan)

# ReelForge Partner Intelligence (docs/rpi/): /rpi/api/v1, /rpi/public, /rpi/ui
from rpi.api.router import mount as mount_rpi

mount_rpi(app)


DEFAULT_ADMIN_API_KEY = "change-me-in-production"


async def require_admin_key(x_admin_key: Optional[str] = Header(default=None)) -> None:
    """Protects operational endpoints (triggers, keyword management, task status).

    Fails closed: when ADMIN_API_KEY is unset or still the placeholder, every
    request is refused.
    """
    configured = settings.admin_api_key or ""
    if not configured or configured == DEFAULT_ADMIN_API_KEY:
        logger.error("ADMIN_API_KEY is not configured; refusing admin endpoint request")
        raise HTTPException(status_code=401, detail="Missing or invalid X-Admin-Key")
    if not x_admin_key or not hmac.compare_digest(x_admin_key.encode(), configured.encode()):
        raise HTTPException(status_code=401, detail="Missing or invalid X-Admin-Key")


ADMIN_ONLY = [Depends(require_admin_key)]


def _brevo_credentials_valid(request: Request, body: bytes, secret: str) -> bool:
    """Brevo authenticates webhooks with a bearer token, credentials in the
    notify URL (basic auth) or custom headers; it does not sign payloads
    (https://developers.brevo.com/docs/secured-webhooks). Accept the shared
    secret through any of those, plus the legacy HMAC signature header."""
    expected = secret.encode()
    authorization = request.headers.get("authorization", "")
    scheme, _, credentials = authorization.partition(" ")
    if scheme.lower() == "bearer" and credentials:
        if hmac.compare_digest(credentials.strip().encode(), expected):
            return True
    if scheme.lower() == "basic" and credentials:
        try:
            _, _, password = base64.b64decode(credentials.strip()).decode().partition(":")
        except (binascii.Error, UnicodeDecodeError):
            password = ""
        if password and hmac.compare_digest(password.encode(), expected):
            return True
    token = request.headers.get("x-brevo-webhook-token", "")
    if token and hmac.compare_digest(token.encode(), expected):
        return True
    signature = request.headers.get("X-Sib-Signature", "") or request.headers.get("X-Brevo-Signature", "")
    if signature:
        digest = hmac.new(expected, body, hashlib.sha256).hexdigest()
        if hmac.compare_digest(signature.replace("sha256=", "").lower().encode(), digest.encode()):
            return True
    return False


async def validate_brevo_webhook(request: Request) -> bytes:
    body = await request.body()
    if not settings.brevo_webhook_secret:
        if settings.environment == "development":
            logger.warning("BREVO_WEBHOOK_SECRET not set; accepting unauthenticated webhook (development only)")
            return body
        logger.error("BREVO_WEBHOOK_SECRET not set; rejecting Brevo webhook")
        raise HTTPException(status_code=401, detail="Webhook authentication is not configured")
    if not _brevo_credentials_valid(request, body, settings.brevo_webhook_secret):
        raise HTTPException(status_code=401, detail="Invalid webhook credentials")
    return body


async def _record_unsubscribe(prospect_id: str) -> bool:
    """Mark the prospect unsubscribed and stop active sequences. Idempotent."""
    db = await get_database_async()
    try:
        async with db.acquire() as conn:
            async with conn.transaction():
                exists = await conn.fetchval("SELECT 1 FROM marketing_prospects WHERE id = $1::uuid", prospect_id)
                if not exists:
                    return False
                await conn.execute(
                    "UPDATE marketing_prospects SET status = 'unsubscribed' WHERE id = $1::uuid AND status IS DISTINCT FROM 'unsubscribed'",
                    prospect_id,
                )
                await conn.execute("""
                    UPDATE outreach_sequences SET status = 'stopped', stopped_reason = 'unsubscribed', completed_at = NOW()
                    WHERE prospect_id = $1::uuid AND status IN ('pending', 'active')
                """, prospect_id)
        return True
    finally:
        await db.close()


def _valid_unsubscribe(prospect_id: str, token: str) -> bool:
    from services.compliance import verify_unsubscribe_token
    try:
        uuid.UUID(prospect_id)
    except ValueError:
        return False
    return verify_unsubscribe_token(settings, prospect_id, token)


UNSUBSCRIBED_PAGE = (
    "<!doctype html><meta charset='utf-8'><meta name='viewport' content='width=device-width'>"
    "<title>Unsubscribed</title><body style='font-family:system-ui,sans-serif;max-width:32rem;margin:4rem auto;padding:0 1rem'>"
    "<h1 style='font-size:1.4rem'>You're unsubscribed</h1>"
    "<p>ReelForge AI won't send you any more partnership emails.</p></body>"
)
INVALID_LINK_PAGE = (
    "<!doctype html><meta charset='utf-8'><title>Link not valid</title>"
    "<body style='font-family:system-ui,sans-serif;max-width:32rem;margin:4rem auto;padding:0 1rem'>"
    "<p>This unsubscribe link isn't valid. Reply \"unsubscribe\" to any of our emails and we'll remove you.</p></body>"
)


@app.get("/unsubscribe/{prospect_id}", response_class=HTMLResponse, include_in_schema=False)
async def unsubscribe_page(prospect_id: str, t: str = ""):
    """Unsubscribe link in every outreach email (signed per prospect)."""
    if not _valid_unsubscribe(prospect_id, t):
        return HTMLResponse(INVALID_LINK_PAGE, status_code=400)
    await _record_unsubscribe(prospect_id)
    return HTMLResponse(UNSUBSCRIBED_PAGE)


@app.post("/unsubscribe/{prospect_id}", include_in_schema=False)
async def unsubscribe_one_click(prospect_id: str, t: str = ""):
    """RFC 8058 one-click unsubscribe (List-Unsubscribe-Post) from mail clients."""
    if not _valid_unsubscribe(prospect_id, t):
        raise HTTPException(status_code=400, detail="Invalid unsubscribe link")
    await _record_unsubscribe(prospect_id)
    return {"status": "unsubscribed"}


@app.get("/", include_in_schema=False)
async def root():
    # The only browser UI is ReelForge Partner Intelligence. A relative Location
    # keeps the https scheme behind Render's proxy.
    return RedirectResponse("/rpi/ui")


@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": APP_VERSION}


@app.get("/status")
async def get_status():
    db = None
    try:
        db = await get_database_async()
        
        counts = await db.fetchrow("""
            SELECT
                (SELECT COUNT(*) FROM marketing_prospects) as prospects,
                (SELECT COUNT(*) FROM marketing_prospects WHERE email IS NOT NULL) as with_email,
                (SELECT COUNT(*) FROM marketing_prospects WHERE email_verified = TRUE) as verified,
                (SELECT COUNT(*) FROM outreach_sequences WHERE status = 'pending') as pending_sequences,
                (SELECT COUNT(*) FROM outreach_sequences WHERE status = 'active') as active_sequences,
                (SELECT COUNT(*) FROM outreach_sequences WHERE status = 'completed') as completed_sequences,
                (SELECT COUNT(*) FROM email_sends) as total_emails_sent,
                (SELECT COUNT(*) FROM email_sends WHERE status = 'delivered') as delivered,
                (SELECT COUNT(*) FROM email_sends WHERE status = 'opened') as opened,
                (SELECT COUNT(*) FROM email_sends WHERE status = 'clicked') as clicked,
                (SELECT COUNT(*) FROM affiliates WHERE status = 'active') as affiliates
        """)
    except Exception as e:
        logger.error("Status check failed", error=str(e))
        counts = None
    finally:
        if db:
            await db.close()
    
    verification_service = None
    if settings.bouncer_api_key:
        verification_service = "bouncer"
    elif settings.clearout_api_key:
        verification_service = "clearout"
    elif settings.hunter_api_key:
        verification_service = "hunter"
    
    return {
        "status": "running",
        "version": APP_VERSION,
        "totals": dict(counts) if counts else {},
        "features": {
            "email_verification": verification_service,
            "webhook_validation": bool(settings.brevo_webhook_secret),
        }
    }


@app.post("/webhooks/brevo")
async def brevo_webhook(request: Request, body: bytes = Depends(validate_brevo_webhook)):
    """Handle Brevo webhook events with minimal DB connections and deduplication."""
    db = None
    try:
        payload = json.loads(body)

        event_type = payload.get("event")
        message_id = payload.get("message-id")
        email = payload.get("email")

        # Skip if no message_id to update
        if not message_id:
            return {"status": "skipped", "reason": "no message_id"}

        timestamp_str = payload.get("date") or payload.get("ts_event")
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')) if timestamp_str else datetime.utcnow()
        except (ValueError, AttributeError, TypeError):
            timestamp = datetime.utcnow()

        # Only connect to DB for events we care about
        if event_type in ("delivered", "opened", "uniqueOpened", "clicked", "uniqueClicked", "hardBounce", "softBounce", "unsubscribed"):
            db = await get_database_async()

            if event_type == "delivered":
                await db.execute("UPDATE email_sends SET status = 'delivered', delivered_at = $1 WHERE brevo_message_id = $2", timestamp, message_id)
            elif event_type in ("opened", "uniqueOpened"):
                # Atomically update and check if this was the first open using RETURNING
                # This prevents race conditions from concurrent webhook deliveries
                was_first_open = await db.fetchval("""
                    UPDATE email_sends
                    SET status = 'opened',
                        first_opened_at = COALESCE(first_opened_at, $1),
                        open_count = CASE WHEN first_opened_at IS NULL THEN 1 ELSE open_count END
                    WHERE brevo_message_id = $2
                    RETURNING (first_opened_at = $1) AS was_first
                """, timestamp, message_id)
                # Only update prospect stats if this was the first open
                if email and was_first_open:
                    await db.execute("UPDATE marketing_prospects SET total_emails_opened = COALESCE(total_emails_opened, 0) + 1 WHERE email = $1", email)
            elif event_type in ("clicked", "uniqueClicked"):
                # Atomically update and check if this was the first click using RETURNING
                # This prevents race conditions from concurrent webhook deliveries
                was_first_click = await db.fetchval("""
                    UPDATE email_sends
                    SET status = 'clicked',
                        first_clicked_at = COALESCE(first_clicked_at, $1),
                        click_count = CASE WHEN first_clicked_at IS NULL THEN 1 ELSE click_count END
                    WHERE brevo_message_id = $2
                    RETURNING (first_clicked_at = $1) AS was_first
                """, timestamp, message_id)
                # Only update prospect stats if this was the first click
                if email and was_first_click:
                    await db.execute("UPDATE marketing_prospects SET total_emails_clicked = COALESCE(total_emails_clicked, 0) + 1 WHERE email = $1", email)
            elif event_type in ("hardBounce", "softBounce"):
                # Only process bounce if not already bounced - use transaction for atomicity
                already_bounced = await db.fetchval("SELECT bounced_at IS NOT NULL FROM email_sends WHERE brevo_message_id = $1", message_id)
                if not already_bounced:
                    async with db.transaction():
                        await db.execute("UPDATE email_sends SET status = 'bounced', bounced_at = $1 WHERE brevo_message_id = $2", timestamp, message_id)
                        if email:
                            await db.execute("UPDATE marketing_prospects SET status = 'bounced' WHERE email = $1", email)
                            # Stop active sequences for bounced emails
                            await db.execute("""
                                UPDATE outreach_sequences SET status = 'stopped', stopped_reason = 'bounced', completed_at = NOW()
                                WHERE prospect_id IN (SELECT id FROM marketing_prospects WHERE email = $1)
                                AND status IN ('pending', 'active')
                            """, email)
            elif event_type == "unsubscribed":
                # Only process unsubscribe if not already processed - use transaction for atomicity
                current_status = await db.fetchval("SELECT status FROM email_sends WHERE brevo_message_id = $1", message_id)
                if current_status != 'unsubscribed':
                    async with db.transaction():
                        await db.execute("UPDATE email_sends SET status = 'unsubscribed' WHERE brevo_message_id = $1", message_id)
                        if email:
                            await db.execute("UPDATE marketing_prospects SET status = 'unsubscribed' WHERE email = $1", email)
                            # Stop active sequences for unsubscribed
                            await db.execute("""
                                UPDATE outreach_sequences SET status = 'stopped', stopped_reason = 'unsubscribed', completed_at = NOW()
                                WHERE prospect_id IN (SELECT id FROM marketing_prospects WHERE email = $1)
                                AND status IN ('pending', 'active')
                            """, email)

        # RPI outreach messages (additive; failures never affect the legacy path above)
        try:
            from app import database
            from rpi.outreach.events import handle_brevo_event

            if database._app_pool is not None:
                await handle_brevo_event(database._app_pool, payload)
        except Exception as rpi_error:
            logger.error("RPI Brevo event handling failed", error=str(rpi_error))

        return {"status": "processed", "event": event_type}
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    except Exception as e:
        logger.error("Webhook processing error", error=str(e))
        # Return 200 to prevent Brevo from retrying
        return {"status": "error", "message": str(e)}
    finally:
        if db:
            await db.close()


@app.post("/trigger/youtube-discovery", dependencies=ADMIN_ONLY)
async def trigger_youtube_discovery():
    from tasks.discovery_tasks import run_youtube_discovery
    task = run_youtube_discovery.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/email-extraction", dependencies=ADMIN_ONLY)
async def trigger_email_extraction():
    from tasks.enrichment_tasks import run_email_extraction
    task = run_email_extraction.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/email-verification", dependencies=ADMIN_ONLY)
async def trigger_email_verification():
    from tasks.enrichment_tasks import run_email_verification
    task = run_email_verification.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/sequence-processing", dependencies=ADMIN_ONLY)
async def trigger_sequence_processing():
    from tasks.outreach_tasks import process_pending_sequences
    task = process_pending_sequences.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/auto-enroll", dependencies=ADMIN_ONLY)
async def trigger_auto_enroll():
    from tasks.outreach_tasks import auto_enroll_prospects
    task = auto_enroll_prospects.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/brevo-sync", dependencies=ADMIN_ONLY)
async def trigger_brevo_sync():
    from tasks.maintenance_tasks import sync_contacts_to_brevo
    task = sync_contacts_to_brevo.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/brevo-sync-full", dependencies=ADMIN_ONLY)
async def trigger_brevo_sync_full():
    """Force sync ALL verified prospects to Brevo, ignoring last sync time."""
    from tasks.maintenance_tasks import sync_contacts_to_brevo
    task = sync_contacts_to_brevo.delay(force_full_sync=True)
    return {"status": "triggered", "task_id": task.id, "mode": "force_full_sync"}


@app.post("/trigger/deliverability-check", dependencies=ADMIN_ONLY)
async def trigger_deliverability_check():
    """Run deliverability metrics check and send alerts if thresholds exceeded."""
    from tasks.maintenance_tasks import check_deliverability_metrics
    task = check_deliverability_metrics.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/keyword-trends", dependencies=ADMIN_ONLY)
async def trigger_keyword_trends():
    """Run Google Trends analysis to update keyword priorities."""
    from tasks.maintenance_tasks import analyze_keyword_trends
    task = analyze_keyword_trends.delay()
    return {"status": "triggered", "task_id": task.id}


@app.get("/keywords/trends/{keyword}", dependencies=ADMIN_ONLY)
async def get_keyword_trend(keyword: str):
    """Get Google Trends data for a specific keyword."""
    settings = get_settings()

    if not settings.serpapi_api_key:
        raise HTTPException(status_code=503, detail="SerpApi not configured")

    try:
        from services.trends_analyzer import TrendsAnalyzer
        analyzer = TrendsAnalyzer()
        result = await analyzer.get_trend_score(keyword)

        if not result:
            raise HTTPException(status_code=404, detail="Could not fetch trend data")

        # Also get related queries
        related = await analyzer.get_related_queries(keyword)
        result["related_queries"] = related

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Trend lookup failed", keyword=keyword, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/keywords/compare", dependencies=ADMIN_ONLY)
async def compare_keywords(request: Request):
    """Compare multiple keywords head-to-head using Google Trends."""
    settings = get_settings()

    if not settings.serpapi_api_key:
        raise HTTPException(status_code=503, detail="SerpApi not configured")

    try:
        data = await request.json()
        keywords = data.get("keywords", [])

        if not keywords or len(keywords) < 2:
            raise HTTPException(status_code=400, detail="At least 2 keywords required")

        from services.trends_analyzer import TrendsAnalyzer
        analyzer = TrendsAnalyzer()
        result = await analyzer.get_competitor_comparison(keywords)

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Keyword comparison failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tasks/{task_id}", dependencies=ADMIN_ONLY)
async def get_task_status(task_id: str):
    from celery.result import AsyncResult
    from celery_config import celery_app
    result = AsyncResult(task_id, app=celery_app)
    return {"task_id": task_id, "status": result.status, "result": result.result if result.ready() else None}


# =====================================================
# Keyword Management API
# =====================================================

@app.get("/keywords", dependencies=ADMIN_ONLY)
async def list_keywords():
    """List all discovery keywords with stats."""
    db = None
    try:
        db = await get_database_async()
        # Use only columns from base schema, with safe defaults for optional columns
        keywords = await db.fetch("""
            SELECT id, competitor_name, keyword, platform, is_active,
                   last_searched_at, priority, created_at
            FROM competitor_keywords
            ORDER BY priority DESC, is_active DESC, competitor_name, keyword
        """)
        return {
            "keywords": [dict(k) for k in keywords],
            "total": len(keywords),
            "active": sum(1 for k in keywords if k["is_active"])
        }
    except Exception as e:
        logger.error("Failed to list keywords", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if db:
            await db.close()


@app.post("/keywords", dependencies=ADMIN_ONLY)
async def add_keyword(request: Request):
    """Add a new discovery keyword."""
    db = None
    try:
        data = await request.json()
        keyword = data.get("keyword", "").strip()
        competitor_name = data.get("competitor_name", "General").strip()
        platform = data.get("platform", "youtube")
        priority = data.get("priority", 0)

        if not keyword:
            raise HTTPException(status_code=400, detail="Keyword is required")

        db = await get_database_async()

        # Check for duplicate
        existing = await db.fetchval(
            "SELECT id FROM competitor_keywords WHERE keyword = $1 AND platform = $2",
            keyword, platform
        )
        if existing:
            raise HTTPException(status_code=409, detail="Keyword already exists")

        result = await db.fetchrow("""
            INSERT INTO competitor_keywords (competitor_name, keyword, platform, priority, is_active)
            VALUES ($1, $2, $3, $4, TRUE)
            RETURNING id, competitor_name, keyword, platform, is_active, priority
        """, competitor_name, keyword, platform, priority)

        logger.info("Keyword added", keyword=keyword, competitor=competitor_name)
        return {"status": "created", "keyword": dict(result)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to add keyword", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if db:
            await db.close()


@app.patch("/keywords/{keyword_id}", dependencies=ADMIN_ONLY)
async def update_keyword(keyword_id: str, request: Request):
    """Update a keyword (toggle active, change priority, etc.)."""
    db = None
    try:
        data = await request.json()
        db = await get_database_async()

        # Build dynamic update
        updates = []
        params = []
        param_idx = 1

        if "is_active" in data:
            updates.append(f"is_active = ${param_idx}")
            params.append(data["is_active"])
            param_idx += 1

        if "priority" in data:
            updates.append(f"priority = ${param_idx}")
            params.append(data["priority"])
            param_idx += 1

        if "competitor_name" in data:
            updates.append(f"competitor_name = ${param_idx}")
            params.append(data["competitor_name"])
            param_idx += 1

        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")

        params.append(keyword_id)
        query = f"UPDATE competitor_keywords SET {', '.join(updates)} WHERE id = ${param_idx} RETURNING *"

        result = await db.fetchrow(query, *params)
        if not result:
            raise HTTPException(status_code=404, detail="Keyword not found")

        logger.info("Keyword updated", keyword_id=keyword_id)
        return {"status": "updated", "keyword": dict(result)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to update keyword", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if db:
            await db.close()


@app.delete("/keywords/{keyword_id}", dependencies=ADMIN_ONLY)
async def delete_keyword(keyword_id: str):
    """Delete a keyword."""
    db = None
    try:
        db = await get_database_async()
        result = await db.execute(
            "DELETE FROM competitor_keywords WHERE id = $1",
            keyword_id
        )
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Keyword not found")

        logger.info("Keyword deleted", keyword_id=keyword_id)
        return {"status": "deleted", "keyword_id": keyword_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to delete keyword", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if db:
            await db.close()
