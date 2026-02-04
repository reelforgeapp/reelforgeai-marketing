"""
ReelForge Marketing Engine - Main Application
"""

import json
import hmac
import hashlib
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException, Depends
import structlog

from app.config import get_settings
from app.database import init_database, close_database, get_database_async

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
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


app = FastAPI(title="ReelForge Marketing Engine", version="3.0.0", lifespan=lifespan)


async def validate_brevo_webhook(request: Request) -> bytes:
    if not settings.brevo_webhook_secret:
        return await request.body()
    
    signature = request.headers.get("X-Sib-Signature", "") or request.headers.get("X-Brevo-Signature", "")
    body = await request.body()
    
    expected = hmac.new(settings.brevo_webhook_secret.encode(), body, hashlib.sha256).hexdigest()
    signature_clean = signature.replace("sha256=", "").lower()
    
    if not hmac.compare_digest(signature_clean, expected.lower()):
        raise HTTPException(status_code=401, detail="Invalid signature")
    
    return body


@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": "3.0.0"}


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
        "version": "3.3.0",
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
        except:
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
                # Only process bounce if not already bounced
                already_bounced = await db.fetchval("SELECT bounced_at IS NOT NULL FROM email_sends WHERE brevo_message_id = $1", message_id)
                if not already_bounced:
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
                # Only process unsubscribe if not already processed
                current_status = await db.fetchval("SELECT status FROM email_sends WHERE brevo_message_id = $1", message_id)
                if current_status != 'unsubscribed':
                    await db.execute("UPDATE email_sends SET status = 'unsubscribed' WHERE brevo_message_id = $1", message_id)
                    if email:
                        await db.execute("UPDATE marketing_prospects SET status = 'unsubscribed' WHERE email = $1", email)
                        # Stop active sequences for unsubscribed
                        await db.execute("""
                            UPDATE outreach_sequences SET status = 'stopped', stopped_reason = 'unsubscribed', completed_at = NOW()
                            WHERE prospect_id IN (SELECT id FROM marketing_prospects WHERE email = $1)
                            AND status IN ('pending', 'active')
                        """, email)

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


@app.post("/trigger/youtube-discovery")
async def trigger_youtube_discovery():
    from tasks.discovery_tasks import run_youtube_discovery
    task = run_youtube_discovery.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/email-extraction")
async def trigger_email_extraction():
    from tasks.enrichment_tasks import run_email_extraction
    task = run_email_extraction.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/email-verification")
async def trigger_email_verification():
    from tasks.enrichment_tasks import run_email_verification
    task = run_email_verification.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/sequence-processing")
async def trigger_sequence_processing():
    from tasks.outreach_tasks import process_pending_sequences
    task = process_pending_sequences.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/auto-enroll")
async def trigger_auto_enroll():
    from tasks.outreach_tasks import auto_enroll_prospects
    task = auto_enroll_prospects.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/brevo-sync")
async def trigger_brevo_sync():
    from tasks.maintenance_tasks import sync_contacts_to_brevo
    task = sync_contacts_to_brevo.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/brevo-sync-full")
async def trigger_brevo_sync_full():
    """Force sync ALL verified prospects to Brevo, ignoring last sync time."""
    from tasks.maintenance_tasks import sync_contacts_to_brevo
    task = sync_contacts_to_brevo.delay(force_full_sync=True)
    return {"status": "triggered", "task_id": task.id, "mode": "force_full_sync"}


@app.post("/trigger/deliverability-check")
async def trigger_deliverability_check():
    """Run deliverability metrics check and send alerts if thresholds exceeded."""
    from tasks.maintenance_tasks import check_deliverability_metrics
    task = check_deliverability_metrics.delay()
    return {"status": "triggered", "task_id": task.id}


@app.post("/trigger/keyword-trends")
async def trigger_keyword_trends():
    """Run Google Trends analysis to update keyword priorities."""
    from tasks.maintenance_tasks import analyze_keyword_trends
    task = analyze_keyword_trends.delay()
    return {"status": "triggered", "task_id": task.id}


@app.get("/keywords/trends/{keyword}")
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


@app.post("/keywords/compare")
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


@app.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    from celery.result import AsyncResult
    from celery_config import celery_app
    result = AsyncResult(task_id, app=celery_app)
    return {"task_id": task_id, "status": result.status, "result": result.result if result.ready() else None}


# =====================================================
# Keyword Management API
# =====================================================

@app.get("/keywords")
async def list_keywords():
    """List all discovery keywords with stats."""
    db = None
    try:
        db = await get_database_async()
        keywords = await db.fetch("""
            SELECT id, competitor_name, keyword, platform, is_active,
                   last_searched_at, results_count, priority, created_at
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


@app.post("/keywords")
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


@app.patch("/keywords/{keyword_id}")
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


@app.delete("/keywords/{keyword_id}")
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
