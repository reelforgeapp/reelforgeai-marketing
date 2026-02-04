"""
ReelForge Marketing Engine - Maintenance Tasks
"""
import sys
sys.path.insert(0, '/app')

import asyncio
from datetime import datetime, timedelta
import httpx
import structlog

from celery_config import celery_app, BaseTaskWithRetry
from app.config import get_settings
from app.database import get_database_async, DatabaseTransaction

logger = structlog.get_logger()


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def sync_contacts_to_brevo(self, force_full_sync: bool = False):
    return asyncio.run(_sync_brevo_async(force_full_sync=force_full_sync))


async def _sync_brevo_async(force_full_sync: bool = False) -> dict:
    """Sync verified prospects to Brevo CRM.

    Args:
        force_full_sync: If True, sync ALL verified prospects regardless of brevo_synced_at
    """
    settings = get_settings()
    db = await get_database_async()

    results = {"synced": 0, "updated": 0, "errors": 0, "skipped": 0, "force_sync": force_full_sync}

    if not settings.brevo_api_key:
        logger.warning("Brevo API key not configured, skipping sync")
        return {"status": "skipped", "reason": "No Brevo API key"}

    # Brevo list ID for ReelForge Prospects (configurable via env var)
    BREVO_LIST_ID = int(getattr(settings, 'brevo_list_id', 3))

    try:
        if force_full_sync:
            # Force sync: get ALL verified prospects (no time filter)
            logger.info("Starting FORCED full sync to Brevo")
            prospects = await db.fetch("""
                SELECT id, email, full_name, primary_platform,
                       youtube_handle, youtube_subscribers,
                       instagram_handle, instagram_followers,
                       tiktok_handle, tiktok_followers,
                       status, relevance_score, brevo_synced_at, updated_at
                FROM marketing_prospects
                WHERE email_verified = TRUE
                  AND email IS NOT NULL
                  AND status NOT IN ('bounced', 'unsubscribed')
                ORDER BY
                    CASE WHEN brevo_synced_at IS NULL THEN 0 ELSE 1 END,
                    relevance_score DESC
                LIMIT 500
            """)
        else:
            # Normal sync: only prospects that need syncing
            # 1. Never synced (brevo_synced_at IS NULL)
            # 2. Status changed since last sync (updated_at > brevo_synced_at)
            # 3. Not synced in last 7 days (for periodic refresh)
            prospects = await db.fetch("""
                SELECT id, email, full_name, primary_platform,
                       youtube_handle, youtube_subscribers,
                       instagram_handle, instagram_followers,
                       tiktok_handle, tiktok_followers,
                       status, relevance_score, brevo_synced_at, updated_at
                FROM marketing_prospects
                WHERE email_verified = TRUE
                  AND email IS NOT NULL
                  AND status NOT IN ('bounced', 'unsubscribed')
                  AND (
                      brevo_synced_at IS NULL
                      OR updated_at > brevo_synced_at
                      OR brevo_synced_at < NOW() - INTERVAL '7 days'
                  )
                ORDER BY
                    CASE WHEN brevo_synced_at IS NULL THEN 0 ELSE 1 END,
                    relevance_score DESC
                LIMIT 100
            """)
        
        if not prospects:
            logger.info("No prospects to sync to Brevo")
            return {"status": "no_prospects", "synced": 0, "updated": 0, "errors": 0}

        logger.info("Syncing prospects to Brevo", count=len(prospects))
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            for p in prospects:
                try:
                    # Build contact attributes
                    full_name = p["full_name"] or ""
                    name_parts = full_name.split()
                    first_name = name_parts[0] if name_parts else ""
                    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
                    
                    followers = (
                        p["youtube_subscribers"] or 
                        p["instagram_followers"] or 
                        p["tiktok_followers"] or 0
                    )
                    
                    handle = (
                        p["youtube_handle"] or 
                        p["instagram_handle"] or 
                        p["tiktok_handle"] or ""
                    )
                    
                    contact = {
                        "email": p["email"],
                        "attributes": {
                            "FIRSTNAME": first_name,
                            "LASTNAME": last_name,
                            "PLATFORM": p["primary_platform"] or "unknown",
                            "HANDLE": handle,
                            "FOLLOWERS": followers,
                            "STATUS": p["status"] or "discovered",
                            "RELEVANCE_SCORE": float(p["relevance_score"] or 0)
                        },
                        "listIds": [BREVO_LIST_ID],
                        "updateEnabled": True
                    }
                    
                    resp = await client.post(
                        "https://api.brevo.com/v3/contacts",
                        headers={
                            "api-key": settings.brevo_api_key,
                            "Content-Type": "application/json"
                        },
                        json=contact
                    )

                    # Brevo returns:
                    # 201 = new contact created
                    # 204 = contact updated (when updateEnabled=True and contact exists)
                    # 400 = bad request (invalid email format, etc.)
                    # 401 = unauthorized (bad API key)
                    if resp.status_code == 201:
                        results["synced"] += 1
                        logger.debug("Brevo contact created", email=p["email"])
                    elif resp.status_code == 204:
                        results["updated"] += 1
                        logger.debug("Brevo contact updated", email=p["email"])
                    elif resp.status_code == 400:
                        # Check if it's a duplicate error (contact already exists without updateEnabled working)
                        error_msg = resp.text
                        if "duplicate" in error_msg.lower() or "already exist" in error_msg.lower():
                            results["updated"] += 1
                            logger.debug("Brevo contact already exists", email=p["email"])
                        else:
                            logger.warning("Brevo sync bad request", email=p["email"], status=resp.status_code, response=error_msg[:200])
                            results["errors"] += 1
                            continue
                    elif resp.status_code == 401:
                        logger.error("Brevo API key invalid or expired")
                        results["errors"] += 1
                        break  # Stop processing if auth fails
                    else:
                        logger.warning("Brevo sync failed", email=p["email"], status=resp.status_code, response=resp.text[:200])
                        results["errors"] += 1
                        continue

                    # Mark as synced (don't update updated_at to avoid sync loop)
                    await db.execute(
                        "UPDATE marketing_prospects SET brevo_synced_at = NOW() WHERE id = $1",
                        p["id"]
                    )
                    
                    # Rate limit
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    logger.error("Brevo contact sync error", email=p["email"], error=str(e))
                    results["errors"] += 1
        
        logger.info("Brevo sync complete", **results)
        
    except Exception as e:
        logger.error("Brevo sync failed", error=str(e))
        results["error"] = str(e)
    finally:
        await db.close()
    
    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def purge_expired_data(self):
    return asyncio.run(_purge_data_async())


async def _purge_data_async() -> dict:
    settings = get_settings()
    db = None

    results = {"prospects_purged": 0, "errors": 0}

    try:
        db = await get_database_async()
        cutoff = datetime.utcnow() - timedelta(days=settings.data_retention_days)

        expired = await db.fetch("""
            SELECT id FROM marketing_prospects
            WHERE discovered_at < $1
              AND status NOT IN ('converted', 'active_affiliate')
            LIMIT 100
        """, cutoff)

        for prospect in expired:
            try:
                async with DatabaseTransaction() as conn:
                    await conn.execute("DELETE FROM outreach_sequences WHERE prospect_id = $1", prospect["id"])
                    await conn.execute("DELETE FROM email_sends WHERE prospect_id = $1", prospect["id"])
                    await conn.execute("DELETE FROM marketing_prospects WHERE id = $1", prospect["id"])
                results["prospects_purged"] += 1
            except Exception as e:
                results["errors"] += 1

        logger.info("Data purge complete", **results)

    except Exception as e:
        logger.error("Data purge failed", error=str(e))
        results["error"] = str(e)
    finally:
        if db:
            await db.close()

    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def cleanup_old_data(self):
    return asyncio.run(_cleanup_async())


async def _cleanup_async() -> dict:
    db = None
    results = {"cleaned": 0}

    try:
        db = await get_database_async()
        await db.execute("DELETE FROM idempotency_keys WHERE expires_at < NOW()")
        results["cleaned"] = 1
        logger.info("Cleanup complete")
    except Exception as e:
        logger.error("Cleanup failed", error=str(e))
        results["error"] = str(e)
    finally:
        if db:
            await db.close()

    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def check_deliverability_metrics(self):
    return asyncio.run(_check_deliverability_async())


async def _check_deliverability_async() -> dict:
    """Check email deliverability metrics and send alerts if thresholds exceeded."""
    settings = get_settings()
    db = None

    results = {
        "total_sent_24h": 0,
        "bounced_24h": 0,
        "bounce_rate": 0.0,
        "spam_complaints_24h": 0,
        "spam_rate": 0.0,
        "open_rate_24h": 0.0,
        "alerts_sent": 0
    }

    if not settings.brevo_api_key:
        return {"status": "skipped", "reason": "No Brevo API key"}

    try:
        db = await get_database_async()

        # Get metrics from last 24 hours
        metrics = await db.fetchrow("""
            SELECT
                COUNT(*) as total_sent,
                COUNT(*) FILTER (WHERE status = 'bounced') as bounced,
                COUNT(*) FILTER (WHERE status IN ('opened', 'clicked')) as opened,
                COUNT(*) FILTER (WHERE status = 'unsubscribed') as unsubscribed
            FROM email_sends
            WHERE sent_at >= NOW() - INTERVAL '24 hours'
        """)

        if metrics and metrics["total_sent"] > 0:
            total = metrics["total_sent"]
            results["total_sent_24h"] = total
            results["bounced_24h"] = metrics["bounced"] or 0
            results["bounce_rate"] = results["bounced_24h"] / total
            results["open_rate_24h"] = (metrics["opened"] or 0) / total

            # Get spam complaints from Brevo (via webhook status or API)
            # For now, use unsubscribes as a proxy indicator
            results["spam_complaints_24h"] = metrics["unsubscribed"] or 0
            results["spam_rate"] = results["spam_complaints_24h"] / total

        # Check thresholds and send alerts
        alerts = []

        if results["bounce_rate"] > settings.bounce_rate_threshold:
            alerts.append(
                f"⚠️ HIGH BOUNCE RATE: {results['bounce_rate']:.1%} "
                f"({results['bounced_24h']}/{results['total_sent_24h']} emails bounced in 24h)\n"
                f"Threshold: {settings.bounce_rate_threshold:.1%}"
            )

        if results["spam_rate"] > settings.spam_rate_threshold:
            alerts.append(
                f"🚨 HIGH SPAM/UNSUBSCRIBE RATE: {results['spam_rate']:.2%} "
                f"({results['spam_complaints_24h']}/{results['total_sent_24h']} in 24h)\n"
                f"Threshold: {settings.spam_rate_threshold:.2%}"
            )

        # Send low open rate warning (informational, not critical)
        if results["total_sent_24h"] >= 20 and results["open_rate_24h"] < 0.10:
            alerts.append(
                f"📉 LOW OPEN RATE: {results['open_rate_24h']:.1%} in last 24h\n"
                f"Consider reviewing subject lines and sender reputation."
            )

        if alerts and settings.alert_email:
            await _send_alert_email(settings, alerts, results)
            results["alerts_sent"] = len(alerts)
            logger.warning("Deliverability alerts sent", alert_count=len(alerts), to=settings.alert_email)
        else:
            logger.info("Deliverability check complete - no alerts", **results)

    except Exception as e:
        logger.error("Deliverability check failed", error=str(e))
        results["error"] = str(e)
    finally:
        if db:
            await db.close()

    return results


async def _send_alert_email(settings, alerts: list, metrics: dict) -> None:
    """Send deliverability alert email via Brevo."""
    alert_body = "\n\n".join(alerts)

    html_content = f"""
    <h2>ReelForge Marketing - Deliverability Alert</h2>
    <p>The following issues were detected in your email campaign:</p>
    <div style="background: #fff3cd; padding: 15px; border-radius: 5px; margin: 15px 0;">
        <pre style="white-space: pre-wrap;">{alert_body}</pre>
    </div>
    <h3>24-Hour Metrics Summary</h3>
    <ul>
        <li><strong>Emails Sent:</strong> {metrics['total_sent_24h']}</li>
        <li><strong>Bounced:</strong> {metrics['bounced_24h']} ({metrics['bounce_rate']:.1%})</li>
        <li><strong>Open Rate:</strong> {metrics['open_rate_24h']:.1%}</li>
        <li><strong>Unsubscribes:</strong> {metrics['spam_complaints_24h']}</li>
    </ul>
    <h3>Recommended Actions</h3>
    <ul>
        <li>Review recent email content for spam triggers</li>
        <li>Check email verification is working properly</li>
        <li>Consider reducing send volume temporarily</li>
        <li>Review Brevo dashboard for detailed analytics</li>
    </ul>
    <p style="color: #666; font-size: 12px;">
        This alert was generated automatically by ReelForge Marketing Engine.
    </p>
    """

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(
                "https://api.brevo.com/v3/smtp/email",
                headers={
                    "api-key": settings.brevo_api_key,
                    "Content-Type": "application/json"
                },
                json={
                    "sender": {
                        "name": "ReelForge Alerts",
                        "email": settings.brevo_sender_email
                    },
                    "to": [{"email": settings.alert_email}],
                    "subject": "⚠️ ReelForge Deliverability Alert - Action Required",
                    "htmlContent": html_content,
                    "tags": ["system-alert", "deliverability"]
                }
            )

            if response.status_code not in (200, 201):
                logger.error("Failed to send alert email", status=response.status_code, body=response.text[:200])
        except Exception as e:
            logger.error("Alert email send failed", error=str(e))


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=2, queue='default')
def analyze_keyword_trends(self):
    """Analyze Google Trends data and update keyword priorities."""
    return asyncio.run(_analyze_trends_async())


async def _analyze_trends_async() -> dict:
    """Run trends analysis on all keywords."""
    settings = get_settings()

    if not settings.serpapi_api_key:
        logger.warning("SerpApi key not configured, skipping trends analysis")
        return {"status": "skipped", "reason": "No SerpApi key configured"}

    try:
        from services.trends_analyzer import TrendsAnalyzer

        analyzer = TrendsAnalyzer()
        results = await analyzer.analyze_all_keywords()

        # Send summary email if significant changes
        if settings.alert_email and (results.get("boosted", 0) + results.get("deactivated", 0) > 3):
            await _send_trends_summary_email(settings, results)

        return results

    except ImportError as e:
        logger.error("Trends analyzer import failed", error=str(e))
        return {"status": "error", "reason": str(e)}
    except Exception as e:
        logger.error("Trends analysis failed", error=str(e))
        return {"status": "error", "reason": str(e)}


async def _send_trends_summary_email(settings, results: dict) -> None:
    """Send trends analysis summary email."""
    html_content = f"""
    <h2>ReelForge Marketing - Keyword Trends Update</h2>
    <p>Monthly keyword trends analysis has been completed.</p>

    <h3>Summary</h3>
    <ul>
        <li><strong>Keywords Analyzed:</strong> {results.get('analyzed', 0)}</li>
        <li><strong>Boosted (trending up):</strong> {results.get('boosted', 0)}</li>
        <li><strong>Demoted (declining):</strong> {results.get('demoted', 0)}</li>
        <li><strong>Deactivated (low interest):</strong> {results.get('deactivated', 0)}</li>
        <li><strong>New Keywords Discovered:</strong> {results.get('new_suggestions', 0)}</li>
        <li><strong>Errors:</strong> {results.get('errors', 0)}</li>
    </ul>

    <p>Review keywords at: <a href="https://your-app.onrender.com/keywords">/keywords</a></p>

    <p style="color: #666; font-size: 12px;">
        This report was generated automatically by ReelForge Marketing Engine using Google Trends data.
    </p>
    """

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            await client.post(
                "https://api.brevo.com/v3/smtp/email",
                headers={
                    "api-key": settings.brevo_api_key,
                    "Content-Type": "application/json"
                },
                json={
                    "sender": {
                        "name": "ReelForge Reports",
                        "email": settings.brevo_sender_email
                    },
                    "to": [{"email": settings.alert_email}],
                    "subject": "📈 ReelForge Keyword Trends Update",
                    "htmlContent": html_content,
                    "tags": ["system-report", "trends"]
                }
            )
        except Exception as e:
            logger.error("Trends summary email failed", error=str(e))
