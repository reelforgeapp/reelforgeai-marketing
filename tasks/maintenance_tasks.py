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

    # Brevo list ID for ReelForge Prospects
    BREVO_LIST_ID = 3

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

                    # Mark as synced
                    await db.execute(
                        "UPDATE marketing_prospects SET brevo_synced_at = NOW(), updated_at = NOW() WHERE id = $1",
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
    db = await get_database_async()
    
    results = {"prospects_purged": 0, "errors": 0}
    
    try:
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
    
    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='default')
def cleanup_old_data(self):
    return asyncio.run(_cleanup_async())


async def _cleanup_async() -> dict:
    db = await get_database_async()
    results = {"cleaned": 0}
    
    try:
        await db.execute("DELETE FROM idempotency_keys WHERE expires_at < NOW()")
        logger.info("Cleanup complete")
    except Exception as e:
        logger.error("Cleanup failed", error=str(e))
        results["error"] = str(e)
    
    return results
