"""
ReelForge Marketing Engine - Outreach Tasks
"""

import asyncio
import json
from datetime import datetime, timedelta
from jinja2 import Template
import structlog
import redis.asyncio as redis

from celery_config import celery_app, BaseTaskWithRetry
from app.config import get_settings
from app.database import get_database_async, DatabaseTransaction

logger = structlog.get_logger()


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='outreach')
def process_pending_sequences(self):
    return asyncio.run(_process_sequences_async())


async def _process_sequences_async() -> dict:
    settings = get_settings()
    db = await get_database_async()
    
    results = {
        "processed": 0,
        "sent": 0,
        "skipped": 0,
        "errors": 0,
        "limit_reached": False
    }
    
    if not settings.brevo_api_key:
        return {"status": "error", "error": "Brevo API key not configured"}
    
    try:
        from outreach.brevo_client import BrevoClient
        brevo = BrevoClient()
        
        async with redis.from_url(settings.redis_url) as redis_client:
            today = datetime.utcnow().strftime("%Y%m%d")
            daily_key = f"email_count:{today}"
            
            current_count = await redis_client.get(daily_key)
            current_count = int(current_count) if current_count else 0
            
            if current_count >= settings.daily_email_limit:
                results["limit_reached"] = True
                return results
            
            remaining = settings.daily_email_limit - current_count
            
            pending = await db.fetch("""
                SELECT os.id, os.prospect_id, os.sequence_name, os.current_step,
                       os.total_steps, os.personalization_data,
                       mp.email, mp.full_name,
                       st.steps
                FROM outreach_sequences os
                JOIN marketing_prospects mp ON os.prospect_id = mp.id
                JOIN sequence_templates st ON os.sequence_name = st.name
                WHERE os.status IN ('pending', 'active')
                  AND os.next_send_at <= NOW()
                  AND mp.email IS NOT NULL
                  AND mp.email_verified = TRUE
                  AND mp.status NOT IN ('unsubscribed', 'bounced')
                ORDER BY os.next_send_at ASC
                LIMIT $1
            """, remaining)
            
            for seq in pending:
                results["processed"] += 1
                
                try:
                    steps = seq.get("steps") or []
                    current_step = seq["current_step"]
                    
                    if current_step >= len(steps):
                        await db.execute(
                            "UPDATE outreach_sequences SET status = 'completed' WHERE id = $1",
                            seq["id"]
                        )
                        continue
                    
                    step = steps[current_step]
                    template_name = step.get("body_template")
                    
                    if not template_name:
                        results["skipped"] += 1
                        continue
                    
                    email_template = await db.fetchrow(
                        "SELECT subject_template, html_template, text_template FROM email_templates WHERE name = $1",
                        template_name
                    )
                    
                    if not email_template:
                        results["skipped"] += 1
                        continue
                    
                    pdata = seq.get("personalization_data") or {}
                    
                    subject = Template(email_template["subject_template"]).render(**pdata)
                    html_content = Template(email_template["html_template"]).render(**pdata)
                    
                    result = await brevo.send_email(
                        to_email=seq["email"],
                        to_name=seq["full_name"] or "",
                        subject=subject,
                        html_content=html_content,
                        tags=[f"sequence:{seq['sequence_name']}", f"step:{current_step + 1}"]
                    )
                    
                    if result.get("success"):
                        await redis_client.incr(daily_key)
                        await redis_client.expire(daily_key, 86400)
                        
                        async with DatabaseTransaction() as conn:
                            await conn.execute("""
                                INSERT INTO email_sends (
                                    sequence_id, prospect_id, step_number, template_name,
                                    subject, to_email, brevo_message_id, status, sent_at,
                                    open_count, click_count
                                ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'sent', NOW(), 0, 0)
                            """, seq["id"], seq["prospect_id"], current_step + 1, template_name,
                                subject, seq["email"], result.get("message_id"))
                            
                            next_step = current_step + 1
                            if next_step >= len(steps):
                                await conn.execute(
                                    "UPDATE outreach_sequences SET status = 'completed', current_step = $1 WHERE id = $2",
                                    next_step, seq["id"]
                                )
                            else:
                                next_send = datetime.utcnow() + timedelta(days=steps[next_step].get("delay_days", 3))
                                await conn.execute(
                                    "UPDATE outreach_sequences SET status = 'active', current_step = $1, next_send_at = $2 WHERE id = $3",
                                    next_step, next_send, seq["id"]
                                )
                        
                        results["sent"] += 1
                    else:
                        results["errors"] += 1
                    
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error("Sequence processing error", sequence_id=str(seq["id"]), error=str(e))
                    results["errors"] += 1
        
        logger.info("Sequence processing complete", **results)
        
    except Exception as e:
        logger.error("Sequence processing failed", error=str(e))
        results["error"] = str(e)
    
    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='outreach')
def auto_enroll_prospects(self):
    return asyncio.run(_auto_enroll_async())


async def _auto_enroll_async() -> dict:
    settings = get_settings()
    db = await get_database_async()
    
    results = {"enrolled": 0, "skipped": 0, "errors": 0}
    
    prospects = await db.fetch("""
        SELECT mp.id, mp.email, mp.full_name, mp.primary_platform, mp.relevance_score
        FROM marketing_prospects mp
        WHERE mp.email IS NOT NULL
          AND mp.email_verified = TRUE
          AND mp.status IN ('discovered', 'enriched')
          AND mp.relevance_score >= $1
          AND NOT EXISTS (
              SELECT 1 FROM outreach_sequences os
              WHERE os.prospect_id = mp.id AND os.status IN ('pending', 'active')
          )
        ORDER BY mp.relevance_score DESC
        LIMIT 25
    """, settings.min_relevance_score)
    
    for prospect in prospects:
        try:
            platform = prospect["primary_platform"] or "youtube"
            sequence_name = f"{platform}_creator"
            
            template = await db.fetchrow(
                "SELECT id, total_steps, steps FROM sequence_templates WHERE name = $1 AND is_active = TRUE",
                sequence_name
            )
            
            if not template:
                template = await db.fetchrow(
                    "SELECT id, total_steps, steps FROM sequence_templates WHERE is_active = TRUE LIMIT 1"
                )
            
            if not template:
                results["skipped"] += 1
                continue
            
            steps = template.get("steps") or []
            first_delay = steps[0].get("delay_days", 1) if steps else 1
            first_send = datetime.utcnow() + timedelta(days=first_delay)
            
            first_name = (prospect["full_name"] or "").split()[0] if prospect["full_name"] else "there"
            
            pdata = {
                "first_name": first_name,
                "full_name": prospect["full_name"] or "",
                "affiliate_link": f"{settings.affiliate_signup_base_url}?ref={str(prospect['id'])[:8]}"
            }
            
            await db.execute("""
                INSERT INTO outreach_sequences (
                    prospect_id, sequence_template_id, sequence_name, total_steps,
                    current_step, status, next_send_at, personalization_data, created_at
                ) VALUES ($1, $2, $3, $4, 0, 'pending', $5, $6, NOW())
            """, prospect["id"], template["id"], sequence_name, template["total_steps"],
                first_send, json.dumps(pdata))
            
            results["enrolled"] += 1
            
        except Exception as e:
            logger.error("Enrollment failed", prospect_id=str(prospect["id"]), error=str(e))
            results["errors"] += 1
    
    logger.info("Auto-enrollment complete", **results)
    return results
