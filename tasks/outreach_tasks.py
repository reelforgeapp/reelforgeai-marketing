"""
ReelForge Marketing Engine - Outreach Tasks (Fixed)
"""

import asyncio
import json
from datetime import datetime, timedelta
import structlog
import redis.asyncio as aioredis

from celery_config import celery_app, BaseTaskWithRetry
from app.config import get_settings
from app.database import get_db_connection, DatabaseTransaction
from services.idempotency import IdempotencyService
from outreach.brevo_client import BrevoClient

logger = structlog.get_logger()

# Lua script for atomic check-and-increment
DAILY_LIMIT_SCRIPT = """
local current = redis.call('GET', KEYS[1])
current = tonumber(current) or 0
if current >= tonumber(ARGV[1]) then
    return -1
end
local new_count = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
return new_count
"""


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, default_retry_delay=300, queue='outreach')
def process_pending_sequences(self):
    """Process all pending email sequences."""
    return asyncio.run(_process_sequences_async())


async def _process_sequences_async() -> dict:
    settings = get_settings()
    brevo = BrevoClient()
    idempotency = IdempotencyService()
    
    results = {"processed": 0, "sent": 0, "skipped_duplicate": 0, "errors": 0, "limit_reached": False}
    
    redis_client = aioredis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)
    
    try:
        today = datetime.utcnow().strftime("%Y%m%d")
        daily_key = f"email_count:{today}"
        
        current_count = await redis_client.get(daily_key)
        current_count = int(current_count) if current_count else 0
        
        if current_count >= settings.daily_email_limit:
            results["limit_reached"] = True
            return results
        
        remaining = settings.daily_email_limit - current_count
        
        async with get_db_connection() as db:
            pending = await db.fetch("""
                SELECT os.id, os.prospect_id, os.sequence_name, os.current_step,
                       os.total_steps, os.personalization_data, os.status,
                       mp.email, mp.full_name, mp.email_verified, mp.verification_status,
                       st.steps, st.stop_on
                FROM outreach_sequences os
                JOIN marketing_prospects mp ON os.prospect_id = mp.id
                JOIN sequence_templates st ON os.sequence_name = st.name
                WHERE os.status IN ('pending', 'active')
                  AND os.next_send_at <= NOW()
                  AND mp.email IS NOT NULL
                  AND mp.email_verified = TRUE
                  AND mp.verification_status IN ('valid', 'catch_all')
                  AND mp.status NOT IN ('unsubscribed', 'complained', 'bounced', 'converted')
                ORDER BY os.next_send_at ASC
                LIMIT $1
            """, remaining)
            
            logger.info(f"Processing {len(pending)} pending sequences")
            
            for seq in pending:
                results["processed"] += 1
                
                try:
                    idempotency_key = idempotency.generate_key(
                        sequence_id=str(seq["id"]),
                        step_number=seq["current_step"] + 1,
                        prospect_email=seq["email"]
                    )
                    
                    can_send = await idempotency.check_and_acquire(idempotency_key)
                    if not can_send:
                        results["skipped_duplicate"] += 1
                        continue
                    
                    should_stop = await _check_stop_conditions(seq)
                    if should_stop:
                        await idempotency.mark_completed(idempotency_key)
                        continue
                    
                    steps = seq.get("steps") or []
                    if not steps:
                        await idempotency.mark_failed(idempotency_key, "no_steps")
                        results["errors"] += 1
                        continue
                    
                    current_step = seq["current_step"]
                    if current_step >= len(steps):
                        await _complete_sequence(seq["id"])
                        await idempotency.mark_completed(idempotency_key)
                        continue
                    
                    step = steps[current_step]
                    
                    new_count = await redis_client.eval(
                        DAILY_LIMIT_SCRIPT, 1, daily_key, 
                        settings.daily_email_limit, 86400
                    )
                    
                    if new_count == -1:
                        results["limit_reached"] = True
                        await idempotency.mark_failed(idempotency_key, "daily_limit")
                        break
                    
                    success = await _send_step_email(brevo, seq, step, idempotency_key, settings)
                    
                    if success:
                        results["sent"] += 1
                        await idempotency.mark_completed(idempotency_key)
                        await _advance_sequence(seq, steps)
                    else:
                        results["errors"] += 1
                        await idempotency.mark_failed(idempotency_key, "send_failed")
                        await redis_client.decr(daily_key)
                    
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error("Error processing sequence", sequence_id=str(seq["id"]), error=str(e))
                    results["errors"] += 1
        
        logger.info("Sequence processing complete", **results)
        
    finally:
        await redis_client.close()
    
    return results


async def _check_stop_conditions(seq: dict) -> bool:
    stop_on = seq.get("stop_on") or []
    
    async with get_db_connection() as db:
        if "replied" in stop_on:
            reply = await db.fetchval(
                "SELECT replied_at FROM marketing_prospects WHERE id = $1 AND replied_at IS NOT NULL",
                seq["prospect_id"]
            )
            if reply:
                await _stop_sequence(seq["id"], "replied")
                return True
        
        events = await db.fetch(
            "SELECT status FROM email_sends WHERE sequence_id = $1 ORDER BY created_at DESC LIMIT 5",
            seq["id"]
        )
        
        for event in events:
            if event["status"] in stop_on:
                await _stop_sequence(seq["id"], event["status"])
                return True
    
    return False


async def _send_step_email(brevo, seq, step, idempotency_key, settings) -> bool:
    template_name = step.get("body_template")
    if not template_name:
        return False
    
    async with get_db_connection() as db:
        email_template = await db.fetchrow(
            "SELECT subject_template, html_template, text_template FROM email_templates WHERE name = $1 AND is_active = TRUE",
            template_name
        )
        
        if not email_template:
            return False
        
        pdata = seq.get("personalization_data") or {}
        
        try:
            from jinja2 import Template
            subject = Template(email_template["subject_template"]).render(**pdata)
            html_content = Template(email_template["html_template"]).render(**pdata)
            text_content = Template(email_template["text_template"]).render(**pdata)
        except Exception as e:
            logger.error(f"Template rendering failed for {template_name}: {e}")
            return False
        
        result = await brevo.send_email(
            to_email=seq["email"],
            to_name=seq["full_name"] or "",
            subject=subject,
            html_content=html_content,
            text_content=text_content,
            reply_to=settings.brevo_sender_email,
            tags=[f"sequence:{seq['sequence_name']}", f"step:{seq['current_step'] + 1}"]
        )
        
        if not result.get("success"):
            return False
        
        await db.execute("""
            INSERT INTO email_sends (sequence_id, prospect_id, step_number, template_name, subject, to_email, brevo_message_id, idempotency_key, status, sent_at, open_count, click_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'sent', NOW(), 0, 0)
        """, seq["id"], seq["prospect_id"], seq["current_step"] + 1, template_name, subject, seq["email"], result.get("message_id"), idempotency_key)
        
        if seq["current_step"] == 0:
            await db.execute(
                "UPDATE marketing_prospects SET first_contacted_at = NOW(), last_contacted_at = NOW(), total_emails_sent = COALESCE(total_emails_sent, 0) + 1, status = 'contacted' WHERE id = $1",
                seq["prospect_id"]
            )
        else:
            await db.execute(
                "UPDATE marketing_prospects SET last_contacted_at = NOW(), total_emails_sent = COALESCE(total_emails_sent, 0) + 1 WHERE id = $1",
                seq["prospect_id"]
            )
    
    return True


async def _advance_sequence(seq, steps):
    next_step = seq["current_step"] + 1
    
    if next_step >= len(steps):
        await _complete_sequence(seq["id"])
    else:
        step = steps[next_step]
        next_send_at = _calculate_send_time(
            delay_days=step.get("delay_days", 3),
            delay_hours=step.get("delay_hours", 0),
            skip_weekends=step.get("skip_weekends", True)
        )
        
        async with get_db_connection() as db:
            await db.execute(
                "UPDATE outreach_sequences SET current_step = $1, status = 'active', next_send_at = $2, last_action_at = NOW() WHERE id = $3",
                next_step, next_send_at, seq["id"]
            )


def _calculate_send_time(delay_days=0, delay_hours=0, skip_weekends=True):
    send_time = datetime.utcnow() + timedelta(days=delay_days, hours=delay_hours)
    send_time = send_time.replace(hour=15, minute=0, second=0, microsecond=0)
    
    if skip_weekends:
        while send_time.weekday() >= 5:
            send_time += timedelta(days=1)
    
    return send_time


async def _complete_sequence(sequence_id):
    async with get_db_connection() as db:
        await db.execute(
            "UPDATE outreach_sequences SET status = 'completed', completed_at = NOW() WHERE id = $1",
            sequence_id
        )


async def _stop_sequence(sequence_id, reason):
    async with get_db_connection() as db:
        await db.execute(
            "UPDATE outreach_sequences SET status = 'stopped', stopped_reason = $1, completed_at = NOW() WHERE id = $2",
            reason, sequence_id
        )


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='outreach')
def auto_enroll_prospects(self):
    """Automatically enroll qualified prospects in sequences."""
    return asyncio.run(_auto_enroll_async())


async def _auto_enroll_async() -> dict:
    settings = get_settings()
    results = {"enrolled": 0, "skipped": 0, "errors": 0}
    
    async with get_db_connection() as db:
        prospects = await db.fetch("""
            SELECT mp.id, mp.email, mp.primary_platform, mp.relevance_score
            FROM marketing_prospects mp
            WHERE mp.email IS NOT NULL
              AND mp.email_verified = TRUE
              AND mp.verification_status IN ('valid', 'catch_all')
              AND mp.status IN ('discovered', 'enriched')
              AND mp.relevance_score >= $1
              AND mp.first_contacted_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM outreach_sequences os
                  WHERE os.prospect_id = mp.id AND os.status IN ('pending', 'active')
              )
            ORDER BY mp.relevance_score DESC LIMIT 25
        """, settings.min_relevance_score)
        
        logger.info(f"Found {len(prospects)} prospects to enroll")
        
        for prospect in prospects:
            try:
                platform = prospect["primary_platform"] or "youtube"
                sequence_name = f"{platform}_creator"
                
                template_exists = await db.fetchval(
                    "SELECT 1 FROM sequence_templates WHERE name = $1 AND is_active = TRUE",
                    sequence_name
                )
                
                if not template_exists:
                    sequence_name = "youtube_creator"
                
                template = await db.fetchrow(
                    "SELECT id, total_steps, steps FROM sequence_templates WHERE name = $1 AND is_active = TRUE",
                    sequence_name
                )
                
                if not template or not template.get("id"):
                    results["skipped"] += 1
                    continue
                
                template_steps = template.get("steps") or []
                if not template_steps:
                    results["skipped"] += 1
                    continue
                
                first_step = template_steps[0]
                first_send_at = _calculate_send_time(
                    delay_days=first_step.get("delay_days", 0),
                    delay_hours=first_step.get("delay_hours", 0),
                    skip_weekends=first_step.get("skip_weekends", True)
                )
                
                full_prospect = await db.fetchrow("SELECT * FROM marketing_prospects WHERE id = $1", prospect["id"])
                if not full_prospect:
                    results["skipped"] += 1
                    continue
                
                pdata = _build_personalization(full_prospect, settings)
                
                await db.execute("""
                    INSERT INTO outreach_sequences (prospect_id, sequence_template_id, sequence_name, total_steps, current_step, status, next_send_at, personalization_data, created_at)
                    VALUES ($1, $2, $3, $4, 0, 'pending', $5, $6, NOW())
                """, prospect["id"], template["id"], sequence_name, template["total_steps"], first_send_at, json.dumps(pdata))
                
                results["enrolled"] += 1
                
            except Exception as e:
                logger.error("Failed to enroll prospect", prospect_id=str(prospect["id"]), error=str(e))
                results["errors"] += 1
    
    logger.info("Auto-enrollment complete", **results)
    return results


def _build_personalization(prospect, settings):
    full_name = prospect.get("full_name", "")
    first_name = full_name.split()[0] if full_name else "there"
    
    competitors = prospect.get("competitor_mentions") or []
    competitor = competitors[0].title() if competitors else "AI video tools"
    
    def format_count(count):
        if not count:
            return ""
        if count >= 1000000:
            return f"{count/1000000:.1f}M"
        elif count >= 1000:
            return f"{count/1000:.0f}K"
        return str(count)
    
    affiliate_link = f"{settings.affiliate_signup_base_url}?ref={str(prospect['id'])[:8]}"
    
    return {
        "first_name": first_name,
        "full_name": full_name,
        "competitor": competitor,
        "affiliate_link": affiliate_link,
        "youtube_handle": prospect.get("youtube_handle", ""),
        "subscriber_count": format_count(prospect.get("youtube_subscribers", 0)),
        "platform": prospect.get("primary_platform", "youtube")
    }
