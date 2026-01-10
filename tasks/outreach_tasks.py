"""
ReelForge Marketing Engine - Outreach Tasks
Celery tasks for email sequence processing with idempotency
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional
from jinja2 import Template
import structlog

from celery_config import celery_app, BaseTaskWithRetry
from app.config import get_settings
from app.database import get_database
from services.idempotency import get_idempotency_service
from outreach.brevo_client import BrevoClient

logger = structlog.get_logger()


@celery_app.task(
    bind=True,
    base=BaseTaskWithRetry,
    max_retries=3,
    default_retry_delay=300,
    queue='outreach'
)
def process_pending_sequences(self):
    """
    Process all pending email sequences.
    
    Runs every 15 minutes via Celery Beat.
    Includes idempotency protection to prevent duplicate sends.
    """
    # Run async code in sync context
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_process_sequences_async())


async def _process_sequences_async() -> dict:
    """Async implementation of sequence processing."""
    settings = get_settings()
    db = get_database()
    brevo = BrevoClient()
    idempotency = get_idempotency_service()
    
    results = {
        "processed": 0,
        "sent": 0,
        "skipped_duplicate": 0,
        "skipped_unverified": 0,
        "errors": 0,
        "limit_reached": False
    }
    
    # Check daily limit using Redis
    import redis.asyncio as redis
    redis_client = redis.from_url(settings.redis_url)
    
    today = datetime.utcnow().strftime("%Y%m%d")
    daily_key = f"email_count:{today}"
    
    current_count = await redis_client.get(daily_key)
    current_count = int(current_count) if current_count else 0
    
    if current_count >= settings.daily_email_limit:
        logger.info("Daily email limit reached", limit=settings.daily_email_limit)
        results["limit_reached"] = True
        await redis_client.close()
        return results
    
    remaining = settings.daily_email_limit - current_count
    
    # Get pending sequences (ONLY verified emails)
    pending = await db.fetch(
        """
        SELECT 
            os.id, os.prospect_id, os.sequence_name, os.current_step,
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
        """,
        remaining
    )
    
    logger.info(f"Processing {len(pending)} pending sequences")
    
    for seq in pending:
        results["processed"] += 1
        
        try:
            # Generate idempotency key
            idempotency_key = idempotency.generate_key(
                sequence_id=str(seq["id"]),
                step_number=seq["current_step"] + 1,
                prospect_email=seq["email"]
            )
            
            # Check idempotency (prevents duplicate sends)
            can_send = await idempotency.check_and_acquire(idempotency_key)
            
            if not can_send:
                logger.info(
                    "Skipping duplicate send",
                    sequence_id=str(seq["id"]),
                    step=seq["current_step"] + 1
                )
                results["skipped_duplicate"] += 1
                continue
            
            # Check stop conditions
            should_stop = await _check_stop_conditions(db, seq)
            if should_stop:
                await idempotency.mark_completed(idempotency_key)
                continue
            
            # Get current step configuration
            steps = seq["steps"]
            current_step = seq["current_step"]
            
            if current_step >= len(steps):
                await _complete_sequence(db, seq["id"])
                await idempotency.mark_completed(idempotency_key)
                continue
            
            step = steps[current_step]
            
            # Send the email
            success = await _send_step_email(db, brevo, seq, step, idempotency_key)
            
            if success:
                results["sent"] += 1
                
                # Increment Redis counter
                await redis_client.incr(daily_key)
                await redis_client.expire(daily_key, 86400)  # TTL 24 hours
                
                # Mark idempotency key as completed
                await idempotency.mark_completed(idempotency_key)
                
                # Advance to next step
                await _advance_sequence(db, seq, steps)
                
                # Check if daily limit reached
                new_count = await redis_client.get(daily_key)
                if int(new_count) >= settings.daily_email_limit:
                    results["limit_reached"] = True
                    break
            else:
                results["errors"] += 1
                await idempotency.mark_failed(idempotency_key)
            
            # Small delay between sends
            await asyncio.sleep(0.5)
            
        except Exception as e:
            logger.error(
                "Error processing sequence",
                sequence_id=str(seq["id"]),
                error=str(e)
            )
            results["errors"] += 1
    
    await redis_client.close()
    logger.info("Sequence processing complete", **results)
    return results


async def _check_stop_conditions(db, seq: dict) -> bool:
    """Check if sequence should be stopped."""
    stop_on = seq.get("stop_on") or []
    
    # Check for replies
    if "replied" in stop_on:
        reply = await db.fetchval(
            """
            SELECT replied_at FROM marketing_prospects
            WHERE id = $1 AND replied_at IS NOT NULL
            """,
            seq["prospect_id"]
        )
        if reply:
            await _stop_sequence(db, seq["id"], "replied")
            return True
    
    # Check email events
    events = await db.fetch(
        """
        SELECT status FROM email_sends
        WHERE sequence_id = $1
        ORDER BY created_at DESC
        LIMIT 5
        """,
        seq["id"]
    )
    
    for event in events:
        if event["status"] in stop_on:
            await _stop_sequence(db, seq["id"], event["status"])
            return True
    
    return False


async def _send_step_email(
    db,
    brevo: BrevoClient,
    seq: dict,
    step: dict,
    idempotency_key: str
) -> bool:
    """Send email for current sequence step with transaction protection."""
    
    # Get email template
    template_name = step.get("body_template")
    email_template = await db.fetchrow(
        """
        SELECT subject_template, html_template, text_template
        FROM email_templates
        WHERE name = $1 AND is_active = TRUE
        """,
        template_name
    )
    
    if not email_template:
        logger.error(f"Email template not found: {template_name}")
        return False
    
    # Personalize content
    pdata = seq.get("personalization_data") or {}
    
    try:
        subject = Template(email_template["subject_template"]).render(**pdata)
        html_content = Template(email_template["html_template"]).render(**pdata)
        text_content = Template(email_template["text_template"]).render(**pdata)
    except Exception as e:
        logger.error(f"Template rendering failed: {e}")
        return False
    
    settings = get_settings()
    
    # Use transaction for atomicity
    async with db.transaction():
        # Insert idempotency record FIRST (before sending)
        await db.execute(
            """
            INSERT INTO idempotency_keys (key, status, created_at, expires_at)
            VALUES ($1, 'processing', NOW(), NOW() + INTERVAL '7 days')
            ON CONFLICT (key) DO NOTHING
            """,
            idempotency_key
        )
        
        # Send via Brevo
        result = await brevo.send_email(
            to_email=seq["email"],
            to_name=seq["full_name"] or "",
            subject=subject,
            html_content=html_content,
            text_content=text_content,
            reply_to=settings.brevo_sender_email,
            tags=[
                f"sequence:{seq['sequence_name']}",
                f"step:{seq['current_step'] + 1}"
            ]
        )
        
        if not result.get("success"):
            logger.error(
                "Failed to send email",
                prospect_email=seq["email"],
                error=result.get("error")
            )
            raise Exception(f"Brevo send failed: {result.get('error')}")
        
        # Log the send
        await db.execute(
            """
            INSERT INTO email_sends (
                sequence_id, prospect_id, step_number,
                template_name, subject, to_email,
                brevo_message_id, idempotency_key, status, sent_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, 'sent', NOW()
            )
            """,
            seq["id"],
            seq["prospect_id"],
            seq["current_step"] + 1,
            template_name,
            subject,
            seq["email"],
            result.get("message_id"),
            idempotency_key
        )
        
        # Update prospect
        is_first_contact = seq["current_step"] == 0
        if is_first_contact:
            await db.execute(
                """
                UPDATE marketing_prospects SET
                    first_contacted_at = NOW(),
                    last_contacted_at = NOW(),
                    total_emails_sent = total_emails_sent + 1,
                    status = 'contacted'
                WHERE id = $1
                """,
                seq["prospect_id"]
            )
        else:
            await db.execute(
                """
                UPDATE marketing_prospects SET
                    last_contacted_at = NOW(),
                    total_emails_sent = total_emails_sent + 1
                WHERE id = $1
                """,
                seq["prospect_id"]
            )
        
        # Log consent (GDPR)
        await db.execute(
            """
            INSERT INTO consent_log (
                prospect_id, consent_type, consent_text, source
            ) VALUES (
                $1, 'legitimate_interest', 
                'Business contact for affiliate partnership inquiry',
                'outreach_sequence'
            )
            ON CONFLICT DO NOTHING
            """,
            seq["prospect_id"]
        )
    
    logger.info(
        "Sent sequence email",
        prospect_email=seq["email"][:5] + "***",
        sequence=seq["sequence_name"],
        step=seq["current_step"] + 1
    )
    
    return True


async def _advance_sequence(db, seq: dict, steps: list):
    """Advance sequence to next step or complete."""
    next_step = seq["current_step"] + 1
    
    if next_step >= len(steps):
        await _complete_sequence(db, seq["id"])
    else:
        step = steps[next_step]
        next_send_at = _calculate_send_time(
            delay_days=step.get("delay_days", 3),
            delay_hours=step.get("delay_hours", 0),
            skip_weekends=step.get("skip_weekends", True)
        )
        
        await db.execute(
            """
            UPDATE outreach_sequences SET
                current_step = $1,
                status = 'active',
                next_send_at = $2,
                last_action_at = NOW()
            WHERE id = $3
            """,
            next_step,
            next_send_at,
            seq["id"]
        )


def _calculate_send_time(
    delay_days: int = 0,
    delay_hours: int = 0,
    skip_weekends: bool = True
) -> datetime:
    """Calculate next send time respecting business hours."""
    send_time = datetime.utcnow() + timedelta(days=delay_days, hours=delay_hours)
    
    # Set to 10 AM EST (3 PM UTC)
    send_time = send_time.replace(hour=15, minute=0, second=0, microsecond=0)
    
    # Skip weekends
    if skip_weekends:
        while send_time.weekday() >= 5:
            send_time += timedelta(days=1)
    
    return send_time


async def _complete_sequence(db, sequence_id: str):
    """Mark sequence as complete."""
    await db.execute(
        """
        UPDATE outreach_sequences SET
            status = 'completed',
            completed_at = NOW()
        WHERE id = $1
        """,
        sequence_id
    )
    logger.info(f"Sequence completed: {sequence_id}")


async def _stop_sequence(db, sequence_id: str, reason: str):
    """Stop sequence with reason."""
    await db.execute(
        """
        UPDATE outreach_sequences SET
            status = 'stopped',
            stopped_reason = $1,
            completed_at = NOW()
        WHERE id = $2
        """,
        reason,
        sequence_id
    )
    logger.info(f"Sequence stopped: {sequence_id}, reason: {reason}")


@celery_app.task(
    bind=True,
    base=BaseTaskWithRetry,
    max_retries=3,
    queue='outreach'
)
def auto_enroll_prospects(self):
    """
    Automatically enroll qualified prospects in sequences.
    
    Runs every 3 hours via Celery Beat.
    Only enrolls prospects with verified emails.
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_auto_enroll_async())


async def _auto_enroll_async() -> dict:
    """Async implementation of auto-enrollment."""
    settings = get_settings()
    db = get_database()
    
    results = {"enrolled": 0, "skipped": 0, "errors": 0}
    
    # Get qualified prospects with VERIFIED emails only
    prospects = await db.fetch(
        """
        SELECT 
            mp.id, mp.email, mp.primary_platform, mp.relevance_score
        FROM marketing_prospects mp
        WHERE mp.email IS NOT NULL
          AND mp.email_verified = TRUE
          AND mp.verification_status IN ('valid', 'catch_all')
          AND mp.status IN ('discovered', 'enriched')
          AND mp.relevance_score >= $1
          AND mp.first_contacted_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM outreach_sequences os
              WHERE os.prospect_id = mp.id
                AND os.status IN ('pending', 'active')
          )
        ORDER BY mp.relevance_score DESC
        LIMIT 25
        """,
        settings.min_relevance_score
    )
    
    logger.info(f"Found {len(prospects)} prospects to enroll")
    
    for prospect in prospects:
        try:
            platform = prospect["primary_platform"] or "youtube"
            sequence_name = f"{platform}_creator"
            
            # Check if sequence template exists
            template_exists = await db.fetchval(
                """
                SELECT 1 FROM sequence_templates
                WHERE name = $1 AND is_active = TRUE
                """,
                sequence_name
            )
            
            if not template_exists:
                sequence_name = "youtube_creator"
            
            # Get template
            template = await db.fetchrow(
                """
                SELECT id, total_steps, steps
                FROM sequence_templates
                WHERE name = $1 AND is_active = TRUE
                """,
                sequence_name
            )
            
            if not template:
                results["skipped"] += 1
                continue
            
            # Calculate first send time
            first_step = template["steps"][0] if template["steps"] else {}
            first_send_at = _calculate_send_time(
                delay_days=first_step.get("delay_days", 0),
                delay_hours=first_step.get("delay_hours", 0),
                skip_weekends=first_step.get("skip_weekends", True)
            )
            
            # Build personalization data
            full_prospect = await db.fetchrow(
                """
                SELECT * FROM marketing_prospects WHERE id = $1
                """,
                prospect["id"]
            )
            
            pdata = _build_personalization(full_prospect, settings)
            
            # Enroll in sequence
            await db.execute(
                """
                INSERT INTO outreach_sequences (
                    prospect_id, sequence_template_id, sequence_name,
                    total_steps, current_step, status,
                    next_send_at, personalization_data, created_at
                ) VALUES (
                    $1, $2, $3, $4, 0, 'pending', $5, $6, NOW()
                )
                """,
                prospect["id"],
                template["id"],
                sequence_name,
                template["total_steps"],
                first_send_at,
                pdata
            )
            
            results["enrolled"] += 1
            
        except Exception as e:
            logger.error(
                "Failed to enroll prospect",
                prospect_id=str(prospect["id"]),
                error=str(e)
            )
            results["errors"] += 1
    
    logger.info("Auto-enrollment complete", **results)
    return results


def _build_personalization(prospect: dict, settings) -> dict:
    """Build personalization data for email templates."""
    full_name = prospect.get("full_name", "")
    first_name = full_name.split()[0] if full_name else "there"
    
    competitors = prospect.get("competitor_mentions") or []
    competitor = competitors[0].title() if competitors else "AI video tools"
    
    def format_count(count: int) -> str:
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
