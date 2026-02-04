"""
ReelForge Marketing Engine - Outreach Tasks
"""
import sys
sys.path.insert(0, '/app')

import asyncio
import json
import re
from datetime import datetime, timedelta
from jinja2 import Template, UndefinedError
import structlog
import redis.asyncio as redis

# RFC 5322 compliant email regex pattern (simplified but robust)
EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

from celery_config import celery_app, BaseTaskWithRetry
from app.config import get_settings
from app.database import get_database_async, DatabaseTransaction

logger = structlog.get_logger()


def safe_json_loads(data, default=None):
    """Safely parse JSON from string or return dict if already parsed."""
    if default is None:
        default = {}
    if data is None:
        return default
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        return data
    if isinstance(data, str):
        try:
            return json.loads(data) if data.strip() else default
        except (json.JSONDecodeError, ValueError):
            logger.warning("Failed to parse JSON", data=data[:100] if len(data) > 100 else data)
            return default
    return default


def safe_render_template(template_str: str, data: dict, default: str = "") -> str:
    """Safely render Jinja2 template with fallback."""
    if not template_str:
        return default
    try:
        return Template(template_str).render(**data)
    except UndefinedError as e:
        logger.warning("Template variable undefined", error=str(e), template=template_str[:50])
        # Try rendering with missing vars replaced by placeholders
        try:
            from jinja2 import Environment, Undefined
            env = Environment(undefined=Undefined)
            return env.from_string(template_str).render(**data)
        except:
            return default
    except Exception as e:
        logger.error("Template render failed", error=str(e))
        return default


async def generate_ai_email(prospect: dict, template_type: str = "initial") -> dict:
    """Generate AI-personalized email for a prospect."""
    settings = get_settings()
    
    if not settings.anthropic_api_key:
        return None
    
    try:
        from services.ai_personalization import AIPersonalizationService, YouTubeVideoFetcher
        
        ai_service = AIPersonalizationService()
        video_fetcher = YouTubeVideoFetcher()
        
        # Fetch latest video for context
        video_data = None
        if prospect.get("youtube_channel_id"):
            video_data = await video_fetcher.get_latest_video(prospect["youtube_channel_id"])
        
        # Generate personalized email
        result = await ai_service.generate_personalized_email(
            prospect=prospect,
            video_data=video_data,
            template_type=template_type
        )
        
        if result.get("subject") and result.get("body"):
            logger.info("AI email generated", prospect_id=str(prospect.get("id", "")), template_type=template_type)
            return result
        
        return None
        
    except Exception as e:
        logger.error("AI email generation failed", error=str(e))
        return None


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='outreach')
def process_pending_sequences(self):
    return asyncio.run(_process_sequences_async())


async def _process_sequences_async() -> dict:
    settings = get_settings()
    db = None
    
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
        db = await get_database_async()
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
            
            # Fetch sequences that are ready to send AND don't already have an email sent for this step
            pending = await db.fetch("""
                SELECT os.id, os.prospect_id, os.sequence_name, os.current_step,
                       os.total_steps, os.personalization_data,
                       mp.email, mp.full_name, mp.competitor_mentions,
                       st.steps
                FROM outreach_sequences os
                JOIN marketing_prospects mp ON os.prospect_id = mp.id
                JOIN sequence_templates st ON os.sequence_name = st.name
                WHERE os.status IN ('pending', 'active')
                  AND os.next_send_at <= NOW()
                  AND mp.email IS NOT NULL
                  AND mp.email_verified = TRUE
                  AND mp.status NOT IN ('unsubscribed', 'bounced')
                  -- Prevent duplicate sends: skip if email already sent for this sequence + step
                  AND NOT EXISTS (
                      SELECT 1 FROM email_sends es
                      WHERE es.sequence_id = os.id
                        AND es.step_number = os.current_step + 1
                  )
                ORDER BY os.next_send_at ASC
                LIMIT $1
            """, remaining)
            
            for seq in pending:
                results["processed"] += 1
                
                try:
                    # Parse steps JSON safely
                    steps = safe_json_loads(seq["steps"], [])
                    current_step = seq["current_step"] or 0
                    
                    # Edge case: current_step beyond available steps
                    if current_step >= len(steps):
                        await db.execute(
                            "UPDATE outreach_sequences SET status = 'completed', completed_at = NOW() WHERE id = $1",
                            seq["id"]
                        )
                        results["skipped"] += 1
                        continue
                    
                    step = steps[current_step]
                    template_name = step.get("body_template")
                    
                    # Edge case: no template name in step config
                    if not template_name:
                        logger.warning("No template in step config", sequence_id=str(seq["id"]), step=current_step)
                        results["skipped"] += 1
                        continue
                    
                    email_template = await db.fetchrow(
                        "SELECT subject_template, html_template, text_template FROM email_templates WHERE name = $1 AND is_active = TRUE",
                        template_name
                    )
                    
                    # Edge case: template doesn't exist
                    if not email_template:
                        logger.warning("Email template not found", template=template_name)
                        results["skipped"] += 1
                        continue
                    
                    # Parse personalization data safely
                    pdata = safe_json_loads(seq["personalization_data"], {})
                    
                    # Edge case: ensure required fields exist with fallbacks
                    if "first_name" not in pdata or not pdata["first_name"]:
                        full_name = seq["full_name"] or ""
                        pdata["first_name"] = full_name.split()[0] if full_name else "there"
                    
                    if "full_name" not in pdata:
                        pdata["full_name"] = seq["full_name"] or ""
                    
                    if "affiliate_link" not in pdata:
                        pdata["affiliate_link"] = f"{settings.affiliate_signup_base_url}?ref={str(seq['prospect_id'])[:8]}"
                    
                    # Edge case: add competitor from prospect if not in pdata
                    if "competitor" not in pdata or not pdata["competitor"]:
                        competitor_mentions = seq.get("competitor_mentions")
                        if competitor_mentions and len(competitor_mentions) > 0:
                            pdata["competitor"] = competitor_mentions[0]
                        else:
                            pdata["competitor"] = "AI video tools"
                    
                    # Edge case: validate email format with proper regex
                    to_email = seq["email"]
                    if not to_email or not EMAIL_PATTERN.match(to_email):
                        logger.warning("Invalid email format", sequence_id=str(seq["id"]), email=to_email[:20] if to_email else None)
                        results["skipped"] += 1
                        continue
                    
                    # Use AI-generated email if available, otherwise use template
                    if pdata.get("use_ai_email") and current_step == 0 and pdata.get("ai_subject"):
                        subject = pdata["ai_subject"]
                        html_content = pdata["ai_body"]
                        logger.info("Using AI-generated email", sequence_id=str(seq["id"]))
                    else:
                        # Render templates safely
                        subject = safe_render_template(
                            email_template["subject_template"], 
                            pdata, 
                            f"Partnership opportunity for {pdata.get('first_name', 'you')}"
                        )
                        html_content = safe_render_template(
                            email_template["html_template"], 
                            pdata,
                            f"<p>Hi {pdata.get('first_name', 'there')},</p><p>We have a partnership opportunity for you.</p>"
                        )
                    
                    # Edge case: empty rendered content
                    if not subject or not html_content:
                        logger.warning("Empty rendered content", sequence_id=str(seq["id"]))
                        results["skipped"] += 1
                        continue

                    # Double-check for duplicates right before sending (race condition protection)
                    existing_send = await db.fetchval("""
                        SELECT id FROM email_sends
                        WHERE sequence_id = $1 AND step_number = $2
                    """, seq["id"], current_step + 1)

                    if existing_send:
                        logger.warning("Duplicate email prevented", sequence_id=str(seq["id"]), step=current_step + 1)
                        results["skipped"] += 1
                        continue

                    result = await brevo.send_email(
                        to_email=to_email,
                        to_name=seq["full_name"] or "",
                        subject=subject,
                        html_content=html_content,
                        tags=[f"sequence:{seq['sequence_name']}", f"step:{current_step + 1}", "ai_personalized" if pdata.get("use_ai_email") else "template"]
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
                                subject, to_email, result.get("message_id"))
                            
                            # Update prospect stats
                            await conn.execute("""
                                UPDATE marketing_prospects 
                                SET total_emails_sent = COALESCE(total_emails_sent, 0) + 1,
                                    last_contacted_at = NOW(),
                                    first_contacted_at = COALESCE(first_contacted_at, NOW()),
                                    status = 'contacted'
                                WHERE id = $1
                            """, seq["prospect_id"])
                            
                            next_step = current_step + 1
                            if next_step >= len(steps):
                                await conn.execute(
                                    "UPDATE outreach_sequences SET status = 'completed', current_step = $1, completed_at = NOW() WHERE id = $2",
                                    next_step, seq["id"]
                                )
                            else:
                                delay_days = steps[next_step].get("delay_days", 3)
                                next_send = datetime.utcnow() + timedelta(days=delay_days)
                                
                                # Edge case: skip weekends if configured
                                if steps[next_step].get("skip_weekends", False):
                                    while next_send.weekday() >= 5:  # 5=Saturday, 6=Sunday
                                        next_send += timedelta(days=1)
                                
                                await conn.execute(
                                    "UPDATE outreach_sequences SET status = 'active', current_step = $1, next_send_at = $2 WHERE id = $3",
                                    next_step, next_send, seq["id"]
                                )
                        
                        results["sent"] += 1
                        logger.info("Email sent", to=to_email, step=current_step + 1, sequence=seq["sequence_name"])
                    else:
                        logger.error("Brevo send failed", error=result.get("error"), to=to_email)
                        results["errors"] += 1
                    
                    # Rate limit between sends
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error("Sequence processing error", sequence_id=str(seq["id"]), error=str(e))
                    results["errors"] += 1
        
        logger.info("Sequence processing complete", **results)
        
    except Exception as e:
        logger.error("Sequence processing failed", error=str(e))
        results["error"] = str(e)
    finally:
        if db:
            await db.close()
    
    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='outreach')
def auto_enroll_prospects(self):
    return asyncio.run(_auto_enroll_async())


async def _auto_enroll_async() -> dict:
    settings = get_settings()
    db = None
    
    results = {"enrolled": 0, "skipped": 0, "errors": 0, "ai_generated": 0}
    
    try:
        db = await get_database_async()
        
        prospects = await db.fetch("""
            SELECT mp.id, mp.email, mp.full_name, mp.primary_platform, 
                   mp.relevance_score, mp.competitor_mentions,
                   mp.youtube_channel_id, mp.youtube_handle, mp.youtube_subscribers,
                   mp.instagram_handle, mp.instagram_followers,
                   mp.tiktok_handle, mp.tiktok_followers
            FROM marketing_prospects mp
            WHERE mp.email IS NOT NULL
              AND mp.email_verified = TRUE
              AND mp.status IN ('discovered', 'enriched')
              AND mp.relevance_score >= $1
              AND NOT EXISTS (
                  SELECT 1 FROM outreach_sequences os
                  WHERE os.prospect_id = mp.id AND os.status IN ('pending', 'active', 'completed')
              )
            ORDER BY mp.relevance_score DESC
            LIMIT $2
        """, settings.min_relevance_score, settings.auto_enrollment_limit)
        
        for prospect in prospects:
            try:
                platform = prospect["primary_platform"] or "youtube"
                sequence_name = f"{platform}_creator"
                
                template = await db.fetchrow(
                    "SELECT id, total_steps, steps FROM sequence_templates WHERE name = $1 AND is_active = TRUE",
                    sequence_name
                )
                
                # Fallback to any active template
                if not template:
                    template = await db.fetchrow(
                        "SELECT id, total_steps, steps FROM sequence_templates WHERE is_active = TRUE ORDER BY created_at DESC LIMIT 1"
                    )
                
                if not template:
                    logger.warning("No sequence template found", platform=platform)
                    results["skipped"] += 1
                    continue
                
                # Parse steps safely
                steps = safe_json_loads(template["steps"], [])
                
                # Edge case: empty steps array
                if not steps:
                    logger.warning("Empty steps in template", template_id=str(template["id"]))
                    results["skipped"] += 1
                    continue
                
                first_delay = steps[0].get("delay_days", 0)
                first_send = datetime.utcnow() + timedelta(days=first_delay)
                
                # Skip weekends for first send if configured
                if steps[0].get("skip_weekends", False):
                    while first_send.weekday() >= 5:
                        first_send += timedelta(days=1)
                
                # Build personalization data
                full_name = prospect["full_name"] or ""
                first_name = full_name.split()[0] if full_name else "there"
                
                # Get competitor from prospect data
                competitor_mentions = prospect.get("competitor_mentions")
                competitor = "AI video tools"
                if competitor_mentions and len(competitor_mentions) > 0:
                    competitor = competitor_mentions[0]
                
                pdata = {
                    "first_name": first_name,
                    "full_name": full_name,
                    "email": prospect["email"],
                    "competitor": competitor,
                    "affiliate_link": f"{settings.affiliate_signup_base_url}?ref={str(prospect['id'])[:8]}"
                }
                
                # Try AI-generated personalized email
                ai_email = await generate_ai_email(dict(prospect), "initial")
                if ai_email:
                    pdata["ai_subject"] = ai_email.get("subject", "")
                    pdata["ai_body"] = ai_email.get("body", "")
                    pdata["ai_text_body"] = ai_email.get("text_body", "")
                    pdata["use_ai_email"] = True
                    results["ai_generated"] += 1
                
                await db.execute("""
                    INSERT INTO outreach_sequences (
                        prospect_id, sequence_template_id, sequence_name, total_steps,
                        current_step, status, next_send_at, personalization_data, created_at
                    ) VALUES ($1, $2, $3, $4, 0, 'pending', $5, $6, NOW())
                """, prospect["id"], template["id"], sequence_name, template["total_steps"],
                    first_send, json.dumps(pdata))
                
                # Update prospect status
                await db.execute(
                    "UPDATE marketing_prospects SET status = 'enrolled' WHERE id = $1",
                    prospect["id"]
                )
                
                results["enrolled"] += 1
                logger.info("Prospect enrolled", prospect_id=str(prospect["id"]), sequence=sequence_name)
                
            except Exception as e:
                logger.error("Enrollment failed", prospect_id=str(prospect["id"]), error=str(e))
                results["errors"] += 1
        
        logger.info("Auto-enrollment complete", **results)
        
    except Exception as e:
        logger.error("Auto-enrollment failed", error=str(e))
        results["error"] = str(e)
    finally:
        if db:
            await db.close()
    
    return results


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='outreach')
def stop_sequence_on_reply(self, prospect_id: str):
    """Stop active sequences when a prospect replies."""
    return asyncio.run(_stop_sequence_async(prospect_id, "replied"))


@celery_app.task(bind=True, base=BaseTaskWithRetry, max_retries=3, queue='outreach')
def stop_sequence_on_unsubscribe(self, prospect_id: str):
    """Stop active sequences when a prospect unsubscribes."""
    return asyncio.run(_stop_sequence_async(prospect_id, "unsubscribed"))


async def _stop_sequence_async(prospect_id: str, reason: str) -> dict:
    db = None
    try:
        db = await get_database_async()
        
        result = await db.execute("""
            UPDATE outreach_sequences 
            SET status = 'stopped', stopped_reason = $1, completed_at = NOW()
            WHERE prospect_id = $2 AND status IN ('pending', 'active')
        """, reason, prospect_id)
        
        # Update prospect status
        await db.execute(
            "UPDATE marketing_prospects SET status = $1, replied_at = CASE WHEN $1 = 'replied' THEN NOW() ELSE replied_at END WHERE id = $2",
            reason, prospect_id
        )
        
        logger.info("Sequence stopped", prospect_id=prospect_id, reason=reason)
        return {"success": True, "reason": reason}
        
    except Exception as e:
        logger.error("Failed to stop sequence", error=str(e))
        return {"success": False, "error": str(e)}
    finally:
        if db:
            await db.close()
