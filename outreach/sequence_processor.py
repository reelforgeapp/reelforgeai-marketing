"""
ReelForge Marketing Engine - Sequence Processor
Handles multi-step email sequence automation
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional
from jinja2 import Template
import structlog

from app.config import get_settings
from app.database import get_database
from outreach.brevo_client import BrevoClient

logger = structlog.get_logger()


class SequenceProcessor:
    """
    Processes email sequences for prospect outreach.
    
    Features:
    - Multi-step sequences with configurable delays
    - Personalization using Jinja2 templates
    - Skip weekends option
    - Stop conditions (replied, clicked, bounced, etc.)
    - Daily send limits (respects Brevo free tier)
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.db = get_database()
        self.brevo = BrevoClient()
        self.daily_limit = self.settings.daily_email_limit
        self.emails_sent_today = 0
        self.last_reset_date = datetime.utcnow().date()
    
    def _reset_daily_counter_if_needed(self):
        """Reset daily email counter at midnight."""
        today = datetime.utcnow().date()
        if today != self.last_reset_date:
            self.emails_sent_today = 0
            self.last_reset_date = today
            logger.info("Daily email counter reset")
    
    async def enroll_prospect(
        self,
        prospect_id: str,
        sequence_name: str,
        personalization_data: Optional[dict] = None
    ) -> Optional[str]:
        """
        Enroll a prospect in an email sequence.
        
        Args:
            prospect_id: UUID of the prospect
            sequence_name: Name of the sequence template
            personalization_data: Additional data for email personalization
        
        Returns:
            Sequence ID if successful, None otherwise
        """
        # Get prospect
        prospect = await self.db.fetchrow(
            """
            SELECT id, email, full_name, status, primary_platform,
                   youtube_handle, instagram_handle, tiktok_handle,
                   youtube_subscribers, instagram_followers, tiktok_followers,
                   competitor_mentions, first_contacted_at
            FROM marketing_prospects
            WHERE id = $1
            """,
            prospect_id
        )
        
        if not prospect:
            logger.warning(f"Prospect not found: {prospect_id}")
            return None
        
        if not prospect["email"]:
            logger.warning(f"Prospect has no email: {prospect_id}")
            return None
        
        if prospect["first_contacted_at"]:
            logger.warning(f"Prospect already contacted: {prospect_id}")
            return None
        
        # Check for existing active sequence
        existing = await self.db.fetchrow(
            """
            SELECT id FROM outreach_sequences
            WHERE prospect_id = $1 AND status IN ('pending', 'active')
            """,
            prospect_id
        )
        
        if existing:
            logger.warning(f"Prospect already in sequence: {prospect_id}")
            return None
        
        # Get sequence template
        template = await self.db.fetchrow(
            """
            SELECT id, name, total_steps, steps, stop_on, min_relevance_score
            FROM sequence_templates
            WHERE name = $1 AND is_active = TRUE
            """,
            sequence_name
        )
        
        if not template:
            logger.error(f"Sequence template not found: {sequence_name}")
            return None
        
        # Build personalization data
        pdata = self._build_personalization_data(prospect, personalization_data)
        
        # Calculate first send time
        first_step = template["steps"][0] if template["steps"] else {}
        first_send_at = self._calculate_send_time(
            delay_days=first_step.get("delay_days", 0),
            delay_hours=first_step.get("delay_hours", 0),
            preferred_time=first_step.get("send_time_preference", "10:00"),
            skip_weekends=first_step.get("skip_weekends", True)
        )
        
        # Create sequence enrollment
        sequence_id = await self.db.fetchval(
            """
            INSERT INTO outreach_sequences (
                prospect_id, sequence_template_id, sequence_name,
                total_steps, current_step, status,
                next_send_at, personalization_data, created_at
            ) VALUES (
                $1, $2, $3, $4, 0, 'pending', $5, $6, NOW()
            )
            RETURNING id
            """,
            prospect_id,
            template["id"],
            sequence_name,
            template["total_steps"],
            first_send_at,
            pdata
        )
        
        logger.info(
            "Enrolled prospect in sequence",
            prospect_id=prospect_id,
            sequence_name=sequence_name,
            sequence_id=str(sequence_id),
            first_send_at=first_send_at.isoformat()
        )
        
        return str(sequence_id)
    
    def _build_personalization_data(
        self,
        prospect: dict,
        extra_data: Optional[dict] = None
    ) -> dict:
        """Build personalization data for email templates."""
        # Extract first name
        full_name = prospect.get("full_name", "")
        first_name = full_name.split()[0] if full_name else "there"
        
        # Get primary competitor mention
        competitors = prospect.get("competitor_mentions") or []
        competitor = competitors[0].title() if competitors else "AI video tools"
        
        # Format subscriber/follower count
        def format_count(count: int) -> str:
            if not count:
                return ""
            if count >= 1000000:
                return f"{count/1000000:.1f}M"
            elif count >= 1000:
                return f"{count/1000:.0f}K"
            return str(count)
        
        # Build affiliate link
        affiliate_link = f"{self.settings.affiliate_signup_base_url}?ref={str(prospect['id'])[:8]}"
        
        data = {
            "first_name": first_name,
            "full_name": full_name,
            "competitor": competitor,
            "affiliate_link": affiliate_link,
            "youtube_handle": prospect.get("youtube_handle", ""),
            "instagram_handle": prospect.get("instagram_handle", ""),
            "tiktok_handle": prospect.get("tiktok_handle", ""),
            "subscriber_count": format_count(prospect.get("youtube_subscribers", 0)),
            "follower_count": format_count(
                prospect.get("instagram_followers", 0) or 
                prospect.get("tiktok_followers", 0)
            ),
            "platform": prospect.get("primary_platform", "youtube")
        }
        
        if extra_data:
            data.update(extra_data)
        
        return data
    
    def _calculate_send_time(
        self,
        delay_days: int = 0,
        delay_hours: int = 0,
        preferred_time: str = "10:00",
        skip_weekends: bool = True
    ) -> datetime:
        """Calculate the next send time respecting business hours."""
        # Start from now
        base_time = datetime.utcnow()
        
        # Add delay
        send_time = base_time + timedelta(days=delay_days, hours=delay_hours)
        
        # Parse preferred time (assumes EST, convert to UTC)
        try:
            hour, minute = map(int, preferred_time.split(":"))
            # EST is UTC-5, so add 5 hours to get UTC
            utc_hour = (hour + 5) % 24
            send_time = send_time.replace(hour=utc_hour, minute=minute, second=0, microsecond=0)
        except (ValueError, AttributeError):
            pass
        
        # Skip weekends if needed
        if skip_weekends:
            while send_time.weekday() >= 5:  # Saturday=5, Sunday=6
                send_time += timedelta(days=1)
        
        return send_time
    
    async def process_pending_sequences(self) -> dict:
        """
        Process all pending sequence sends.
        
        This should be called every 15 minutes by the scheduler.
        
        Returns:
            Summary of processing results
        """
        self._reset_daily_counter_if_needed()
        
        results = {
            "processed": 0,
            "sent": 0,
            "skipped": 0,
            "errors": 0,
            "limit_reached": False
        }
        
        # Check if daily limit reached
        if self.emails_sent_today >= self.daily_limit:
            logger.info("Daily email limit reached", limit=self.daily_limit)
            results["limit_reached"] = True
            return results
        
        # Get sequences due to send
        pending = await self.db.fetch(
            """
            SELECT 
                os.id, os.prospect_id, os.sequence_name, os.current_step,
                os.total_steps, os.personalization_data, os.status,
                mp.email, mp.full_name,
                st.steps, st.stop_on
            FROM outreach_sequences os
            JOIN marketing_prospects mp ON os.prospect_id = mp.id
            JOIN sequence_templates st ON os.sequence_name = st.name
            WHERE os.status IN ('pending', 'active')
              AND os.next_send_at <= NOW()
              AND mp.email IS NOT NULL
              AND mp.status NOT IN ('unsubscribed', 'complained', 'bounced', 'converted')
            ORDER BY os.next_send_at ASC
            LIMIT $1
            """,
            self.daily_limit - self.emails_sent_today
        )
        
        logger.info(f"Processing {len(pending)} pending sequences")
        
        for seq in pending:
            try:
                results["processed"] += 1
                
                # Check stop conditions
                should_stop = await self._check_stop_conditions(seq)
                if should_stop:
                    results["skipped"] += 1
                    continue
                
                # Get current step
                current_step = seq["current_step"]
                steps = seq["steps"]
                
                if current_step >= len(steps):
                    # Sequence complete
                    await self._complete_sequence(seq["id"])
                    results["skipped"] += 1
                    continue
                
                step = steps[current_step]
                
                # Check skip conditions for this step
                if await self._should_skip_step(seq, step):
                    await self._advance_to_next_step(seq, steps)
                    results["skipped"] += 1
                    continue
                
                # Send the email
                success = await self._send_step_email(seq, step)
                
                if success:
                    results["sent"] += 1
                    self.emails_sent_today += 1
                    
                    # Advance to next step
                    await self._advance_to_next_step(seq, steps)
                    
                    # Check if we hit daily limit
                    if self.emails_sent_today >= self.daily_limit:
                        logger.info("Daily limit reached during processing")
                        results["limit_reached"] = True
                        break
                else:
                    results["errors"] += 1
                
                # Small delay between sends
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(
                    "Error processing sequence",
                    sequence_id=str(seq["id"]),
                    error=str(e)
                )
                results["errors"] += 1
        
        logger.info("Sequence processing complete", **results)
        return results
    
    async def _check_stop_conditions(self, seq: dict) -> bool:
        """Check if sequence should be stopped."""
        stop_on = seq.get("stop_on") or []
        
        # Check for replies
        if "replied" in stop_on:
            reply = await self.db.fetchval(
                """
                SELECT replied_at FROM marketing_prospects
                WHERE id = $1 AND replied_at IS NOT NULL
                """,
                seq["prospect_id"]
            )
            if reply:
                await self._stop_sequence(seq["id"], "replied")
                return True
        
        # Check email events
        events = await self.db.fetch(
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
                await self._stop_sequence(seq["id"], event["status"])
                return True
        
        return False
    
    async def _should_skip_step(self, seq: dict, step: dict) -> bool:
        """Check if current step should be skipped due to engagement."""
        skip_if = step.get("skip_if") or []
        
        if not skip_if:
            return False
        
        # Check for clicks
        if "clicked" in skip_if:
            clicked = await self.db.fetchval(
                """
                SELECT 1 FROM email_sends
                WHERE sequence_id = $1 AND click_count > 0
                LIMIT 1
                """,
                seq["id"]
            )
            if clicked:
                return True
        
        # Check for replies
        if "replied" in skip_if:
            replied = await self.db.fetchval(
                """
                SELECT replied_at FROM marketing_prospects
                WHERE id = $1 AND replied_at IS NOT NULL
                """,
                seq["prospect_id"]
            )
            if replied:
                return True
        
        return False
    
    async def _send_step_email(self, seq: dict, step: dict) -> bool:
        """Send email for current sequence step."""
        # Get email template
        template_name = step.get("body_template")
        email_template = await self.db.fetchrow(
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
        
        # Send via Brevo
        result = await self.brevo.send_email(
            to_email=seq["email"],
            to_name=seq["full_name"] or "",
            subject=subject,
            html_content=html_content,
            text_content=text_content,
            reply_to=self.settings.brevo_sender_email,
            tags=[
                f"sequence:{seq['sequence_name']}",
                f"step:{seq['current_step'] + 1}"
            ]
        )
        
        if result.get("success"):
            # Log the send
            await self.db.execute(
                """
                INSERT INTO email_sends (
                    sequence_id, prospect_id, step_number,
                    template_name, subject, to_email,
                    brevo_message_id, status, sent_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, 'sent', NOW()
                )
                """,
                seq["id"],
                seq["prospect_id"],
                seq["current_step"] + 1,
                template_name,
                subject,
                seq["email"],
                result.get("message_id")
            )
            
            # Update prospect
            is_first_contact = seq["current_step"] == 0
            if is_first_contact:
                await self.db.execute(
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
                await self.db.execute(
                    """
                    UPDATE marketing_prospects SET
                        last_contacted_at = NOW(),
                        total_emails_sent = total_emails_sent + 1
                    WHERE id = $1
                    """,
                    seq["prospect_id"]
                )
            
            logger.info(
                "Sent sequence email",
                prospect_email=seq["email"],
                sequence=seq["sequence_name"],
                step=seq["current_step"] + 1
            )
            
            return True
        else:
            logger.error(
                "Failed to send email",
                prospect_email=seq["email"],
                error=result.get("error")
            )
            return False
    
    async def _advance_to_next_step(self, seq: dict, steps: list):
        """Advance sequence to next step or complete."""
        next_step = seq["current_step"] + 1
        
        if next_step >= len(steps):
            # Sequence complete
            await self._complete_sequence(seq["id"])
        else:
            # Calculate next send time
            step = steps[next_step]
            next_send_at = self._calculate_send_time(
                delay_days=step.get("delay_days", 3),
                delay_hours=step.get("delay_hours", 0),
                preferred_time=step.get("send_time_preference", "10:00"),
                skip_weekends=step.get("skip_weekends", True)
            )
            
            await self.db.execute(
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
    
    async def _complete_sequence(self, sequence_id: str):
        """Mark sequence as complete."""
        await self.db.execute(
            """
            UPDATE outreach_sequences SET
                status = 'completed',
                completed_at = NOW()
            WHERE id = $1
            """,
            sequence_id
        )
        logger.info(f"Sequence completed: {sequence_id}")
    
    async def _stop_sequence(self, sequence_id: str, reason: str):
        """Stop sequence with reason."""
        await self.db.execute(
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
    
    async def auto_enroll_qualified_prospects(self, limit: int = 50) -> dict:
        """
        Automatically enroll qualified prospects in appropriate sequences.
        
        This finds prospects that:
        - Have valid email
        - Meet minimum relevance score
        - Haven't been contacted yet
        - Aren't already in a sequence
        
        Returns:
            Summary of enrollments
        """
        results = {"enrolled": 0, "skipped": 0, "errors": 0}
        
        # Get qualified prospects
        prospects = await self.db.fetch(
            """
            SELECT 
                mp.id, mp.email, mp.primary_platform, mp.relevance_score,
                mp.youtube_subscribers, mp.instagram_followers, mp.tiktok_followers
            FROM marketing_prospects mp
            WHERE mp.email IS NOT NULL
              AND mp.status IN ('discovered', 'enriched')
              AND mp.relevance_score >= $1
              AND mp.first_contacted_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM outreach_sequences os
                  WHERE os.prospect_id = mp.id
                    AND os.status IN ('pending', 'active')
              )
            ORDER BY mp.relevance_score DESC
            LIMIT $2
            """,
            self.settings.min_relevance_score,
            limit
        )
        
        logger.info(f"Found {len(prospects)} prospects to enroll")
        
        for prospect in prospects:
            try:
                # Determine sequence based on platform
                platform = prospect["primary_platform"] or "youtube"
                sequence_name = f"{platform}_creator"
                
                # Check if sequence template exists
                template_exists = await self.db.fetchval(
                    """
                    SELECT 1 FROM sequence_templates
                    WHERE name = $1 AND is_active = TRUE
                    """,
                    sequence_name
                )
                
                if not template_exists:
                    # Fall back to youtube_creator
                    sequence_name = "youtube_creator"
                
                # Enroll
                sequence_id = await self.enroll_prospect(
                    prospect_id=str(prospect["id"]),
                    sequence_name=sequence_name
                )
                
                if sequence_id:
                    results["enrolled"] += 1
                else:
                    results["skipped"] += 1
                    
            except Exception as e:
                logger.error(
                    "Failed to enroll prospect",
                    prospect_id=str(prospect["id"]),
                    error=str(e)
                )
                results["errors"] += 1
        
        logger.info("Auto-enrollment complete", **results)
        return results
