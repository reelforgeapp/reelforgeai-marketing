"""
ReelForge Marketing Engine - Brevo Email Client
Integration with Brevo (Sendinblue) for sending outreach emails
"""

import httpx
from typing import Optional
from datetime import datetime
import structlog

from app.config import get_settings

logger = structlog.get_logger()


class BrevoClient:
    """
    Client for Brevo (formerly Sendinblue) transactional email API.
    
    Free tier: 300 emails/day = 9,000/month
    This is plenty for affiliate recruitment outreach.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.brevo_api_key
        self.base_url = "https://api.brevo.com/v3"
        self.sender_email = self.settings.brevo_sender_email
        self.sender_name = self.settings.brevo_sender_name
    
    async def send_email(
        self,
        to_email: str,
        to_name: str,
        subject: str,
        html_content: str,
        text_content: str,
        reply_to: Optional[str] = None,
        tags: Optional[list[str]] = None
    ) -> dict:
        """
        Send a transactional email via Brevo.
        
        Args:
            to_email: Recipient email address
            to_name: Recipient name
            subject: Email subject line
            html_content: HTML body content
            text_content: Plain text body content
            reply_to: Reply-to email address
            tags: List of tags for tracking
        
        Returns:
            dict with messageId and status
        """
        payload = {
            "sender": {
                "name": self.sender_name,
                "email": self.sender_email
            },
            "to": [
                {
                    "email": to_email,
                    "name": to_name
                }
            ],
            "subject": subject,
            "htmlContent": html_content,
            "textContent": text_content,
            "tags": tags or ["affiliate-outreach"]
        }
        
        if reply_to:
            payload["replyTo"] = {"email": reply_to}
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/smtp/email",
                headers={
                    "api-key": self.api_key,
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                },
                json=payload
            )
            
            if response.status_code == 201:
                data = response.json()
                logger.info(
                    "Email sent successfully",
                    to=to_email,
                    message_id=data.get("messageId")
                )
                return {
                    "success": True,
                    "message_id": data.get("messageId"),
                    "status": "sent"
                }
            else:
                error_data = response.json() if response.content else {}
                logger.error(
                    "Failed to send email",
                    to=to_email,
                    status_code=response.status_code,
                    error=error_data
                )
                return {
                    "success": False,
                    "status": "failed",
                    "error": error_data.get("message", str(response.status_code))
                }
    
    async def add_contact(
        self,
        email: str,
        attributes: dict,
        list_ids: Optional[list[int]] = None,
        update_enabled: bool = True
    ) -> dict:
        """
        Add or update a contact in Brevo.
        
        Args:
            email: Contact email
            attributes: Contact attributes (FIRSTNAME, LASTNAME, etc.)
            list_ids: List IDs to add contact to
            update_enabled: Whether to update if exists
        
        Returns:
            dict with status
        """
        payload = {
            "email": email,
            "attributes": attributes,
            "updateEnabled": update_enabled
        }
        
        if list_ids:
            payload["listIds"] = list_ids
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/contacts",
                headers={
                    "api-key": self.api_key,
                    "Content-Type": "application/json"
                },
                json=payload
            )
            
            if response.status_code in [201, 204]:
                logger.debug(f"Contact added/updated: {email}")
                return {"success": True, "status": "created" if response.status_code == 201 else "updated"}
            else:
                error_data = response.json() if response.content else {}
                logger.warning(f"Failed to add contact: {email}", error=error_data)
                return {"success": False, "error": error_data}
    
    async def get_email_events(
        self,
        email: Optional[str] = None,
        message_id: Optional[str] = None,
        event: Optional[str] = None,
        limit: int = 50
    ) -> list:
        """
        Get email events (opens, clicks, bounces, etc.).
        
        Args:
            email: Filter by recipient email
            message_id: Filter by message ID
            event: Filter by event type (delivered, opened, clicked, bounced, etc.)
            limit: Maximum events to return
        
        Returns:
            List of event records
        """
        params = {"limit": limit}
        
        if email:
            params["email"] = email
        if message_id:
            params["messageId"] = message_id
        if event:
            params["event"] = event
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/smtp/statistics/events",
                headers={
                    "api-key": self.api_key,
                    "Accept": "application/json"
                },
                params=params
            )
            
            if response.status_code == 200:
                return response.json().get("events", [])
            else:
                logger.warning(f"Failed to get email events: {response.status_code}")
                return []
    
    async def check_email_status(self, message_id: str) -> dict:
        """
        Check the delivery status of a specific email.
        
        Args:
            message_id: Brevo message ID
        
        Returns:
            dict with delivery status and events
        """
        events = await self.get_email_events(message_id=message_id)
        
        status = {
            "message_id": message_id,
            "delivered": False,
            "opened": False,
            "clicked": False,
            "bounced": False,
            "events": []
        }
        
        for event in events:
            event_type = event.get("event", "").lower()
            status["events"].append({
                "type": event_type,
                "date": event.get("date"),
                "data": event
            })
            
            if event_type == "delivered":
                status["delivered"] = True
            elif event_type in ["opened", "uniqueOpened"]:
                status["opened"] = True
            elif event_type in ["clicked", "uniqueClicked"]:
                status["clicked"] = True
            elif event_type in ["hardBounce", "softBounce", "blocked"]:
                status["bounced"] = True
                status["bounce_type"] = event_type
        
        return status
    
    async def get_account_info(self) -> dict:
        """Get Brevo account information including sending limits."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/account",
                headers={
                    "api-key": self.api_key,
                    "Accept": "application/json"
                }
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {}
    
    async def get_daily_stats(self, start_date: str, end_date: str) -> dict:
        """
        Get email statistics for a date range.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        
        Returns:
            dict with aggregated statistics
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/smtp/statistics/aggregatedReport",
                headers={
                    "api-key": self.api_key,
                    "Accept": "application/json"
                },
                params={
                    "startDate": start_date,
                    "endDate": end_date
                }
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {}


class BrevoWebhookHandler:
    """
    Handler for Brevo webhook events.
    
    Brevo can send webhooks for:
    - delivered
    - opened
    - clicked
    - softBounce
    - hardBounce
    - unsubscribed
    - complaint
    """
    
    def __init__(self, db):
        self.db = db
        self.settings = get_settings()
    
    async def process_webhook(self, payload: dict) -> dict:
        """
        Process incoming Brevo webhook.
        
        Args:
            payload: Webhook payload from Brevo
        
        Returns:
            dict with processing result
        """
        event_type = payload.get("event")
        message_id = payload.get("message-id")
        email = payload.get("email")
        timestamp = payload.get("date") or payload.get("ts_event")
        
        logger.info(
            "Processing Brevo webhook",
            event=event_type,
            message_id=message_id,
            email=email
        )
        
        if not message_id:
            return {"status": "ignored", "reason": "no message_id"}
        
        # Find the email send record
        send_record = await self.db.fetchrow(
            """
            SELECT id, sequence_id, prospect_id, step_number
            FROM email_sends
            WHERE brevo_message_id = $1
            """,
            message_id
        )
        
        if not send_record:
            logger.warning(f"Email send record not found for message: {message_id}")
            return {"status": "ignored", "reason": "record not found"}
        
        # Update based on event type
        if event_type == "delivered":
            await self._handle_delivered(send_record, timestamp)
        elif event_type in ["opened", "uniqueOpened"]:
            await self._handle_opened(send_record, timestamp)
        elif event_type in ["clicked", "uniqueClicked"]:
            await self._handle_clicked(send_record, timestamp)
        elif event_type in ["hardBounce", "softBounce"]:
            await self._handle_bounced(send_record, event_type, timestamp, payload)
        elif event_type == "unsubscribed":
            await self._handle_unsubscribed(send_record, timestamp)
        elif event_type == "complaint":
            await self._handle_complaint(send_record, timestamp)
        
        return {"status": "processed", "event": event_type}
    
    async def _handle_delivered(self, record: dict, timestamp: str):
        """Handle delivered event."""
        await self.db.execute(
            """
            UPDATE email_sends SET
                status = 'delivered',
                delivered_at = $1
            WHERE id = $2
            """,
            timestamp,
            record["id"]
        )
    
    async def _handle_opened(self, record: dict, timestamp: str):
        """Handle opened event."""
        await self.db.execute(
            """
            UPDATE email_sends SET
                status = 'opened',
                first_opened_at = COALESCE(first_opened_at, $1),
                last_opened_at = $1,
                open_count = open_count + 1
            WHERE id = $2
            """,
            timestamp,
            record["id"]
        )
        
        # Update prospect stats
        await self.db.execute(
            """
            UPDATE marketing_prospects SET
                total_emails_opened = total_emails_opened + 1
            WHERE id = $1
            """,
            record["prospect_id"]
        )
    
    async def _handle_clicked(self, record: dict, timestamp: str):
        """Handle clicked event."""
        await self.db.execute(
            """
            UPDATE email_sends SET
                status = 'clicked',
                first_clicked_at = COALESCE(first_clicked_at, $1),
                last_clicked_at = $1,
                click_count = click_count + 1
            WHERE id = $2
            """,
            timestamp,
            record["id"]
        )
        
        # Update prospect stats
        await self.db.execute(
            """
            UPDATE marketing_prospects SET
                total_emails_clicked = total_emails_clicked + 1
            WHERE id = $1
            """,
            record["prospect_id"]
        )
        
        # Check if we should stop the sequence (clicked = interested)
        if record["sequence_id"]:
            await self._check_sequence_stop(record["sequence_id"], "clicked")
    
    async def _handle_bounced(self, record: dict, bounce_type: str, timestamp: str, payload: dict):
        """Handle bounce event."""
        reason = payload.get("reason") or payload.get("tag")
        
        await self.db.execute(
            """
            UPDATE email_sends SET
                status = 'bounced',
                bounced_at = $1,
                bounce_type = $2,
                bounce_reason = $3
            WHERE id = $4
            """,
            timestamp,
            bounce_type,
            reason,
            record["id"]
        )
        
        # Mark prospect email as invalid if hard bounce
        if bounce_type == "hardBounce":
            await self.db.execute(
                """
                UPDATE marketing_prospects SET
                    email_verified = FALSE,
                    status = 'bounced'
                WHERE id = $1
                """,
                record["prospect_id"]
            )
        
        # Stop the sequence
        if record["sequence_id"]:
            await self._stop_sequence(record["sequence_id"], "bounced")
    
    async def _handle_unsubscribed(self, record: dict, timestamp: str):
        """Handle unsubscribe event."""
        await self.db.execute(
            """
            UPDATE email_sends SET
                status = 'unsubscribed',
                unsubscribed_at = $1
            WHERE id = $2
            """,
            timestamp,
            record["id"]
        )
        
        # Update prospect
        await self.db.execute(
            """
            UPDATE marketing_prospects SET
                status = 'unsubscribed',
                reply_sentiment = 'unsubscribe'
            WHERE id = $1
            """,
            record["prospect_id"]
        )
        
        # Stop the sequence
        if record["sequence_id"]:
            await self._stop_sequence(record["sequence_id"], "unsubscribed")
    
    async def _handle_complaint(self, record: dict, timestamp: str):
        """Handle spam complaint event."""
        await self.db.execute(
            """
            UPDATE email_sends SET
                status = 'complained',
                complained_at = $1
            WHERE id = $2
            """,
            timestamp,
            record["id"]
        )
        
        # Mark prospect as do-not-contact
        await self.db.execute(
            """
            UPDATE marketing_prospects SET
                status = 'complained'
            WHERE id = $1
            """,
            record["prospect_id"]
        )
        
        # Stop the sequence
        if record["sequence_id"]:
            await self._stop_sequence(record["sequence_id"], "complained")
    
    async def _stop_sequence(self, sequence_id: str, reason: str):
        """Stop an outreach sequence."""
        await self.db.execute(
            """
            UPDATE outreach_sequences SET
                status = 'stopped',
                stopped_reason = $1,
                completed_at = NOW()
            WHERE id = $2 AND status = 'active'
            """,
            reason,
            sequence_id
        )
    
    async def _check_sequence_stop(self, sequence_id: str, event: str):
        """Check if sequence should stop based on engagement."""
        # Get sequence template to check stop conditions
        sequence = await self.db.fetchrow(
            """
            SELECT st.stop_on
            FROM outreach_sequences os
            JOIN sequence_templates st ON os.sequence_name = st.name
            WHERE os.id = $1
            """,
            sequence_id
        )
        
        if sequence and event in (sequence.get("stop_on") or []):
            await self._stop_sequence(sequence_id, event)
