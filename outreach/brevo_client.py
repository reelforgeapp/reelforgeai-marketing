"""
ReelForge Marketing Engine - Brevo Email Client
"""

import httpx
from typing import Optional
from datetime import datetime
import structlog

from app.config import get_settings

logger = structlog.get_logger()


class BrevoClient:
    """Client for Brevo transactional email API."""
    
    DEFAULT_TIMEOUT = 30.0
    
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
        payload = {
            "sender": {"name": self.sender_name, "email": self.sender_email},
            "to": [{"email": to_email, "name": to_name}],
            "subject": subject,
            "htmlContent": html_content,
            "textContent": text_content,
            "tags": tags or ["affiliate-outreach"]
        }
        
        if reply_to:
            payload["replyTo"] = {"email": reply_to}
        
        async with httpx.AsyncClient(timeout=self.DEFAULT_TIMEOUT) as client:
            try:
                response = await client.post(
                    f"{self.base_url}/smtp/email",
                    headers={"api-key": self.api_key, "Content-Type": "application/json", "Accept": "application/json"},
                    json=payload
                )
                
                if response.status_code == 201:
                    data = response.json()
                    masked_email = to_email[:3] + "***@" + to_email.split("@")[-1] if "@" in to_email else "***"
                    logger.info("Email sent successfully", to=masked_email, message_id=data.get("messageId"))
                    return {"success": True, "message_id": data.get("messageId"), "status": "sent"}
                else:
                    error_data = response.json() if response.content else {}
                    logger.error("Failed to send email", status_code=response.status_code, error=error_data)
                    return {"success": False, "status": "failed", "error": error_data.get("message", str(response.status_code))}
            
            except httpx.TimeoutException:
                logger.error("Brevo API timeout")
                return {"success": False, "status": "timeout", "error": "Request timed out"}
            
            except Exception as e:
                logger.error(f"Brevo API error: {e}")
                return {"success": False, "status": "error", "error": str(e)[:100]}
    
    async def add_contact(self, email: str, attributes: dict, list_ids: Optional[list[int]] = None, update_enabled: bool = True) -> dict:
        payload = {"email": email, "attributes": attributes, "updateEnabled": update_enabled}
        if list_ids:
            payload["listIds"] = list_ids
        
        async with httpx.AsyncClient(timeout=self.DEFAULT_TIMEOUT) as client:
            try:
                response = await client.post(
                    f"{self.base_url}/contacts",
                    headers={"api-key": self.api_key, "Content-Type": "application/json"},
                    json=payload
                )
                
                if response.status_code in [201, 204]:
                    return {"success": True, "status": "created" if response.status_code == 201 else "updated"}
                else:
                    error_data = response.json() if response.content else {}
                    return {"success": False, "error": error_data}
            except Exception as e:
                return {"success": False, "error": str(e)}
    
    async def get_email_events(self, email: Optional[str] = None, message_id: Optional[str] = None, event: Optional[str] = None, limit: int = 50) -> list:
        params = {"limit": limit}
        if email:
            params["email"] = email
        if message_id:
            params["messageId"] = message_id
        if event:
            params["event"] = event
        
        async with httpx.AsyncClient(timeout=self.DEFAULT_TIMEOUT) as client:
            try:
                response = await client.get(
                    f"{self.base_url}/smtp/statistics/events",
                    headers={"api-key": self.api_key, "Accept": "application/json"},
                    params=params
                )
                
                if response.status_code == 200:
                    return response.json().get("events", [])
                return []
            except Exception:
                return []
    
    async def check_email_status(self, message_id: str) -> dict:
        events = await self.get_email_events(message_id=message_id)
        
        status = {"message_id": message_id, "delivered": False, "opened": False, "clicked": False, "bounced": False, "events": []}
        
        for event in events:
            event_type = event.get("event", "").lower()
            status["events"].append({"type": event_type, "date": event.get("date"), "data": event})
            
            if event_type == "delivered":
                status["delivered"] = True
            elif event_type in ["opened", "uniqueopened"]:
                status["opened"] = True
            elif event_type in ["clicked", "uniqueclicked"]:
                status["clicked"] = True
            elif event_type in ["hardbounce", "softbounce", "blocked"]:
                status["bounced"] = True
                status["bounce_type"] = event_type
        
        return status
    
    async def get_account_info(self) -> dict:
        async with httpx.AsyncClient(timeout=self.DEFAULT_TIMEOUT) as client:
            try:
                response = await client.get(f"{self.base_url}/account", headers={"api-key": self.api_key, "Accept": "application/json"})
                return response.json() if response.status_code == 200 else {"error": f"Failed: {response.status_code}"}
            except Exception as e:
                return {"error": str(e)}


class BrevoWebhookHandler:
    """Handler for Brevo webhook events."""
    
    def __init__(self, db):
        self.db = db
        self.settings = get_settings()
    
    async def process_webhook(self, payload: dict) -> dict:
        event_type = payload.get("event")
        message_id = payload.get("message-id")
        
        timestamp_str = payload.get("date") or payload.get("ts_event")
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')) if timestamp_str else datetime.utcnow()
        except:
            timestamp = datetime.utcnow()
        
        if not message_id:
            return {"status": "ignored", "reason": "no message_id"}
        
        send_record = await self.db.fetchrow(
            "SELECT id, sequence_id, prospect_id, step_number FROM email_sends WHERE brevo_message_id = $1",
            message_id
        )
        
        if not send_record:
            return {"status": "ignored", "reason": "record not found"}
        
        if event_type == "delivered":
            await self.db.execute("UPDATE email_sends SET status = 'delivered', delivered_at = $1 WHERE id = $2", timestamp, send_record["id"])
        elif event_type in ["opened", "uniqueOpened"]:
            await self.db.execute(
                "UPDATE email_sends SET status = 'opened', first_opened_at = COALESCE(first_opened_at, $1), last_opened_at = $1, open_count = COALESCE(open_count, 0) + 1 WHERE id = $2",
                timestamp, send_record["id"]
            )
            await self.db.execute("UPDATE marketing_prospects SET total_emails_opened = COALESCE(total_emails_opened, 0) + 1 WHERE id = $1", send_record["prospect_id"])
        elif event_type in ["clicked", "uniqueClicked"]:
            await self.db.execute(
                "UPDATE email_sends SET status = 'clicked', first_clicked_at = COALESCE(first_clicked_at, $1), last_clicked_at = $1, click_count = COALESCE(click_count, 0) + 1 WHERE id = $2",
                timestamp, send_record["id"]
            )
            await self.db.execute("UPDATE marketing_prospects SET total_emails_clicked = COALESCE(total_emails_clicked, 0) + 1 WHERE id = $1", send_record["prospect_id"])
        elif event_type in ["hardBounce", "softBounce"]:
            reason = payload.get("reason") or payload.get("tag")
            await self.db.execute(
                "UPDATE email_sends SET status = 'bounced', bounced_at = $1, bounce_type = $2, bounce_reason = $3 WHERE id = $4",
                timestamp, event_type, reason, send_record["id"]
            )
            if event_type == "hardBounce":
                await self.db.execute("UPDATE marketing_prospects SET email_verified = FALSE, status = 'bounced' WHERE id = $1", send_record["prospect_id"])
            if send_record["sequence_id"]:
                await self.db.execute("UPDATE outreach_sequences SET status = 'stopped', stopped_reason = $1, completed_at = NOW() WHERE id = $2 AND status = 'active'", "bounced", send_record["sequence_id"])
        elif event_type == "unsubscribed":
            await self.db.execute("UPDATE email_sends SET status = 'unsubscribed', unsubscribed_at = $1 WHERE id = $2", timestamp, send_record["id"])
            await self.db.execute("UPDATE marketing_prospects SET status = 'unsubscribed', reply_sentiment = 'unsubscribe' WHERE id = $1", send_record["prospect_id"])
            if send_record["sequence_id"]:
                await self.db.execute("UPDATE outreach_sequences SET status = 'stopped', stopped_reason = $1, completed_at = NOW() WHERE id = $2 AND status = 'active'", "unsubscribed", send_record["sequence_id"])
        elif event_type == "complaint":
            await self.db.execute("UPDATE email_sends SET status = 'complained', complained_at = $1 WHERE id = $2", timestamp, send_record["id"])
            await self.db.execute("UPDATE marketing_prospects SET status = 'complained' WHERE id = $1", send_record["prospect_id"])
            if send_record["sequence_id"]:
                await self.db.execute("UPDATE outreach_sequences SET status = 'stopped', stopped_reason = $1, completed_at = NOW() WHERE id = $2 AND status = 'active'", "complained", send_record["sequence_id"])
        
        return {"status": "processed", "event": event_type}
