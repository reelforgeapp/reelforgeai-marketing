"""
ReelForge Marketing Engine - Brevo Email Client
"""

import httpx
import structlog

from app.config import get_settings

logger = structlog.get_logger()


class BrevoClient:
    BASE_URL = "https://api.brevo.com/v3"
    
    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.brevo_api_key
    
    async def send_email(
        self,
        to_email: str,
        to_name: str,
        subject: str,
        html_content: str,
        text_content: str = None,
        reply_to: str = None,
        tags: list = None
    ) -> dict:
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                payload = {
                    "sender": {
                        "name": self.settings.brevo_sender_name,
                        "email": self.settings.brevo_sender_email
                    },
                    "to": [{"email": to_email, "name": to_name}],
                    "subject": subject,
                    "htmlContent": html_content
                }
                
                if text_content:
                    payload["textContent"] = text_content
                
                if reply_to:
                    payload["replyTo"] = {"email": reply_to}
                
                if tags:
                    payload["tags"] = tags
                
                response = await client.post(
                    f"{self.BASE_URL}/smtp/email",
                    headers={
                        "api-key": self.api_key,
                        "Content-Type": "application/json"
                    },
                    json=payload
                )
                
                if response.status_code in (200, 201):
                    data = response.json()
                    return {
                        "success": True,
                        "message_id": data.get("messageId")
                    }
                else:
                    logger.error("Brevo send failed", status=response.status_code, body=response.text)
                    return {
                        "success": False,
                        "error": response.text
                    }
                    
            except Exception as e:
                logger.error("Brevo request failed", error=str(e))
                return {
                    "success": False,
                    "error": str(e)
                }
