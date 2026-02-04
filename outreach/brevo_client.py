"""
ReelForge Marketing Engine - Brevo Email Client
"""

import structlog

from app.config import get_settings
from services.http_client import get_brevo_client

logger = structlog.get_logger()


class BrevoClient:
    BASE_URL = "https://api.brevo.com/v3"

    def __init__(self):
        self.settings = get_settings()
        self.api_key = self.settings.brevo_api_key
        self.http_client = get_brevo_client(self.api_key)

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

            response = await self.http_client.post(
                f"{self.BASE_URL}/smtp/email",
                json=payload
            )

            if response.status_code in (200, 201):
                data = response.json()
                return {
                    "success": True,
                    "message_id": data.get("messageId")
                }
            else:
                logger.error("Brevo send failed", status=response.status_code, body=response.text[:200])
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
