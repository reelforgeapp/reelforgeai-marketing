"""
PostHog behavioral analytics bridge.

The product (reelforge-ai) already captures, with distinct_id = user UUID and
snake_case names: user_signed_up, first_value_reached, video_rendered,
video_downloaded, social_post_published, trial_started, subscription_created,
subscription_cancelled, payment_succeeded, ... Re-emitting them as
partner_signup, partner_first_render_success, etc. would duplicate events and
split funnels. Instead RPI:

  * sets person properties on the product user (partner_first_touch_campaign_id,
    partner_last_touch_campaign_id, partner_creator_id, partner_referral_code,
    partner_attribution_model) so every existing event can be broken down by
    partner, and
  * emits one new event, `partner_attribution_assigned`.

`partner_referral_visit` belongs in the website (client-side), because only
the browser sees the visit; see ATTRIBUTION_SPECIFICATION.md.

No email, name or other PII is sent. With rpi_posthog_requires_consent (the
default) nothing is sent unless consent_state == 'granted'. The product has
no consent mechanism today, so this bridge stays silent until one exists.
PostHog is never used as a financial ledger.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import httpx
import structlog

logger = structlog.get_logger()

ALLOWED_PROPERTIES = {
    "partner_id", "campaign_id", "creator_id", "referral_code", "utm_source", "utm_medium", "utm_campaign",
    "landing_page_variant", "subscription_plan", "billing_period", "attribution_model",
    "partner_first_touch_campaign_id", "partner_last_touch_campaign_id", "partner_creator_id",
    "partner_referral_code", "partner_attribution_model",
}


class PostHogBridge:
    def __init__(self, api_key: str, host: str, *, requires_consent: bool = True,
                 transport: httpx.AsyncBaseTransport | None = None):
        self.api_key = api_key
        self.host = host.rstrip("/")
        self.requires_consent = requires_consent
        self.transport = transport

    def enabled_for(self, consent_state: str) -> bool:
        if not self.api_key:
            return False
        return consent_state == "granted" or not self.requires_consent

    async def attribution_assigned(self, *, distinct_id: str, consent_state: str, properties: dict) -> dict:
        if not self.enabled_for(consent_state):
            return {"sent": False, "reason": "posthog disabled or consent not granted"}
        clean = {k: v for k, v in properties.items() if k in ALLOWED_PROPERTIES and v is not None}
        person = {
            "partner_first_touch_campaign_id": clean.get("partner_first_touch_campaign_id"),
            "partner_last_touch_campaign_id": clean.get("partner_last_touch_campaign_id"),
            "partner_creator_id": clean.get("partner_creator_id"),
            "partner_referral_code": clean.get("partner_referral_code"),
            "partner_attribution_model": clean.get("partner_attribution_model"),
        }
        body = {
            "api_key": self.api_key,
            "event": "partner_attribution_assigned",
            "distinct_id": distinct_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "properties": {**clean, "source": "server", "$set": {k: v for k, v in person.items() if v is not None}},
        }
        try:
            async with httpx.AsyncClient(timeout=10.0, transport=self.transport) as client:
                resp = await client.post(f"{self.host}/capture/", json=body)
            return {"sent": resp.status_code < 300, "status": resp.status_code}
        except httpx.HTTPError as exc:
            logger.warning("rpi_posthog_failed", error=str(exc))
            return {"sent": False, "reason": "network error"}
