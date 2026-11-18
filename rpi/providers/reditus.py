"""
Reditus affiliate program API, read only.

Docs: https://docs.getreditus.com/affiliate-program-api (Bearer key from the
Reditus app settings; 100 requests per 5 minutes; JSON:API-style records with
`meta.pagination.has_more`, at most 50 per page).

The live API returns more than the docs show (checked 2026-11-18):
  partners      attributes also carry `email` and `display_name`
  partnerships  `attributes.partner` embeds the partner (name, website_url,
                country, paypal_email) and adds referrals_count,
                paid_referrals_count, total_sold, commission_generated,
                tier_name, currency; created_at is an ISO string
  referrals     attributes also carry `uid` (the product user id sent with the
                conversion), partner_id and partner_name; times are epoch seconds
Commission amounts are in cents (documented). The unit of total_sold and
commission_generated is not documented, so RPI keeps them as reported.

Only GET requests are made. RPI never changes anything in Reditus.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

import httpx

from rpi.providers.http import ProviderAuthError, ProviderRateLimited, ProviderSchemaError, ProviderUnavailable

PROVIDER = "reditus"
BASE_URL = "https://api.getreditus.com/api"
PER_PAGE = 50
# 100 requests per 5 minutes is one every 3 s; stay a little under it.
MIN_INTERVAL_S = 3.1


def parse_time(value: Any) -> Optional[datetime]:
    """Reditus sends epoch seconds on some resources and ISO strings on others."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _norm_email(value: Any) -> Optional[str]:
    return value.strip().lower() if isinstance(value, str) and "@" in value else None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _slug_from_link(link: Optional[str]) -> Optional[str]:
    if not link:
        return None
    from urllib.parse import parse_qs, urlparse

    values = parse_qs(urlparse(link).query).get("red")
    return values[0] if values else None


def partner_record(item: dict) -> dict:
    a = item.get("attributes") or {}
    name = a.get("name") or a.get("display_name") or None
    return {"partner_id": str(item.get("id") or a.get("id")), "slug": a.get("slug") or None,
            "email_normalized": _norm_email(a.get("email")),
            # Reditus falls back to the login email as display name; never show that as a name.
            "name": None if _norm_email(name) else name,
            "has_payout_method": bool(a.get("paypal_email"))}


def partnership_record(item: dict) -> dict:
    a = item.get("attributes") or {}
    embedded = ((a.get("partner") or {}).get("data") or {}) if isinstance(a.get("partner"), dict) else {}
    pa = embedded.get("attributes") or {}
    rel = ((item.get("relationships") or {}).get("partner") or {}).get("data") or {}
    partner_id = embedded.get("id") or pa.get("id") or rel.get("id")
    if not partner_id or not a.get("state"):
        raise ProviderSchemaError(PROVIDER, f"partnership {item.get('id')} has no partner or state")
    name = pa.get("name") or pa.get("display_name") or None
    return {
        "partnership_id": str(item["id"]), "partner_id": str(partner_id),
        "slug": _slug_from_link(a.get("referral_link")), "referral_link": a.get("referral_link"),
        # A display name may be the partner's email address; never show that as a name.
        "name": None if _norm_email(name) else name,
        "email_normalized": _norm_email(pa.get("email")),
        "website_url": pa.get("website_url") or None, "country": pa.get("country") or None,
        "state": a["state"], "tier_name": a.get("tier_name"),
        "referrals_count": _int(a.get("referrals_count")), "paid_referrals_count": _int(a.get("paid_referrals_count")),
        "total_sold_reported": a.get("total_sold"), "commission_generated_reported": a.get("commission_generated"),
        "currency": a.get("currency"), "has_payout_method": bool(pa.get("paypal_email")),
        "applied_at": parse_time(a.get("created_at")), "state_set_at": parse_time(a.get("state_set_at")),
    }


def referral_record(item: dict) -> dict:
    a = item.get("attributes") or {}
    if not a.get("state"):
        raise ProviderSchemaError(PROVIDER, f"referral {item.get('id')} has no state")
    uid = a.get("uid")
    return {"referral_id": str(item["id"]), "partner_id": str(a["partner_id"]) if a.get("partner_id") else None,
            "customer_ref": str(uid).strip() if uid not in (None, "") else None,
            "state": a["state"], "reject_reason": a.get("reject_reason"),
            "created_at": parse_time(a.get("created_at")), "state_set_at": parse_time(a.get("state_set_at"))}


def commission_record(item: dict) -> dict:
    a = item.get("attributes") or {}
    if not a.get("state") or a.get("amount") is None:
        raise ProviderSchemaError(PROVIDER, f"commission {item.get('id')} has no state or amount")
    return {"commission_id": str(item["id"]), "partner_id": str(a["partner_id"]) if a.get("partner_id") else None,
            "referral_id": str(a["referral_id"]) if a.get("referral_id") else None,
            "state": a["state"], "amount_minor": _int(a.get("amount")), "currency": (a.get("currency") or "usd").lower(),
            "one_off_reason": a.get("one_off_reason"), "reject_reason": a.get("reject_reason"),
            "created_at": parse_time(a.get("created_at")), "paid_at": parse_time(a.get("paid_at"))}


class ReditusClient:
    def __init__(self, api_key: str, *, base_url: str = BASE_URL, transport: Optional[httpx.AsyncBaseTransport] = None,
                 min_interval_s: float = MIN_INTERVAL_S, max_pages: int = 60, timeout_s: float = 30.0,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep):
        if not api_key:
            raise ValueError("Reditus API key is not set")
        self._key = api_key
        self._base = base_url.rstrip("/")
        self._transport = transport
        self._min_interval = min_interval_s
        self._max_pages = max_pages
        self._timeout = timeout_s
        self._sleep = sleep
        self._last = 0.0
        self.requests = 0

    async def _get(self, client: httpx.AsyncClient, path: str, params: dict) -> dict:
        for attempt in range(3):
            wait = self._min_interval - (time.monotonic() - self._last)
            if self.requests and wait > 0:
                await self._sleep(wait)
            self._last = time.monotonic()
            self.requests += 1
            try:
                resp = await client.get(self._base + path, params=params)
            except httpx.HTTPError as exc:
                if attempt == 2:
                    raise ProviderUnavailable(PROVIDER, f"{path}: {type(exc).__name__}") from exc
                await self._sleep(5 * (attempt + 1))
                continue
            if resp.status_code in (401, 403):
                raise ProviderAuthError(PROVIDER, f"{path}: HTTP {resp.status_code} (check REDITUS_API_KEY)", resp.status_code)
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt == 2:
                    cls = ProviderRateLimited if resp.status_code == 429 else ProviderUnavailable
                    raise cls(PROVIDER, f"{path}: HTTP {resp.status_code}", resp.status_code)
                retry_after = resp.headers.get("retry-after", "")
                await self._sleep(float(retry_after) if retry_after.isdigit() else 30.0 * (attempt + 1))
                continue
            if resp.status_code != 200:
                raise ProviderSchemaError(PROVIDER, f"{path}: HTTP {resp.status_code}", resp.status_code)
            try:
                body = resp.json()
            except ValueError as exc:
                raise ProviderSchemaError(PROVIDER, f"{path}: response is not JSON") from exc
            if not isinstance(body, dict) or not isinstance(body.get("data"), list):
                raise ProviderSchemaError(PROVIDER, f"{path}: no data list in the response")
            return body
        raise ProviderUnavailable(PROVIDER, path)  # not reached

    async def list_all(self, path: str, params: Optional[dict] = None) -> list[dict]:
        """Every record of a list endpoint. Raises rather than return a partial list."""
        out: list[dict] = []
        headers = {"Authorization": f"Bearer {self._key}", "Accept": "application/json",
                   "User-Agent": "reelforge-rpi/reditus-sync"}
        async with httpx.AsyncClient(transport=self._transport, timeout=self._timeout, headers=headers) as client:
            for page in range(1, self._max_pages + 1):
                body = await self._get(client, path, {**(params or {}), "page": page, "per_page": PER_PAGE})
                out.extend(body["data"])
                more = ((body.get("meta") or {}).get("pagination") or {}).get("has_more")
                if not more or not body["data"]:
                    return out
        raise ProviderSchemaError(PROVIDER, f"{path}: more than {self._max_pages} pages; raise max_pages")

    async def fetch_program(self) -> dict:
        """Partners, partnerships, referrals and commissions, normalized."""
        partners = [partner_record(p) for p in await self.list_all("/v1/partners")]
        partnerships = [partnership_record(p) for p in await self.list_all("/v1/partnerships")]
        referrals = [referral_record(r) for r in await self.list_all("/v1/referrals")]
        commissions = [commission_record(c) for c in await self.list_all("/v1/commissions")]
        return {"partners": partners, "partnerships": partnerships, "referrals": referrals, "commissions": commissions}
