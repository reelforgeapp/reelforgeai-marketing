"""
Referral touches, identity linking and first/last-touch attribution.

Journey: partner referral → landing → signup → first render → export/publish
→ paid → renewal → retention.

Rules (docs/rpi/ATTRIBUTION_SPECIFICATION.md):
  * A touch is a partner-referred visit (referral code or a campaign UTM).
  * Only touches within the campaign's attribution window before signup count.
  * A referral code recorded at signup by the product counts as a touch at
    signup time.
  * first-touch and last-touch campaign are stored separately; reports show
    both. The affiliate program pays on last-click.
  * Attribution is locked at the first paid charge so later visits cannot
    move revenue between partners.
  * Anonymous ids are stored only as salted hashes and only with consent
    'granted' or 'unknown' (first-party, contractual referral tracking);
    with consent 'denied' the touch is counted without an identifier.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from app.config import get_settings


def hash_anonymous_id(anonymous_id: str) -> str:
    salt = get_settings().rpi_attribution_hash_salt or "rpi-attribution"
    return hashlib.sha256(f"{salt}:anon:{anonymous_id}".encode()).hexdigest()


@dataclass
class Touch:
    campaign_id: str
    touched_at: datetime
    window_days: int


@dataclass
class AttributionResult:
    first_touch: Optional[Touch]
    last_touch: Optional[Touch]
    eligible_count: int


def attribute(touches: list[Touch], signup_at: datetime) -> AttributionResult:
    eligible = [
        t for t in touches
        if t.touched_at <= signup_at and signup_at - t.touched_at <= timedelta(days=t.window_days)
    ]
    if not eligible:
        return AttributionResult(None, None, 0)
    eligible.sort(key=lambda t: t.touched_at)
    return AttributionResult(eligible[0], eligible[-1], len(eligible))


async def resolve_campaign(conn, referral_code: Optional[str], utm_campaign: Optional[str]) -> Optional[dict]:
    if referral_code:
        row = await conn.fetchrow(
            "SELECT id, attribution_window_days FROM rpi_campaigns WHERE lower(referral_code) = lower($1)", referral_code)
        if row:
            return dict(row)
    if utm_campaign:
        row = await conn.fetchrow(
            "SELECT id, attribution_window_days FROM rpi_campaigns WHERE utm_campaign = $1", utm_campaign)
        if row:
            return dict(row)
    return None


async def record_touch(conn, payload: dict) -> dict:
    campaign = await resolve_campaign(conn, payload.get("referral_code"), payload.get("utm_campaign"))
    consent = payload.get("consent_state", "unknown")
    anon_hash = hash_anonymous_id(payload["anonymous_id"]) if payload.get("anonymous_id") and consent != "denied" else None
    row_id = await conn.fetchval(
        """INSERT INTO rpi_attribution_touches
           (campaign_id, anonymous_id_hash, referral_code, utm_source, utm_medium, utm_campaign, landing_page_variant,
            consent_state, touched_at, idempotency_key)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
           ON CONFLICT (idempotency_key) DO NOTHING RETURNING id""",
        campaign["id"] if campaign else None, anon_hash, payload.get("referral_code"), payload.get("utm_source"),
        payload.get("utm_medium"), payload.get("utm_campaign"), payload.get("landing_page_variant"), consent,
        payload["touched_at"], payload.get("idempotency_key"),
    )
    return {"recorded": row_id is not None, "duplicate": row_id is None,
            "campaign_id": str(campaign["id"]) if campaign else None, "identifier_stored": anon_hash is not None}


async def identify(conn, payload: dict) -> dict:
    """Link a product user (customer_ref) to their touches and compute
    first/last-touch attribution as of signup."""
    customer_ref = payload["customer_ref"]
    signup_at: datetime = payload["signed_up_at"]
    existing = await conn.fetchrow("SELECT * FROM rpi_customers WHERE customer_ref = $1 FOR UPDATE", customer_ref)
    if existing is None and payload.get("billing_customer_id"):
        # A billing webhook may have arrived first and created a placeholder.
        placeholder = await conn.fetchrow(
            "SELECT * FROM rpi_customers WHERE billing_customer_id = $1 FOR UPDATE", payload["billing_customer_id"])
        if placeholder and placeholder["customer_ref"] != customer_ref:
            await merge_customer(conn, placeholder["customer_ref"], customer_ref)
            existing = await conn.fetchrow("SELECT * FROM rpi_customers WHERE customer_ref = $1 FOR UPDATE", customer_ref)

    if payload.get("anonymous_id"):
        await conn.execute(
            "UPDATE rpi_attribution_touches SET customer_ref = $1 WHERE anonymous_id_hash = $2 AND customer_ref IS NULL",
            customer_ref, hash_anonymous_id(payload["anonymous_id"]),
        )
    signup_code = payload.get("signup_referral_code")
    if signup_code:
        campaign = await resolve_campaign(conn, signup_code, None)
        if campaign:
            await conn.execute(
                """INSERT INTO rpi_attribution_touches (campaign_id, customer_ref, referral_code, consent_state, touched_at,
                                                        idempotency_key)
                   VALUES ($1, $2, $3, 'unknown', $4, $5) ON CONFLICT (idempotency_key) DO NOTHING""",
                campaign["id"], customer_ref, signup_code, signup_at, f"signup-code:{customer_ref}",
            )

    rows = await conn.fetch(
        """SELECT t.campaign_id, t.touched_at, c.attribution_window_days
           FROM rpi_attribution_touches t JOIN rpi_campaigns c ON c.id = t.campaign_id
           WHERE t.customer_ref = $1""",
        customer_ref,
    )
    result = attribute([Touch(str(r["campaign_id"]), r["touched_at"], r["attribution_window_days"]) for r in rows], signup_at)

    locked = existing is not None and existing["attribution_locked_at"] is not None
    if locked:
        first_id, last_id = existing["first_touch_campaign_id"], existing["last_touch_campaign_id"]
        first_at, last_at = existing["first_touch_at"], existing["last_touch_at"]
    else:
        first_id = result.first_touch.campaign_id if result.first_touch else None
        last_id = result.last_touch.campaign_id if result.last_touch else None
        first_at = result.first_touch.touched_at if result.first_touch else None
        last_at = result.last_touch.touched_at if result.last_touch else None

    await conn.execute(
        """INSERT INTO rpi_customers (customer_ref, billing_customer_id, signed_up_at, signup_referral_code,
                                      first_touch_campaign_id, last_touch_campaign_id, first_touch_at, last_touch_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
           ON CONFLICT (customer_ref) DO UPDATE SET
             billing_customer_id = COALESCE(EXCLUDED.billing_customer_id, rpi_customers.billing_customer_id),
             signed_up_at = COALESCE(rpi_customers.signed_up_at, EXCLUDED.signed_up_at),
             signup_referral_code = COALESCE(rpi_customers.signup_referral_code, EXCLUDED.signup_referral_code),
             first_touch_campaign_id = CASE WHEN rpi_customers.attribution_locked_at IS NULL THEN EXCLUDED.first_touch_campaign_id ELSE rpi_customers.first_touch_campaign_id END,
             last_touch_campaign_id = CASE WHEN rpi_customers.attribution_locked_at IS NULL THEN EXCLUDED.last_touch_campaign_id ELSE rpi_customers.last_touch_campaign_id END,
             first_touch_at = CASE WHEN rpi_customers.attribution_locked_at IS NULL THEN EXCLUDED.first_touch_at ELSE rpi_customers.first_touch_at END,
             last_touch_at = CASE WHEN rpi_customers.attribution_locked_at IS NULL THEN EXCLUDED.last_touch_at ELSE rpi_customers.last_touch_at END,
             updated_at = NOW()""",
        customer_ref, payload.get("billing_customer_id"), signup_at, signup_code, _uuid(first_id), _uuid(last_id),
        first_at, last_at,
    )
    # If the customer already paid (billing webhook arrived before identify),
    # attribution is now computed and must be locked.
    await conn.execute(
        """UPDATE rpi_customers SET attribution_locked_at = NOW()
           WHERE customer_ref = $1 AND first_paid_at IS NOT NULL AND attribution_locked_at IS NULL""",
        customer_ref,
    )
    return {
        "customer_ref": customer_ref,
        "first_touch_campaign_id": str(first_id) if first_id else None,
        "last_touch_campaign_id": str(last_id) if last_id else None,
        "eligible_touches": result.eligible_count,
        "locked": locked,
    }


async def attribute_existing_signups(conn, campaign_id, referral_code: str) -> int:
    """Customers who signed up with this referral code before its campaign
    existed (a creator's Reditus code used before RPI tracked them). Unlocked
    customers are re-attributed like a fresh identify; customers already locked
    without any campaign get this one, because the code was known at signup and
    no other partner is displaced. Returns how many customers now point here."""
    rows = await conn.fetch(
        """SELECT customer_ref, signed_up_at FROM rpi_customers
           WHERE lower(signup_referral_code) = lower($1) AND signed_up_at IS NOT NULL""", referral_code)
    n = 0
    for r in rows:
        await identify(conn, {"customer_ref": r["customer_ref"], "signed_up_at": r["signed_up_at"],
                              "signup_referral_code": referral_code})
        await conn.execute(
            """UPDATE rpi_customers SET first_touch_campaign_id = $2, last_touch_campaign_id = $2,
                   first_touch_at = signed_up_at, last_touch_at = signed_up_at, updated_at = NOW()
               WHERE customer_ref = $1 AND first_touch_campaign_id IS NULL AND last_touch_campaign_id IS NULL""",
            r["customer_ref"], _uuid(campaign_id))
        n += bool(await conn.fetchval(
            "SELECT 1 FROM rpi_customers WHERE customer_ref = $1 AND last_touch_campaign_id = $2",
            r["customer_ref"], _uuid(campaign_id)))
    return n


async def record_milestone(conn, customer_ref: str, milestone: str, at: datetime) -> bool:
    column = {"first_render": "first_render_at", "first_export": "first_export_at"}.get(milestone)
    if column is None:
        raise ValueError(f"unknown milestone {milestone}")
    res = await conn.execute(
        f"UPDATE rpi_customers SET {column} = COALESCE({column}, $2), updated_at = NOW() WHERE customer_ref = $1",
        customer_ref, at,
    )
    return res.endswith("1")


async def merge_customer(conn, from_ref: str, to_ref: str) -> None:
    """Move a billing placeholder customer (stripe:cus_...) onto the product user id."""
    src = await conn.fetchrow("SELECT * FROM rpi_customers WHERE customer_ref = $1", from_ref)
    if not src:
        return
    await conn.execute("UPDATE rpi_ledger_entries SET customer_ref = $2 WHERE customer_ref = $1", from_ref, to_ref)
    await conn.execute("UPDATE rpi_attribution_touches SET customer_ref = $2 WHERE customer_ref = $1", from_ref, to_ref)
    await conn.execute("UPDATE rpi_customers SET billing_customer_id = NULL WHERE customer_ref = $1", from_ref)
    await conn.execute(
        """INSERT INTO rpi_customers (customer_ref, billing_customer_id, first_paid_at, canceled_at, current_plan, billing_period)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (customer_ref) DO UPDATE SET
             billing_customer_id = COALESCE(rpi_customers.billing_customer_id, EXCLUDED.billing_customer_id),
             first_paid_at = LEAST(rpi_customers.first_paid_at, EXCLUDED.first_paid_at),
             current_plan = COALESCE(EXCLUDED.current_plan, rpi_customers.current_plan),
             billing_period = COALESCE(EXCLUDED.billing_period, rpi_customers.billing_period)""",
        to_ref, src["billing_customer_id"], src["first_paid_at"], src["canceled_at"], src["current_plan"], src["billing_period"],
    )
    await conn.execute("DELETE FROM rpi_customers WHERE customer_ref = $1", from_ref)


def _uuid(value):
    import uuid as _u

    if value is None:
        return None
    return value if isinstance(value, _u.UUID) else _u.UUID(str(value))
