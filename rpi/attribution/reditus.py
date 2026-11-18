"""
Reditus affiliate program inside RPI.

Reditus runs the affiliate program: an affiliate's link is
`reelforgeai.io/?red=<slug>`, the product reports each referred signup to
Reditus (browser tag and tracking API), and Reditus' own Stripe connection
turns that customer's payments into commissions. RPI reads the program back
through the API once a day (`sync`) and:

1. keeps a copy of partnerships, referrals and commissions
   (rpi_reditus_*; no customer email);
2. links each affiliate to the RPI creator it belongs to, by an existing
   campaign code, the creator's email, the email of a partnership-bot
   conversation with the creator, or a YouTube channel link on the Reditus
   profile. A match must be unique;
3. starts campaign tracking (referral code = slug) for an accepted affiliate
   linked to a creator, which also attributes their earlier signups;
4. raises alerts: an application waiting for a decision, an affiliate that
   matches more than one creator, referred signups that never reached Reditus
   (its tracking broke for six months in 2026), and paying referred customers
   Reditus has not credited (its Stripe connection);
5. reports clicks, signups, paid referrals and Reditus' commissions per
   affiliate next to RPI's accrued estimate (`partner_report`).

RPI never writes to Reditus: accepting applications and paying commissions
stay in Reditus.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import structlog

from rpi.attribution.campaigns import CampaignConflict, create_campaign
from rpi.core.audit import audit
from rpi.crm.external_conversations import identifiers

logger = structlog.get_logger()

ACTOR = "reditus-sync"
# The product's Reditus tracking was broken from 2026-05-24 until reelforge-backend
# PR #499 was deployed on 2026-11-14. Signups before that are known to be missing.
TRACKING_CHECK_FROM = datetime(2026, 11, 14, tzinfo=timezone.utc)
CHECK_WINDOW = timedelta(days=30)
SIGNUP_GRACE = timedelta(days=1)   # the browser tag and the API report within seconds
PAYMENT_GRACE = timedelta(days=2)  # Reditus' Stripe sync is not instant
SIGNUP_STATES = ("converted", "paid", "churned")
UNPAID_STATES = ("new", "clicked", "converted")


def _now(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


async def _alert(conn, severity: str, message: str, dedupe_key: str) -> bool:
    res = await conn.execute(
        """INSERT INTO rpi_alerts (kind, severity, message, dedupe_key) VALUES ('reditus', $1, $2, $3)
           ON CONFLICT (dedupe_key) DO NOTHING""", severity, message, dedupe_key)
    return res.endswith("1")


def _label(p) -> str:
    return p["name"] or (f"?red={p['slug']}" if p["slug"] else f"partner {p['partner_id'][:8]}")


# --------------------------------------------------------------------------
# Store
# --------------------------------------------------------------------------


async def store_program(conn, program: dict, *, synced_at: datetime) -> dict:
    """Upsert the fetched program and drop what Reditus no longer lists. The
    lists are complete (ReditusClient raises rather than return a partial
    list), so anything not seen in this sync is gone from Reditus."""
    partners = {p["partner_id"]: p for p in program["partners"]}
    for s in program["partnerships"]:
        partner = partners.get(s["partner_id"], {})
        await conn.execute(
            """INSERT INTO rpi_reditus_partnerships
                 (partnership_id, partner_id, slug, referral_link, name, email_normalized, website_url, country, state,
                  tier_name, referrals_count, paid_referrals_count, total_sold_reported, commission_generated_reported,
                  currency, has_payout_method, applied_at, state_set_at, synced_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
               ON CONFLICT (partnership_id) DO UPDATE SET
                 partner_id = EXCLUDED.partner_id, slug = EXCLUDED.slug, referral_link = EXCLUDED.referral_link,
                 name = EXCLUDED.name, email_normalized = EXCLUDED.email_normalized, website_url = EXCLUDED.website_url,
                 country = EXCLUDED.country, state = EXCLUDED.state, tier_name = EXCLUDED.tier_name,
                 referrals_count = EXCLUDED.referrals_count, paid_referrals_count = EXCLUDED.paid_referrals_count,
                 total_sold_reported = EXCLUDED.total_sold_reported,
                 commission_generated_reported = EXCLUDED.commission_generated_reported,
                 currency = EXCLUDED.currency, has_payout_method = EXCLUDED.has_payout_method,
                 applied_at = EXCLUDED.applied_at, state_set_at = EXCLUDED.state_set_at, synced_at = EXCLUDED.synced_at""",
            s["partnership_id"], s["partner_id"], partner.get("slug") or s["slug"], s["referral_link"],
            s["name"] or partner.get("name"), s["email_normalized"] or partner.get("email_normalized"),
            s["website_url"], s["country"], s["state"], s["tier_name"], s["referrals_count"], s["paid_referrals_count"],
            _num(s["total_sold_reported"]), _num(s["commission_generated_reported"]), s["currency"],
            s["has_payout_method"] or partner.get("has_payout_method", False), s["applied_at"], s["state_set_at"], synced_at)
    for r in program["referrals"]:
        await conn.execute(
            """INSERT INTO rpi_reditus_referrals (referral_id, partner_id, customer_ref, state, reject_reason, created_at,
                                                  state_set_at, synced_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
               ON CONFLICT (referral_id) DO UPDATE SET partner_id = EXCLUDED.partner_id,
                 customer_ref = EXCLUDED.customer_ref, state = EXCLUDED.state, reject_reason = EXCLUDED.reject_reason,
                 created_at = EXCLUDED.created_at, state_set_at = EXCLUDED.state_set_at, synced_at = EXCLUDED.synced_at""",
            r["referral_id"], r["partner_id"], r["customer_ref"], r["state"], r["reject_reason"], r["created_at"],
            r["state_set_at"], synced_at)
    for c in program["commissions"]:
        await conn.execute(
            """INSERT INTO rpi_reditus_commissions (commission_id, partner_id, referral_id, state, amount_minor, currency,
                                                    one_off_reason, reject_reason, created_at, paid_at, synced_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
               ON CONFLICT (commission_id) DO UPDATE SET partner_id = EXCLUDED.partner_id,
                 referral_id = EXCLUDED.referral_id, state = EXCLUDED.state, amount_minor = EXCLUDED.amount_minor,
                 currency = EXCLUDED.currency, one_off_reason = EXCLUDED.one_off_reason,
                 reject_reason = EXCLUDED.reject_reason, created_at = EXCLUDED.created_at, paid_at = EXCLUDED.paid_at,
                 synced_at = EXCLUDED.synced_at""",
            c["commission_id"], c["partner_id"], c["referral_id"], c["state"], c["amount_minor"], c["currency"],
            c["one_off_reason"], c["reject_reason"], c["created_at"], c["paid_at"], synced_at)
    removed = 0
    for table in ("rpi_reditus_partnerships", "rpi_reditus_referrals", "rpi_reditus_commissions"):
        res = await conn.execute(f"DELETE FROM {table} WHERE synced_at < $1", synced_at)
        removed += int(res.split()[-1])
    return {"partnerships": len(program["partnerships"]), "referrals": len(program["referrals"]),
            "commissions": len(program["commissions"]), "removed": removed}


def _num(value) -> Optional[Decimal]:
    try:
        return Decimal(str(value)) if value is not None else None
    except ArithmeticError:
        return None


# --------------------------------------------------------------------------
# Creator matching and campaigns
# --------------------------------------------------------------------------


async def _creators(conn, sql: str, *args) -> set:
    """Live creator ids from a query returning creator_id (merged creators resolve to the survivor)."""
    rows = await conn.fetch(
        f"""SELECT DISTINCT COALESCE(cr.merged_into_id, cr.id) AS id
            FROM ({sql}) m JOIN rpi_creators cr ON cr.id = m.creator_id""", *args)
    return {r["id"] for r in rows}


async def candidates(conn, p) -> tuple[Optional[str], set]:
    """(basis, creator ids) for a partnership: the first rule that finds anyone."""
    if p["slug"]:
        found = await _creators(conn, "SELECT creator_id FROM rpi_campaigns WHERE lower(referral_code) = lower($1)", p["slug"])
        if found:
            return "campaign_code", found
    if p["email_normalized"]:
        found = await _creators(
            conn, "SELECT creator_id FROM rpi_creator_contacts WHERE contact_type = 'email' AND value_normalized = $1",
            p["email_normalized"])
        if found:
            return "email", found
        if await conn.fetchval("SELECT to_regclass('public.rpi_external_contacts') IS NOT NULL"):
            found = await _creators(
                conn, """SELECT a.creator_id FROM rpi_external_contacts x JOIN rpi_creator_social_accounts a
                           ON (a.handle_normalized = ANY(x.handles)
                               OR (a.platform = 'youtube' AND a.platform_account_id = ANY(x.channel_ids)))
                         WHERE x.email_normalized = $1""", p["email_normalized"])
            if found:
                return "conversation_email", found
    if p["website_url"]:
        handles, channel_ids = identifiers([], [p["website_url"]])
        if handles or channel_ids:
            found = await _creators(
                conn, """SELECT creator_id FROM rpi_creator_social_accounts
                         WHERE platform = 'youtube' AND (platform_account_id = ANY($1::text[])
                                                         OR handle_normalized = ANY($2::text[]))""",
                channel_ids, handles)
            if found:
                return "website", found
    return None, set()


async def match_creators(conn) -> dict:
    """Link unlinked partnerships to a creator when exactly one creator matches."""
    linked, ambiguous = [], []
    for p in await conn.fetch("SELECT * FROM rpi_reditus_partnerships WHERE creator_id IS NULL ORDER BY applied_at"):
        basis, found = await candidates(conn, p)
        if len(found) == 1:
            creator_id = next(iter(found))
            await conn.execute(
                "UPDATE rpi_reditus_partnerships SET creator_id = $2, match_basis = $3 WHERE partnership_id = $1",
                p["partnership_id"], creator_id, basis)
            await audit(conn, ACTOR, "reditus.partner_linked", "creator", creator_id,
                        {"partnership_id": p["partnership_id"], "slug": p["slug"], "basis": basis})
            linked.append({"partnership_id": p["partnership_id"], "creator_id": str(creator_id), "basis": basis})
        elif len(found) > 1:
            ambiguous.append(p["partnership_id"])
            await _alert(conn, "warning",
                         f"Reditus affiliate {_label(p)} matches {len(found)} RPI creators by {basis.replace('_', ' ')}; "
                         "RPI linked none. Start campaign tracking with their slug on the right creator's page.",
                         f"reditus:ambiguous:{p['partnership_id']}")
    return {"linked": linked, "ambiguous": ambiguous}


async def link_campaign(conn, campaign_id) -> bool:
    """After an operator starts tracking a code: link the Reditus affiliate with that slug."""
    if not await migrated(conn):
        return False
    res = await conn.execute(
        """UPDATE rpi_reditus_partnerships p SET campaign_id = ca.id, creator_id = ca.creator_id,
                  match_basis = COALESCE(p.match_basis, 'campaign_code')
           FROM rpi_campaigns ca
           WHERE ca.id = $1 AND lower(p.slug) = lower(ca.referral_code)
             AND (p.creator_id IS NULL OR p.creator_id = ca.creator_id)""", uuid.UUID(str(campaign_id)))
    return res.split()[-1] != "0"


async def ensure_campaigns(conn, *, window_days: int) -> list[dict]:
    """Campaign tracking for every accepted affiliate linked to a creator."""
    created = []
    rows = await conn.fetch(
        """SELECT p.*, cr.display_name FROM rpi_reditus_partnerships p JOIN rpi_creators cr ON cr.id = p.creator_id
           WHERE p.state = 'accepted' AND p.campaign_id IS NULL AND p.slug IS NOT NULL""")
    for p in rows:
        existing = await conn.fetchrow(
            "SELECT id, creator_id FROM rpi_campaigns WHERE lower(referral_code) = lower($1)", p["slug"])
        if existing:
            if existing["creator_id"] == p["creator_id"]:
                await conn.execute("UPDATE rpi_reditus_partnerships SET campaign_id = $2 WHERE partnership_id = $1",
                                   p["partnership_id"], existing["id"])
            else:
                await _alert(conn, "warning",
                             f"Reditus affiliate {_label(p)} (?red={p['slug']}) is linked to {p['display_name']}, but "
                             "another creator's campaign already uses that code. Check which creator it belongs to.",
                             f"reditus:code_conflict:{p['partnership_id']}")
            continue
        who = p["display_name"] or _label(p)
        try:
            result = await create_campaign(
                conn, creator_id=p["creator_id"], name=f"{who} (Reditus affiliate)", actor=ACTOR, window_days=window_days,
                referral_code=p["slug"], utm_source="reditus", utm_medium="affiliate",
                audit_detail={"source": "reditus", "partnership_id": p["partnership_id"], "basis": p["match_basis"]})
        except CampaignConflict as exc:
            await _alert(conn, "warning", f"Could not start tracking Reditus affiliate {_label(p)} for {who}: {exc}.",
                         f"reditus:campaign_failed:{p['partnership_id']}")
            continue
        await conn.execute("UPDATE rpi_reditus_partnerships SET campaign_id = $2 WHERE partnership_id = $1",
                           p["partnership_id"], uuid.UUID(result["id"]))
        earlier = result["earlier_signups_attributed"]
        await _alert(conn, "info",
                     f"{who} joined the affiliate program (?red={p['slug']}). RPI started campaign tracking"
                     + (f" and attributed {earlier} earlier signup(s)." if earlier else "."),
                     f"reditus:joined:{p['partnership_id']}")
        created.append({"partnership_id": p["partnership_id"], "campaign_id": result["id"], "earlier_signups": earlier})
    return created


# --------------------------------------------------------------------------
# Program health
# --------------------------------------------------------------------------


async def pending_applications(conn, *, now: Optional[datetime] = None) -> list[dict]:
    rows = await conn.fetch(
        """SELECT p.partnership_id, p.partner_id, p.name, p.slug, p.country, p.applied_at, p.website_url, p.creator_id,
                  cr.display_name AS creator_name
           FROM rpi_reditus_partnerships p LEFT JOIN rpi_creators cr ON cr.id = p.creator_id
           WHERE p.state = 'pending' ORDER BY p.applied_at""")
    out = []
    for p in rows:
        days = (_now(now) - p["applied_at"]).days if p["applied_at"] else None
        out.append(dict(p) | {"days_waiting": days})
    return out


async def tracking_checks(conn, *, now: Optional[datetime] = None) -> dict:
    """Referred signups Reditus never received, and paying referred customers it has not credited."""
    now = _now(now)
    since = max(TRACKING_CHECK_FROM, now - CHECK_WINDOW)
    missing = await conn.fetch(
        """SELECT c.customer_ref, c.signed_up_at, p.slug
           FROM rpi_customers c
           JOIN rpi_reditus_partnerships p ON lower(p.slug) = lower(c.signup_referral_code) AND p.state = 'accepted'
           WHERE c.signed_up_at >= $1 AND c.signed_up_at < $2
             AND NOT EXISTS (SELECT 1 FROM rpi_reditus_referrals r WHERE r.customer_ref = c.customer_ref)
           ORDER BY c.signed_up_at""", since, now - SIGNUP_GRACE)
    uncredited = await conn.fetch(
        """SELECT c.customer_ref, c.first_paid_at, r.state, r.partner_id
           FROM rpi_customers c JOIN rpi_reditus_referrals r ON r.customer_ref = c.customer_ref
           WHERE c.first_paid_at IS NOT NULL AND c.first_paid_at < $1 AND r.state = ANY($2::text[])
           ORDER BY c.first_paid_at""", now - PAYMENT_GRACE, list(UNPAID_STATES))
    referred = await conn.fetchval(
        """SELECT COUNT(*) FROM rpi_customers c
           JOIN rpi_reditus_partnerships p ON lower(p.slug) = lower(c.signup_referral_code) AND p.state = 'accepted'
           WHERE c.signed_up_at >= $1 AND c.signed_up_at < $2""", since, now - SIGNUP_GRACE)
    paying_known = await conn.fetchval("SELECT COUNT(*) FROM rpi_customers WHERE first_paid_at IS NOT NULL")
    return {
        "since": since, "referred_signups_checked": referred,
        "signups_missing_in_reditus": [dict(r) for r in missing],
        "payments_not_credited": [dict(r) for r in uncredited],
        # Without Stripe events RPI knows no payments, so the credit check cannot find anything.
        "payment_check_has_data": bool(paying_known),
    }


async def raise_alerts(conn, checks: dict, pending: list[dict], *, now: Optional[datetime] = None) -> int:
    now = _now(now)
    week = now.strftime("%G-W%V")
    n = 0
    for p in pending:
        who = p["name"] or (f"?red={p['slug']}" if p["slug"] else "an unnamed partner")
        rpi = f" This is RPI creator {p['creator_name']}." if p["creator_name"] else ""
        since = p["applied_at"].date().isoformat() if p["applied_at"] else "an unknown date"
        n += await _alert(conn, "warning" if p["creator_id"] else "info",
                          f"Reditus application from {who}" + (f" ({p['country']})" if p["country"] else "")
                          + f" has waited since {since}: accept or decline it in Reditus.{rpi}",
                          f"reditus:pending:{p['partnership_id']}")
    missing = checks["signups_missing_in_reditus"]
    if missing:
        first, last = missing[0]["signed_up_at"].date(), missing[-1]["signed_up_at"].date()
        n += await _alert(conn, "critical" if len(missing) >= 3 else "warning",
                          f"{len(missing)} of {checks['referred_signups_checked']} signups through Reditus affiliate codes "
                          f"({first} to {last}) are not in Reditus, so their affiliates will not be paid for them. "
                          "The product's Reditus tracking may be broken (or the visitor used ?ref= instead of ?red=).",
                          f"reditus:signups_missing:{week}")
    uncredited = checks["payments_not_credited"]
    if uncredited:
        n += await _alert(conn, "critical",
                          f"Reditus has not credited {len(uncredited)} referred customer(s) who paid (first payment "
                          f"{uncredited[0]['first_paid_at'].date()}). Check that Stripe is connected in Reditus; until it is, "
                          "affiliates earn no commission.",
                          f"reditus:uncredited:{week}")
    return n


# --------------------------------------------------------------------------
# Sync
# --------------------------------------------------------------------------


async def migrated(conn) -> bool:
    """Code is deployed before migration 014 runs; until then RPI skips Reditus."""
    return bool(await conn.fetchval("SELECT to_regclass('public.rpi_reditus_partnerships') IS NOT NULL"))


async def sync(pool, client, *, window_days: int, now: Optional[datetime] = None) -> dict:
    synced_at = _now(now)
    async with pool.acquire() as conn:
        if not await migrated(conn):
            return {"status": "not_migrated"}
    try:
        program = await client.fetch_program()
    except Exception as exc:  # noqa: BLE001 - any failure becomes one alert a day
        async with pool.acquire() as conn:
            await _alert(conn, "warning", f"Reditus sync failed: {exc}", f"reditus:sync_failed:{synced_at.date()}")
        logger.warning("rpi_reditus_sync_failed", error=str(exc))
        return {"status": "failed", "error": str(exc)}
    async with pool.acquire() as conn:
        async with conn.transaction():
            stored = await store_program(conn, program, synced_at=synced_at)
            matched = await match_creators(conn)
        created = await ensure_campaigns(conn, window_days=window_days)
        checks = await tracking_checks(conn, now=synced_at)
        pending = await pending_applications(conn, now=synced_at)
        alerts = await raise_alerts(conn, checks, pending, now=synced_at)
    result = {"status": "ok", "requests": client.requests, **stored, "linked": matched["linked"],
              "ambiguous": matched["ambiguous"], "campaigns_created": created, "pending_applications": len(pending),
              "signups_missing_in_reditus": len(checks["signups_missing_in_reditus"]),
              "payments_not_credited": len(checks["payments_not_credited"]), "alerts_raised": alerts}
    logger.info("rpi_reditus_sync", **{k: v for k, v in result.items() if not isinstance(v, list)})
    return result


# --------------------------------------------------------------------------
# Reports
# --------------------------------------------------------------------------


def _usd(minor_by_state: dict, state: str) -> Decimal:
    return (Decimal(minor_by_state.get(state, 0)) / 100).quantize(Decimal("0.01"))


async def partner_report(conn, *, now: Optional[datetime] = None) -> dict:
    """Per affiliate: Reditus funnel and commissions next to RPI's attribution and accrued estimate."""
    if not await migrated(conn):
        return {"configured": False, "last_synced_at": None, "partners": []}
    last = await conn.fetchval("SELECT MAX(synced_at) FROM rpi_reditus_partnerships")
    counts = defaultdict(lambda: defaultdict(int))
    for r in await conn.fetch("SELECT partner_id, state, COUNT(*) AS n FROM rpi_reditus_referrals GROUP BY 1, 2"):
        counts[r["partner_id"]][r["state"]] = r["n"]
    money = defaultdict(lambda: defaultdict(int))
    currencies = set()
    for c in await conn.fetch(
            "SELECT partner_id, state, currency, SUM(amount_minor) AS minor FROM rpi_reditus_commissions GROUP BY 1, 2, 3"):
        currencies.add(c["currency"])
        if c["currency"] == "usd":  # the program pays in USD; other currencies are listed, not converted
            money[c["partner_id"]][c["state"]] += int(c["minor"])
    rows = await conn.fetch(
        """SELECT p.*, cr.display_name AS creator_name,
                  (SELECT COUNT(*) FROM rpi_customers c WHERE c.last_touch_campaign_id = p.campaign_id) AS rpi_customers,
                  (SELECT COUNT(*) FROM rpi_customers c WHERE c.last_touch_campaign_id = p.campaign_id
                                                         AND c.first_paid_at IS NOT NULL) AS rpi_paying,
                  (SELECT -COALESCE(SUM(e.amount_usd), 0) FROM rpi_ledger_entries e
                    WHERE e.campaign_id = p.campaign_id AND e.entry_type = 'affiliate_commission') AS rpi_accrued_usd
           FROM rpi_reditus_partnerships p LEFT JOIN rpi_creators cr ON cr.id = p.creator_id
           ORDER BY p.applied_at""")
    partners = []
    for p in rows:
        c, m = counts[p["partner_id"]], money[p["partner_id"]]
        partners.append({
            "partnership_id": p["partnership_id"], "name": p["name"], "slug": p["slug"], "state": p["state"],
            "country": p["country"], "tier_name": p["tier_name"], "applied_at": p["applied_at"],
            "has_payout_method": p["has_payout_method"], "creator_id": p["creator_id"], "creator_name": p["creator_name"],
            "campaign_id": p["campaign_id"], "match_basis": p["match_basis"],
            "clicks": c["new"] + c["clicked"], "signups": sum(c[s] for s in SIGNUP_STATES), "paid": c["paid"],
            "churned": c["churned"], "rejected": c["rejected"],
            "commission_pending_usd": _usd(m, "pending"), "commission_paid_usd": _usd(m, "paid"),
            "commission_rejected_usd": _usd(m, "rejected"),
            "rpi_customers": p["rpi_customers"], "rpi_paying_customers": p["rpi_paying"],
            "rpi_accrued_commission_usd": Decimal(p["rpi_accrued_usd"] or 0).quantize(Decimal("0.01")),
        })
    referral_states = defaultdict(int)
    for r in await conn.fetch("SELECT state, COUNT(*) AS n FROM rpi_reditus_referrals GROUP BY 1"):
        referral_states[r["state"]] = r["n"]
    checks = await tracking_checks(conn, now=now)
    return {
        "configured": last is not None, "last_synced_at": last,
        "currencies": sorted(currencies) or ["usd"],
        "totals": {
            "affiliates_accepted": sum(1 for p in partners if p["state"] == "accepted"),
            "applications_pending": sum(1 for p in partners if p["state"] == "pending"),
            "active_affiliates": sum(1 for p in partners if p["clicks"] or p["signups"]),
            "clicks": referral_states["new"] + referral_states["clicked"],
            "signups": sum(referral_states[s] for s in SIGNUP_STATES), "paid": referral_states["paid"],
            "commission_pending_usd": sum((p["commission_pending_usd"] for p in partners), Decimal("0")),
            "commission_paid_usd": sum((p["commission_paid_usd"] for p in partners), Decimal("0")),
        },
        "checks": checks,
        "partners": partners,
    }


async def creator_affiliate(conn, creator_id) -> Optional[dict]:
    """The creator's Reditus affiliate account, for the creator page."""
    if not await migrated(conn):
        return None
    p = await conn.fetchrow(
        "SELECT * FROM rpi_reditus_partnerships WHERE creator_id = $1 ORDER BY applied_at DESC LIMIT 1",
        uuid.UUID(str(creator_id)))
    if not p:
        return None
    c = {r["state"]: r["n"] for r in await conn.fetch(
        "SELECT state, COUNT(*) AS n FROM rpi_reditus_referrals WHERE partner_id = $1 GROUP BY 1", p["partner_id"])}
    m = {r["state"]: int(r["minor"]) for r in await conn.fetch(
        """SELECT state, SUM(amount_minor) AS minor FROM rpi_reditus_commissions
           WHERE partner_id = $1 AND currency = 'usd' GROUP BY 1""",
        p["partner_id"])}
    return {"slug": p["slug"], "referral_link": p["referral_link"], "state": p["state"], "applied_at": p["applied_at"],
            "match_basis": p["match_basis"], "campaign_id": p["campaign_id"], "has_payout_method": p["has_payout_method"],
            "clicks": c.get("new", 0) + c.get("clicked", 0), "signups": sum(c.get(s, 0) for s in SIGNUP_STATES),
            "paid": c.get("paid", 0), "commission_pending_usd": _usd(m, "pending"), "commission_paid_usd": _usd(m, "paid"),
            "synced_at": p["synced_at"]}
