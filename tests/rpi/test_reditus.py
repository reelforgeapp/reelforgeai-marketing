"""Reditus affiliate program: API client, sync into RPI, creator linking, campaign tracking and program checks."""

from datetime import datetime, timedelta, timezone

import httpx
import pytest

from rpi.attribution import reditus, touches
from rpi.providers.http import ProviderAuthError
from rpi.providers.reditus import ReditusClient, parse_time, partner_record, partnership_record

V1 = "/rpi/api/v1"
NOW = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
EPOCH = int((NOW - timedelta(days=3)).timestamp())


# --------------------------------------------------------------------------
# Payloads shaped like the live API (2026-11-18), with synthetic people
# --------------------------------------------------------------------------


def partner(pid, slug, email=None, name="", paypal=None):
    return {"id": pid, "type": "partner", "attributes": {"id": pid, "name": name, "display_name": name or email,
                                                       "email": email, "paypal_email": paypal, "slug": slug}}


def partnership(sid, pid, slug, state="accepted", email=None, name="", website="", country="US", created="2026-11-01T10:00:00.000Z"):
    return {"id": sid, "type": "partnership", "attributes": {
        "id": sid, "state": state, "invitation_pending": False, "state_set_at": None, "created_at": created,
        "updated_at": created, "pk": "abcd", "total_sold": 0, "commission_generated": 0, "competitor_slugs": [],
        "partner": {"data": {"id": pid, "type": "partner", "attributes": {
            "id": pid, "name": name, "display_name": name or email, "email": email, "paypal_email": None,
            "website_url": website, "country": country}}},
        "referral_link": f"https://reelforgeai.io/?red={slug}", "currency": "usd", "referrals_count": 0,
        "paid_referrals_count": 0, "tier_name": "Standard Tier"}}


def referral(rid, pid, state, uid=None, at=EPOCH):
    return {"id": rid, "type": "referral", "attributes": {
        "email": "someone@example.com" if uid else None, "uid": uid, "state": state, "partner_id": pid,
        "partner_name": "x", "partner_email": "x@example.com", "state_set_at": at, "created_at": at, "updated_at": at}}


def commission(cid, pid, amount, state="pending", referral_id=None):
    return {"id": cid, "type": "commission", "attributes": {
        "state": state, "amount": amount, "currency": "usd", "partner_id": pid, "referral_id": referral_id,
        "one_off_reason": None, "state_set_at": None, "paid_at": None, "created_at": EPOCH, "updated_at": EPOCH}}


def page(items, more=False):
    return {"data": items, "meta": {"pagination": {"page": 1, "per_page": 50, "has_more": more}}}


async def no_sleep(_):
    return None


def mock_api(routes: dict, seen: list):
    """routes: path -> list of pages (each a list of items)."""
    def handler(request: httpx.Request):
        seen.append(request)
        pages = routes[request.url.path.removeprefix("/api")]
        n = int(request.url.params.get("page", "1"))
        return httpx.Response(200, json=page(pages[n - 1], more=n < len(pages)))
    return httpx.MockTransport(handler)


# --------------------------------------------------------------------------
# Client
# --------------------------------------------------------------------------


async def test_client_pages_through_every_list_and_normalizes_records():
    seen = []
    routes = {
        "/v1/partners": [[partner("p1", "alpha", email="Alpha@Example.com", name="Alpha", paypal="pp@example.com")]],
        "/v1/partnerships": [[partnership("s1", "p1", "alpha", email="Alpha@Example.com", name="Alpha")]],
        "/v1/referrals": [[referral("r1", "p1", "clicked")], [referral("r2", "p1", "converted", uid="user-2")]],
        "/v1/commissions": [[commission("c1", "p1", 1110)]],
    }
    client = ReditusClient("test-key", transport=mock_api(routes, seen), sleep=no_sleep)
    program = await client.fetch_program()

    assert client.requests == 5 and all(r.method == "GET" for r in seen)
    assert seen[0].headers["authorization"] == "Bearer test-key" and seen[0].url.params["per_page"] == "50"
    assert [r["referral_id"] for r in program["referrals"]] == ["r1", "r2"]
    assert program["referrals"][1]["customer_ref"] == "user-2" and program["referrals"][0]["customer_ref"] is None
    assert "email" not in program["referrals"][1]  # no customer email leaves the client
    s = program["partnerships"][0]
    assert s["slug"] == "alpha" and s["email_normalized"] == "alpha@example.com" and s["applied_at"].year == 2026
    assert program["partners"][0]["has_payout_method"] is True
    assert program["commissions"][0]["amount_minor"] == 1110


def test_a_display_name_that_is_an_email_is_not_used_as_a_name():
    s = partnership_record(partnership("s1", "p1", "abc", email="person@example.com", name=""))
    assert s["name"] is None and s["email_normalized"] == "person@example.com"
    assert partner_record(partner("p1", "abc", email="person@example.com"))["name"] is None


def test_times_come_as_epoch_seconds_or_iso_strings():
    assert parse_time(0) == datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert parse_time("2026-01-03T19:37:42.750Z").tzinfo is not None
    assert parse_time(None) is None and parse_time("garbage") is None


async def test_client_retries_rate_limits_and_fails_loudly_on_a_bad_key():
    calls = {"n": 0}

    def limited(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"retry-after": "1"})
        return httpx.Response(200, json=page([]))

    waits = []

    async def record(s):
        waits.append(s)

    client = ReditusClient("k", transport=httpx.MockTransport(limited), sleep=record, min_interval_s=0)
    assert await client.list_all("/v1/partners") == []
    assert calls["n"] == 2 and 1.0 in waits

    bad = ReditusClient("k", transport=httpx.MockTransport(lambda r: httpx.Response(401, json={})), sleep=no_sleep)
    with pytest.raises(ProviderAuthError):
        await bad.list_all("/v1/partners")


# --------------------------------------------------------------------------
# Sync
# --------------------------------------------------------------------------


class FakeClient:
    def __init__(self, program=None, error=None):
        self.program, self.error, self.requests = program, error, 4

    async def fetch_program(self):
        if self.error:
            raise self.error
        from rpi.providers.reditus import commission_record, partner_record, partnership_record, referral_record

        p = self.program
        return {"partners": [partner_record(x) for x in p.get("partners", [])],
                "partnerships": [partnership_record(x) for x in p.get("partnerships", [])],
                "referrals": [referral_record(x) for x in p.get("referrals", [])],
                "commissions": [commission_record(x) for x in p.get("commissions", [])]}


async def creator(conn, name, *, email=None, handle=None, channel=None):
    cid = await conn.fetchval("INSERT INTO rpi_creators (display_name) VALUES ($1) RETURNING id", name)
    if email:
        await conn.execute(
            """INSERT INTO rpi_creator_contacts (creator_id, contact_type, value, value_normalized, source)
               VALUES ($1, 'email', $2, lower($2), 'operator')""", cid, email)
    if handle or channel:
        await conn.execute(
            """INSERT INTO rpi_creator_social_accounts (creator_id, platform, platform_account_id, handle, handle_normalized)
               VALUES ($1, 'youtube', $2, $3, lower($3))""", cid, channel or f"UC{name:_<22}"[:24], handle)
    return cid


def program_for_test():
    return {
        "partners": [partner("pa", "alice", email="alice@example.com", name="Alice"),
                     partner("pb", "bobvids", email="bob@agency.example", name="Bob"),
                     partner("pc", "carol30"), partner("pd", "dave"), partner("pe", "eve")],
        "partnerships": [
            partnership("sa", "pa", "alice", email="ALICE@example.com", name="Alice"),
            partnership("sb", "pb", "bobvids", email="bob@agency.example", name="Bob", website="https://youtube.com/@BobMakesVideos"),
            partnership("sc", "pc", "carol30", name="Carol"),
            partnership("sd", "pd", "dave", state="pending", name="Dave", country="IN", created="2026-06-08T10:00:00Z"),
            partnership("se", "pe", "eve", name="Eve"),
        ],
        "referrals": [referral("r1", "pa", "clicked"), referral("r2", "pa", "converted", uid="u-alice-1"),
                      referral("r3", "pe", "clicked")],
        "commissions": [commission("c1", "pa", 1110)],
    }


async def run_sync(pool, client):
    return await reditus.sync(pool, client, window_days=60, now=NOW)


async def test_sync_links_affiliates_to_creators_and_starts_campaign_tracking(pool):
    async with pool.acquire() as conn:
        alice = await creator(conn, "Alice AI", email="alice@example.com")
        bob = await creator(conn, "Bob Makes Videos", handle="bobmakesvideos")
        carol = await creator(conn, "Carol")
        carol_campaign = await conn.fetchval(
            """INSERT INTO rpi_campaigns (creator_id, name, referral_code, utm_campaign, attribution_window_days)
               VALUES ($1, 'Carol', 'CAROL30', 'CAROL30', 60) RETURNING id""", carol)
        # Alice's link was used before RPI knew she was an affiliate.
        await touches.identify(conn, {"customer_ref": "u-alice-1", "signed_up_at": NOW - timedelta(days=3),
                                      "signup_referral_code": "alice"})

    result = await run_sync(pool, FakeClient(program_for_test()))
    assert result["status"] == "ok" and result["partnerships"] == 5

    async with pool.acquire() as conn:
        rows = {r["partnership_id"]: r for r in await conn.fetch("SELECT * FROM rpi_reditus_partnerships")}
        assert (rows["sa"]["creator_id"], rows["sa"]["match_basis"]) == (alice, "email")
        assert (rows["sb"]["creator_id"], rows["sb"]["match_basis"]) == (bob, "website")
        assert (rows["sc"]["creator_id"], rows["sc"]["campaign_id"]) == (carol, carol_campaign)
        assert rows["se"]["creator_id"] is None and rows["sd"]["campaign_id"] is None  # no match; pending gets no campaign

        alice_campaign = await conn.fetchrow("SELECT * FROM rpi_campaigns WHERE creator_id = $1", alice)
        assert alice_campaign["referral_code"] == "alice" and alice_campaign["utm_source"] == "reditus"
        assert rows["sa"]["campaign_id"] == alice_campaign["id"]
        assert await conn.fetchval("SELECT last_touch_campaign_id FROM rpi_customers WHERE customer_ref = 'u-alice-1'") \
            == alice_campaign["id"]  # earlier signup attributed

        alerts = {a["dedupe_key"]: a for a in await conn.fetch("SELECT * FROM rpi_alerts WHERE kind = 'reditus'")}
        assert "reditus:joined:sa" in alerts and "1 earlier signup" in alerts["reditus:joined:sa"]["message"]
        assert alerts["reditus:pending:sd"]["severity"] == "info" and "2026-06-08" in alerts["reditus:pending:sd"]["message"]
        assert not any("referral" in k for k in alerts)  # no customer emails, no per-referral noise

    again = await run_sync(pool, FakeClient(program_for_test()))
    assert again["campaigns_created"] == [] and again["linked"] == [] and again["alerts_raised"] == 0
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_campaigns") == 3


async def test_an_affiliate_matching_two_creators_is_not_linked(pool):
    async with pool.acquire() as conn:
        await creator(conn, "One", email="shared@example.com")
        await creator(conn, "Two", email="shared@example.com")
    program = {"partners": [partner("p1", "shared", email="shared@example.com")],
               "partnerships": [partnership("s1", "p1", "shared", email="shared@example.com", name="Shared")]}
    result = await run_sync(pool, FakeClient(program))
    assert result["ambiguous"] == ["s1"] and result["campaigns_created"] == []
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT creator_id FROM rpi_reditus_partnerships") is None
        assert "matches 2 RPI creators" in await conn.fetchval(
            "SELECT message FROM rpi_alerts WHERE dedupe_key = 'reditus:ambiguous:s1'")


async def test_an_unnamed_applicant_is_named_by_slug_never_by_email(pool):
    program = {"partners": [partner("p1", "14413b419d", email="applicant@example.com")],
               "partnerships": [partnership("s1", "p1", "14413b419d", state="pending", email="applicant@example.com")]}
    await run_sync(pool, FakeClient(program))
    async with pool.acquire() as conn:
        msg = await conn.fetchval("SELECT message FROM rpi_alerts WHERE dedupe_key = 'reditus:pending:s1'")
    assert "?red=14413b419d" in msg and "@" not in msg


async def test_a_pending_application_from_a_known_creator_is_a_warning(pool):
    async with pool.acquire() as conn:
        await creator(conn, "Known", email="known@example.com")
    program = {"partners": [partner("p1", "known", email="known@example.com")],
               "partnerships": [partnership("s1", "p1", "known", state="pending", email="known@example.com", name="Known")]}
    await run_sync(pool, FakeClient(program))
    async with pool.acquire() as conn:
        a = await conn.fetchrow("SELECT * FROM rpi_alerts WHERE dedupe_key = 'reditus:pending:s1'")
        assert a["severity"] == "warning" and "This is RPI creator Known" in a["message"]
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_campaigns") == 0


async def test_referred_signups_missing_in_reditus_and_uncredited_payments_raise_alerts(pool):
    async with pool.acquire() as conn:
        for ref, days, paid in (("u-tracked", 5, True), ("u-lost", 4, False), ("u-today", 0, False), ("u-old", 200, False)):
            await touches.identify(conn, {"customer_ref": ref, "signed_up_at": NOW - timedelta(days=days, hours=1),
                                          "signup_referral_code": "alice"})
            if paid:
                await conn.execute("UPDATE rpi_customers SET first_paid_at = $2 WHERE customer_ref = $1",
                                   ref, NOW - timedelta(days=4))
    program = program_for_test()
    program["referrals"] = [referral("r9", "pa", "converted", uid="u-tracked")]
    await run_sync(pool, FakeClient(program))

    async with pool.acquire() as conn:
        checks = await reditus.tracking_checks(conn, now=NOW)
        assert [r["customer_ref"] for r in checks["signups_missing_in_reditus"]] == ["u-lost"]  # not today's, not pre-fix
        assert [r["customer_ref"] for r in checks["payments_not_credited"]] == ["u-tracked"]
        msgs = {a["dedupe_key"].rsplit(":", 1)[0]: a for a in await conn.fetch("SELECT * FROM rpi_alerts")}
        assert "1 of 2 signups" in msgs["reditus:signups_missing"]["message"]
        assert msgs["reditus:uncredited"]["severity"] == "critical"
        assert "Stripe is connected in Reditus" in msgs["reditus:uncredited"]["message"]


async def test_records_gone_from_reditus_are_removed_and_a_failed_sync_alerts(pool):
    await run_sync(pool, FakeClient(program_for_test()))
    smaller = program_for_test()
    smaller["partnerships"] = smaller["partnerships"][:1]
    smaller["referrals"], smaller["commissions"] = [], []
    result = await reditus.sync(pool, FakeClient(smaller), window_days=60, now=NOW + timedelta(minutes=5))
    assert result["removed"] == 4 + 3 + 1
    failed = await reditus.sync(pool, FakeClient(error=ProviderAuthError("reditus", "HTTP 401")), window_days=60, now=NOW)
    assert failed["status"] == "failed"
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_reditus_partnerships") == 1  # a failure deletes nothing
        assert await conn.fetchval("SELECT severity FROM rpi_alerts WHERE dedupe_key LIKE 'reditus:sync_failed:%'") == "warning"


async def test_report_and_creator_page_show_the_reditus_funnel(api, pool):
    async with pool.acquire() as conn:
        alice = await creator(conn, "Alice AI", email="alice@example.com")
    await run_sync(pool, FakeClient(program_for_test()))

    r = await api.get(f"{V1}/reditus/partners", headers=api.admin)
    assert r.status_code == 200, r.text
    body = r.json()
    a = next(p for p in body["partners"] if p["slug"] == "alice")
    assert (a["clicks"], a["signups"], a["paid"]) == (1, 1, 0) and a["commission_pending_usd"] == "11.10"
    assert a["creator_name"] == "Alice AI" and body["totals"]["applications_pending"] == 1
    assert body["totals"]["affiliates_accepted"] == 4 and body["totals"]["clicks"] == 2

    page_ = await api.get(f"{V1}/creators/{alice}", headers=api.admin)
    aff = page_.json()["affiliate"]
    assert aff["slug"] == "alice" and aff["signups"] == 1 and aff["commission_pending_usd"] == "11.10"


async def test_an_operator_campaign_links_the_affiliate_with_that_slug(api, pool):
    program = program_for_test()
    await run_sync(pool, FakeClient(program))  # Eve is not linked to anyone
    async with pool.acquire() as conn:
        eve = await creator(conn, "Eve")
    r = await api.post(f"{V1}/campaigns", headers=api.admin, json={"creator_id": str(eve), "name": "Eve", "referral_code": "EVE"})
    assert r.status_code == 200, r.text
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM rpi_reditus_partnerships WHERE partnership_id = 'se'")
    assert row["creator_id"] == eve and str(row["campaign_id"]) == r.json()["id"] and row["match_basis"] == "campaign_code"


async def test_sync_endpoint_needs_a_key(api):
    r = await api.post(f"{V1}/reditus/sync", headers=api.admin)
    assert r.status_code == 409 and "REDITUS_API_KEY" in r.text
