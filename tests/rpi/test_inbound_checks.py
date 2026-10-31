"""Creators who write in are checked for bought views automatically: added from their YouTube links, screened by the autopilot."""

from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from rpi.administration.autopilot import credits_used, run_cycle
from rpi.crm.external_conversations import youtube_identifiers
from rpi.providers.base import ResultStatus
from rpi.providers.fixture_provider import FixtureProvider
from rpi.providers.youtube_data import YouTubeDataAdapter

V1 = "/rpi/api/v1"
YT = "https://www.googleapis.com/youtube/v3"
NOW = datetime.now(timezone.utc)


def test_only_youtube_links_identify_a_partner_to_add():
    handles, ids = youtube_identifiers(["https://www.youtube.com/@Grab.Akku", "https://www.tiktok.com/@nitincreates1",
                                        "https://youtube.com/channel/UCabcdefghijklmnopqrstuv", "https://instagram.com/x"])
    assert handles == ["grab.akku"] and ids == ["UCabcdefghijklmnopqrstuv"]


@respx.mock
async def test_youtube_adapter_finds_the_channel_behind_a_handle():
    route = respx.get(f"{YT}/channels").mock(return_value=httpx.Response(200, json={"items": [
        {"id": "UC0123456789abcdefghijkl", "snippet": {"title": "Techvid AI", "customUrl": "@techvidaii"}}]}))
    result = await YouTubeDataAdapter("yt-test-key").get_creator_profile("@techvidaii")
    assert result.status == ResultStatus.OK and result.data.platform_account_id == "UC0123456789abcdefghijkl"
    assert route.calls.last.request.url.params["forHandle"] == "@techvidaii"
    assert "id" not in route.calls.last.request.url.params


async def bot_key(api) -> dict:
    ingest = await api.post(f"{V1}/admin/principals", headers=api.admin, json={"name": "partnership-bot", "role": "ingest"})
    return {"X-RPI-Key": ingest.json()["key"]}


def contact(email, urls, days_ago=2, **kw) -> dict:
    return {"email": email, "channel_urls": urls, "last_inbound_at": (NOW - timedelta(days=days_ago)).isoformat(), **kw}


@pytest.mark.db
async def test_bot_sync_adds_recent_partners_from_youtube_links_only(api, pool):
    bot = await bot_key(api)
    sync = await api.post(f"{V1}/crm/external-contacts", headers=bot, json={"source": "partnership_bot", "contacts": [
        contact("deco@example.com", ["https://www.youtube.com/@synthetic3"], name="Deco (deco@example.com)"),
        contact("tiktok@example.com", ["https://www.tiktok.com/@synthetic4"], handles=["@synthetic4"]),
        contact("old@example.com", ["https://www.youtube.com/@synthetic5"], days_ago=200),
        contact("unknown@example.com", ["https://www.youtube.com/@nobodyhere"]),
        contact("id@example.com", ["https://www.youtube.com/channel/UCabcdefghijklmnopqrstuv"], name="By channel id"),
    ]})
    assert sync.status_code == 200, sync.text
    assert sorted(sync.json()["partners_added"]) == ["By channel id", "Deco"]
    async with pool.acquire() as conn:
        accounts = {r["platform_account_id"]: r for r in await conn.fetch(
            "SELECT a.platform_account_id, a.handle_normalized, c.display_name FROM rpi_creator_social_accounts a "
            "JOIN rpi_creators c ON c.id = a.creator_id")}
    assert set(accounts) == {"UCFIXTURE000003", "UCabcdefghijklmnopqrstuv"}
    assert accounts["UCFIXTURE000003"]["handle_normalized"] == "synthetic3"
    # A second sync finds them by handle and channel id and adds nobody.
    again = await api.post(f"{V1}/crm/external-contacts", headers=bot, json={"source": "partnership_bot", "contacts": [
        contact("deco@example.com", ["https://www.youtube.com/@synthetic3"]),
        contact("id@example.com", ["https://www.youtube.com/channel/UCabcdefghijklmnopqrstuv"])]})
    assert again.json()["partners_added"] == []


async def enable_autopilot(pool, cap=1000):
    async with pool.acquire() as conn:
        # target_queue 0: no discovery research, so only the inbound check spends credits.
        await conn.execute("""UPDATE rpi_autopilot_settings SET enabled = TRUE, auto_approve = TRUE, weekly_credit_cap = $1,
                                  target_queue = 0, presets = ARRAY['none']""", cap)


@pytest.mark.db
async def test_autopilot_checks_inbound_partners_for_bought_views(api, pool, monkeypatch):
    original = FixtureProvider.get_creator_content

    async def bought_for_synthetic3(self, platform_account_id, limit=30):
        result = await original(self, platform_account_id, limit)
        if platform_account_id == "UCFIXTURE000003":  # ~2,500 likes on every video whatever the views
            result.data = [c.model_copy(update={"views": 20_000 * (i + 1), "likes": 2_500, "is_short": False})
                           for i, c in enumerate(result.data)]
        return result

    monkeypatch.setattr(FixtureProvider, "get_creator_content", bought_for_synthetic3)
    bot = await bot_key(api)
    await api.post(f"{V1}/crm/external-contacts", headers=bot, json={"source": "partnership_bot", "contacts": [
        contact("bought@example.com", ["https://www.youtube.com/@synthetic3"], name="Bought Views"),
        contact("real@example.com", ["https://www.youtube.com/@synthetic1"], name="Real Views", days_ago=5)]})
    await enable_autopilot(pool, cap=3)
    capped = await run_cycle(pool, api.ctx)
    assert capped["inbound"] == "2 inbound partner(s) wait: the weekly allowance is used"

    await enable_autopilot(pool)
    fixture = api.ctx.registry.get("fixture")
    fixture.calls.clear()
    summary = await run_cycle(pool, api.ctx)
    inbound = summary["inbound"]
    assert list(inbound["checked"]) == ["Bought Views", "Real Views"]  # most recent first
    assert inbound["bought_engagement"] == ["Bought Views"]
    assert inbound["checked"]["Real Views"] in ("recommend", "investigate", "hold")
    assert fixture.calls.count("profile") == 2  # lifetime views checked too (bought subscribers)
    async with pool.acquire() as conn:
        reasons = {r["display_name"]: r["disqualified_reason"] for r in await conn.fetch(
            "SELECT display_name, disqualified_reason FROM rpi_creators")}
        assert await credits_used(conn, "fixture", datetime.now(timezone.utc)) > 0  # counts against the allowance
    assert reasons["Bought Views"] == "suspect_engagement" and reasons["Real Views"] is None

    assert (await run_cycle(pool, api.ctx))["inbound"] == "no unchecked inbound partners"


@pytest.mark.db
async def test_deep_analysis_skips_the_contact_lookup_for_creators_who_wrote_in(api, pool):
    bot = await bot_key(api)
    await api.post(f"{V1}/crm/external-contacts", headers=bot, json={"source": "partnership_bot", "contacts": [
        contact("real@example.com", ["https://www.youtube.com/@synthetic1"], name="Real Views")]})
    await enable_autopilot(pool)
    summary = await run_cycle(pool, api.ctx)
    assert summary["inbound"]["checked"]["Real Views"] == "recommend", summary
    async with pool.acquire() as conn:
        cid = str(await conn.fetchval("SELECT id FROM rpi_creators WHERE display_name = 'Real Views'"))
    fixture = api.ctx.registry.get("fixture")
    fixture.calls.clear()
    deep = await api.post(f"{V1}/research", headers=api.admin, json={"creator_ids": [cid], "stage": "deep", "inline": True})
    assert deep.json()["progress"]["results"] == {cid: "recommend"}
    assert "contacts" not in fixture.calls
