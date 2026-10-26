"""The partnership bot's conversations: no cold email to creators it is talking to; replies marked in the CRM."""

from datetime import datetime, timedelta, timezone

import pytest

from rpi.crm.external_conversations import SOURCE_BOT, identifiers, mark_replies, upsert
from rpi.outreach.sender import check_gates, send_due_drafts
from tests.rpi.conftest import FakeSender, make_context
from tests.rpi.test_outreach import enable_sending, seed_draft

V1 = "/rpi/api/v1"
NOW = datetime.now(timezone.utc)


def test_identifiers_normalize_handles_and_channel_urls():
    handles, channels = identifiers(["@ZapiWala", "zapi wala", ""],
                                    ["https://www.youtube.com/@Zapiwala_AI", "https://youtube.com/channel/UCabcdefghijklmnopqrstuv"])
    assert handles == ["zapiwala", "zapiwala_ai"]  # "zapi wala" is not a handle
    assert channels == ["UCabcdefghijklmnopqrstuv"]


async def gates(pool, draft_id):
    async with pool.acquire() as conn:
        draft = dict(await conn.fetchrow("SELECT * FROM rpi_outreach_drafts WHERE id = $1", draft_id))
        contact = dict(await conn.fetchrow("SELECT * FROM rpi_creator_contacts WHERE id = $1", draft["contact_id"]))
        settings = dict(await conn.fetchrow("SELECT * FROM rpi_outreach_settings LIMIT 1"))
        return await check_gates(conn, draft, contact, settings)


@pytest.mark.db
async def test_no_cold_email_to_a_creator_the_bot_is_talking_to(pool):
    await enable_sending(pool)
    by_email, _ = await seed_draft(pool, email="sam@example.com")
    by_handle, handle_creator = await seed_draft(pool, email="lee@example.com")
    old, _ = await seed_draft(pool, email="old@example.com")
    not_partner, _ = await seed_draft(pool, email="support@example.com")
    async with pool.acquire() as conn:
        await conn.execute("UPDATE rpi_creator_social_accounts SET handle_normalized = 'leecreates' WHERE creator_id = $1",
                           handle_creator)
        await upsert(conn, SOURCE_BOT, [
            {"email": "SAM@example.com", "name": "Sam", "last_inbound_at": NOW - timedelta(days=3),
             "last_outbound_at": NOW - timedelta(days=1)},
            {"email": "manager@agency.example", "handles": ["@LeeCreates"], "last_inbound_at": NOW - timedelta(days=10)},
            {"email": "old@example.com", "last_inbound_at": NOW - timedelta(days=400)},
            {"email": "support@example.com", "is_partner": False, "last_inbound_at": NOW - timedelta(days=2)},
        ])
    blocked = await gates(pool, by_email)
    assert not blocked.allowed and any("partnership bot is in conversation with Sam" in r for r in blocked.reasons)
    assert not (await gates(pool, by_handle)).allowed  # matched by YouTube handle, through a manager's address
    assert (await gates(pool, old)).allowed            # last exchange is older than the conversation window
    assert (await gates(pool, not_partner)).allowed    # the bot did not treat that sender as a partner

    async with pool.acquire() as conn:
        await conn.execute("UPDATE rpi_outreach_drafts SET scheduled_send_at = NOW() - interval '1 minute' WHERE id = $1", by_email)
    sender = FakeSender()
    result = await send_due_drafts(pool, sender, public_base_url="https://rpi.test", window_open=True)
    assert str(by_email) in result["cancelled"] and sender.sent == []
    async with pool.acquire() as conn:
        reason = await conn.fetchval("SELECT failure_reason FROM rpi_outreach_drafts WHERE id = $1", by_email)
    assert "partnership bot" in reason


@pytest.mark.db
async def test_inbox_message_after_an_rpi_email_marks_the_creator_replied(api, pool):
    sent_at = NOW - timedelta(days=2)
    replied, replied_creator = await seed_draft(pool, status="sent", pipeline="contacted", email="sam@example.com")
    earlier, earlier_creator = await seed_draft(pool, status="sent", pipeline="contacted", email="lee@example.com")
    async with pool.acquire() as conn:
        await conn.execute("UPDATE rpi_outreach_drafts SET sent_at = $1 WHERE id = ANY($2::uuid[])", sent_at, [replied, earlier])
    ingest = await api.post(f"{V1}/admin/principals", headers=api.admin, json={"name": "partnership-bot", "role": "ingest"})
    bot = {"X-RPI-Key": ingest.json()["key"]}
    body = {"source": "partnership_bot", "contacts": [
        {"email": "sam@example.com", "name": "Sam", "last_inbound_at": (NOW - timedelta(days=1)).isoformat()},
        {"email": "lee@example.com", "last_inbound_at": (NOW - timedelta(days=5)).isoformat()},  # before the RPI email
    ]}
    first = await api.post(f"{V1}/crm/external-contacts", headers=bot, json=body)
    assert first.status_code == 200, first.text
    assert first.json()["upserted"] == 2 and [r["creator"] for r in first.json()["replies_marked"]] == ["Sam"]
    again = await api.post(f"{V1}/crm/external-contacts", headers=bot, json=body)
    assert again.json()["replies_marked"] == []  # idempotent
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT pipeline_status FROM rpi_creators WHERE id = $1", replied_creator) == "replied"
        assert await conn.fetchval("SELECT pipeline_status FROM rpi_creators WHERE id = $1", earlier_creator) == "contacted"
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_outreach_events WHERE event_type = 'replied'") == 1

    analyst = await api.post(f"{V1}/admin/principals", headers=api.admin, json={"name": "an-analyst", "role": "analyst"})
    denied = await api.post(f"{V1}/crm/external-contacts", headers={"X-RPI-Key": analyst.json()["key"]}, json=body)
    assert denied.status_code == 403
    profile = (await api.get(f"{V1}/creators/{replied_creator}", headers=api.admin)).json()
    assert profile["external_conversation"]["email"] == "sam@example.com"


@pytest.mark.db
async def test_autopilot_does_not_draft_for_a_creator_in_a_bot_conversation(pool, fake_llm):
    from rpi.administration.autopilot import run_cycle
    from tests.rpi.test_autopilot import configure

    await configure(pool, auto_approve=False)
    ctx = make_context(pool, llm=fake_llm)
    await run_cycle(pool, ctx)
    async with pool.acquire() as conn:
        creator_id, email = await conn.fetchrow(
            """SELECT d.creator_id, k.value FROM rpi_outreach_drafts d JOIN rpi_creator_contacts k ON k.id = d.contact_id
               ORDER BY d.created_at LIMIT 1""")
        await conn.execute("DELETE FROM rpi_outreach_drafts WHERE creator_id = $1", creator_id)
        await upsert(conn, SOURCE_BOT, [{"email": email, "name": "Busy Creator", "last_inbound_at": NOW - timedelta(days=1)}])
        await conn.execute("UPDATE rpi_autopilot_settings SET weekly_credit_cap = 0")
    summary = await run_cycle(pool, ctx)
    assert summary["drafted"] == [] and any("partnership bot is in conversation with Busy Creator" in s for s in summary["skipped"])
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_outreach_drafts WHERE creator_id = $1", creator_id) == 0
