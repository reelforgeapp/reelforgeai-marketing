"""Outreach validation and application-level send gates."""

import asyncio
import json

import pytest

from rpi.crm import suppression
from rpi.outreach.drafts import DraftContext, build_grounding, generate, template_draft
from rpi.outreach.sender import send_draft
from rpi.outreach.validator import Grounding, validate_draft
from tests.rpi.conftest import FakeLLM, FakeSender

G = Grounding(content_titles=["How I Run 5 Faceless YouTube Channels With AI"], allowed_numbers={"5"},
              allowed_urls={"https://www.reelforgeai.io/become-affiliate"}, commission_rate_pct="30",
              sender_email="jr@reelforgeai.io")
GOOD = ("Hi Sam,\n\nYour video \"How I Run 5 Faceless YouTube Channels With AI\" covers exactly the workflow ReelForge "
        "AI automates. Our affiliate program pays 30% recurring. Details: https://www.reelforgeai.io/become-affiliate\n\nJr")


def test_valid_draft_passes():
    assert validate_draft("Partnership idea", GOOD, G).ok


@pytest.mark.parametrize("bad, fragment", [
    ("I just watched your video and loved it.", "familiarity"),
    ("I've been following you for years.", "familiarity"),
    ("Creators are earning $1,000+/month with us.", "income"),
    ("We pay 40% commission.", "differs from program commission"),
    ("You have 48,200 subscribers!", "not in the verified facts"),
    ("Email me at boss@elsewhere.com", "email address"),
    ("See https://evil.example/phish", "URL"),
])
def test_validator_blocks_risky_content(bad, fragment):
    result = validate_draft("Hi", GOOD + "\n" + bad, G)
    assert not result.ok and any(fragment in e for e in result.errors), result.errors


def test_draft_must_reference_a_verified_video():
    body = "Hi Sam, ReelForge AI pays 30% recurring. https://www.reelforgeai.io/become-affiliate"
    assert any("specific verified video" in e for e in validate_draft("Hi", body, G).errors)


def ctx(**kw):
    base = dict(creator_id="c1", creator_name="Sam Lee", videos=[{"ref": "v1", "title": G.content_titles[0], "url": None}],
                primary_labels=["faceless_youtube_automation"], capabilities=[], commission_rate_pct="30",
                commission_duration_text="for the lifetime of referred customers", proposition="paid sponsorship",
                request_audience_data=True, sender_name="Jr")
    base.update(kw)
    return DraftContext(**base)


def test_template_draft_is_valid():
    c = ctx()
    subject, body = template_draft(c)
    assert validate_draft(subject, body, build_grounding(c, None)).ok


async def test_claude_draft_rejected_when_it_fabricates():
    llm = FakeLLM(lambda s, u, t: {"subject": "Huge fan!", "body": "I've been watching you for years. Creators make $2k/month."})
    from rpi.content_intelligence.claude_client import ModelPrice
    from decimal import Decimal

    out = await generate(llm, ModelPrice("m", Decimal(1), Decimal(1)), ctx(), sender_email=None)
    assert out["generator"] == "template"  # fell back to the deterministic template
    assert "failed validation" in out["validation"]["llm_note"]
    assert out["validation"]["ok"]


# --------------------------------------------------------------------------
# Send gates (database)
# --------------------------------------------------------------------------


async def seed_draft(pool, *, status="approved", email="sam@example.com", pipeline="outreach_approved", approved_by="approver:a"):
    async with pool.acquire() as conn:
        cid = await conn.fetchval("INSERT INTO rpi_creators (display_name, pipeline_status) VALUES ('Sam', $1) RETURNING id", pipeline)
        await conn.execute("INSERT INTO rpi_creator_social_accounts (creator_id, platform, platform_account_id) VALUES ($1, 'youtube', $2)",
                           cid, f"UC{cid.hex[:20]}")
        contact = await conn.fetchval(
            """INSERT INTO rpi_creator_contacts (creator_id, contact_type, value, value_normalized, source)
               VALUES ($1, 'email', $2, lower($2), 'creatordb') RETURNING id""", cid, email)
        grounding = {"videos": [{"ref": "v1", "title": G.content_titles[0]}], "allowed_numbers": ["5"],
                     "allowed_urls": sorted(G.allowed_urls), "commission_rate_pct": "30"}
        did = await conn.fetchval(
            """INSERT INTO rpi_outreach_drafts (creator_id, contact_id, subject, body_text, grounding, validation, generator,
                                                status, created_by, approved_by, idempotency_key)
               VALUES ($1,$2,'Partnership idea',$3,$4::jsonb,'{"ok": true}','template',$5,'analyst:x',$6,$7) RETURNING id""",
            cid, contact, GOOD, json.dumps(grounding), status, approved_by, f"k-{cid}")
    return str(did), cid


async def enable_sending(pool, limit=10):
    async with pool.acquire() as conn:
        await conn.execute("""UPDATE rpi_outreach_settings SET sending_enabled = TRUE, daily_send_limit = $1,
                              sender_name = 'Jr', sender_email = 'jr@reelforgeai.io', postal_address = '1 Main St, Austin TX'""", limit)


@pytest.mark.db
async def test_sending_disabled_by_default(pool):
    did, _ = await seed_draft(pool)
    sender = FakeSender()
    result = await send_draft(pool, did, "approver:a", sender, public_base_url="https://rpi.test")
    assert not result["sent"] and any("disabled" in r for r in result["reasons"])
    assert sender.sent == []


@pytest.mark.db
async def test_unapproved_draft_never_sent(pool):
    await enable_sending(pool)
    did, _ = await seed_draft(pool, status="draft", approved_by=None)
    sender = FakeSender()
    result = await send_draft(pool, did, "approver:a", sender, public_base_url="https://rpi.test")
    assert not result["sent"] and sender.sent == []


@pytest.mark.db
async def test_successful_send_includes_compliance_footer_and_moves_crm(pool):
    await enable_sending(pool)
    did, cid = await seed_draft(pool)
    sender = FakeSender()
    result = await send_draft(pool, did, "approver:a", sender, public_base_url="https://rpi.test")
    assert result["sent"], result
    text = sender.sent[0]["text_content"]
    assert "1 Main St, Austin TX" in text and "/rpi/public/unsubscribe/" in text
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT pipeline_status FROM rpi_creators WHERE id = $1", cid) == "contacted"
        assert await conn.fetchval("SELECT status FROM rpi_outreach_drafts WHERE id = $1", did) == "sent"


@pytest.mark.db
async def test_concurrent_sends_are_exactly_once(pool):
    await enable_sending(pool)
    did, _ = await seed_draft(pool)
    sender = FakeSender()
    results = await asyncio.gather(*[send_draft(pool, did, "approver:a", sender, public_base_url="https://rpi.test")
                                     for _ in range(5)])
    assert sum(r["sent"] for r in results) == 1 and len(sender.sent) == 1


@pytest.mark.db
async def test_suppressed_and_legacy_opted_out_contacts_blocked(pool):
    await enable_sending(pool)
    did, _ = await seed_draft(pool, email="optout@example.com")
    async with pool.acquire() as conn:
        await suppression.add(conn, "domain", "example.com", "domain opt-out", "test")
    r = await send_draft(pool, did, "approver:a", FakeSender(), public_base_url="https://rpi.test")
    assert not r["sent"] and any("suppressed" in x for x in r["reasons"])

    did2, _ = await seed_draft(pool, email="legacy@other.org")
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO marketing_prospects (email, status) VALUES ('legacy@other.org', 'unsubscribed')")
    r = await send_draft(pool, did2, "approver:a", FakeSender(), public_base_url="https://rpi.test")
    assert not r["sent"] and any("Partner Scout" in x for x in r["reasons"])


@pytest.mark.db
async def test_daily_limit_and_recent_contact_dedupe(pool):
    await enable_sending(pool, limit=1)
    d1, _ = await seed_draft(pool, email="a@one.com")
    d2, _ = await seed_draft(pool, email="b@two.com")
    assert (await send_draft(pool, d1, "approver:a", FakeSender(), public_base_url="https://rpi.test"))["sent"]
    r = await send_draft(pool, d2, "approver:a", FakeSender(), public_base_url="https://rpi.test")
    assert not r["sent"] and any("daily send limit" in x for x in r["reasons"])

    await enable_sending(pool, limit=10)
    d3, _ = await seed_draft(pool, email="a@one.com")  # same address, different creator record
    r = await send_draft(pool, d3, "approver:a", FakeSender(), public_base_url="https://rpi.test")
    assert not r["sent"] and any("already contacted" in x for x in r["reasons"])


@pytest.mark.db
async def test_tampered_draft_fails_revalidation_at_send(pool):
    await enable_sending(pool)
    did, _ = await seed_draft(pool)
    async with pool.acquire() as conn:
        await conn.execute("UPDATE rpi_outreach_drafts SET body_text = body_text || ' Creators earn $5,000/month!' WHERE id = $1", did)
    r = await send_draft(pool, did, "approver:a", FakeSender(), public_base_url="https://rpi.test")
    assert not r["sent"] and any("fails validation" in x for x in r["reasons"])
