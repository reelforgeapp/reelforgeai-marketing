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


def test_template_asks_about_exclusivity_for_recent_competitor_deals():
    from rpi.outreach.drafts import DraftContext, build_grounding, template_draft
    from rpi.outreach.validator import validate_draft

    ctx = DraftContext(creator_id="c", creator_name="Mark", videos=[{"ref": "v", "title": "AI Edited My Video", "url": None}],
                       primary_labels=["ai_video_tools"], capabilities=[], commission_rate_pct="30",
                       commission_duration_text="for the lifetime of referred customers", proposition="paid sponsorship",
                       request_audience_data=False, sender_name="Larry", competitor_deals=("InVideo",))
    subject, body = template_draft(ctx)
    assert "exclusivity with InVideo" in body and "when does it end" in body
    assert validate_draft(subject, body, build_grounding(ctx, None)).ok
    plain = template_draft(DraftContext(**{**ctx.__dict__, "competitor_deals": ()}))[1]
    assert "exclusivity" not in plain


def test_template_skips_income_and_competitor_titles_and_greets_the_channel():
    from rpi.outreach.drafts import DraftContext, build_grounding, template_draft
    from rpi.outreach.validator import validate_draft

    videos = [{"ref": "1", "title": "How I Make $120,642.83/month Posting AI Videos", "url": None},
              {"ref": "2", "title": "AI Edited My ENTIRE Video in Invideo (One Prompt)", "url": None},
              {"ref": "3", "title": "Kling 4 is FINALLY Here", "url": None}]
    ctx = DraftContext(creator_id="c", creator_name="Atomic Gains", videos=videos, primary_labels=["ai_video_tools"],
                       capabilities=[], commission_rate_pct="30", commission_duration_text="for 12 months",
                       proposition="paid sponsorship", request_audience_data=True, sender_name="Larry",
                       avoid_title_terms=("InVideo", "InVideo AI"))
    subject, body = template_draft(ctx)
    assert '"Kling 4 is FINALLY Here"' in body and "$120" not in body and "Invideo" not in body
    assert body.startswith("Hi Atomic Gains,") and subject == "Partnership idea for your AI video tools content"
    assert validate_draft(subject, body, build_grounding(ctx, None)).ok


def test_template_skips_titles_naming_other_channels_or_hiding_characters():
    from rpi.outreach.drafts import DraftContext, build_grounding, template_draft
    from rpi.outreach.validator import validate_draft

    videos = [{"ref": "1", "title": "Make 30 Minutes Long Ai Videos Like \u202a@sarvatrahindutva\u202c | FULL COURSE", "url": None},
              {"ref": "2", "title": "Go viral\u200b with this trick", "url": None},
              {"ref": "3", "title": "Every individual creator needs this workflow", "url": None}]
    ctx = DraftContext(creator_id="c", creator_name="Chad Grow", videos=videos, primary_labels=["ai_video_tools"],
                       capabilities=[], commission_rate_pct="30", commission_duration_text="for 12 months",
                       proposition="paid sponsorship", request_audience_data=True, sender_name="Larry",
                       avoid_title_terms=("Vidu",))  # whole words only: "individual" is not Vidu
    subject, body = template_draft(ctx)
    assert '"Every individual creator needs this workflow"' in body
    assert "@" not in body and "\u202a" not in body and "\u200b" not in body
    assert validate_draft(subject, body, build_grounding(ctx, None)).ok


def test_validator_blocks_hidden_characters_and_other_accounts():
    bad = GOOD.replace("Hi Sam,", "Hi Sam \u202e@someone\u202c,")
    result = validate_draft("Partnership idea", bad, G)
    assert any("hidden formatting characters: U+202C, U+202E" in e for e in result.errors)
    assert any("names another channel or account: '@someone'" in e for e in result.errors)
    signed = GOOD + "\n\nReply to jr@reelforgeai.io"
    assert validate_draft("Partnership idea", signed, Grounding(**{**G.__dict__, "sender_email": "jr@reelforgeai.io"})).ok


@pytest.mark.db
async def test_new_registry_competitors_link_sponsorships_recorded_before_them(pool):
    from datetime import datetime, timedelta, timezone

    from rpi.enrichment.service import creator_competitor_deals
    from scripts.rpi_migrate import seed_competitors

    async with pool.acquire() as conn:
        cid = await conn.fetchval("INSERT INTO rpi_creators (display_name) VALUES ('Chad') RETURNING id")
        recent = datetime.now(timezone.utc) - timedelta(days=20)
        await conn.execute(
            """INSERT INTO rpi_sponsorship_relationships (creator_id, brand_name, relationship_type, source, published_at)
               VALUES ($1, 'TopviewAI', 'confirmed_paid', 'creatordb', $2), ($1, 'Fish Audio', 'confirmed_paid', 'creatordb', $2)""",
            cid, recent)
        result = await seed_competitors(conn)
        assert result["sponsorships_linked"] == 1  # Fish Audio is not a competitor
        deals = await creator_competitor_deals(conn, cid, now=datetime.now(timezone.utc))
    assert [brand for brand, _ in deals] == ["TopviewAI"]  # direct competitor: the email asks about exclusivity


# --------------------------------------------------------------------------
# Partner Scout email history
# --------------------------------------------------------------------------


async def add_prospect(pool, *, channel=None, email=None, sent=3, days_ago=60, status="contacted"):
    from datetime import datetime, timedelta

    last = datetime.utcnow() - timedelta(days=days_ago)  # Partner Scout stores naive UTC
    async with pool.acquire() as conn:
        return await conn.fetchval(
            """INSERT INTO marketing_prospects (full_name, email, youtube_channel_id, status, total_emails_sent,
                                                first_contacted_at, last_contacted_at)
               VALUES ('Sam', $1, $2, $3, $4, $5, $6) RETURNING id""",
            email, channel, status, sent, last - timedelta(days=14), last)


@pytest.mark.db
async def test_partner_scout_history_matches_channel_legacy_id_and_email(pool):
    from rpi.crm.contact_history import partner_scout_history

    did, cid = await seed_draft(pool)
    async with pool.acquire() as conn:
        assert await partner_scout_history(conn, cid) is None
        channel = await conn.fetchval("SELECT platform_account_id FROM rpi_creator_social_accounts WHERE creator_id = $1", cid)
    await add_prospect(pool, channel=channel, days_ago=40)
    async with pool.acquire() as conn:
        prior = await partner_scout_history(conn, cid)
        assert prior.emails_sent == 3 and prior.status == "contacted" and prior.last_contacted_at.tzinfo is not None
        assert "3 time(s)" in prior.summary()
        other = await conn.fetchval("INSERT INTO rpi_creators (display_name) VALUES ('Other') RETURNING id")
        assert await partner_scout_history(conn, other) is None
        assert (await partner_scout_history(conn, other, email="nobody@example.com")) is None
    pid = await add_prospect(pool, email="lee@example.com", sent=1, days_ago=10)
    async with pool.acquire() as conn:
        assert (await partner_scout_history(conn, other, email="LEE@example.com")).emails_sent == 1
        await conn.execute("UPDATE rpi_creators SET legacy_prospect_id = $2 WHERE id = $1", other, pid)
        assert (await partner_scout_history(conn, other)).emails_sent == 1


@pytest.mark.db
async def test_send_gate_counts_recent_partner_scout_email(pool):
    await enable_sending(pool)
    did, cid = await seed_draft(pool, email="recent@example.com")
    await add_prospect(pool, email="recent@example.com", days_ago=5)
    sender = FakeSender()
    result = await send_draft(pool, did, "approver:a", sender, public_base_url="https://rpi.test")
    assert not result["sent"] and sender.sent == []
    assert any("Partner Scout emailed this creator" in r for r in result["reasons"])


@pytest.mark.db
async def test_old_partner_scout_email_does_not_block_and_shows_on_profile(pool):
    from rpi.storage.creators import creator_profile

    await enable_sending(pool)
    did, cid = await seed_draft(pool, email="old@example.com")
    await add_prospect(pool, email="old@example.com", days_ago=200)
    async with pool.acquire() as conn:
        profile = await creator_profile(conn, cid)
    assert profile["partner_scout_history"]["emails_sent"] == 3
    result = await send_draft(pool, did, "approver:a", FakeSender(), public_base_url="https://rpi.test")
    assert result["sent"], result


def test_template_follow_up_acknowledges_earlier_contact_without_claiming_no_reply():
    c = DraftContext(**{**ctx().__dict__, "prior_contact_month": "February 2026"})
    subject, body = template_draft(c)
    assert "We emailed you about ReelForge AI in February 2026" in body
    assert "reaching out" not in body and "hear back" not in body
    assert validate_draft(subject, body, build_grounding(c, None)).ok
    assert "We emailed you" not in template_draft(ctx())[1]


# --------------------------------------------------------------------------
# Sender identity, one-click unsubscribe, test sends
# --------------------------------------------------------------------------


@pytest.mark.db
async def test_send_uses_rpi_sender_and_one_click_unsubscribe(pool):
    await enable_sending(pool)
    did, _ = await seed_draft(pool)
    sender = FakeSender()
    assert (await send_draft(pool, did, "approver:a", sender, public_base_url="https://rpi.test"))["sent"]
    msg = sender.sent[0]
    assert msg["sender_email"] == "jr@reelforgeai.io" and msg["sender_name"] == "Jr" and msg["reply_to"] == "jr@reelforgeai.io"
    link = msg["headers"]["List-Unsubscribe"].strip("<>")
    assert link.startswith(f"https://rpi.test/rpi/public/unsubscribe/{did}?t=") and link in msg["text_content"]
    assert msg["headers"]["List-Unsubscribe-Post"] == "List-Unsubscribe=One-Click"


@pytest.mark.db
async def test_test_send_only_reaches_allowed_inbox_and_changes_nothing(pool):
    from rpi.outreach.sender import test_send

    await enable_sending(pool)
    async with pool.acquire() as conn:
        await conn.execute("UPDATE rpi_outreach_settings SET sending_enabled = FALSE")
    did, cid = await seed_draft(pool, status="draft", approved_by=None, pipeline="recommended")
    sender = FakeSender()
    allowed = {"marketing@reelforgeai.io"}
    refused = await test_send(pool, did, "sam@example.com", "approver:a", sender, public_base_url="https://rpi.test",
                              allowed_recipients=allowed)
    assert not refused["sent"] and sender.sent == []  # never the creator, never an unlisted address
    assert not (await test_send(pool, did, "marketing@reelforgeai.io", "approver:a", sender,
                                public_base_url="https://rpi.test", allowed_recipients=set()))["sent"]
    ok = await test_send(pool, did, "Marketing@ReelForgeAI.io", "approver:a", sender, public_base_url="https://rpi.test",
                         allowed_recipients=allowed)
    assert ok["sent"], ok  # works while sending is disabled and before approval
    msg = sender.sent[0]
    assert msg["to_email"] == "marketing@reelforgeai.io" and msg["subject"].startswith("[TEST] ")
    assert "rpi_test" in msg["tags"] and not any(t.startswith("rpi_draft:") for t in msg["tags"])
    assert "/rpi/public/unsubscribe/TEST-PREVIEW" in msg["text_content"] and f"unsubscribe/{did}" not in msg["text_content"]
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT status FROM rpi_outreach_drafts WHERE id = $1", did) == "draft"
        assert await conn.fetchval("SELECT provider_message_id FROM rpi_outreach_drafts WHERE id = $1", did) is None
        assert await conn.fetchval("SELECT pipeline_status FROM rpi_creators WHERE id = $1", cid) == "recommended"
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_outreach_events WHERE draft_id = $1", did) == 0


@pytest.mark.db
async def test_one_click_unsubscribe_and_inactive_test_link(api, pool):
    from rpi.outreach.sender import unsubscribe_token

    did, cid = await seed_draft(pool)
    test_page = await api.get("/rpi/public/unsubscribe/TEST-PREVIEW")
    assert test_page.status_code == 200 and "test email" in test_page.text
    bad = await api.post(f"/rpi/public/unsubscribe/{did}?t=wrong")
    assert bad.json() == {"unsubscribed": False}
    ok = await api.post(f"/rpi/public/unsubscribe/{did}?t={unsubscribe_token(did)}", data={"List-Unsubscribe": "One-Click"})
    assert ok.json() == {"unsubscribed": True}
    async with pool.acquire() as conn:
        assert await suppression.is_suppressed(conn, creator_id=cid)


def test_template_uses_one_idea_per_paragraph():
    c = DraftContext(**{**ctx().__dict__, "competitor_deals": ("InVideo",), "request_audience_data": True})
    subject, body = template_draft(c)
    paragraphs = body.split("\n\n")
    assert paragraphs[0].startswith("Hi ") and paragraphs[-1] == c.sender_name
    assert all("\n" not in p for p in paragraphs)  # no stacked single lines
    offer = next(p for p in paragraphs if "affiliate program" in p)
    assert "exclusivity with InVideo" in offer
    assert any(p.startswith("Program details: https://") for p in paragraphs)
    assert validate_draft(subject, body, build_grounding(c, None)).ok


def test_html_is_escaped_linked_and_keeps_paragraphs():
    from rpi.outreach.sender import message_fields

    settings_row = {"sender_name": "Larry <B>", "sender_email": "m@reelforgeai.io", "postal_address": "1 Main & Co"}
    body = 'Hi Sam,\n\nYour video "<script>alert(1)</script> & Co" is great.\n\nProgram details: https://www.reelforgeai.io/become-affiliate\n\nLarry'
    msg = message_fields(settings_row, body, "d1", "https://rpi.test")
    h = msg["html_content"]
    assert "<script>" not in h and "&lt;script&gt;" in h and "&amp; Co" in h
    assert h.count('<p style="margin:0 0 16px">') == 4
    assert '<a href="https://www.reelforgeai.io/become-affiliate"' in h and ">reelforgeai.io/become-affiliate</a>" in h
    assert "Larry &lt;B&gt; &middot; ReelForge AI" in h and "1 Main &amp; Co" in h
    assert 'href="https://rpi.test/rpi/public/unsubscribe/d1?t=' in h and "opt out here" in h
    assert msg["text_content"].startswith(body) and "https://rpi.test/rpi/public/unsubscribe/d1?t=" in msg["text_content"]
    test_h = message_fields(settings_row, body, "d1", "https://rpi.test", test=True)["html_content"]
    assert "unsubscribe/TEST-PREVIEW" in test_h and "Test send" in test_h and "unsubscribe/d1" not in test_h


def test_signature_replaces_the_signed_name_in_html_and_text(monkeypatch):
    from app.config import get_settings
    from rpi.outreach.sender import message_fields

    monkeypatch.setattr(get_settings(), "rpi_signature_title", "Founder & CEO")
    row = {"sender_name": "Larry Barksdale", "sender_email": "marketing@reelforgeai.io", "postal_address": "1 Main St"}
    msg = message_fields(row, "Hi Sam,\n\nShort note.\n\nLarry Barksdale", "d1", "https://rpi.test")
    h, t = msg["html_content"], msg["text_content"]
    assert h.count("Larry Barksdale") == 2  # signature block + footer, not the old plain sign-off
    assert "Founder &amp; CEO, ReelForge AI" in h and 'alt="ReelForge AI"' in h
    assert 'src="https://www.reelforgeai.io/logos/reelforge-icon-192.png"' in h
    assert 'href="mailto:marketing@reelforgeai.io"' in h and ">reelforgeai.io</a>" in h
    assert "Short note.\n\nLarry Barksdale\nFounder & CEO, ReelForge AI\nreelforgeai.io · marketing@reelforgeai.io" in t
    closing = message_fields(row, "Hi Sam,\n\nShort note.\n\nBest,\nLarry Barksdale", "d1", "https://rpi.test")
    assert "<p style=\"margin:0 0 16px\">Best,</p>" in closing["html_content"]
    assert "Best,\n\nLarry Barksdale\nFounder & CEO" in closing["text_content"]
    unsigned = message_fields(row, "Hi Sam,\n\nShort note.", "d1", "https://rpi.test")["text_content"]
    assert "Short note.\n\nLarry Barksdale\nFounder & CEO" in unsigned  # signature added when the draft has none


# --------------------------------------------------------------------------
# Scheduled sends
# --------------------------------------------------------------------------


def test_send_window_boundaries():
    from datetime import datetime, timezone

    from rpi.outreach.sender import in_send_window

    def at(iso):
        return in_send_window(datetime.fromisoformat(iso).astimezone(timezone.utc), tz="America/New_York",
                              window="09:00-17:00", weekdays_only=True)
    assert at("2026-10-26T11:00:00-04:00")       # Monday 11:00 ET
    assert not at("2026-10-26T08:59:00-04:00")   # before the window
    assert not at("2026-10-26T17:01:00-04:00")   # after it
    assert not at("2026-10-31T11:00:00-04:00")   # Saturday
    assert at("2026-11-02T11:00:00-05:00")       # after the DST change, still 11:00 local


async def schedule(pool, did, when):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE rpi_outreach_drafts SET scheduled_send_at = $2 WHERE id = $1", did, when)


@pytest.mark.db
async def test_scheduled_sends_respect_window_due_time_and_daily_limit(pool):
    from datetime import datetime, timedelta, timezone

    from rpi.outreach.sender import send_due_drafts

    await enable_sending(pool, limit=1)
    now = datetime.now(timezone.utc)
    first, _ = await seed_draft(pool, email="first@example.com")
    second, _ = await seed_draft(pool, email="second@example.com")
    later, _ = await seed_draft(pool, email="later@example.com")
    await schedule(pool, first, now - timedelta(minutes=10))
    await schedule(pool, second, now - timedelta(minutes=5))
    await schedule(pool, later, now + timedelta(days=1))
    sender = FakeSender()
    assert (await send_due_drafts(pool, sender, public_base_url="https://rpi.test", window_open=False))["status"] == "outside_window"
    assert sender.sent == []
    result = await send_due_drafts(pool, sender, public_base_url="https://rpi.test", window_open=True)
    assert result["sent"] == [first] and [m["to_email"] for m in sender.sent] == ["first@example.com"]
    again = await send_due_drafts(pool, sender, public_base_url="https://rpi.test", window_open=True)
    assert again == {"status": "daily_limit_reached"} and len(sender.sent) == 1  # second waits for tomorrow
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT scheduled_send_at IS NOT NULL FROM rpi_outreach_drafts WHERE id = $1", second)
        assert await conn.fetchval("SELECT status FROM rpi_outreach_drafts WHERE id = $1", later) == "approved"
        refusals = await conn.fetchval("SELECT COUNT(*) FROM rpi_audit_log WHERE action = 'outreach.send_refused'")
    assert refusals == 0  # a full daily limit is not logged every five minutes


@pytest.mark.db
async def test_scheduled_send_is_cancelled_when_refused_for_good(pool):
    from datetime import datetime, timedelta, timezone

    from rpi.outreach.sender import send_due_drafts

    await enable_sending(pool)
    did, cid = await seed_draft(pool, email="optout@example.com")
    await schedule(pool, did, datetime.now(timezone.utc) - timedelta(minutes=1))
    async with pool.acquire() as conn:
        await suppression.suppress_creator(conn, cid, "unsubscribe link", "recipient")
    sender = FakeSender()
    result = await send_due_drafts(pool, sender, public_base_url="https://rpi.test", window_open=True)
    assert result["cancelled"] == [did] and sender.sent == []
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status, scheduled_send_at, failure_reason FROM rpi_outreach_drafts WHERE id = $1", did)
    assert row["status"] == "approved" and row["scheduled_send_at"] is None and "scheduled send refused" in row["failure_reason"]


@pytest.mark.db
async def test_schedule_endpoint_requires_approval_and_timezone(api, pool):
    did, _ = await seed_draft(pool, status="draft", approved_by=None, pipeline="recommended")
    url = f"/rpi/api/v1/outreach/drafts/{did}/schedule"
    assert (await api.post(url, json={"send_at": "2026-10-27T11:00:00-04:00"}, headers=api.admin)).status_code == 409
    approved, _ = await seed_draft(pool, email="ok@example.com")
    url = f"/rpi/api/v1/outreach/drafts/{approved}/schedule"
    assert (await api.post(url, json={"send_at": "2026-10-27T11:00:00"}, headers=api.admin)).status_code == 422
    ok = await api.post(url, json={"send_at": "2026-10-27T11:00:00-04:00"}, headers=api.admin)
    assert ok.status_code == 200
    async with pool.acquire() as conn:
        assert (await conn.fetchval("SELECT scheduled_send_at FROM rpi_outreach_drafts WHERE id = $1", approved)).isoformat() \
            == "2026-10-27T15:00:00+00:00"
    assert (await api.post(url, json={"send_at": None}, headers=api.admin)).status_code == 200
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT scheduled_send_at FROM rpi_outreach_drafts WHERE id = $1", approved) is None
