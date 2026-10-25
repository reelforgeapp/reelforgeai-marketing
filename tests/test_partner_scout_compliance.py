"""
Outreach compliance: footer, signed unsubscribe, copy rules, send-time gate,
no YouTube scraping, 30-day YouTube data scrub.

Database tests use a local PostgreSQL database whose name contains "test"
(PARTNER_SCOUT_TEST_DATABASE_URL, default postgresql:///partner_scout_test) and
are skipped when it is unavailable.
"""

import os
import re
import uuid
from pathlib import Path

import asyncpg
import httpx
import pytest

from app import main
from services import compliance
from services.ai_personalization import AIPersonalizationService

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "database" / "migrations" / "004_compliant_email_templates.sql"
ADDRESS = "123 Example Street, Suite 4, Fayetteville, GA 30214, USA"
ADMIN_KEY = "test-admin-key-0123456789abcdef"
PID = "6f1c2c8e-8a4b-4d39-9b8f-3c0b7a5d1e22"


@pytest.fixture
def settings(monkeypatch):
    s = main.settings  # same cached Settings object used by every module
    monkeypatch.setattr(s, "admin_api_key", ADMIN_KEY)
    monkeypatch.setattr(s, "unsubscribe_secret", "")
    monkeypatch.setattr(s, "company_postal_address", ADDRESS)
    monkeypatch.setattr(s, "public_base_url", "https://marketing.example.test")
    monkeypatch.setattr(s, "environment", "production")
    return s


# --------------------------------------------------------------------------
# Footer, tokens, gate
# --------------------------------------------------------------------------


def test_missing_address_blocks_sending(settings, monkeypatch):
    assert compliance.compliance_problems(settings) == []
    monkeypatch.setattr(settings, "company_postal_address", "")
    assert any("COMPANY_POSTAL_ADDRESS" in p for p in compliance.compliance_problems(settings))
    monkeypatch.setattr(settings, "company_postal_address", ADDRESS)
    monkeypatch.setattr(settings, "admin_api_key", compliance.DEFAULT_ADMIN_API_KEY)
    assert any("signing key" in p for p in compliance.compliance_problems(settings))


def test_footer_and_headers(settings):
    html, text, headers = compliance.finalize_email(settings, PID, "<p>Hi there,</p>", "Hi there,")
    url = compliance.unsubscribe_url(settings, PID)
    assert url.startswith(f"https://marketing.example.test/unsubscribe/{PID}?t=")
    for body in (html, text):
        assert "Lydell Security LLC d/b/a ReelForge AI" in body and "Fayetteville, GA 30214" in body
    assert url in text and url.replace("&", "&amp;") in html
    assert headers == {"List-Unsubscribe": f"<{url}>", "List-Unsubscribe-Post": "List-Unsubscribe=One-Click"}


def test_tokens_are_per_prospect_and_not_the_admin_key(settings):
    token = compliance.unsubscribe_token(settings, PID)
    assert compliance.verify_unsubscribe_token(settings, PID, token)
    assert not compliance.verify_unsubscribe_token(settings, str(uuid.uuid4()), token)
    assert not compliance.verify_unsubscribe_token(settings, PID, "forged")
    assert ADMIN_KEY not in token


async def test_sequence_processing_refuses_without_address(settings, monkeypatch):
    from tasks import outreach_tasks

    async def no_db():
        raise AssertionError("database must not be touched when sending is blocked")

    monkeypatch.setattr(settings, "brevo_api_key", "xkeysib-test")
    monkeypatch.setattr(settings, "company_postal_address", "")
    monkeypatch.setattr(outreach_tasks, "get_database_async", no_db)
    result = await outreach_tasks._process_sequences_async()
    assert result["status"] == "blocked" and "COMPANY_POSTAL_ADDRESS" in result["reasons"][0]


async def test_pause_switch_stops_sequences_and_enrollment(settings, monkeypatch):
    from tasks import outreach_tasks

    async def no_db():
        raise AssertionError("database must not be touched while outreach is paused")

    monkeypatch.setattr(settings, "brevo_api_key", "xkeysib-test")
    monkeypatch.setattr(settings, "partner_scout_auto_outreach_enabled", False)
    monkeypatch.setattr(outreach_tasks, "get_database_async", no_db)
    assert (await outreach_tasks._process_sequences_async())["status"] == "paused"
    assert (await outreach_tasks._auto_enroll_async())["status"] == "paused"


# --------------------------------------------------------------------------
# Copy rules
# --------------------------------------------------------------------------

OLD_COPY = [
    "I just watched your video on Pictory and was genuinely impressed by your insightful breakdown.",
    "many partners are already seeing $1,000+ monthly without extra work",
    "creators in our program have generated $500–$2,000/month simply by sharing honest reviews",
    "Spots are filling up fast—any thoughts or questions?",
    "{{first_name}}, creators are earning $1k+/mo with this—your link inside",
    "I've been following your TikTok content and love your take on AI tools!",
    "Imagine earning passive income month after month",
    "Love what you're doing on TikTok! Your content style really stands out.",
    "I came across your videos about Pictory and really enjoyed your content style.",
]


@pytest.mark.parametrize("text", OLD_COPY)
def test_old_claims_are_rejected(text):
    assert compliance.content_violations(text)


def migration_strings() -> list[str]:
    sql = MIGRATION.read_text()
    body = sql.split("BEGIN;", 1)[1]
    return re.findall(r"\$([sht])\$(.*?)\$\1\$", body, re.S)


def test_every_migrated_template_passes_the_copy_rules():
    strings = migration_strings()
    assert len(strings) == 21  # 7 templates x (subject, html, text)
    for kind, text in strings:
        assert compliance.content_violations(text) == [], text[:80]
        assert "30%" not in text or "recurring" in text


def test_ai_fallback_templates_pass_the_copy_rules():
    svc = AIPersonalizationService.__new__(AIPersonalizationService)
    for kind in ("initial", "followup_1", "followup_2"):
        out = svc._fallback_template({"full_name": "Sam Lee", "competitor_mentions": ["Pictory"]}, kind)
        assert compliance.content_violations(out["subject"], out["body"], out["text_body"]) == []


def test_ai_prompt_forbids_claims():
    svc = AIPersonalizationService.__new__(AIPersonalizationService)
    prompt = svc._build_prompt({"full_name": "Sam", "youtube_subscribers": 1000}, None, "initial")
    assert "Never state or imply earnings" in prompt and "$1,000" not in prompt and "FREE" not in prompt


# --------------------------------------------------------------------------
# Unsubscribe endpoints
# --------------------------------------------------------------------------


@pytest.fixture
async def client(settings, monkeypatch):
    calls = []

    async def fake_record(prospect_id):
        calls.append(prospect_id)
        return True

    monkeypatch.setattr(main, "_record_unsubscribe", fake_record)
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        c.calls = calls
        yield c


async def test_unsubscribe_link_works_and_rejects_forgeries(client, settings):
    token = compliance.unsubscribe_token(settings, PID)
    ok = await client.get(f"/unsubscribe/{PID}?t={token}")
    assert ok.status_code == 200 and "unsubscribed" in ok.text and client.calls == [PID]
    assert (await client.get(f"/unsubscribe/{PID}?t=forged")).status_code == 400
    assert (await client.get(f"/unsubscribe/not-a-uuid?t={token}")).status_code == 400
    one_click = await client.post(f"/unsubscribe/{PID}?t={token}")
    assert one_click.status_code == 200 and client.calls == [PID, PID]


# --------------------------------------------------------------------------
# No YouTube scraping
# --------------------------------------------------------------------------


async def test_extractor_never_fetches_youtube(monkeypatch):
    from discovery import hybrid_email_extractor as hx

    class Boom:
        def __init__(self, *a, **k):
            raise AssertionError("no HTTP request may be made for a YouTube URL")

    monkeypatch.setattr(hx.httpx, "AsyncClient", Boom)
    extractor = hx.HybridEmailExtractor()
    for url in ("https://www.youtube.com/channel/UCabc/about", "youtu.be/xyz", "https://m.youtube.com/@creator"):
        assert await extractor._extract_from_url(url) == (None, None)
    assert not hasattr(extractor, "_extract_from_youtube")
    assert not hx._is_youtube("https://creator-site.com/contact")


# --------------------------------------------------------------------------
# Database: migration and 30-day scrub (real PostgreSQL)
# --------------------------------------------------------------------------

DB_URL = os.environ.get("PARTNER_SCOUT_TEST_DATABASE_URL", "postgresql:///partner_scout_test")


@pytest.fixture
async def db():
    try:
        conn = await asyncpg.connect(DB_URL)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"test database unavailable: {exc}")
    if "test" not in await conn.fetchval("SELECT current_database()"):
        await conn.close()
        pytest.exit("refusing to reset a database whose name does not contain 'test'")
    await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await conn.execute((ROOT / "database" / "schema.sql").read_text())
    yield conn
    await conn.close()


async def test_migration_seeds_compliant_templates_idempotently(db):
    sql = MIGRATION.read_text()
    await db.execute(sql)
    await db.execute(sql)
    rows = await db.fetch("SELECT name, subject_template, html_template, text_template FROM email_templates")
    assert len(rows) == 7
    for r in rows:
        assert compliance.content_violations(r["subject_template"], r["html_template"], r["text_template"]) == [], r["name"]
    steps = await db.fetchval("SELECT steps::text FROM sequence_templates WHERE name = 'youtube_creator'")
    assert "$1k" not in steps and "Free Pro" not in steps and "Closing the loop" in steps


async def test_youtube_scrub_clears_old_statistics_only(db):
    from tasks.maintenance_tasks import SCRUB_YOUTUBE_SQL

    await db.execute(
        """INSERT INTO marketing_prospects (youtube_channel_id, full_name, email, status, youtube_subscribers,
                                            youtube_avg_views, youtube_total_videos, source_video_title, raw_data, discovered_at)
           VALUES ('UCold', 'Old Channel', 'old@example.test', 'unsubscribed', 50000, 1200, 300, 'A video', '{"x": 1}',
                   NOW() - INTERVAL '45 days'),
                  ('UCnew', 'New Channel', 'new@example.test', 'discovered', 20000, 800, 90, 'Another', '{"y": 2}',
                   NOW() - INTERVAL '5 days')""")
    assert await db.execute(SCRUB_YOUTUBE_SQL, 30) == "UPDATE 1"
    old = await db.fetchrow("SELECT * FROM marketing_prospects WHERE youtube_channel_id = 'UCold'")
    new = await db.fetchrow("SELECT * FROM marketing_prospects WHERE youtube_channel_id = 'UCnew'")
    assert old["youtube_subscribers"] is None and old["source_video_title"] is None and old["raw_data"] in ("{}", {})
    assert old["email"] == "old@example.test" and old["status"] == "unsubscribed" and old["full_name"] == "Old Channel"
    assert new["youtube_subscribers"] == 20000
    assert await db.execute(SCRUB_YOUTUBE_SQL, 30) == "UPDATE 0"  # idempotent


async def test_brevo_payload_omits_empty_recipient_name(settings, monkeypatch):
    from outreach import brevo_client

    sent = []

    class Recorder:
        async def post(self, url, json):
            sent.append(json)

            class R:
                status_code = 201

                def json(self):
                    return {"messageId": "<m@test>"}
            return R()

    monkeypatch.setattr(brevo_client, "get_brevo_client", lambda key: Recorder())
    client = brevo_client.BrevoClient()
    assert (await client.send_email("a@example.com", "", "s", "<p>x</p>", sender_name="Larry", sender_email="m@ex.io"))["success"]
    assert sent[-1]["to"] == [{"email": "a@example.com"}] and sent[-1]["sender"] == {"name": "Larry", "email": "m@ex.io"}
    await client.send_email("b@example.com", "Sam", "s", "<p>x</p>")
    assert sent[-1]["to"] == [{"email": "b@example.com", "name": "Sam"}]
