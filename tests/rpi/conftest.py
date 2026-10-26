"""
Shared fixtures.

Database tests run against a real PostgreSQL database (default
postgresql:///rpi_test; override with RPI_TEST_DATABASE_URL). The schema is
rebuilt once per session from database/schema.sql (Partner Scout) and
database/migrations/005_rpi_core.sql, and every rpi_* table is truncated
before each DB test. Tests are skipped, not faked, when no database is
reachable.
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import asyncpg
import pytest

os.environ.setdefault("RPI_ATTRIBUTION_HASH_SALT", "test-salt-0123456789abcdef")
os.environ.setdefault("ENVIRONMENT", "test")

from app.config import get_settings  # noqa: E402

get_settings.cache_clear()

ROOT = Path(__file__).resolve().parents[2]
DB_URL = os.environ.get("RPI_TEST_DATABASE_URL", "postgresql:///rpi_test")


@pytest.fixture(scope="session")
async def db_pool():
    try:
        pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=8)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"PostgreSQL test database unavailable: {exc}")
    async with pool.acquire() as conn:
        name = await conn.fetchval("SELECT current_database()")
        if "test" not in name:
            await pool.close()
            pytest.exit(f"refusing to reset database '{name}': test database names must contain 'test'")
        await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await conn.execute((ROOT / "database" / "schema.sql").read_text())
        for migration in ("005_rpi_core.sql", "006_rpi_scheduled_sends.sql", "007_rpi_autopilot.sql",
                          "008_rpi_outside_range_presets.sql", "009_rpi_external_conversations.sql"):
            await conn.execute((ROOT / "database" / "migrations" / migration).read_text())
        from scripts.rpi_migrate import seed

        await seed(conn)
    yield pool
    await pool.close()


RESET_KEEP = {"rpi_workspaces", "rpi_schema_migrations", "rpi_data_retention_rules", "rpi_budgets",
              "rpi_outreach_settings", "rpi_competitors", "rpi_autopilot_settings"}


@pytest.fixture
async def pool(db_pool):
    async with db_pool.acquire() as conn:
        tables = [r["tablename"] for r in await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'rpi_%'")]
        to_clear = [t for t in tables if t not in RESET_KEEP]
        await conn.execute("TRUNCATE " + ", ".join(to_clear) + " RESTART IDENTITY CASCADE")
        await conn.execute("TRUNCATE marketing_prospects CASCADE")
        await conn.execute("UPDATE rpi_outreach_settings SET sending_enabled = FALSE, daily_send_limit = 10, "
                           "min_days_between_contacts = 30, sender_name = NULL, sender_email = NULL, postal_address = NULL")
        await conn.execute("UPDATE rpi_autopilot_settings SET enabled = FALSE, auto_approve = FALSE, weekly_credit_cap = 0, "
                           "target_queue = 5, send_time = '11:00', discover_per_run = 25, screen_per_run = 10, "
                           "analyze_per_run = 5, presets = DEFAULT, discovery_cursor = '{}', "
                           "last_run_at = NULL, last_run = '{}'")
        await conn.execute("UPDATE rpi_budgets SET active = TRUE")
        await conn.execute("UPDATE rpi_budgets SET limit_usd = 200 WHERE scope = 'global' AND period = 'monthly'")
        await conn.execute("UPDATE rpi_budgets SET limit_usd = 25 WHERE scope = 'global' AND period = 'daily'")
    return db_pool


class FakeLLM:
    """Deterministic stand-in for Claude. `responder(system, user, tool)`
    returns the tool input dict; prompts are recorded for inspection."""

    def __init__(self, responder=None, cost: str = "0.002"):
        self.responder = responder
        self.cost = Decimal(cost)
        self.calls: list[dict[str, Any]] = []

    async def call_tool(self, *, price, system, user, tool, max_tokens, job_id=None, creator_id=None, priority=None):
        from rpi.content_intelligence.claude_client import ToolCallResult

        self.calls.append({"model": price.model, "system": system, "user": user, "tool": tool["name"]})
        if self.responder is None:
            return ToolCallResult(False, None, price.model, error="no responder")
        data = self.responder(system, user, tool)
        return ToolCallResult(True, data, price.model, 100, 50, self.cost)


def label_everything_responder(label: str = "faceless_youtube_automation", relevance: float = 0.9, confidence: float = 0.8):
    import re

    def respond(system, user, tool):
        if tool["name"] == "report_creator_analysis":
            ids = re.findall(r'<untrusted_content id="([^"]+)"', user)
            return {
                "videos": [{"id": i, "labels": [label], "relevance": relevance, "audience_problems": ["high_production_time"],
                            "buyer_intent": "specific_workflow_pain_point", "promotional": False, "note": "about automation"}
                           for i in ids],
                "creator_summary": {"primary_labels": [label], "reelforge_relevance": relevance,
                                    "audience_problem": "producing videos takes too long",
                                    "addressable_problems": ["high_production_time"], "confidence": confidence,
                                    "uncertainty_reasons": []},
            }
        return {}

    return respond


class FakeSender:
    def __init__(self, succeed: bool = True):
        self.succeed = succeed
        self.sent: list[dict] = []

    async def __call__(self, **kwargs) -> dict:
        self.sent.append(kwargs)
        if self.succeed:
            return {"success": True, "message_id": f"<msg-{len(self.sent)}@test>"}
        return {"success": False, "error": "provider down"}


@pytest.fixture
def fake_llm():
    return FakeLLM(label_everything_responder())


@pytest.fixture
def fake_sender():
    return FakeSender()


def make_context(pool, *, llm=None, send_fn=None, fixture: bool = True, settings_overrides: Optional[dict] = None):
    from rpi.api.context import build_context
    from rpi.providers.fixture_provider import FixtureProvider
    from rpi.providers.registry import ProviderRegistry

    settings = get_settings().model_copy(update={
        "rpi_bootstrap_admin_key": "bootstrap-admin-key-for-tests-only-123456",
        "anthropic_api_key": "", "brevo_api_key": "", "rpi_public_base_url": "https://rpi.test",
        **(settings_overrides or {}),
    })
    registry = ProviderRegistry({"fixture": FixtureProvider()} if fixture else {})
    return build_context(pool, settings, registry=registry, llm=llm, send_fn=send_fn)


@pytest.fixture
async def api(pool, fake_llm, fake_sender):
    """HTTP client against the real FastAPI app with an injected context."""
    import httpx

    from app.main import app
    from rpi.api import deps

    ctx = make_context(pool, llm=fake_llm, send_fn=fake_sender)
    deps.set_context(ctx)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        client.ctx = ctx
        client.admin = {"X-RPI-Key": "bootstrap-admin-key-for-tests-only-123456"}
        yield client
    deps.set_context(None)
