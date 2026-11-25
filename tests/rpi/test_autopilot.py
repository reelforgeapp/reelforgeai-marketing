"""Autopilot: unattended research, drafting, approval and scheduling inside a weekly credit allowance."""

import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from rpi.administration.autopilot import AUTOPILOT_ACTOR, next_slots, run_cycle
from rpi.administration.budget import InMemoryBudget
from rpi.providers.base import Capability, ResultStatus
from rpi.providers.fixture_provider import FixtureProvider
from rpi.providers.gateway import CallContext, ProviderGateway
from tests.rpi.conftest import make_context

ET = ZoneInfo("America/New_York")
V1 = "/rpi/api/v1"


def test_next_slots_skip_weekends_full_days_and_passed_times():
    now = datetime(2026, 10, 30, 15, 30, tzinfo=timezone.utc)  # Fri 11:30 ET: today's 11:00 has passed
    slots = next_slots(3, now=now, taken={date(2026, 11, 2): 1}, per_day=1, tz="America/New_York",
                       send_time=time(11, 0), window_end=time(17, 0), weekdays_only=True)
    local = [s.astimezone(ET) for s in slots]
    # Sat/Sun skipped, Mon Nov 2 already has a send, so Tue-Thu at 11:00 (EST after the DST change).
    assert [(d.date().isoformat(), d.strftime("%H:%M")) for d in local] == [
        ("2026-11-03", "11:00"), ("2026-11-04", "11:00"), ("2026-11-05", "11:00")]


def test_next_slots_space_several_sends_a_day_inside_the_window():
    now = datetime(2026, 11, 2, 13, 0, tzinfo=timezone.utc)  # Mon 08:00 ET
    slots = next_slots(4, now=now, taken={date(2026, 11, 2): 1}, per_day=3, tz="America/New_York",
                       send_time=time(16, 30), window_end=time(17, 0), weekdays_only=True)
    local = [s.astimezone(ET).strftime("%a %H:%M") for s in slots]
    # One send already booked at 16:30; 16:50 fits, 17:10 is past the window.
    assert local == ["Mon 16:50", "Tue 16:30", "Tue 16:50", "Wed 16:30"]
    assert next_slots(2, now=now, taken={}, per_day=0, tz="America/New_York", send_time=time(11, 0),
                      window_end=time(17, 0), weekdays_only=True) == []


async def test_gateway_job_credit_cap_blocks_calls_past_the_allowance():
    gateway = ProviderGateway(budget=InMemoryBudget([]))
    provider = FixtureProvider()
    ctx = CallContext(job_id="job-1", unit_caps={"fixture": Decimal("2")})
    results = [await gateway.call(provider, Capability.CREATOR_PROFILE, f"UCFIXTURE{i:06d}", ctx=ctx) for i in range(3)]
    assert [r.status for r in results[:2]] == [ResultStatus.OK, ResultStatus.OK]
    assert results[2].status == ResultStatus.BUDGET_DENIED and "credit cap 2" in results[2].error
    assert provider.calls == ["profile", "profile"]  # the third call never reached the provider
    other_job = await gateway.call(provider, Capability.CREATOR_PROFILE, "UCFIXTURE000009",
                                   ctx=CallContext(job_id="job-2", unit_caps={"fixture": Decimal("2")}))
    assert other_job.ok  # the cap is per job


async def configure(pool, *, enabled=True, auto_approve=True, cap=1000, target=5, daily_limit=1):
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE rpi_autopilot_settings SET enabled = $1, auto_approve = $2, weekly_credit_cap = $3,
                   target_queue = $4, presets = ARRAY['none']""", enabled, auto_approve, cap, target)
        await conn.execute("UPDATE rpi_outreach_settings SET daily_send_limit = $1", daily_limit)


@pytest.mark.db
async def test_disabled_autopilot_does_nothing(pool, fake_llm):
    ctx = make_context(pool, llm=fake_llm)
    assert (await run_cycle(pool, ctx))["status"] == "disabled"
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_research_jobs") == 0
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_discovery_runs") == 0


@pytest.mark.db
async def test_full_cycle_researches_drafts_approves_and_schedules(pool, fake_llm):
    await configure(pool)
    ctx = make_context(pool, llm=fake_llm)
    summary = await run_cycle(pool, ctx)
    assert summary["status"] == "ok", summary
    research = summary["research"]
    assert research["status"] == "succeeded" and research["screened"] > 0, research
    assert summary["drafted"] and summary["approved"] == summary["drafted"], summary
    assert len(summary["scheduled"]) == len(summary["approved"])

    async with pool.acquire() as conn:
        drafts = await conn.fetch(
            "SELECT d.*, c.pipeline_status FROM rpi_outreach_drafts d JOIN rpi_creators c ON c.id = d.creator_id")
        cursor = await conn.fetchval("SELECT discovery_cursor->>'none' FROM rpi_autopilot_settings")
        units = await conn.fetchval("SELECT SUM(actual_units) FROM rpi_spend_ledger WHERE status = 'committed'")
    assert all(d["generator"] == "template" and d["created_by"] == AUTOPILOT_ACTOR for d in drafts)
    assert all(d["status"] == "approved" and d["approved_by"] == AUTOPILOT_ACTOR for d in drafts)
    assert all(d["pipeline_status"] == "outreach_approved" for d in drafts)
    sends = sorted(d["scheduled_send_at"].astimezone(ET) for d in drafts)
    assert all(s.strftime("%H:%M") == "11:00" and s.weekday() < 5 for s in sends)
    assert len({s.date() for s in sends}) == len(sends)  # daily limit 1: one per day
    assert cursor == "25"  # the next search continues after the first page
    assert Decimal(research["credits"]) == units

    # Nothing is drafted twice and no research is needed while the queue is full.
    await configure(pool, target=len(drafts))
    again = await run_cycle(pool, ctx)
    assert again["drafted"] == [] and again["scheduled"] == [] and "not needed" in again["research"]
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_research_jobs") == 1


@pytest.mark.db
async def test_weekly_credit_allowance_stops_research(pool, fake_llm):
    await configure(pool, cap=6)
    ctx = make_context(pool, llm=fake_llm)
    summary = await run_cycle(pool, ctx)
    assert summary["research"]["stopped"] == "spending cap reached", summary["research"]
    async with pool.acquire() as conn:
        units = await conn.fetchval("SELECT SUM(actual_units) FROM rpi_spend_ledger WHERE status = 'committed'")
        denied = await conn.fetchval("SELECT COUNT(*) FROM rpi_provider_requests WHERE status = 'budget_denied'")
    # Each stage stops before a creator the allowance cannot pay for in full, so no
    # analysis is left half-paid and the gateway never has to refuse a call.
    assert units <= 6 and denied == 0
    # The allowance is used up: the next cycle does not start research at all.
    again = await run_cycle(pool, ctx)
    assert "paused" in again["research"]
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_research_jobs") == 1
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_alerts WHERE kind = 'autopilot'") == 1


@pytest.mark.db
async def test_without_auto_approve_drafts_wait_for_a_person(pool, fake_llm):
    await configure(pool, auto_approve=False)
    ctx = make_context(pool, llm=fake_llm)
    summary = await run_cycle(pool, ctx)
    assert summary["drafted"] and summary["approved"] == [] and summary["scheduled"] == []
    async with pool.acquire() as conn:
        statuses = {r["status"] for r in await conn.fetch("SELECT status FROM rpi_outreach_drafts")}
        assert statuses == {"draft"}
        # Waiting drafts count toward the queue, so research does not keep running.
        await conn.execute("UPDATE rpi_autopilot_settings SET target_queue = $1", len(summary["drafted"]))
    again = await run_cycle(pool, ctx)
    assert "not needed" in again["research"]


@pytest.mark.db
async def test_manually_approved_drafts_are_scheduled_after_existing_sends(pool):
    from tests.rpi.test_outreach import seed_draft

    await configure(pool, cap=0)
    first, _ = await seed_draft(pool)
    second, _ = await seed_draft(pool, email="lee@example.com")
    async with pool.acquire() as conn:
        await conn.execute("UPDATE rpi_outreach_drafts SET approved_at = NOW() - interval '1 hour' WHERE id = $1", first)
    summary = await run_cycle(pool, make_context(pool))
    assert summary["research"] == "off: weekly credit allowance is 0"
    async with pool.acquire() as conn:
        a, b = [await conn.fetchval("SELECT scheduled_send_at FROM rpi_outreach_drafts WHERE id = $1", d) for d in (first, second)]
    assert a.astimezone(ET).date() < b.astimezone(ET).date()  # oldest approval first, one per day


@pytest.mark.db
async def test_autopilot_api_is_admin_only_and_validates(api):
    body = {"enabled": True, "auto_approve": True, "weekly_credit_cap": 150, "send_time": "11:00"}
    analyst = await api.post(f"{V1}/admin/principals", headers=api.admin, json={"name": "an-analyst", "role": "analyst"})
    denied = await api.put(f"{V1}/autopilot", headers={"X-RPI-Key": analyst.json()["key"]}, json=body)
    assert denied.status_code == 403
    late = await api.put(f"{V1}/autopilot", headers=api.admin, json={**body, "send_time": "20:00"})
    assert late.status_code == 422 and "send window" in late.text
    bad = await api.put(f"{V1}/autopilot", headers=api.admin, json={**body, "presets": ["everything"]})
    assert bad.status_code == 422
    ok = await api.put(f"{V1}/autopilot", headers=api.admin, json=body)
    assert ok.status_code == 200
    status = (await api.get(f"{V1}/autopilot", headers=api.admin)).json()
    assert status["enabled"] and status["auto_approve"] and status["weekly_credit_cap"] == 150
    assert status["credits_left_7d"] == "150" and status["next_search"] == {"preset": "english_based", "page_token": None, "repeat": False}
    async with api.ctx.pool.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_audit_log WHERE action = 'autopilot.settings_changed'") == 1


@pytest.mark.db
async def test_contact_lookup_refused_by_the_cap_is_retried_once_allowance_returns(pool):
    from rpi.administration.jobs import StageSizes, _autopilot, create_job

    class RecordingResearch:
        def __init__(self):
            self.deep_ids: list[str] = []

        async def screen(self, creator_id, ctx, actor):
            raise AssertionError("nothing to screen")

        async def deep(self, creator_id, ctx, actor):
            self.deep_ids.append(str(creator_id))
            return {"qualification": {"recommendation": "recommend"}, "notes": []}

    async with pool.acquire() as conn:
        async def recommended(name, contacts_status):
            cid = await conn.fetchval(
                "INSERT INTO rpi_creators (display_name, pipeline_status) VALUES ($1, 'recommended') RETURNING id", name)
            if contacts_status:
                await conn.execute(
                    """INSERT INTO rpi_provider_requests (provider, operation, request_hash, creator_id, status)
                       VALUES ('creatordb', 'get_creator_contacts', 'h', $1, $2)""", cid, contacts_status)
            return cid
        denied = await recommended("Cap refused the lookup", "budget_denied")
        await recommended("Lookup found no email", "ok")
        job_id = await create_job(conn, "autopilot", {}, AUTOPILOT_ACTOR)
        await conn.execute("UPDATE rpi_autopilot_settings SET enabled = TRUE")

    research = RecordingResearch()
    status, progress = await _autopilot(pool, None, research, CallContext(job_id=job_id), {}, StageSizes(),
                                        AUTOPILOT_ACTOR, job_id, {"screen": 10, "analyze": 5})
    assert status == "succeeded" and progress["discovery"] == {"skipped": "no discovery preset left"}
    assert research.deep_ids == [str(denied)]


def test_stage_credits_cover_every_paid_lookup_of_the_stage():
    from rpi.enrichment.service import ResearchConfig, ResearchService
    from rpi.providers.creatordb import CreatorDBAdapter
    from rpi.providers.registry import ProviderRegistry

    registry = ProviderRegistry({"creatordb": CreatorDBAdapter("test-key")})
    research = ResearchService(None, registry, None, None, ResearchConfig(None, None, True))
    assert research.stage_credits("screen") == {"creatordb": Decimal(3)}  # content detail
    # Profile 2 + sponsorship history 5 + audience countries 10 + contacts 15.
    assert research.stage_credits("deep") == {"creatordb": Decimal(32)}
    research.config.fetch_audience_demographics = False
    assert research.stage_credits("deep") == {"creatordb": Decimal(22)}


@pytest.mark.db
async def test_a_stage_stops_before_a_creator_the_cap_cannot_pay_for_in_full(pool):
    from rpi.administration.jobs import _process, create_job

    class CostedResearch:
        def __init__(self):
            self.deep_ids: list[str] = []

        def stage_credits(self, kind):
            return {"creatordb": Decimal(32)}

        async def deep(self, creator_id, ctx, actor):
            self.deep_ids.append(creator_id)
            async with pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO rpi_spend_ledger (provider, operation, job_id, actual_units, status)
                       VALUES ('creatordb', 'get_audience_demographics', $1, 30, 'committed')""", ctx.job_id)
            return {"qualification": {"recommendation": "recommend"}, "notes": []}

    async with pool.acquire() as conn:
        capped = await create_job(conn, "autopilot", {}, AUTOPILOT_ACTOR)
        manual = await create_job(conn, "deep", {}, "admin:test")
    research = CostedResearch()
    # 70 credits: 32 fit before the first creator and before the second (30 spent), not before the third (60 spent).
    status, progress = await _process(pool, research, "deep", ["a", "b", "c"],
                                      CallContext(job_id=capped, unit_caps={"creatordb": Decimal(70)}), {},
                                      AUTOPILOT_ACTOR, capped)
    assert status == "paused_budget" and research.deep_ids == ["a", "b"] and progress["done"] == ["a", "b"]
    # A job without a credit cap (a person's research request) is not held back.
    research.deep_ids.clear()
    status, _ = await _process(pool, research, "deep", ["a", "b", "c"], CallContext(job_id=manual), {}, "admin:test",
                               manual)
    assert status == "succeeded" and research.deep_ids == ["a", "b", "c"]


@pytest.mark.db
async def test_deep_analysis_a_cap_cut_short_is_repeated_to_buy_the_audience_countries(pool):
    """2026-11-05: Prince Asiamah (0.74 overall, 0.66 niche) was analyzed with 9 credits
    left; the audience lookup was refused and he stayed "investigate" for good."""
    from rpi.administration.jobs import StageSizes, _autopilot, create_job
    from rpi.enrichment.service import ResearchConfig

    class RecordingResearch:
        def __init__(self, fetch_audience=True):
            self.config = ResearchConfig(None, None, True, fetch_audience_demographics=fetch_audience)
            self.deep_ids: list[str] = []

        async def screen(self, creator_id, ctx, actor):
            raise AssertionError("nothing to screen")

        async def deep(self, creator_id, ctx, actor):
            self.deep_ids.append(str(creator_id))
            return {"qualification": {"recommendation": "investigate"}, "notes": []}

    async with pool.acquire() as conn:
        async def analyzed(name, overall, niche, *, audience=None, verdict="unknown", risk="no_flags"):
            cid = await conn.fetchval(
                "INSERT INTO rpi_creators (display_name, pipeline_status) VALUES ($1, 'deep_analysis') RETURNING id", name)
            await conn.execute(
                """INSERT INTO rpi_qualification_scores
                       (creator_id, scoring_version, stage, niche_relevance, overall, risk_level, recommendation, components)
                   VALUES ($1, 'test', 3, $2, $3, $4, 'investigate', $5::jsonb)""",
                cid, niche, overall, risk, json.dumps({"audience_fit": {"geography": {"verdict": verdict}}}))
            if audience:
                await conn.execute(
                    """INSERT INTO rpi_provider_requests (provider, operation, request_hash, creator_id, status)
                       VALUES ('creatordb', 'get_audience_demographics', 'h', $1, $2)""", cid, audience)
            return str(cid)

        refused = await analyzed("Cap refused the audience lookup", 0.74, 0.66, audience="budget_denied")
        at_bar = await analyzed("Exactly at the bar, never looked up", 0.7, 0.6)  # REAL 0.7 is 0.69999999
        await analyzed("Niche below the bar", 0.74, 0.55, audience="budget_denied")
        await analyzed("Lookup found no breakdown", 0.8, 0.79, audience="ok")
        await analyzed("Elevated risk", 0.8, 0.79, audience="budget_denied", risk="elevated_review")
        await analyzed("Geography already failed", 0.8, 0.79, audience="budget_denied", verdict="failed")
        jobs = [await create_job(conn, "autopilot", {}, AUTOPILOT_ACTOR) for _ in range(2)]
        await conn.execute("UPDATE rpi_autopilot_settings SET enabled = TRUE")

    params = {"screen": 10, "analyze": 10}
    research = RecordingResearch()
    status, _ = await _autopilot(pool, None, research, CallContext(job_id=jobs[0]), {}, StageSizes(),
                                 AUTOPILOT_ACTOR, jobs[0], params)
    assert status == "succeeded" and sorted(research.deep_ids) == sorted([refused, at_bar])
    # With audience lookups turned off, repeating the analysis could not change the outcome.
    research = RecordingResearch(fetch_audience=False)
    await _autopilot(pool, None, research, CallContext(job_id=jobs[1]), {}, StageSizes(), AUTOPILOT_ACTOR, jobs[1],
                     params)
    assert research.deep_ids == []


# --------------------------------------------------------------------------
# Review findings
# --------------------------------------------------------------------------


@pytest.mark.db
async def test_failed_research_job_is_canceled_keeps_its_page_and_cannot_be_resumed(api, pool, fake_llm, monkeypatch):
    from rpi.administration import jobs

    await configure(pool)
    ctx = make_context(pool, llm=fake_llm)
    real_funnel = jobs._funnel

    async def funnel_then_crash(*args, **kwargs):
        status, progress = await real_funnel(*args, **kwargs)  # saves progress after every creator
        assert progress["deep"]["done"]
        raise RuntimeError("database went away")

    monkeypatch.setattr(jobs, "_funnel", funnel_then_crash)
    summary = await run_cycle(pool, ctx)
    assert "failed: database went away" in summary["research"]
    async with pool.acquire() as conn:
        job = await conn.fetchrow("SELECT id, status, progress::text FROM rpi_research_jobs")
        cursor = await conn.fetchval("SELECT discovery_cursor->>'none' FROM rpi_autopilot_settings")
    assert job["status"] == "canceled"
    # Per-creator saves keep the job's other stages (2026-10-26 a deep-stage save overwrote the discovery
    # progress, so a failure lost the page it had searched).
    assert '"discovery"' in job["progress"] and '"screen"' in job["progress"]
    assert cursor == "25"  # the page it searched before failing is not bought again
    resumed = await api.post(f"{V1}/jobs/{job['id']}/resume?inline=true", headers=api.admin)
    assert "only inside an autopilot cycle" in resumed.text
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT status FROM rpi_research_jobs WHERE id = $1", job["id"]) == "canceled"


@pytest.mark.db
async def test_drafts_edited_by_a_person_are_left_for_an_approver(pool, fake_llm):
    await configure(pool, auto_approve=False)
    ctx = make_context(pool, llm=fake_llm)
    first = await run_cycle(pool, ctx)
    assert len(first["drafted"]) >= 2
    async with pool.acquire() as conn:
        edited = await conn.fetchval("SELECT id FROM rpi_outreach_drafts ORDER BY created_at LIMIT 1")
        await conn.execute("UPDATE rpi_outreach_drafts SET body_text = body_text || ' Edited.' WHERE id = $1", edited)
        await conn.execute("UPDATE rpi_autopilot_settings SET auto_approve = TRUE, weekly_credit_cap = 0")
    second = await run_cycle(pool, ctx)
    assert len(second["approved"]) == len(first["drafted"]) - 1
    assert any("edited by a person" in note for note in second["skipped"])
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT status FROM rpi_outreach_drafts WHERE id = $1", edited) == "draft"


@pytest.mark.db
async def test_drafts_never_use_a_contact_known_to_be_invalid(pool, fake_llm):
    await configure(pool, auto_approve=False)
    ctx = make_context(pool, llm=fake_llm)
    await run_cycle(pool, ctx)
    async with pool.acquire() as conn:
        creator_id = await conn.fetchval("SELECT creator_id FROM rpi_outreach_drafts ORDER BY created_at LIMIT 1")
        await conn.execute("DELETE FROM rpi_outreach_drafts WHERE creator_id = $1", creator_id)
        bad = await conn.fetchval(
            """INSERT INTO rpi_creator_contacts (creator_id, contact_type, value, value_normalized, source,
                   is_business_contact, verification_status, created_at)
               VALUES ($1, 'email', 'bounced@example.com', 'bounced@example.com', 'creatordb', TRUE, 'invalid',
                       NOW() - interval '1 year') RETURNING id""", creator_id)
        await conn.execute("UPDATE rpi_autopilot_settings SET weekly_credit_cap = 0")
    await run_cycle(pool, ctx)
    async with pool.acquire() as conn:
        contact = await conn.fetchval("SELECT contact_id FROM rpi_outreach_drafts WHERE creator_id = $1", creator_id)
    assert contact is not None and contact != bad


@pytest.mark.db
async def test_held_drafts_get_the_next_free_day_and_sent_mail_counts_on_its_send_day(pool):
    from tests.rpi.test_outreach import seed_draft

    await configure(pool, cap=0)
    held, _ = await seed_draft(pool)                                # due last Friday, held back
    new, _ = await seed_draft(pool, email="new@example.com")        # approved, not scheduled
    sent, _ = await seed_draft(pool, status="sent", email="sent@example.com")
    friday = datetime(2026, 10, 30, 15, 0, tzinfo=timezone.utc)     # Fri 11:00 EDT
    tuesday = datetime(2026, 11, 3, 16, 0, tzinfo=timezone.utc)     # Tue 11:00 EST
    async with pool.acquire() as conn:
        await conn.execute("UPDATE rpi_outreach_drafts SET scheduled_send_at = $2 WHERE id = $1", held, friday)
        # Scheduled for Tuesday but sent by hand last Friday: it fills Friday, not Tuesday.
        await conn.execute("UPDATE rpi_outreach_drafts SET scheduled_send_at = $2, sent_at = $3 WHERE id = $1",
                           sent, tuesday, friday)
    monday_0730 = datetime(2026, 11, 2, 12, 30, tzinfo=timezone.utc)
    summary = await run_cycle(pool, make_context(pool), now=monday_0730)
    async with pool.acquire() as conn:
        when = {d: (await conn.fetchval("SELECT scheduled_send_at FROM rpi_outreach_drafts WHERE id = $1", d)).astimezone(ET)
                for d in (held, new)}
    assert when[held].strftime("%a %H:%M") == "Mon 11:00" and when[new].strftime("%a %H:%M") == "Tue 11:00"
    assert any("held_since" in s for s in summary["scheduled"])


@pytest.mark.db
async def test_turning_autopilot_off_stops_its_sends(api, pool, fake_llm):
    from rpi.outreach.sender import send_due_drafts
    from tests.rpi.conftest import FakeSender
    from tests.rpi.test_outreach import enable_sending, seed_draft

    await configure(pool)
    summary = await run_cycle(pool, make_context(pool, llm=fake_llm))
    assert summary["scheduled"]
    body = {"enabled": False, "auto_approve": True, "weekly_credit_cap": 1000}
    off = await api.put(f"{V1}/autopilot", headers=api.admin, json=body)
    assert off.json()["unscheduled_autopilot_drafts"] == len(summary["scheduled"])

    # Even a due autopilot draft is skipped while it is off; a person's approval still sends.
    human, _ = await seed_draft(pool, email="human@example.com")
    async with pool.acquire() as conn:
        auto = await conn.fetchval("SELECT id FROM rpi_outreach_drafts WHERE approved_by = $1 LIMIT 1", AUTOPILOT_ACTOR)
        await conn.execute("UPDATE rpi_outreach_drafts SET scheduled_send_at = NOW() - interval '1 minute' "
                           "WHERE id = ANY($1::uuid[])", [auto, human])
    await enable_sending(pool, limit=10)
    sender = FakeSender()
    result = await send_due_drafts(pool, sender, public_base_url="https://rpi.test", window_open=True)
    assert result["sent"] == [str(human)] and len(sender.sent) == 1


@pytest.mark.db
async def test_english_based_migration_replaces_the_old_presets_once(pool):
    from pathlib import Path

    migrations = Path(__file__).resolve().parents[2] / "database" / "migrations"
    up = [(migrations / f).read_text() for f in ("008_rpi_outside_range_presets.sql", "011_rpi_english_based_presets.sql")]
    down = (migrations / "011_rpi_english_based_presets_down.sql").read_text()
    presets = "SELECT presets FROM rpi_autopilot_settings"
    english_based = ["english_based", "english_based_small", "english_based_large"]
    async with pool.acquire() as conn:
        await conn.execute((migrations / "013_rpi_wider_presets_down.sql").read_text())
        assert await conn.fetchval(presets) == english_based  # the column default before 013
        # Production before 011: the four presets that check only the largest audience country.
        await conn.execute(down)
        await conn.execute("UPDATE rpi_autopilot_settings SET presets = presets || ARRAY['none']")
        assert await conn.fetchval(presets) == [
            "quality", "quality_broad", "outside_range_small", "outside_range_large", "none"]
        for sql in up:
            await conn.execute(sql)
        assert await conn.fetchval(presets) == english_based + ["none"]
        await conn.execute("UPDATE rpi_autopilot_settings SET presets = ARRAY['quality']")  # an admin's choice
        for sql in up:
            await conn.execute(sql)  # migrations re-run on every deploy
        assert await conn.fetchval(presets) == ["quality"]
        await conn.execute((migrations / "013_rpi_wider_presets.sql").read_text())  # restore the column default


@pytest.mark.db
async def test_wider_presets_migration_appends_them_once(pool):
    from pathlib import Path

    migrations = Path(__file__).resolve().parents[2] / "database" / "migrations"
    up = (migrations / "013_rpi_wider_presets.sql").read_text()
    down = (migrations / "013_rpi_wider_presets_down.sql").read_text()
    presets = "SELECT presets FROM rpi_autopilot_settings"
    english_based = ["english_based", "english_based_small", "english_based_large"]
    wider = ["english_based_broad", "english_based_topics"]
    async with pool.acquire() as conn:
        assert await conn.fetchval(presets) == english_based + wider  # the column default
        # Production before 013, with an admin's extra preset after the english_based ones and
        # cursors written before 0.2.17, which do not say when a preset finished.
        await conn.execute(down)
        await conn.execute("UPDATE rpi_autopilot_settings SET presets = presets || ARRAY['quality'], "
                           """discovery_cursor = '{"english_based": "end", "english_based_small": "25"}'""")
        await conn.execute(up)
        assert await conn.fetchval(presets) == english_based + ["quality"] + wider
        today = await conn.fetchval("SELECT CURRENT_DATE::text")
        cursor = "SELECT discovery_cursor::text FROM rpi_autopilot_settings"
        assert json.loads(await conn.fetchval(cursor)) == {"english_based": f"end:{today}", "english_based_small": "25"}
        await conn.execute("UPDATE rpi_autopilot_settings SET discovery_cursor = '{}'")
        await conn.execute(up)
        assert await conn.fetchval(cursor) == "{}"
        await conn.execute("UPDATE rpi_autopilot_settings SET presets = ARRAY['english_based']")  # an admin's choice
        await conn.execute(up)  # migrations re-run on every deploy
        assert await conn.fetchval(presets) == ["english_based"]
        await conn.execute("UPDATE rpi_autopilot_settings SET presets = ARRAY['english_based_topics']")
        await conn.execute(down)
        assert await conn.fetchval(presets) == english_based  # never left without a preset
        assert await conn.fetchval(
            "SELECT COUNT(*) FROM rpi_schema_migrations WHERE version = '013_rpi_wider_presets'") == 0
        await conn.execute(up)  # restore the column default for later tests


@pytest.mark.db
async def test_finished_preset_is_searched_again_after_thirty_days(pool, fake_llm):
    from datetime import timedelta

    from rpi.administration.autopilot import status

    await configure(pool, target=100)
    ctx = make_context(pool, llm=fake_llm)
    cursor = "SELECT discovery_cursor->>'none' FROM rpi_autopilot_settings"
    async with pool.acquire() as conn:
        # The fixture search has 60 creators: the page from #51 is its last.
        await conn.execute("""UPDATE rpi_autopilot_settings SET discovery_cursor = '{"none": "50"}'""")
    nov18 = datetime(2026, 11, 18, 12, 30, tzinfo=timezone.utc)
    first = await run_cycle(pool, ctx, now=nov18)
    assert first["research"]["discovery"]["total_found"] > 0, first["research"]
    async with pool.acquire() as conn:
        assert await conn.fetchval(cursor) == "end:2026-11-18"
        alert = await conn.fetchval("SELECT message FROM rpi_alerts WHERE dedupe_key = 'autopilot:exhausted:2026-11-18'")
        shown = await status(conn, ctx, now=nov18)
    assert "again 30 days after its last page, from 2026-12-18" in alert
    assert shown["next_search"] is None and shown["next_repeat_search_on"] == "2026-12-18"

    waiting = await run_cycle(pool, ctx, now=nov18 + timedelta(days=29))
    assert waiting["research"]["discovery"] == {"skipped": "no discovery preset left"}

    again = await run_cycle(pool, ctx, now=nov18 + timedelta(days=30))
    research = again["research"]
    assert research["preset"] == "none" and research["page_token"] is None and research["repeat_search"], research
    assert research["discovery"]["total_found"] > 0
    async with pool.acquire() as conn:
        assert await conn.fetchval(cursor) == "25"  # continues page by page from there
        shown = await status(conn, ctx, now=nov18 + timedelta(days=30))
    assert shown["next_search"] == {"preset": "none", "page_token": "25", "repeat": False}
    assert shown["next_repeat_search_on"] is None


@pytest.mark.db
async def test_one_creator_failing_does_not_stop_research(pool, fake_llm):
    await configure(pool)
    ctx = make_context(pool, llm=fake_llm)
    real_screen = ctx.research.screen
    broken: list[str] = []

    async def flaky_screen(creator_id, call_ctx, actor):
        if not broken:
            broken.append(str(creator_id))
            raise RuntimeError("unreadable model response")
        return await real_screen(creator_id, call_ctx, actor)

    ctx.research.screen = flaky_screen
    summary = await run_cycle(pool, ctx)
    research = summary["research"]
    assert research["status"] == "succeeded" and research["screened"] > 1, research
    assert research["failed"] == [f"{broken[0]}: failed: unreadable model response"]
    assert summary["drafted"]  # drafting and scheduling still ran after research
    async with pool.acquire() as conn:
        alert = await conn.fetchval("SELECT message FROM rpi_alerts WHERE dedupe_key LIKE 'autopilot:creator_failed:%'")
    assert alert and "skipped 1 creator" in alert
