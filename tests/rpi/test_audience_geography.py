"""Audience geography gate: at least half the audience in English-speaking countries before a recommendation."""

from datetime import datetime, timedelta, timezone

import pytest

from rpi.administration.autopilot import AUTOPILOT_ACTOR, run_cycle
from rpi.audience_intelligence.fit import AudienceEvidence, english_speaking_share
from rpi.core.provenance import MeasurementType
from rpi.outreach.sender import send_draft
from rpi.qualification.scoring import MIN_ENGLISH_AUDIENCE_SHARE, QualificationInput, Sourced, score

NOW = datetime(2026, 10, 30, tzinfo=timezone.utc)
V1 = "/rpi/api/v1"


def strong(**kw) -> QualificationInput:
    defaults = dict(
        niche_relevance=Sourced(0.95, "creatordb", "content_inferred"), classification_confidence=0.9,
        best_icp_fit=Sourced(0.8, None, "mixed"), icp_completeness=0.8,
        last_upload_at=NOW - timedelta(days=3), uploads_last_90d=12, activity_provider="creatordb",
        subscribers=50_000, subscribers_provider="creatordb", relevant_sponsorships=1,
        expected_fee_usd=None, risk_level="no_flags", scoring_allowed={"creatordb": True},
    )
    defaults.update(kw)
    return QualificationInput(**defaults)


def share(value: float) -> Sourced:
    return Sourced(value, "creatordb", "third_party_modeled")


def test_english_speaking_share_counts_six_countries_from_the_best_source():
    modeled = AudienceEvidence("country", {"US": 0.30, "UK": 0.10, "CA": 0.05, "AU": 0.02, "NZ": 0.01, "IE": 0.02,
                                           "IN": 0.40, "DE": 0.10}, MeasurementType.THIRD_PARTY_MODELED, "creatordb")
    submitted = AudienceEvidence("country", {"US": 0.90}, MeasurementType.MANUAL_SUBMITTED, "manual")
    geo = english_speaking_share([submitted, modeled])
    assert round(geo.share, 3) == 0.5 and geo.provider == "creatordb"  # modeled beats creator-sent; UK counts as GB
    assert english_speaking_share([AudienceEvidence("age", {"25-34": 0.4}, MeasurementType.THIRD_PARTY_MODELED,
                                                    "creatordb")]) is None


def test_audience_outside_english_countries_rejects_an_otherwise_strong_creator():
    r = score(strong(english_audience=share(0.33)), now=NOW)
    assert r.recommendation == "reject_audience_geography"
    assert any(reason.startswith("33% of the audience is in English-speaking countries") for reason in r.reasons)
    assert r.components["audience_fit"]["geography"]["verdict"] == "failed"
    # Known shares apply at screening too, before deep analysis would require them.
    assert score(strong(english_audience=share(0.2), require_english_audience=False), now=NOW).recommendation \
        == "reject_audience_geography"


def test_unknown_geography_holds_a_recommendation_from_deep_analysis_on():
    held = score(strong(require_english_audience=True), now=NOW)
    assert held.recommendation == "investigate" and held.awaiting_geography
    assert "audience geography (a recommendation needs it)" in held.missing_data
    screened = score(strong(), now=NOW)  # stage 2: no country data is bought yet
    assert screened.recommendation == "recommend" and not screened.awaiting_geography
    weak = score(strong(niche_relevance=Sourced(0.5, "creatordb", "content_inferred"), require_english_audience=True),
                 now=NOW)
    assert not weak.awaiting_geography  # country data could not make it a recommendation


def test_majority_english_audience_is_recommended():
    r = score(strong(english_audience=share(MIN_ENGLISH_AUDIENCE_SHARE), require_english_audience=True), now=NOW)
    assert r.recommendation == "recommend"
    assert r.components["audience_fit"]["geography"] == {
        "english_speaking_share": 0.5, "required": 0.5, "verdict": "passed", "provider": "creatordb",
        "basis": "third_party_modeled"}


def test_policy_blocked_geography_is_excluded_not_used():
    r = score(strong(english_audience=share(0.1), require_english_audience=True, scoring_allowed={"creatordb": False}),
              now=NOW)
    assert r.recommendation != "reject_audience_geography"
    assert any("audience geography from creatordb" in e for e in r.excluded_sources)


async def screened_creators(api) -> dict:
    admin = api.admin
    run = await api.post(f"{V1}/discovery/runs", headers=admin, json={
        "preset": "none", "strategies": ["niche"], "max_candidates": 30, "inline": True,
        "criteria": {"min_subscribers": 1000, "max_subscribers": 1000000}})
    assert run.status_code == 200, run.text
    ids = [c["id"] for c in (await api.get(f"{V1}/creators", headers=admin)).json()]
    screen = await api.post(f"{V1}/research", headers=admin, json={"creator_ids": ids, "stage": "screen", "inline": True})
    return screen.json()["progress"]["results"]


@pytest.mark.db
async def test_deep_analysis_buys_country_data_only_for_creators_it_would_recommend(api, pool):
    screened = await screened_creators(api)
    ids = [cid for cid, rec in screened.items() if rec in ("recommend", "investigate")]
    deep = await api.post(f"{V1}/research", headers=api.admin, json={"creator_ids": ids, "stage": "deep", "inline": True})
    results = deep.json()["progress"]["results"]
    async with pool.acquire() as conn:
        bought = {str(r["creator_id"]) for r in await conn.fetch(
            "SELECT creator_id FROM rpi_provider_requests WHERE operation = 'get_audience_demographics'")}
        disqualified = {str(r["id"]): r["disqualified_reason"] for r in await conn.fetch(
            "SELECT id, disqualified_reason FROM rpi_creators WHERE disqualified_reason IS NOT NULL")}
    # The fixture gives India-based creators a mostly Indian audience and the rest a mostly US/UK one.
    assert "recommend" in results.values() and "reject_audience_geography" in results.values(), results
    assert bought == {cid for cid, rec in results.items() if rec in ("recommend", "reject_audience_geography")}
    assert all(disqualified.get(cid) == "audience_outside_english_countries"
               for cid, rec in results.items() if rec == "reject_audience_geography")
    rejected = next(cid for cid, rec in results.items() if rec == "reject_audience_geography")
    profile = (await api.get(f"{V1}/creators/{rejected}", headers=api.admin)).json()
    assert profile["qualification"]["components"]["audience_fit"]["geography"]["verdict"] == "failed"


@pytest.mark.db
async def test_disqualifying_a_scheduled_creator_cancels_the_autopilot_send(api, pool):
    async with pool.acquire() as conn:
        await conn.execute("""UPDATE rpi_autopilot_settings SET enabled = TRUE, auto_approve = TRUE, weekly_credit_cap = 1000,
                                  target_queue = 5, presets = ARRAY['none']""")
    summary = await run_cycle(pool, api.ctx)
    assert summary["scheduled"], summary
    async with pool.acquire() as conn:
        draft = await conn.fetchrow(
            "SELECT id, creator_id FROM rpi_outreach_drafts WHERE status = 'approved' AND approved_by = $1 "
            "AND scheduled_send_at IS NOT NULL LIMIT 1", AUTOPILOT_ACTOR)
        # New country data: most of this creator's audience is outside English-speaking countries.
        await conn.execute("UPDATE rpi_audience_estimates SET share = CASE WHEN bucket = 'IN' THEN 0.8 ELSE 0.05 END "
                           "WHERE creator_id = $1 AND dimension = 'country'", draft["creator_id"])
    cid = str(draft["creator_id"])
    deep = await api.post(f"{V1}/research", headers=api.admin, json={"creator_ids": [cid], "stage": "deep", "inline": True})
    assert deep.json()["progress"]["results"] == {cid: "reject_audience_geography"}
    async with pool.acquire() as conn:
        held = await conn.fetchrow("SELECT status, scheduled_send_at, failure_reason FROM rpi_outreach_drafts WHERE id = $1",
                                   draft["id"])
    assert held["status"] == "approved" and held["scheduled_send_at"] is None
    assert held["failure_reason"] == "creator disqualified: audience_outside_english_countries"

    # Backstop at send time, and the autopilot does not give it a new day.
    result = await send_draft(pool, str(draft["id"]), "tester", api.ctx.send_fn, public_base_url="https://rpi.test")
    assert "creator disqualified (audience_outside_english_countries) after the autopilot approved this draft" \
        in result["reasons"]
    async with pool.acquire() as conn:
        await conn.execute("UPDATE rpi_outreach_drafts SET failure_reason = NULL WHERE id = $1", draft["id"])
    again = await run_cycle(pool, api.ctx)
    async with pool.acquire() as conn:
        name = await conn.fetchval("SELECT display_name FROM rpi_creators WHERE id = $1", draft["creator_id"])
        assert await conn.fetchval("SELECT scheduled_send_at FROM rpi_outreach_drafts WHERE id = $1", draft["id"]) is None
    assert name not in {s["creator"] for s in again["scheduled"]}
