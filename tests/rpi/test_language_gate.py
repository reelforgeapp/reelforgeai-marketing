"""Spoken-language gate: non-English channels are dropped before any CreatorDB credit is spent on them."""

from decimal import Decimal

import httpx
import pytest
import respx

from rpi.core.provenance import CommercialUse, MeasurementType, Provenance, utcnow
from rpi.providers.base import (Capability, CostActual, CostEstimate, ProviderAdapter, ProviderResult, ResultStatus,
                                VideoLanguages)
from rpi.providers.youtube_data import YouTubeDataAdapter
from rpi.qualification.language_gate import judge

V1 = "/rpi/api/v1"
YT = "https://www.googleapis.com/youtube/v3"


def test_judge_uses_the_declared_spoken_language():
    hindi = judge(["hi"] * 10)  # Hindi AI Gyaan, 2026-10-26
    assert hindi.verdict == "non_english" and hindi.rejected and hindi.declared == {"hi": 10}
    assert "hi" not in hindi.detail and "Hindi" not in hindi.detail  # the stored text names no language
    assert judge(["en-US"] * 10).verdict == "english"  # regional tags count as English
    assert judge(["en", "en"] + [None] * 8).verdict == "unknown"  # too few declared: passes unchecked
    assert judge([None] * 10).verdict == "unknown"
    assert judge(["en", "hi", "en", "hi"]).verdict == "english"  # half English is enough
    assert judge(["en", "hi", "hi", "tr"]).verdict == "non_english"
    assert judge(["zxx", "zxx", "zxx", "en", "en", "en"]).verdict == "english"  # music-only videos say nothing


@respx.mock
async def test_youtube_adapter_reads_the_newest_uploads_audio_language():
    playlist = respx.get(f"{YT}/playlistItems").mock(return_value=httpx.Response(200, json={"items": [
        {"contentDetails": {"videoId": v}} for v in ("v3", "v2", "v1")]}))
    videos = respx.get(f"{YT}/videos").mock(return_value=httpx.Response(200, json={"items": [
        {"id": "v1", "snippet": {"defaultAudioLanguage": "en"}},
        {"id": "v3", "snippet": {"defaultAudioLanguage": "hi"}},
        {"id": "v2", "snippet": {}}]}))
    yt = YouTubeDataAdapter("yt-test-key")
    result = await yt.get_video_languages("UC7SwCtUnwLSoTfb-JgMZSOQ")
    assert result.status == ResultStatus.OK and result.data.languages == ["hi", None, "en"]  # playlist order
    assert playlist.calls.last.request.url.params["playlistId"] == "UU7SwCtUnwLSoTfb-JgMZSOQ"
    assert videos.calls.last.request.url.params["id"] == "v3,v2,v1"
    assert result.cost.units == Decimal("2") and result.data.provenance.derived_metrics_prohibited
    assert yt.estimate_cost(Capability.VIDEO_LANGUAGES).units == Decimal("2")


@respx.mock
async def test_youtube_adapter_missing_playlist_is_not_found():
    respx.get(f"{YT}/playlistItems").mock(return_value=httpx.Response(404, json={"error": {"message": "not found"}}))
    result = await YouTubeDataAdapter("yt-test-key").get_video_languages("UCgone")
    assert result.status == ResultStatus.NOT_FOUND
    assert (await YouTubeDataAdapter("k").get_video_languages("@handle")).status == ResultStatus.NOT_FOUND


class FakeYouTube(ProviderAdapter):
    """Stands in for the YouTube Data API: every channel speaks English except `hindi`."""

    name = "youtube_data_api"
    policy_key = "youtube_data_api"
    capabilities = frozenset({Capability.VIDEO_LANGUAGES})

    def __init__(self, hindi: set[str]):
        self.hindi = hindi
        self.calls: list[str] = []

    def is_configured(self) -> bool:
        return True

    def estimate_cost(self, operation, **params):
        return CostEstimate(self.name, operation.value, Decimal("2"), Decimal("0"), basis="quota_units")

    async def get_video_languages(self, platform_account_id: str, limit: int = 10):
        self.calls.append(platform_account_id)
        lang = "hi" if platform_account_id in self.hindi else "en-US"
        prov = Provenance(provider=self.name, operation="get_video_languages", source_record=platform_account_id,
                          retrieved_at=utcnow(), data_type="video_languages", measurement=MeasurementType.OBSERVED,
                          retention_policy="youtube_data_api:30d", commercial_use=CommercialUse.INTERNAL_DISPLAY_ONLY,
                          derived_metrics_prohibited=True)
        return ProviderResult(self.name, "get_video_languages", ResultStatus.OK,
                              VideoLanguages(platform_account_id=platform_account_id, languages=[lang] * 10,
                                             provenance=prov), CostActual(Decimal("2")))


async def discover(api, fake=None):
    """A fixture discovery run, with the language check when `fake` is given. (Verdicts are
    cached for 30 days like any YouTube result, so tests check each channel once.)"""
    if fake:
        api.ctx.registry.adapters["youtube_data_api"] = fake
    run = await api.post(f"{V1}/discovery/runs", headers=api.admin, json={
        "preset": "none", "strategies": ["niche"], "max_candidates": 12, "inline": True,
        "criteria": {"min_subscribers": 1000, "max_subscribers": 1000000}})
    assert run.status_code == 200, run.text
    return run.json()


@pytest.mark.db
async def test_discovery_drops_non_english_channels_before_screening(api, pool):
    fake = FakeYouTube(hindi={"UCFIXTURE000001", "UCFIXTURE000002"})
    body = await discover(api, fake)
    assert body["progress"]["summary"]["non_english"] == 2
    async with pool.acquire() as conn:
        warnings = await conn.fetchval("SELECT warnings::text FROM rpi_discovery_runs")
        assert "spoken-language" not in warnings  # every channel was checked
        rows = await conn.fetch(
            """SELECT a.platform_account_id, c.disqualified_reason, d.preliminary_qualified, d.inclusion_reasons
               FROM rpi_creators c JOIN rpi_creator_social_accounts a ON a.creator_id = c.id
               JOIN rpi_discovery_candidates d ON d.creator_id = c.id""")
    by_channel = {r["platform_account_id"]: r for r in rows}
    for cid in fake.hindi:
        assert by_channel[cid]["disqualified_reason"] == "non_english_audio"
        assert not by_channel[cid]["preliminary_qualified"]
        assert "spoken language is not English" in by_channel[cid]["inclusion_reasons"]
    english = [r for c, r in by_channel.items() if c not in fake.hindi]
    assert english and all(r["disqualified_reason"] is None for r in english)
    assert any(r["preliminary_qualified"] for r in english)


@pytest.mark.db
async def test_screening_checks_language_before_paying_for_content(api, pool):
    await discover(api)  # no language check at discovery
    async with pool.acquire() as conn:
        cid = await conn.fetchval(
            """SELECT c.id FROM rpi_creators c JOIN rpi_creator_social_accounts a ON a.creator_id = c.id
               WHERE a.platform_account_id = 'UCFIXTURE000003'""")
    api.ctx.registry.adapters["youtube_data_api"] = FakeYouTube(hindi={"UCFIXTURE000003"})
    fixture = api.ctx.registry.get("fixture")
    fixture.calls.clear()
    job = await api.post(f"{V1}/research", headers=api.admin, json={"creator_ids": [str(cid)], "stage": "screen",
                                                                     "inline": True})
    assert job.status_code == 200, job.text
    assert job.json()["progress"]["results"] == {str(cid): "reject_non_english"}
    assert "content" not in fixture.calls  # no CreatorDB call was made for this creator
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT disqualified_reason FROM rpi_creators WHERE id = $1", cid) == "non_english_audio"
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_qualification_scores WHERE creator_id = $1", cid) == 0


@pytest.mark.db
async def test_language_check_endpoint_reports_and_disqualifies(api, pool):
    await discover(api)
    async with pool.acquire() as conn:
        ids = {r["platform_account_id"]: str(r["creator_id"]) for r in await conn.fetch(
            "SELECT platform_account_id, creator_id FROM rpi_creator_social_accounts WHERE platform_account_id IN "
            "('UCFIXTURE000004', 'UCFIXTURE000005')")}
    fake = FakeYouTube(hindi={"UCFIXTURE000004"})
    api.ctx.registry.adapters["youtube_data_api"] = fake
    res = await api.post(f"{V1}/research/language-check", headers=api.admin, json={"creator_ids": list(ids.values())})
    assert res.status_code == 200, res.text
    verdicts = {r["creator_id"]: r for r in res.json()}
    assert verdicts[ids["UCFIXTURE000004"]]["verdict"] == "non_english"
    assert verdicts[ids["UCFIXTURE000004"]]["declared_languages"] == {"hi": 10}  # in the response, not stored
    assert verdicts[ids["UCFIXTURE000005"]]["verdict"] == "english"
    async with pool.acquire() as conn:
        reasons = dict(await conn.fetch("SELECT id::text, disqualified_reason FROM rpi_creators WHERE id::text = ANY($1)",
                                        list(ids.values())))
    assert reasons == {ids["UCFIXTURE000004"]: "non_english_audio", ids["UCFIXTURE000005"]: None}


@pytest.mark.db
async def test_autopilot_does_not_draft_for_disqualified_creators(pool, fake_llm):
    from datetime import datetime, timezone

    from rpi.administration.autopilot import _draft_new
    from tests.rpi.conftest import make_context

    async with pool.acquire() as conn:
        async def recommended(name, reason):
            cid = await conn.fetchval(
                "INSERT INTO rpi_creators (display_name, pipeline_status, disqualified_reason) "
                "VALUES ($1, 'recommended', $2) RETURNING id", name, reason)
            await conn.execute(
                """INSERT INTO rpi_qualification_scores (creator_id, scoring_version, stage, recommendation)
                   VALUES ($1, 'test', 4, 'recommend')""", cid)
            await conn.execute(
                """INSERT INTO rpi_creator_contacts (creator_id, contact_type, value, value_normalized, source)
                   VALUES ($1, 'email', $2, $2, 'operator')""", cid, f"{name.lower()}@example.com")
        await recommended("Speaks Hindi", "non_english_audio")
        await recommended("Speaks English", None)
        summary = {"drafted": [], "skipped": []}
        await _draft_new(conn, make_context(pool, llm=fake_llm), datetime.now(timezone.utc), summary)
    # The English creator is tried (and refused here for lack of research); the other is never considered.
    assert [s.split(":")[0] for s in summary["skipped"]] == ["Speaks English"]


@pytest.mark.db
async def test_funnel_skips_disqualified_creators_waiting_for_deep_analysis(pool):
    from rpi.administration.jobs import StageSizes, _funnel, create_job
    from rpi.providers.gateway import CallContext

    class RecordingResearch:
        def __init__(self):
            self.deep_ids: list[str] = []

        async def screen(self, creator_id, ctx, actor):
            raise AssertionError("nothing to screen")

        async def deep(self, creator_id, ctx, actor):
            self.deep_ids.append(str(creator_id))
            return {"qualification": {"recommendation": "recommend"}, "notes": []}

    async with pool.acquire() as conn:
        async def qualified(name, reason):
            cid = await conn.fetchval(
                "INSERT INTO rpi_creators (display_name, pipeline_status, disqualified_reason) "
                "VALUES ($1, 'preliminary_qualified', $2) RETURNING id", name, reason)
            await conn.execute("""INSERT INTO rpi_qualification_scores (creator_id, scoring_version, stage, overall,
                                      recommendation) VALUES ($1, 'test', 2, 0.9, 'investigate')""", cid)
            return cid
        await qualified("Speaks Turkish", "non_english_audio")
        english = await qualified("Speaks English", None)
        job_id = await create_job(conn, "funnel", {}, "test")
    research = RecordingResearch()
    status, _ = await _funnel(pool, research, CallContext(job_id=job_id), {}, StageSizes(), "test", job_id,
                              {"screen": 10, "analyze": 5})
    assert status == "succeeded" and research.deep_ids == [str(english)]
