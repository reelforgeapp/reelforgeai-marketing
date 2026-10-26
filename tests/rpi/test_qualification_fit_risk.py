"""Audience fit (no imputation), risk screening wording, qualification policy gating."""

from datetime import datetime, timedelta, timezone

import pytest

from rpi.audience_intelligence.fit import DEFAULT_ICPS, AudienceEvidence, score_icp
from rpi.audience_intelligence.quality import SCREENING_LABEL, SnapshotStat, VideoStats, screen
from rpi.core.provenance import MeasurementType
from rpi.qualification.scoring import QualificationInput, Sourced, score, size_score

NOW = datetime(2026, 10, 25, tzinfo=timezone.utc)
ICP_A = DEFAULT_ICPS[0]


def test_missing_audience_data_is_excluded_not_imputed():
    fit = score_icp(ICP_A, video_labels=[["faceless_youtube_automation"]] * 4, audience_problems=[],
                    audience=[], creator_language=None)
    comps = {c.name: c for c in fit.components}
    assert comps["geography"].score is None and comps["geography"].basis == "unavailable"
    assert comps["age"].score is None
    assert fit.fit == 1.0  # only the content component is available
    assert fit.completeness < 0.5
    assert "audience geography" in fit.missing


def test_measured_source_preferred_over_modeled():
    modeled = AudienceEvidence("country", {"US": 0.2}, MeasurementType.THIRD_PARTY_MODELED, "creatordb")
    measured = AudienceEvidence("country", {"US": 0.7}, MeasurementType.FIRST_PARTY_MEASURED, "youtube_analytics_api")
    fit = score_icp(ICP_A, video_labels=[], audience_problems=[], audience=[modeled, measured], creator_language="en")
    geo = next(c for c in fit.components if c.name == "geography")
    assert geo.score == 0.7 and geo.basis == "first_party_measured"


def test_risk_screen_is_labeled_and_flags_anomalies():
    vids = [VideoStats(NOW - timedelta(days=100 + i * 7), v, int(v * 0.03), 5)
            for i, v in enumerate([1000, 1200, 900, 1100, 950, 80000])]
    snaps = [SnapshotStat(NOW - timedelta(days=d), s) for d, s in [(40, 10000), (30, 10100), (20, 10200), (10, 30000)]]
    result = screen(vids, snaps, subscribers=30000, now=NOW)
    types = {s["signal_type"] for s in result["signals"]}
    assert {"view_concentration", "inactive", "growth_spike"} <= types
    assert result["label"] == SCREENING_LABEL
    assert "fraud" not in str(result["signals"]).lower()


# @BeerMoneyForum, 2026-10-26: ~2,500 likes and ~290 comments on every video whatever its views.
BOUGHT = [(33_823, 2_471, 292), (63_480, 2_428, 292), (49_238, 2_549, 287), (112_593, 2_511, 302),
          (59_198, 2_517, 301), (59_018, 2_491, 282), (160_857, 2_575, 297)]


def videos(rows, *, age_days=10, short=None):
    return [VideoStats(NOW - timedelta(days=age_days + i), v, l, c, short) for i, (v, l, c) in enumerate(rows)]


def test_flat_engagement_is_elevated():
    result = screen(videos(BOUGHT), [], subscribers=504_000, now=NOW)
    flat = next(s for s in result["signals"] if s["signal_type"] == "flat_engagement")
    assert flat["severity"] == "elevated" and flat["value"] < 0.1
    assert "2,428-2,575" in flat["detail"] and "7 videos" in flat["detail"]
    assert result["level"] == "elevated_review"  # one elevated signal is enough
    # The checks that existed before all miss it.
    assert [s["signal_type"] for s in result["signals"]] == ["flat_engagement"]


def test_engagement_that_scales_with_views_is_not_flagged():
    # Like rates wobble between 2% and 5% but likes still follow views.
    rows = [(v, int(v * r), int(v * r / 10)) for v, r in
            [(4_000, 0.05), (9_000, 0.02), (15_000, 0.04), (30_000, 0.03), (52_000, 0.02), (80_000, 0.045)]]
    assert not [s for s in screen(videos(rows), [], subscribers=60_000, now=NOW)["signals"]
                if s["signal_type"] == "flat_engagement"]


def test_flat_engagement_needs_enough_evidence():
    def flagged(vids):
        return any(s["signal_type"] == "flat_engagement" for s in screen(vids, [], subscribers=None, now=NOW)["signals"])

    assert flagged(videos(BOUGHT))
    assert not flagged(videos(BOUGHT[:5]))  # fewer than 6 videos
    assert not flagged(videos(BOUGHT, age_days=1))  # still collecting likes
    assert not flagged(videos(BOUGHT, short=True))  # Shorts are judged separately from long-form
    # Views too similar for the slope to mean anything (p90/p10 below 2).
    assert not flagged(videos([(50_000 + i * 2_000, 2_500, 290) for i in range(8)]))


def test_size_is_soft_preference():
    assert size_score(50_000, 10_000, 150_000) == 1.0
    assert 0.4 <= size_score(400_000, 10_000, 150_000) < 1.0
    assert size_score(None, 10_000, 150_000) is None


def base_input(**kw):
    defaults = dict(
        niche_relevance=Sourced(0.95, "creatordb", "content_inferred"), classification_confidence=0.9,
        best_icp_fit=Sourced(0.8, None, "mixed"), icp_completeness=0.8,
        last_upload_at=NOW - timedelta(days=3), uploads_last_90d=12, activity_provider="creatordb",
        subscribers=400_000, subscribers_provider="creatordb", relevant_sponsorships=1,
        expected_fee_usd=None, risk_level="no_flags", scoring_allowed={"creatordb": True},
    )
    defaults.update(kw)
    return QualificationInput(**defaults)


def test_highly_relevant_large_creator_still_recommended():
    r = score(base_input(), now=NOW)
    assert r.recommendation == "recommend"
    assert r.components["size_fit"]["value"] < 1.0


def test_policy_blocked_provider_is_excluded():
    r = score(base_input(scoring_allowed={"creatordb": False}), now=NOW)
    assert r.components["niche_relevance"]["value"] is None
    assert any("creatordb" in e for e in r.excluded_sources)
    assert r.recommendation == "needs_data"


def test_niche_mismatch_rejected_only_when_confident():
    assert score(base_input(niche_relevance=Sourced(0.1, "creatordb", "content_inferred")), now=NOW).recommendation == "reject_niche_mismatch"
    low_conf = score(base_input(niche_relevance=Sourced(0.1, "creatordb", "content_inferred"), classification_confidence=0.3), now=NOW)
    assert low_conf.recommendation != "reject_niche_mismatch"


def test_elevated_risk_blocks_recommend():
    r = score(base_input(risk_level="elevated_review"), now=NOW)
    assert r.recommendation != "recommend"


def test_flat_engagement_rejects_an_otherwise_strong_creator():
    detail = "likes stay at 2,428-2,575 per video while views vary 3.3x across 7 videos"
    r = score(base_input(risk_level="elevated_review", risk_blockers=[detail]), now=NOW)
    assert r.recommendation == "reject_suspect_engagement"
    assert f"risk screening: {detail}" in r.reasons
    assert r.overall is not None  # still scored, so the creator page shows why


@pytest.mark.db
async def test_screening_disqualifies_flat_engagement(api, monkeypatch):
    from rpi.providers.fixture_provider import FixtureProvider

    original = FixtureProvider.get_creator_content

    async def bought_engagement(self, platform_account_id, limit=30):
        result = await original(self, platform_account_id, limit)
        result.data = [c.model_copy(update={"views": 20_000 * (i + 1), "likes": 2_500, "comments": 290, "is_short": False})
                       for i, c in enumerate(result.data)]
        return result

    monkeypatch.setattr(FixtureProvider, "get_creator_content", bought_engagement)
    v1, admin = "/rpi/api/v1", api.admin
    run = await api.post(f"{v1}/discovery/runs", headers=admin, json={
        "preset": "none", "strategies": ["niche"], "max_candidates": 10, "inline": True,
        "criteria": {"min_subscribers": 1000, "max_subscribers": 1000000}})
    assert run.status_code == 200, run.text
    ids = [c["id"] for c in (await api.get(f"{v1}/creators", headers=admin)).json()]
    screen_job = await api.post(f"{v1}/research", headers=admin, json={"creator_ids": ids, "stage": "screen", "inline": True})
    assert screen_job.status_code == 200, screen_job.text
    results = screen_job.json()["progress"]["results"]
    flagged = [cid for cid, rec in results.items() if rec == "reject_suspect_engagement"]
    assert flagged and "recommend" not in results.values() and "investigate" not in results.values()

    profile = (await api.get(f"{v1}/creators/{flagged[0]}", headers=admin)).json()
    assert profile["creator"]["disqualified_reason"] == "suspect_engagement"
    assert profile["creator"]["pipeline_status"] == "discovered"  # never moved on to deep analysis
    assert profile["qualification"]["risk_level"] == "elevated_review"
    assert [s["severity"] for s in profile["risk_signals"] if s["signal_type"] == "flat_engagement"] == ["elevated"]


def test_recent_competitor_deal_is_a_flag_not_a_penalty():
    plain = score(base_input(), now=NOW)
    flagged = score(base_input(recent_competitor_deals=["InVideo (paid, 2026-10-17)"]), now=NOW)
    assert flagged.overall == plain.overall and flagged.recommendation == plain.recommendation
    assert flagged.components["commercial_fit"]["flags"] == ["check exclusivity before outreach: InVideo (paid, 2026-10-17)"]
    assert any("exclusivity" in r for r in flagged.reasons)


def test_sponsor_saturation_and_sponsored_view_ratio():
    base = score(base_input(), now=NOW).components["commercial_fit"]["value"]
    light = score(base_input(sponsored_share=0.2, sponsored_sample=10), now=NOW).components["commercial_fit"]
    heavy = score(base_input(sponsored_share=0.4, sponsored_sample=10), now=NOW).components["commercial_fit"]
    saturated = score(base_input(sponsored_share=0.7, sponsored_sample=10), now=NOW).components["commercial_fit"]
    too_few = score(base_input(sponsored_share=0.7, sponsored_sample=3), now=NOW).components["commercial_fit"]
    assert light["value"] == base and "20% of 10 recent videos sponsored" in light["detail"]
    assert heavy["value"] == pytest.approx(base - 0.1) and saturated["value"] == pytest.approx(base - 0.2)
    assert too_few["value"] == base  # not judged on fewer than 5 videos
    tunes_out = score(base_input(sponsored_view_ratio=0.4), now=NOW).components["commercial_fit"]
    holds = score(base_input(sponsored_view_ratio=1.1), now=NOW).components["commercial_fit"]
    assert tunes_out["value"] == pytest.approx(base - 0.1) and holds["value"] == pytest.approx(base + 0.05)


def test_saturation_and_deal_helpers():
    from rpi.enrichment.service import recent_competitor_deals, sponsor_saturation

    def video(i, views, flag=False, short=False):
        return {"id": f"v{i}", "views": views, "sponsored_flag": flag, "is_short": short}
    content = [video(1, 1000, flag=True), video(2, 800), video(3, 4000), video(4, 5000), video(5, 3000),
               video(6, 200, short=True, flag=True)]
    s = sponsor_saturation(content, paid_content_ids={"v2"})
    assert s["sample"] == 5 and s["share"] == 0.4  # v1 (platform flag) and v2 (confirmed paid); shorts ignored
    assert s["view_ratio"] == pytest.approx(900 / 4000)
    assert sponsor_saturation([video(1, 10, short=True)], set()) == {"share": None, "sample": 0, "view_ratio": None}
    spons = [{"brand_name": "InVideo", "overlap": "direct", "relationship_type": "confirmed_paid", "published_at": NOW - timedelta(days=8)},
             {"brand_name": "InVideo", "overlap": "direct", "relationship_type": "confirmed_paid", "published_at": NOW - timedelta(days=40)},
             {"brand_name": "Pictory", "overlap": "direct", "relationship_type": "confirmed_paid", "published_at": NOW - timedelta(days=200)},
             {"brand_name": "CapCut", "overlap": "adjacent", "relationship_type": "confirmed_paid", "published_at": NOW - timedelta(days=3)},
             {"brand_name": "OpusClip", "overlap": "direct", "relationship_type": "ordinary_mention", "published_at": NOW}]
    assert recent_competitor_deals(spons, now=NOW) == [f"InVideo (paid, {(NOW - timedelta(days=8)).date().isoformat()})"]
