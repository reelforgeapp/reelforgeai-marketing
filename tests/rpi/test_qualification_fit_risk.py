"""Audience fit (no imputation), risk screening wording, qualification policy gating."""

from datetime import datetime, timedelta, timezone

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


def test_size_is_soft_preference():
    assert size_score(50_000, 10_000, 150_000) == 1.0
    assert 0.4 <= size_score(400_000, 10_000, 150_000) < 1.0
    assert size_score(None, 10_000, 150_000) is None


def base_input(**kw):
    defaults = dict(
        niche_relevance=Sourced(0.95, "creatordb", "content_inferred"), classification_confidence=0.9,
        best_icp_fit=Sourced(0.8, None, "mixed"), icp_completeness=0.8,
        last_upload_at=NOW - timedelta(days=3), uploads_last_90d=12, activity_provider="creatordb",
        subscribers=400_000, subscribers_provider="creatordb", relevant_sponsorships=1, competitor_conflict=False,
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
