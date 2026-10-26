"""
Provider contract tests with sanitized fixtures.

These verify adapter behavior against documented request shapes and error
semantics. They do NOT prove the live APIs behave this way; live verification
is scripts/rpi_verify_creatordb.py (see TEST_STRATEGY.md).
"""

import json
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx

from rpi.providers.base import Capability, CreatorSearchCriteria, ResultStatus
from rpi.discovery.presets import apply_preset
from rpi.providers.creatordb import CreatorDBAdapter, build_search_body, search_credits
from rpi.providers.http import CircuitBreaker, ResilientHTTP, full_jitter_backoff
from rpi.providers.sparktoro import SparkToroAdapter

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "creatordb_profile.json").read_text())
BASE = "https://apiv3.creatordb.app"
KEY = "cdb-secret-key-abcdef123456"


def adapter(**kw):
    a = CreatorDBAdapter(KEY, usd_per_credit=0.02, rate_per_second=1000, **kw)
    a.http._sleep = _no_sleep
    return a


async def _no_sleep(_s):
    return None


def ok(data, credits=1):
    return httpx.Response(200, json={"data": data, "creditsUsed": credits, "creditsAvailable": 900,
                                     "planCreditsAvailable": 900, "purchasedCreditsAvailable": 0,
                                     "traceId": "t", "timestamp": 0, "errorCode": "", "errorDescription": "",
                                     "success": True})


@respx.mock
async def test_profile_request_shape_and_normalization():
    route = respx.post(f"{BASE}/youtube/profile").mock(return_value=httpx.Response(200, json=FIXTURE))
    a = adapter()
    result = await a.get_creator_profile("UCFIXTURE000001")
    sent = route.calls.last.request
    assert json.loads(sent.content) == {"channelId": "UCFIXTURE000001"}
    assert sent.headers["api-key"] == KEY
    assert result.status == ResultStatus.OK
    c = result.data
    assert c.platform_account_id == "UCFIXTURE000001" and c.subscribers == 48200
    assert c.handle == "@examplefacelesslab" and c.display_name == "Example Faceless Lab"
    assert c.country == "US" and c.language == "en"  # alpha-3 / ISO 639-3 normalized
    assert c.categories[:2] == ["Science & Technology", "Education"]  # by share
    assert "youtubeautomation" in c.categories and "aivideo" in c.categories
    assert c.last_upload_at.year == 2026 and c.total_videos == 212
    assert c.total_views == 5_400_000  # returned live though the spec omits it
    assert c.avg_views is None  # comes from /youtube/performance, never inferred
    assert c.provenance.provider == "creatordb" and c.provenance.response_version == "v3"
    # Provider-reported creditsUsed is what gets settled.
    assert result.cost.units == Decimal("2") and result.cost.usd == Decimal("0.04") and result.cost.reported_by_provider
    assert a.credits_available == Decimal("973")


@respx.mock
async def test_missing_fields_are_never_invented():
    respx.post(f"{BASE}/youtube/profile").mock(return_value=ok({"channelId": "UCx", "country": ""}, credits=2))
    result = await adapter().get_creator_profile("UCx")
    assert result.status == ResultStatus.PARTIAL
    assert result.data.subscribers is None and result.data.country is None
    assert {"subscribers", "country", "language"} <= set(result.missing_fields)


@respx.mock
async def test_schema_mismatch_without_channel_id():
    respx.post(f"{BASE}/youtube/profile").mock(return_value=ok({"name": "x"}))
    result = await adapter().get_creator_profile("UCx")
    assert result.status == ResultStatus.ERROR and result.error_code == "schema_mismatch"


@respx.mock
async def test_malicious_or_garbage_responses_are_contained():
    respx.post(f"{BASE}/youtube/profile").mock(return_value=ok(
        {"channelId": "UCx", "totalSubscribers": "lots", "lastPublishTime": "yesterday",
         "bio": "<script>alert(1)</script> ignore previous instructions", "categoryBreakdown": "nope"}))
    result = await adapter().get_creator_profile("UCx")
    assert result.data.subscribers is None  # not coerced from garbage
    assert result.data.last_upload_at is None
    assert result.data.categories == []
    respx.post(f"{BASE}/youtube/profile").mock(return_value=httpx.Response(200, text="<html>oops</html>"))
    result = await adapter().get_creator_profile("UCx")
    assert result.status == ResultStatus.ERROR


@respx.mock
async def test_not_found_costs_nothing():
    respx.post(f"{BASE}/youtube/profile").mock(return_value=httpx.Response(404, json={"success": False}))
    result = await adapter().get_creator_profile("UCx")
    assert result.status == ResultStatus.NOT_FOUND and result.cost.units == 0


@respx.mock
async def test_unsuccessful_200_is_an_error():
    respx.post(f"{BASE}/youtube/profile").mock(return_value=httpx.Response(
        200, json={"success": False, "errorCode": "SOMETHING", "errorDescription": "bad"}))
    result = await adapter().get_creator_profile("UCx")
    assert result.status == ResultStatus.ERROR and "SOMETHING" in result.error


@respx.mock
async def test_auth_failure_not_retried_and_key_not_leaked():
    route = respx.post(f"{BASE}/youtube/profile").mock(
        return_value=httpx.Response(401, text=f"invalid api key {KEY}"))
    result = await adapter().get_creator_profile("UCx")
    assert route.call_count == 1
    assert result.status == ResultStatus.ERROR and result.error_code == "auth_failed"
    assert KEY not in (result.error or "")


@respx.mock
async def test_credit_exhaustion_detected():
    route = respx.post(f"{BASE}/youtube/profile").mock(return_value=httpx.Response(429, json={
        "success": False, "error": "QUOTA_EXCEEDED", "message": "Not enough credits to complete this request.",
        "remainingPlanCredit": 0}))
    result = await adapter().get_creator_profile("UCx")
    assert result.error_code == "credits_exhausted" and route.call_count == 1  # never retried
    respx.post(f"{BASE}/youtube/profile").mock(return_value=httpx.Response(403, json={"error": "Insufficient credits"}))
    assert (await adapter().get_creator_profile("UCx")).error_code == "credits_exhausted"


@respx.mock
async def test_fractional_calls_error_is_specific():
    respx.post(f"{BASE}/youtube/profile").mock(return_value=httpx.Response(403, json={
        "success": False, "error": "FRACTIONAL_CALLS_NOT_ENABLED", "message": "x"}))
    assert (await adapter().get_creator_profile("UCx")).error_code == "fractional_calls_not_enabled"


@respx.mock
async def test_rate_limit_retry_after_then_success():
    route = respx.post(f"{BASE}/youtube/profile").mock(side_effect=[
        httpx.Response(429, headers={"retry-after": "1"}, json={"success": False, "error": "RATE_LIMIT_EXCEEDED",
                                                                "message": "Too many requests. Please try again later."}),
        httpx.Response(200, json=FIXTURE)])
    result = await adapter().get_creator_profile("UCFIXTURE000001")
    assert route.call_count == 2 and result.ok


@respx.mock
async def test_server_errors_retry_then_fail():
    route = respx.post(f"{BASE}/youtube/profile").mock(return_value=httpx.Response(503))
    result = await adapter().get_creator_profile("UCx")
    assert route.call_count == 4  # 1 + 3 retries
    assert result.error_code == "unavailable"


async def test_circuit_breaker_opens_and_half_opens():
    clock = [0.0]
    b = CircuitBreaker(failure_threshold=2, cooldown_seconds=10, clock=lambda: clock[0])
    b.record_failure(); b.record_failure()
    assert b.state == "open" and not b.allow()
    clock[0] = 11
    assert b.state == "half_open" and b.allow() and not b.allow()
    b.record_success()
    assert b.state == "closed"


@respx.mock
async def test_open_circuit_short_circuits_requests():
    respx.post(f"{BASE}/youtube/profile").mock(return_value=httpx.Response(503))
    a = adapter()
    a.http.breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=60)
    await a.get_creator_profile("UCx")
    result = await a.get_creator_profile("UCx")
    assert result.error_code == "circuit_open"


def test_backoff_is_bounded():
    assert all(0 <= full_jitter_backoff(n, cap=5) <= 5 for n in range(20))


def test_documented_endpoints_enabled_and_costed():
    a = adapter()
    for cap in (Capability.SEARCH_CREATORS, Capability.CREATOR_PROFILE, Capability.CREATOR_PERFORMANCE,
                Capability.CREATOR_CONTENT, Capability.AUDIENCE_DEMOGRAPHICS, Capability.SPONSORSHIP_HISTORY,
                Capability.CREATOR_CONTACTS, Capability.USAGE_COSTS):
        assert a.supports(cap)
    assert a.estimate_cost(Capability.CREATOR_CONTENT).units == Decimal("3")
    assert a.estimate_cost(Capability.AUDIENCE_DEMOGRAPHICS).units == Decimal("10")
    assert a.estimate_cost(Capability.SEARCH_CREATORS, limit=250).units == Decimal("3")  # 3 pages
    assert a.estimate_cost(Capability.USAGE_COSTS).units == 0


def test_search_filters_translate_criteria():
    body, unapplied, applied = build_search_body(CreatorSearchCriteria(
        keywords=["Faceless YouTube", "faceless youtube", "AI shorts!"], languages=["en", "xx"],
        countries=["US", "GB", "ZZ"], min_subscribers=10_000, max_subscribers=150_000, min_avg_views=1000,
        active_within_days=30, niches=["id_aivideo_ScienceTechnology"], limit=500, page_token="200"), now_ms=10**13)
    f = {(x["filterName"], x["op"]): x["value"] for x in body["filters"]}
    assert f[("hashtags", "in")] == ["#aishorts", "#facelessyoutube"]
    assert f[("mainLanguage", "in")] == ["eng"] and f[("country", "in")] == ["GBR", "USA"]
    assert f[("totalSubscribers", ">")] == 9_999 and f[("totalSubscribers", "<")] == 150_001
    assert f[("avgRecentVideosViews", ">")] == 999
    assert f[("lastPublishTime", ">")] == 10**13 - 30 * 86_400_000
    assert f[("niches", "in")] == ["id_aivideo_ScienceTechnology"]
    assert body["pageSize"] == 100 and body["offset"] == 200 and body["sortBy"] == "medianRecentVideosViews"
    assert unapplied == ["languages:xx", "countries:ZZ"]
    assert "keywords" in applied and "countries" in applied and "min_avg_views" in applied


def test_quality_preset_is_one_credit_of_filters():
    criteria = apply_preset(CreatorSearchCriteria(), "quality")
    body, unapplied, applied = build_search_body(criteria)
    f = {(x["filterName"], x["op"]): x["value"] for x in body["filters"]}
    assert len(body["filters"]) == 10 and search_credits(len(body["filters"])) == 1
    assert "#aitools" in f[("hashtags", "in")] and "#faceless" not in f[("hashtags", "in")]
    assert f[("topics", "in")][0] == "id_ai_ScienceTechnology_youtube" and ("niches", "in") not in f
    assert f[("mainAudienceLocation", "in")] == ["AUS", "CAN", "GBR", "USA"]
    assert f[("medianRecentVideosViews", ">")] == 1_999 and f[("contentsIn30Days", ">")] == 1
    assert f[("avgRecentVideosEngagementRate", ">")] == pytest.approx(0.01995)
    assert f[("hasEmail", "=")] is True and body["sortBy"] == "medianRecentVideosViews"
    assert not unapplied
    broad = build_search_body(apply_preset(CreatorSearchCriteria(), "quality_broad"))[0]["filters"]
    assert len(broad) == 10 and "id_youtubeshorts_Education" not in next(x["value"] for x in broad if x["filterName"] == "niches")
    assert adapter().estimate_cost(Capability.SEARCH_CREATORS, criteria=criteria.model_copy(update={"limit": 25})).units == 1


def test_outside_range_presets_cover_the_gaps_with_stricter_relevance():
    from rpi.discovery.presets import CORE_HASHTAGS, EDUCATOR_HASHTAGS

    assert set(CORE_HASHTAGS) < set(EDUCATOR_HASHTAGS) and "#aitools" not in [f"#{t}" for t in CORE_HASHTAGS]
    bands = {}
    for name in ("outside_range_small", "outside_range_large"):
        criteria = apply_preset(CreatorSearchCriteria(), name)
        body, unapplied, _ = build_search_body(criteria)
        f = {(x["filterName"], x["op"]): x["value"] for x in body["filters"]}
        assert len(body["filters"]) == 10 and search_credits(len(body["filters"])) == 1 and not unapplied
        assert sorted(f[("hashtags", "in")]) == sorted(f"#{t}" for t in CORE_HASHTAGS)
        assert f[("topics", "in")][0] == "id_ai_ScienceTechnology_youtube"  # hashtags AND topics, like quality
        bands[name] = (f[("totalSubscribers", ">")] + 1, f[("totalSubscribers", "<")] - 1,
                       f[("medianRecentVideosViews", ">")] + 1, f[("avgRecentVideosEngagementRate", ">")])
    assert bands["outside_range_small"][:3] == (3_000, 9_999, 1_000)
    assert bands["outside_range_large"][:3] == (150_001, 500_000, 10_000)
    assert bands["outside_range_small"][3] == pytest.approx(0.03995) and bands["outside_range_large"][3] == pytest.approx(0.02995)


def test_autopilot_reaches_outside_range_presets_only_after_the_main_ones():
    from rpi.administration.autopilot import CURSOR_END, current_preset
    from rpi.discovery.presets import AUTOPILOT_PRESETS

    row = {"presets": list(AUTOPILOT_PRESETS), "discovery_cursor": {}}
    assert current_preset(row) == ("quality", None)
    row["discovery_cursor"] = {"quality": CURSOR_END, "quality_broad": "50"}
    assert current_preset(row) == ("quality_broad", "50")
    row["discovery_cursor"] = {"quality": CURSOR_END, "quality_broad": CURSOR_END}
    assert current_preset(row) == ("outside_range_small", None)
    row["discovery_cursor"].update(outside_range_small=CURSOR_END, outside_range_large=CURSOR_END)
    assert current_preset(row) == (None, None)


def test_preset_yields_to_explicit_fields():
    mine = apply_preset(CreatorSearchCriteria(keywords=["capcut"], max_subscribers=500_000, audience_countries=[]), "quality")
    assert mine.niches == [] and mine.topics == [] and mine.keywords == ["capcut"]  # caller chose relevance
    assert mine.max_subscribers == 500_000 and mine.audience_countries == []
    assert mine.min_median_views == 2_000 and mine.require_email
    assert apply_preset(CreatorSearchCriteria(), "quality_broad").niches and not apply_preset(CreatorSearchCriteria(), "quality").niches
    assert apply_preset(CreatorSearchCriteria(), "none").niches == []
    with pytest.raises(ValueError):
        apply_preset(CreatorSearchCriteria(), "bogus")


@respx.mock
async def test_search_pagination_and_sparse_results():
    route = respx.post(f"{BASE}/youtube/search").mock(return_value=httpx.Response(200, json={
        "data": {"creatorList": [{"displayName": "A", "uniqueId": "@a", "channelId": "UCa", "avatarUrl": "u",
                                  "totalSubscribers": 20_000},
                                 {"displayName": "no id"}],
                 "totalResults": 30, "hasNextPage": True, "nextOffset": 2},
        "creditsUsed": 1, "success": True}))
    result = await adapter().search_creators(CreatorSearchCriteria(keywords=["faceless"], limit=2))
    body = json.loads(route.calls.last.request.content)
    assert body["offset"] == 0 and body["pageSize"] == 2 and body["filters"]
    assert [c.platform_account_id for c in result.data] == ["UCa"]
    assert result.data[0].subscribers == 20_000 and result.data[0].language is None  # not filled from the filter
    assert result.status == ResultStatus.PARTIAL and result.next_page_token == "2"
    assert result.total_available == 30 and result.applied_criteria == ["keywords", "languages"]
    assert result.cost.units == Decimal("1")


@respx.mock
async def test_search_last_page_and_enriched_records():
    respx.post(f"{BASE}/youtube/search").mock(return_value=ok({
        "creatorList": [{"channelId": "UCe", "displayName": "E", "uniqueId": "@e", "totalSubscribers": 5,
                         "profile": {"bio": "faceless", "country": "CAN", "mainLanguage": "eng"},
                         "performance": {"videosPerformanceRecent": {"views": {"avg": 900, "median": 700},
                                                                     "engagement": {"avgEngagementRate": 0.05}},
                                         "contentCountByDays": {"90d": 12}}}],
        "totalResults": 1, "hasNextPage": False, "nextOffset": 1}))
    result = await adapter().search_creators(CreatorSearchCriteria(keywords=["faceless"]))
    c = result.data[0]
    assert result.next_page_token is None
    assert c.country == "CA" and c.description == "faceless" and c.avg_views == 900 and c.uploads_last_90d == 12


async def test_brand_search_is_unsupported_not_guessed():
    result = await adapter().search_creators(CreatorSearchCriteria(sponsored_brands=["OpusClip"]))
    assert result.status == ResultStatus.UNSUPPORTED


@respx.mock
async def test_performance_content_audience_sponsorship_contact_usage():
    a = adapter()
    respx.post(f"{BASE}/youtube/performance").mock(return_value=ok({
        "contentCountByDays": {"7d": 1, "30d": 4, "90d": 11},
        "videosPerformanceRecent": {"views": {"avg": 12000, "median": 8000}, "engagement": {"avgEngagementRate": 0.043}}},
        credits=3))
    perf = await a.get_creator_performance("UCx")
    assert perf.data.avg_views == 12000 and perf.data.median_views == 8000
    assert perf.data.engagement_rate == pytest.approx(0.043) and perf.data.uploads_last_90d == 11

    respx.post(f"{BASE}/youtube/content-detail").mock(return_value=ok({
        "recentVideos": [{"publishTime": 1792508400000, "contentId": "vid00000001", "title": "T", "description": "D",
                          "length": 600, "isSponsored": True, "isMemberOnly": False, "likes": 10, "comments": 2,
                          "views": 1000, "engagementRate": 0.012, "hashtags": []}],
        "recentShorts": [{"publishTime": 1792508400000, "contentId": "sho00000001", "title": "S", "description": "",
                          "length": 30, "likes": 1, "comments": 0, "views": 50, "engagementRate": 0.02, "hashtags": []}]},
        credits=3))
    content = await a.get_creator_content("UCx")
    video, short = content.data
    assert video.url == "https://www.youtube.com/watch?v=vid00000001" and video.sponsored_flag is True
    assert short.is_short and short.url == "https://www.youtube.com/shorts/sho00000001" and short.sponsored_flag is None
    assert content.status == ResultStatus.PARTIAL  # the short's empty description
    assert [i.platform_content_id for i in (await a.get_creator_content("UCx", limit=1)).data] == ["vid00000001"]

    respx.post(f"{BASE}/youtube/audience").mock(return_value=ok({
        "audienceLocations": [{"country": "USA", "share": 0.78}], "audienceGender": {"maleRatio": 0.6, "femaleRatio": 0.4},
        "audienceAvgAge": 31, "audienceAgeBreakdown": None}, credits=10))
    aud = await a.get_audience_demographics("UCx")
    got = {(b.dimension, b.bucket): b.share for b in aud.data.buckets}
    assert got == {("country", "US"): 0.78, ("gender", "male"): 0.6, ("gender", "female"): 0.4}
    assert aud.missing_fields == ["age"] and aud.cost.units == Decimal("10")

    respx.post(f"{BASE}/youtube/sponsorship").mock(return_value=ok({"sponsorList": [
        {"brandName": "OpusClip", "brandId": "https://www.opus.pro/", "brandIgIds": [],
         "sponsoredVideos": [{"contentId": "vid00000001", "publishTime": 1792508400000}]},
        {"brandName": "NoVideos", "brandId": "brand_123", "brandIgIds": [], "sponsoredVideos": []}]}, credits=5))
    spons = await a.get_sponsorship_history("UCx")
    assert [(s.brand_name, s.brand_domain, s.content_id) for s in spons.data] == [
        ("OpusClip", "opus.pro", "vid00000001"), ("NoVideos", None, None)]

    respx.post(f"{BASE}/youtube/contact").mock(return_value=ok({"emails": ["Biz@Example.com", "biz@example.com", "x"]},
                                                               credits=15))
    contacts = await a.get_creator_contacts("UCx")
    assert [c.value for c in contacts.data] == ["Biz@Example.com"] and contacts.cost.units == Decimal("15")
    respx.post(f"{BASE}/youtube/contact").mock(return_value=httpx.Response(200, json={"data": {"emails": []}, "success": True}))
    none = await a.get_creator_contacts("UCx")
    assert none.status == ResultStatus.NOT_FOUND and none.cost.units == 0  # charged only when an email returns

    respx.get(f"{BASE}/usage").mock(return_value=httpx.Response(200, json={
        "data": {"records": [], "count": 0, "key": {"creditLimit": None, "creditsRemaining": None, "active": True}},
        "creditsUsed": 0, "creditsAvailable": 975, "planCreditsAvailable": 900, "purchasedCreditsAvailable": 75,
        "success": True}))
    usage = await a.get_usage_costs()
    assert usage.data["credits_available"] == 975 and usage.data["key_active"] is True and usage.cost.units == 0
    health = await a.health_check()
    assert health.healthy is True and "975" in health.detail


@respx.mock
async def test_sparktoro_credit_and_building_semantics():
    s = SparkToroAdapter("st-key-123456789", usd_per_credit=0.05)
    s.http._sleep = _no_sleep
    respx.post("https://api.sparktoro.com/v3/describe/create").mock(return_value=httpx.Response(200, json={"report_id": "r1"}))
    respx.get("https://api.sparktoro.com/v3/youtube").mock(return_value=httpx.Response(202, json={}))
    building = await s.research_audience("faceless creators")
    assert building.status == ResultStatus.PARTIAL and building.cost.units == Decimal("10")
    respx.post("https://api.sparktoro.com/v3/describe/create").mock(
        return_value=httpx.Response(402, json={"credits_required": 10, "credits_remaining": 2}))
    broke = await s.research_audience("faceless creators")
    assert broke.error_code == "credits_exhausted" and broke.cost.units == 0
    assert s.estimate_cost(Capability.AUDIENCE_RESEARCH).units == Decimal("11")
