"""Identity resolution: never merge on weak evidence."""

from rpi.identity.resolver import (
    AccountRef,
    IdentityRecord,
    MatchDecision,
    compare,
    domain_of,
    is_distinctive_domain,
    normalize_handle,
    parse_youtube_url,
)

YT_A = AccountRef("youtube", "UCaaaaaaaaaaaaaaaaaaaaaa", "@alpha")
TT_A = AccountRef("tiktok", "7001", "@alpha")


def rec(cid, accounts, **kw):
    return IdentityRecord(cid, accounts, **kw)


def test_same_platform_id_is_same_account():
    r = compare(rec("1", [YT_A]), rec("2", [AccountRef("youtube", YT_A.platform_account_id, "@renamed")]))
    assert r.decision == MatchDecision.SAME_ACCOUNT


def test_same_handle_different_id_goes_to_review():
    r = compare(rec("1", [YT_A]), rec("2", [AccountRef("youtube", "UCbbbbbbbbbbbbbbbbbbbbbb", "@Alpha")]))
    assert r.decision == MatchDecision.REVIEW


def test_display_name_alone_never_matches():
    r = compare(rec("1", [YT_A], display_name="Alpha AI"), rec("2", [TT_A], display_name="alpha ai"))
    assert r.decision == MatchDecision.NO_MATCH


def test_one_way_link_needs_review():
    a = rec("1", [YT_A], outbound_links=[TT_A])
    r = compare(a, rec("2", [TT_A]))
    assert r.decision == MatchDecision.REVIEW


def test_bidirectional_link_auto_merges():
    a = rec("1", [YT_A], outbound_links=[TT_A])
    b = rec("2", [TT_A], outbound_links=[YT_A])
    r = compare(a, b)
    assert r.decision == MatchDecision.AUTO_MERGE and r.confidence >= 0.9


def test_two_independent_strong_signals_auto_merge():
    a = rec("1", [YT_A], provider_mappings=[TT_A], business_emails={"Biz@Alpha.io"})
    b = rec("2", [TT_A], business_emails={"biz@alpha.io"})
    assert compare(a, b).decision == MatchDecision.AUTO_MERGE


def test_generic_link_domains_are_not_evidence():
    a = rec("1", [YT_A], website_domains={"linktr.ee"})
    b = rec("2", [TT_A], website_domains={"linktr.ee"})
    assert compare(a, b).decision == MatchDecision.NO_MATCH
    assert not is_distinctive_domain("beacons.ai") and is_distinctive_domain("alphastudio.com")


def test_normalizers():
    assert normalize_handle(" @Alpha_AI ") == "alpha_ai"
    assert domain_of("https://www.AlphaStudio.com/about") == "alphastudio.com"
    assert parse_youtube_url("https://youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa").platform_account_id.startswith("UC")
    assert parse_youtube_url("https://www.youtube.com/@Alpha").handle == "alpha"
    assert parse_youtube_url("https://example.com/@x") is None
