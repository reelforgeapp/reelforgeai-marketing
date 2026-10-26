"""Sponsorship evidence classification is conservative."""

from rpi.sponsorship_intelligence.detection import DEFAULT_COMPETITORS, classify_link, detect_sponsorships


def run(text, paid_flag=None):
    return {f.brand_name: f for f in detect_sponsorships(text, DEFAULT_COMPETITORS, content_ref="v1",
                                                          content_url="https://youtu.be/v1", platform_paid_flag=paid_flag)}


def test_plain_mention_is_ordinary():
    f = run("I compared OpusClip and Submagic for captions.")
    assert f["OpusClip"].relationship_type == "ordinary_mention"
    assert f["Submagic"].relationship_type == "ordinary_mention"


def test_paid_disclosure_with_brand_is_confirmed():
    f = run("This video is sponsored by Crayo. Get started at https://crayo.ai")
    assert f["Crayo"].relationship_type == "confirmed_paid"
    assert any(e["signal"] == "paid_disclosure" for e in f["Crayo"].evidence)


def test_affiliate_link_without_disclosure_is_only_possible():
    f = run("Tool I use: https://crayo.ai/?via=jane")
    assert f["Crayo"].relationship_type == "possible_promotional"


def test_affiliate_link_with_disclosure_is_disclosed_affiliate():
    f = run("Affiliate link (I may earn a commission): https://revid.ai/?ref=jane")
    assert f["Revid"].relationship_type == "disclosed_affiliate"


def test_promo_code_near_brand():
    f = run("Try InVideo and use code JANE20 for a discount")
    assert f["InVideo"].relationship_type == "possible_promotional"
    assert any("promo_code:JANE20" in e["signal"] for e in f["InVideo"].evidence)


def test_platform_paid_flag_confirms_only_with_brand_present():
    assert run("Thanks for watching!", paid_flag=True) == {}
    assert run("Thanks to Pictory!", paid_flag=True)["Pictory"].relationship_type == "confirmed_paid"


def test_unknown_brand_disclosure_reported_without_guessing():
    f = run("This video is sponsored by SomeNewTool.")
    assert "(unregistered brand)" in f and f["(unregistered brand)"].relationship_type == "possible_promotional"


def test_link_rules_ported_from_affiliate_scanner():
    assert classify_link("https://crayo.pxf.io/abc").signals == ["network_host:crayo.pxf.io"]
    assert any(s.startswith("param:") for s in classify_link("https://x.com/?fpr=abc").signals)
    assert "path:/go/" in classify_link("https://site.com/go/tool").signals
    assert classify_link("https://bit.ly/abc").signals == ["shortener"]
    # Dot-boundary suffix match: "blogx.com" is not "x.com"-style host confusion.
    assert classify_link("https://notpxf.io/a").signals == []


def test_provider_brands_link_to_the_competitor_registry():
    from rpi.enrichment.service import PROVIDER_PAID_LABELS, match_competitor

    assert match_competitor("InVideo", None, DEFAULT_COMPETITORS).name == "InVideo"
    assert match_competitor("invideo ai", "invideo.io", DEFAULT_COMPETITORS).name == "InVideo"  # by domain
    assert match_competitor("Fish Audio", "fish.audio", DEFAULT_COMPETITORS) is None
    assert match_competitor("TopviewAI", None, DEFAULT_COMPETITORS).name == "Topview"  # by alias
    assert "creatordb_detected_sponsorship" in PROVIDER_PAID_LABELS
