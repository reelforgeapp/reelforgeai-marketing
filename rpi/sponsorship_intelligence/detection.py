"""
Sponsorship and promotion evidence detection from video titles/descriptions.

Link rules are ported from affiliate-scanner/app/detection.py (network hosts,
affiliate params and paths). Promo-code and disclosure patterns are new.

Classification is deliberately conservative:
  confirmed_paid        explicit paid disclosure ("sponsored by X", "paid
                        partnership with X") or a provider/platform paid flag
                        tied to the brand
  disclosed_affiliate   an affiliate link/code to the brand, or an explicit
                        "affiliate link" disclosure next to it
  possible_promotional  a tracked-looking link or promo code we cannot tie to
                        a disclosure
  ordinary_mention      the brand is named, nothing more

A brand mention never becomes a paid sponsorship on its own.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, Optional
from urllib.parse import parse_qs, urlparse

# --- ported from affiliate-scanner/app/detection.py -------------------------
NETWORK_HOSTS = (
    "pxf.io", "sjv.io", "ojrq.net",
    "anrdoezrs.net", "tkqlhce.com", "dpbolvw.net", "jdoqocy.com", "kqzyfj.com", "qksrv.net",
    "shareasale.com", "awin1.com", "linksynergy.com", "prf.hn",
    "redirectingat.com", "viglink.com",
    "geni.us", "clkmg.com", "tradedoubler.com", "go2cloud.org",
)
AFFILIATE_PARAMS = {
    "ref", "aff", "aff_id", "affid", "affiliate", "affiliateid",
    "partner", "via", "fpr", "rfsn", "tap_a", "irclickid",
    "sscid", "clickid", "subid", "pscd",
}
AFFILIATE_PATHS = (
    "/go/", "/recommends/", "/out/", "/aff/", "/r/", "/visit/",
    "/link/", "/refer/", "/ref/", "/partner/", "/goto/",
)
# ---------------------------------------------------------------------------

SHORTENER_HOSTS = ("bit.ly", "tinyurl.com", "t.co", "rebrand.ly", "short.io", "ow.ly", "cutt.ly", "lnkd.in")

URL_RE = re.compile(r"https?://[^\s<>\"')\]]+", re.IGNORECASE)

PAID_DISCLOSURE_RE = re.compile(
    r"(sponsored\s+by|this\s+video\s+is\s+sponsored|thanks\s+to\s+(?P<thanks>[\w .&'-]{2,40}?)\s+for\s+sponsoring|"
    r"paid\s+(partnership|promotion)\s+(with|by)|in\s+partnership\s+with|#sponsored\b|#ad\b|\bad:\s)",
    re.IGNORECASE,
)
AFFILIATE_DISCLOSURE_RE = re.compile(
    r"(affiliate\s+link|i\s+(may\s+)?(earn|receive)\s+(a\s+)?(small\s+)?commission|#affiliate\b|"
    r"commission\s+at\s+no\s+(extra\s+)?cost)",
    re.IGNORECASE,
)
PROMO_CODE_RE = re.compile(
    r"\b(?:use|with|enter|apply)\s+(?:my\s+|the\s+)?(?:promo\s+|discount\s+|coupon\s+)?code\s*[:\-]?\s*[\"'“]?(?P<code>[A-Z0-9][A-Z0-9_\-]{2,24})\b",
    re.IGNORECASE,
)


@dataclass
class Competitor:
    name: str
    domains: tuple[str, ...]
    aliases: tuple[str, ...] = ()
    category: str = "ai_video"
    overlap: str = "adjacent"
    id: Optional[str] = None

    def name_patterns(self) -> list[re.Pattern]:
        names = {self.name, *self.aliases}
        return [re.compile(rf"(?<![\w]){re.escape(n)}(?![\w])", re.IGNORECASE) for n in names if n]


@dataclass
class LinkSignal:
    url: str
    host: str
    signals: list[str]
    brand: Optional[str] = None


@dataclass
class SponsorshipFinding:
    brand_name: str
    relationship_type: str
    evidence: list[dict] = field(default_factory=list)
    competitor_id: Optional[str] = None
    brand_domain: Optional[str] = None


def _host(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except ValueError:
        return ""


def _root(host: str) -> str:
    parts = host.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def _suffix_match(host: str, suffixes: Iterable[str]) -> bool:
    return any(host == s or host.endswith("." + s) for s in suffixes)


def classify_link(url: str) -> LinkSignal:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    signals: list[str] = []
    if _suffix_match(host, NETWORK_HOSTS):
        signals.append(f"network_host:{host}")
    params = {k.lower() for k in parse_qs(parsed.query)}
    hit = params & AFFILIATE_PARAMS
    if hit:
        signals.append("param:" + ",".join(sorted(hit)))
    path_hit = next((p for p in AFFILIATE_PATHS if p in parsed.path.lower()), None)
    if path_hit:
        signals.append(f"path:{path_hit}")
    if _suffix_match(host, SHORTENER_HOSTS):
        signals.append("shortener")
    return LinkSignal(url=url, host=host, signals=signals)


def _excerpt(text: str, start: int, end: int, pad: int = 60) -> str:
    lo, hi = max(0, start - pad), min(len(text), end + pad)
    return " ".join(text[lo:hi].split())


def detect_sponsorships(
    text: str,
    competitors: list[Competitor],
    *,
    content_ref: str,
    content_url: Optional[str] = None,
    platform_paid_flag: Optional[bool] = None,
) -> list[SponsorshipFinding]:
    """Find evidence of promotional relationships with registry brands.

    Only brands in the competitor registry are attributed. Unknown brands in
    a "sponsored by" line are reported as a brand-less disclosure so an
    operator can extend the registry instead of the system guessing.
    """
    if not text:
        return []
    findings: dict[str, SponsorshipFinding] = {}
    has_paid_disclosure = bool(PAID_DISCLOSURE_RE.search(text))
    has_affiliate_disclosure = bool(AFFILIATE_DISCLOSURE_RE.search(text))
    links = [classify_link(m.group(0).rstrip(".,;")) for m in URL_RE.finditer(text)]
    codes = [(m.group("code"), m.start(), m.end()) for m in PROMO_CODE_RE.finditer(text)]

    for comp in competitors:
        mentions = [m for p in comp.name_patterns() for m in p.finditer(text)]
        brand_links = [
            l for l in links if any(l.host == d or l.host.endswith("." + d) for d in comp.domains)
        ]
        if not mentions and not brand_links:
            continue

        evidence: list[dict] = []
        for m in mentions[:2]:
            evidence.append({"kind": "content", "ref": content_ref, "url": content_url,
                             "excerpt": _excerpt(text, m.start(), m.end()), "signal": "brand_mention"})
        tracked = [l for l in brand_links if l.signals]
        for l in brand_links[:3]:
            evidence.append({"kind": "content", "ref": content_ref, "url": content_url,
                             "excerpt": l.url, "signal": ",".join(l.signals) or "brand_link"})

        # Promo code near a brand mention (within 160 chars) counts toward the brand.
        near_codes = [
            c for c in codes
            if any(abs(c[1] - m.start()) < 160 for m in mentions)
        ]
        for code, s, e in near_codes[:2]:
            evidence.append({"kind": "content", "ref": content_ref, "url": content_url,
                             "excerpt": _excerpt(text, s, e), "signal": f"promo_code:{code}"})

        paid_line = _brand_in_paid_disclosure(text, mentions)
        if paid_line or (platform_paid_flag and (mentions or brand_links)):
            rtype = "confirmed_paid"
            if paid_line:
                evidence.append({"kind": "content", "ref": content_ref, "url": content_url,
                                 "excerpt": paid_line, "signal": "paid_disclosure"})
            if platform_paid_flag:
                evidence.append({"kind": "provider_field", "ref": content_ref, "signal": "platform_paid_promotion_flag"})
        elif tracked or near_codes:
            # A tracked link or code is promotional; it is a *disclosed*
            # affiliate relationship only when the text says so.
            rtype = "disclosed_affiliate" if has_affiliate_disclosure else "possible_promotional"
        else:
            rtype = "ordinary_mention"

        findings[comp.name] = SponsorshipFinding(
            brand_name=comp.name,
            relationship_type=rtype,
            evidence=evidence,
            competitor_id=comp.id,
            brand_domain=comp.domains[0] if comp.domains else None,
        )

    if has_paid_disclosure and not any(f.relationship_type == "confirmed_paid" for f in findings.values()):
        m = PAID_DISCLOSURE_RE.search(text)
        findings["__unregistered__"] = SponsorshipFinding(
            brand_name="(unregistered brand)",
            relationship_type="possible_promotional",
            evidence=[{"kind": "content", "ref": content_ref, "url": content_url,
                       "excerpt": _excerpt(text, m.start(), m.end()), "signal": "paid_disclosure_unknown_brand"}],
        )
    return list(findings.values())


def _brand_in_paid_disclosure(text: str, mentions: list[re.Match]) -> Optional[str]:
    """Return the disclosure excerpt when a brand mention sits in the same
    sentence/line as a paid-sponsorship disclosure."""
    for d in PAID_DISCLOSURE_RE.finditer(text):
        line_start = max(text.rfind("\n", 0, d.start()), text.rfind(". ", 0, d.start())) + 1
        line_end_candidates = [i for i in (text.find("\n", d.end()), text.find(". ", d.end())) if i != -1]
        line_end = min(line_end_candidates) if line_end_candidates else len(text)
        for m in mentions:
            if line_start <= m.start() <= line_end:
                return _excerpt(text, d.start(), max(d.end(), m.end()))
    return None


# Seed registry: existing Partner Scout competitor_keywords brands plus the
# affiliate-scanner COMPETITOR_DOMAINS list. Editable via /rpi/api/v1/competitors.
DEFAULT_COMPETITORS: list[Competitor] = [
    Competitor("Crayo", ("crayo.ai",), category="faceless_automation", overlap="direct"),
    Competitor("AutoShorts", ("autoshorts.ai",), ("Auto Shorts",), "faceless_automation", "direct"),
    Competitor("Revid", ("revid.ai",), ("Revid AI",), "faceless_automation", "direct"),
    Competitor("Faceless.video", ("faceless.video",), (), "faceless_automation", "direct"),
    Competitor("NextShorts", ("nextshorts.ai",), (), "faceless_automation", "direct"),
    Competitor("Quso", ("quso.ai",), ("vidyo.ai",), "short_form_editor", "direct"),
    Competitor("Zebracat", ("zebracat.ai",), (), "ai_video_generator", "direct"),
    Competitor("OpusClip", ("opus.pro",), ("Opus Clip",), "short_form_editor", "direct"),
    Competitor("Submagic", ("submagic.co",), (), "short_form_editor", "adjacent"),
    Competitor("Vizard", ("vizard.ai",), (), "short_form_editor", "adjacent"),
    Competitor("Klap", ("klap.app",), (), "short_form_editor", "adjacent"),
    Competitor("InVideo", ("invideo.io",), ("InVideo AI",), "ai_video_generator", "direct"),
    Competitor("Pictory", ("pictory.ai",), (), "ai_video_generator", "direct"),
    Competitor("VEED", ("veed.io",), (), "short_form_editor", "adjacent"),
    Competitor("Captions", ("captions.ai",), (), "short_form_editor", "adjacent"),
    Competitor("Fliki", ("fliki.ai",), (), "ai_video_generator", "direct"),
    Competitor("Synthesia", ("synthesia.io",), (), "ai_video_generator", "adjacent"),
    Competitor("HeyGen", ("heygen.com",), (), "ai_video_generator", "adjacent"),
    Competitor("Descript", ("descript.com",), (), "short_form_editor", "adjacent"),
    Competitor("ElevenLabs", ("elevenlabs.io",), ("Eleven Labs",), "ai_voice", "complementary"),
    Competitor("CapCut", ("capcut.com",), (), "short_form_editor", "adjacent"),
    Competitor("Runway", ("runwayml.com",), ("Runway ML",), "ai_video_generator", "adjacent"),
]
