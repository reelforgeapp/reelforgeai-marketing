"""
Creator identity resolution.

Rules (pure functions, unit-tested):

1. Same platform + same stable platform account id  → the same account.
   This is the only fully automatic match.
2. Same platform, same normalized handle, different account id → conflict,
   always sent to review (handles get renamed and recycled).
3. Cross-platform links between two creator records are merged automatically
   only with strong, independent evidence:
     * a bidirectional profile link (A links to B and B links back), or
     * two strong signals from different sources (e.g. a provider mapping
       plus a shared verified business email).
   Anything weaker but non-trivial becomes `pending_review`.
4. Display-name similarity alone never produces a match.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from urllib.parse import urlparse

GENERIC_LINK_DOMAINS = {
    "linktr.ee", "beacons.ai", "bio.link", "linkin.bio", "stan.store", "gumroad.com", "patreon.com",
    "ko-fi.com", "buymeacoffee.com", "youtube.com", "youtu.be", "instagram.com", "tiktok.com",
    "twitter.com", "x.com", "facebook.com", "linkedin.com", "discord.gg", "discord.com", "gmail.com",
    "google.com", "amazon.com", "amzn.to", "bit.ly", "notion.site", "substack.com", "medium.com",
}


class SignalStrength(str, Enum):
    STRONG = "strong"
    MEDIUM = "medium"
    WEAK = "weak"


class MatchDecision(str, Enum):
    SAME_ACCOUNT = "same_account"
    AUTO_MERGE = "auto_merged"
    REVIEW = "pending_review"
    NO_MATCH = "no_match"


@dataclass(frozen=True)
class AccountRef:
    platform: str
    platform_account_id: Optional[str]
    handle: Optional[str] = None
    url: Optional[str] = None


@dataclass
class IdentityRecord:
    """What we know about one creator record when comparing identities."""

    creator_id: Optional[str]
    accounts: list[AccountRef]
    display_name: Optional[str] = None
    # Accounts this record's profiles link out to (from profile bios/links).
    outbound_links: list[AccountRef] = field(default_factory=list)
    # Accounts a licensed provider asserts belong to the same creator.
    provider_mappings: list[AccountRef] = field(default_factory=list)
    business_emails: set[str] = field(default_factory=set)
    website_domains: set[str] = field(default_factory=set)


@dataclass
class Signal:
    kind: str
    strength: SignalStrength
    source: str
    detail: str


@dataclass
class MatchResult:
    decision: MatchDecision
    confidence: float
    signals: list[Signal]
    reason: str


def normalize_handle(handle: Optional[str]) -> Optional[str]:
    if not handle:
        return None
    h = unicodedata.normalize("NFKC", handle).strip().lower()
    h = h.lstrip("@")
    h = re.sub(r"\s+", "", h)
    return h or None


def normalize_email(email: str) -> str:
    return email.strip().lower()


def domain_of(url_or_domain: str) -> Optional[str]:
    value = url_or_domain.strip().lower()
    if "://" not in value:
        value = "https://" + value
    try:
        host = urlparse(value).hostname or ""
    except ValueError:
        return None
    host = host.removeprefix("www.")
    return host or None


def is_distinctive_domain(domain: Optional[str]) -> bool:
    if not domain:
        return False
    return not any(domain == g or domain.endswith("." + g) for g in GENERIC_LINK_DOMAINS)


def parse_youtube_url(url: str) -> Optional[AccountRef]:
    """Extract a YouTube account reference from a channel URL."""
    try:
        parsed = urlparse(url if "://" in url else "https://" + url)
    except ValueError:
        return None
    host = (parsed.hostname or "").removeprefix("www.").removeprefix("m.")
    if host not in ("youtube.com",):
        return None
    parts = [p for p in parsed.path.split("/") if p]
    if not parts:
        return None
    if parts[0] == "channel" and len(parts) > 1 and parts[1].startswith("UC"):
        return AccountRef("youtube", parts[1], None, url)
    if parts[0].startswith("@"):
        return AccountRef("youtube", None, normalize_handle(parts[0]), url)
    if parts[0] in ("c", "user") and len(parts) > 1:
        return AccountRef("youtube", None, normalize_handle(parts[1]), url)
    return None


def _same_account(a: AccountRef, b: AccountRef) -> bool:
    if a.platform != b.platform:
        return False
    if a.platform_account_id and b.platform_account_id:
        return a.platform_account_id == b.platform_account_id
    ha, hb = normalize_handle(a.handle), normalize_handle(b.handle)
    return bool(ha and hb and ha == hb)


def _links_to(record: IdentityRecord, other: IdentityRecord) -> bool:
    return any(_same_account(link, acct) for link in record.outbound_links for acct in other.accounts)


def compare(a: IdentityRecord, b: IdentityRecord) -> MatchResult:
    signals: list[Signal] = []

    # Rule 1 / 2: same-platform comparisons.
    for acct_a in a.accounts:
        for acct_b in b.accounts:
            if acct_a.platform != acct_b.platform:
                continue
            if acct_a.platform_account_id and acct_a.platform_account_id == acct_b.platform_account_id:
                return MatchResult(MatchDecision.SAME_ACCOUNT, 1.0, [
                    Signal("platform_account_id", SignalStrength.STRONG, "platform", acct_a.platform_account_id)
                ], "identical stable platform account id")
            ha, hb = normalize_handle(acct_a.handle), normalize_handle(acct_b.handle)
            if ha and hb and ha == hb and acct_a.platform_account_id and acct_b.platform_account_id:
                return MatchResult(MatchDecision.REVIEW, 0.3, [
                    Signal("handle_conflict", SignalStrength.MEDIUM, "platform",
                           f"handle {ha} on two different {acct_a.platform} account ids")
                ], "same handle, different account ids: possible rename or recycled handle")

    # Rule 3: cross-platform evidence.
    a_to_b, b_to_a = _links_to(a, b), _links_to(b, a)
    if a_to_b and b_to_a:
        signals.append(Signal("bidirectional_link", SignalStrength.STRONG, "profile_links",
                              "each profile links to the other"))
    elif a_to_b or b_to_a:
        signals.append(Signal("one_way_link", SignalStrength.MEDIUM, "profile_links",
                              "one profile links to the other"))

    if any(_same_account(m, acct) for m in a.provider_mappings for acct in b.accounts) or any(
        _same_account(m, acct) for m in b.provider_mappings for acct in a.accounts
    ):
        signals.append(Signal("provider_mapping", SignalStrength.STRONG, "provider",
                              "a licensed provider maps both accounts to one creator"))

    shared_emails = {normalize_email(e) for e in a.business_emails} & {normalize_email(e) for e in b.business_emails}
    if shared_emails:
        signals.append(Signal("shared_business_email", SignalStrength.STRONG, "contacts",
                              f"{len(shared_emails)} shared business email(s)"))

    shared_domains = {d for d in a.website_domains & b.website_domains if is_distinctive_domain(d)}
    if shared_domains:
        signals.append(Signal("shared_website_domain", SignalStrength.MEDIUM, "profile_links",
                              ", ".join(sorted(shared_domains))))

    if a.display_name and b.display_name and _norm_name(a.display_name) == _norm_name(b.display_name):
        signals.append(Signal("display_name_equal", SignalStrength.WEAK, "profile", _norm_name(a.display_name)))

    strong = [s for s in signals if s.strength == SignalStrength.STRONG]
    medium = [s for s in signals if s.strength == SignalStrength.MEDIUM]
    strong_sources = {s.source for s in strong}

    if any(s.kind == "bidirectional_link" for s in strong) or len(strong_sources) >= 2:
        confidence = min(0.99, 0.9 + 0.03 * (len(strong) + len(medium) - 1))
        return MatchResult(MatchDecision.AUTO_MERGE, confidence, signals,
                           "strong independent evidence of a single creator")
    if strong or medium:
        confidence = 0.5 + 0.15 * len(strong) + 0.1 * len(medium)
        return MatchResult(MatchDecision.REVIEW, min(confidence, 0.85), signals,
                           "some evidence of a single creator; needs human review")
    return MatchResult(MatchDecision.NO_MATCH, 0.0, signals,
                       "no evidence beyond display name" if signals else "no shared evidence")


def _norm_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", unicodedata.normalize("NFKC", name).lower())


def dedupe_key(platform: str, platform_account_id: str) -> str:
    return f"{platform.lower()}:{platform_account_id}"
