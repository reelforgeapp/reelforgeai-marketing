"""
Spoken-language gate: keeps channels that do not speak English out of paid research.

CreatorDB's `mainLanguage` follows a channel's titles, so a channel that titles
its videos in English but speaks Hindi passes its language filter (Hindi AI
Gyaan and Chad Grow, 2026-10-26), and its search filters can include languages
but not exclude them. YouTube records the spoken language of each video
(`defaultAudioLanguage`, the "video language" set on upload, which YouTube also
uses for automatic captions); the Data API returns it for 2 quota units per
channel and no CreatorDB credits.

The gate runs after a CreatorDB search and before screening, so a rejected
creator costs one search slot and nothing else. It fails open: with no YouTube
key, a quota error or too few declared languages, research continues as before.

Compliance: the raw values are used only as a pass/fail filter, never as a score
or metric (Developer Policies III.E.4(h)); the gateway cache keeps them at most
30 days, and only the decision (`disqualified_reason`) is stored on the creator.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Optional

from rpi.core.audit import audit
from rpi.outreach.sender import hold_autopilot_sends
from rpi.providers.base import Capability
from rpi.providers.gateway import CallContext, ProviderGateway
from rpi.providers.registry import ProviderRegistry

DISQUALIFIED_REASON = "non_english_audio"
RECOMMENDATION = "reject_non_english"
# Fewer declared languages than this says too little; the creator passes.
MIN_DECLARED = 3
# No linguistic content, undetermined, multiple languages: not a spoken-language signal.
NO_SIGNAL = {"zxx", "und", "mul"}
PREFERENCE = ("youtube_data_api", "fixture")


@dataclass
class LanguageVerdict:
    verdict: str  # english | non_english | unknown
    detail: str  # stored with the decision; names no language (see module docstring)
    declared: dict[str, int] = field(default_factory=dict)  # language -> videos; returned, never stored

    @property
    def rejected(self) -> bool:
        return self.verdict == "non_english"


def primary(code: Optional[str]) -> Optional[str]:
    """Primary language subtag: 'en-US' -> 'en'."""
    return code.split("-")[0].strip().lower() or None if code else None


def judge(languages: list[Optional[str]]) -> LanguageVerdict:
    """English when at least half of the recent videos that declare a spoken
    language declare English; unknown when fewer than MIN_DECLARED declare one."""
    declared = [p for p in map(primary, languages) if p and p not in NO_SIGNAL]
    counts = dict(Counter(declared).most_common())
    if len(declared) < MIN_DECLARED:
        return LanguageVerdict("unknown", f"only {len(declared)} of {len(languages)} recent videos declare a spoken "
                                          "language", counts)
    english = counts.get("en", 0)
    if english * 2 >= len(declared):
        return LanguageVerdict("english", f"{english} of {len(declared)} recent videos declare English", counts)
    return LanguageVerdict("non_english", f"spoken language is not English: {len(declared) - english} of "
                                          f"{len(declared)} recent videos declare another language (YouTube video "
                                          "language)", counts)


async def check(gateway: ProviderGateway, registry: ProviderRegistry, channel_id: Optional[str],
                ctx: CallContext) -> Optional[LanguageVerdict]:
    """The verdict for one YouTube channel, or None when it cannot be checked."""
    adapter = registry.first_supporting(Capability.VIDEO_LANGUAGES, PREFERENCE)
    if adapter is None or not channel_id:
        return None
    result = await gateway.call(adapter, Capability.VIDEO_LANGUAGES, channel_id, ctx=ctx)
    if not result.ok or result.data is None:
        return None
    return judge(result.data.languages)


async def disqualify(conn, creator_id, verdict: LanguageVerdict, actor: str) -> bool:
    """Record a rejection on the creator (once); True when it was newly recorded."""
    done = await conn.fetchval(
        """UPDATE rpi_creators SET disqualified_reason = $2, updated_at = NOW()
           WHERE id = $1 AND disqualified_reason IS NULL RETURNING id""", creator_id, DISQUALIFIED_REASON)
    if done:
        await audit(conn, actor, "research.language_rejected", "creator", creator_id, {"detail": verdict.detail})
        await hold_autopilot_sends(conn, creator_id, f"creator disqualified: {DISQUALIFIED_REASON}")
    return bool(done)
