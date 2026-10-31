"""
Discovery presets: search criteria for the highest-value ReelForge partners.

A preset pushes every qualification rule the search provider can enforce into
the search itself. On CreatorDB up to 10 filters cost one credit per page of
up to 100 creators, while each per-creator call costs 3-15 credits, so paid
enrichment is spent only on creators who already pass these rules.

Fields a caller sets explicitly win over the preset.

Relevance, tuned with live CreatorDB searches on 2026-10-25:
  * hashtags (the creator's own tags) AND CreatorDB topics (its per-creator
    model classification) was the most precise combination;
  * niches are hashtag-derived and noisy (a guitar channel carries
    id_aitool_Education; id_youtubeshorts_Education tags any channel posting
    Shorts), so they are used only in the broad preset, together with topics;
  * #faceless and #aishorts mostly match AI-content channels, whose viewers
    do not buy video tools, so they are left out;
  * mainLanguage follows a channel's titles, so channels that title in English
    but speak another language pass it; the spoken-language gate
    (rpi.qualification.language_gate) drops them before any paid research.
Values inside one filter are ORed; filters are ANDed.

The outside-range presets cover the sizes the main presets leave out (3k-10k
and 150k-500k subscribers), because size is a soft preference: an exceptionally
relevant creator should not be missed for size alone. They are stricter on
relevance (only the core segment hashtags) and on audience response.

Audience geography, measured with a live probe on 2026-10-31: `mainAudienceLocation`
is only the single largest audience country, so a channel with 30% US viewers and
the rest in Brazil and India passes it, and half of the creators deep analysis
paid to check failed the 50% English-speaking-audience gate. CreatorDB has no
filter on a country's share. The creator's own country is the cheap proxy that
works: adding `country` in US/UK/CA/AU/NZ/IE to the quality preset cut its pool
from 215 to 78 creators, and those 78 include none of the 13 creators the gate
has rejected and 8 of the 17 it passed. Creators with no country on CreatorDB
are left out. The autopilot searches only these `english_based` presets (11
filters, two credits per page); the others stay for manual runs.
`relatedCreators` (lookalike search) returned VALIDATION_ERROR with `=` and `in`.
"""

from __future__ import annotations

from typing import Any, Optional

from rpi.audience_intelligence.fit import ENGLISH_SPEAKING_COUNTRIES
from rpi.providers.base import CreatorSearchCriteria

EDUCATOR_HASHTAGS = (
    "aitools", "aitool", "aivideogenerator", "aivideoediting", "texttovideo", "aivoiceover",
    "facelessyoutube", "youtubeautomation", "youtubegrowth", "youtubetips", "aitoolsforcontentcreators",
    "videoediting",
)
TOPICS = (
    "id_ai_ScienceTechnology_youtube",
    "id_digitalcontentcreation_ScienceTechnology_youtube",
    "id_videoediting_FilmAnimation_youtube",
)
# Ids from CreatorDB GET /youtube/niches.
CORE_NICHES = (
    "id_aivideogenerator_ScienceTechnology",
    "id_aivideogenerator_Education",
    "id_aivideoediting_ScienceTechnology",
    "id_aitool_Education",
    "id_aitool_ScienceTechnology",
    "id_aitools2025_Education",
    "id_aiautomation_ScienceTechnology",
    "id_youtubemonetization_ScienceTechnology",
    "id_capcut_ScienceTechnology",
    "id_capcut_FilmAnimation",
)
# Income-focused niches promote tools heavily but include more hype channels.
INCOME_NICHES = (
    "id_makemoneyonline_Education",
    "id_passiveincome_Education",
    "id_sidehustle_Education",
    "id_affiliatemarketing_Education",
)
# The highest-priority segments only: faceless channels, AI Shorts and automated
# video creation, AI voiceover and editing. Broad AI-tool and general YouTube
# growth tags are left out.
CORE_HASHTAGS = (
    "aivideogenerator", "aivideoediting", "texttovideo", "aivoiceover", "facelessyoutube",
    "youtubeautomation", "aitoolsforcontentcreators",
)
RELEVANCE_FIELDS = ("keywords", "topics", "niches")

_GATES: dict[str, Any] = {
    "languages": ["en"],
    # The largest audience country only; English-speaking share is checked in deep analysis.
    "audience_countries": list(ENGLISH_SPEAKING_COUNTRIES),
    "min_subscribers": 10_000,
    "max_subscribers": 150_000,
    "min_median_views": 2_000,
    "min_engagement_rate": 0.02,
    "min_uploads_30d": 2,
    "require_email": True,  # no point paying to qualify a creator we cannot reach
    "sort_by": "median_views",
}

PRESETS: dict[str, dict[str, Any]] = {
    # 10 filters: one credit per page.
    "quality": {**_GATES, "keywords": list(EDUCATOR_HASHTAGS), "topics": list(TOPICS), "niches": []},
    "quality_broad": {**_GATES, "keywords": [], "topics": list(TOPICS), "niches": list(CORE_NICHES + INCOME_NICHES)},
    # Outside the 10k-150k band; also 10 filters. Small channels must show a
    # strong response (median views at least a tenth of the top of the band,
    # 4% engagement); large ones real reach (10k median views).
    "outside_range_small": {**_GATES, "keywords": list(CORE_HASHTAGS), "topics": list(TOPICS), "niches": [],
                            "min_subscribers": 3_000, "max_subscribers": 9_999,
                            "min_median_views": 1_000, "min_engagement_rate": 0.04},
    "outside_range_large": {**_GATES, "keywords": list(CORE_HASHTAGS), "topics": list(TOPICS), "niches": [],
                            "min_subscribers": 150_001, "max_subscribers": 500_000,
                            "min_median_views": 10_000, "min_engagement_rate": 0.03},
}
# The same presets for creators who live in an English-speaking country (11
# filters: two credits per page). quality_broad has no counterpart: its niches
# are noisy and its income niches draw audiences outside these countries.
_ENGLISH_BASED = {"countries": list(ENGLISH_SPEAKING_COUNTRIES)}
PRESETS |= {
    "english_based": {**PRESETS["quality"], **_ENGLISH_BASED},
    "english_based_small": {**PRESETS["outside_range_small"], **_ENGLISH_BASED},
    "english_based_large": {**PRESETS["outside_range_large"], **_ENGLISH_BASED},
}
# Autopilot search order: the outside-range sizes come last, so they are
# searched only after the main preset is exhausted.
AUTOPILOT_PRESETS = ("english_based", "english_based_small", "english_based_large")
DEFAULT_PRESET = "english_based"


def apply_preset(criteria: CreatorSearchCriteria, name: Optional[str]) -> CreatorSearchCriteria:
    """Fill every field the caller did not set explicitly from the preset."""
    if not name or name == "none":
        return criteria
    if name not in PRESETS:
        raise ValueError(f"unknown preset '{name}'; valid: {sorted(PRESETS)} or 'none'")
    explicit = criteria.model_fields_set
    # A caller who names keywords, topics or niches chose their own relevance filter.
    chose_relevance = any(f in explicit and getattr(criteria, f) for f in RELEVANCE_FIELDS)
    updates = {k: v for k, v in PRESETS[name].items()
               if k not in explicit and not (k in RELEVANCE_FIELDS and chose_relevance)}
    return criteria.model_copy(update=updates)


def describe_applied(criteria: CreatorSearchCriteria, applied: list[str]) -> str:
    """Human-readable summary of the filters a provider enforced."""
    parts = {
        "keywords": lambda c: "hashtags " + "/".join(c.keywords[:4]),
        "topics": lambda c: f"{len(c.topics)} topic(s)",
        "niches": lambda c: f"{len(c.niches)} niche(s)",
        "languages": lambda c: "language " + "/".join(c.languages),
        "countries": lambda c: "creator country " + "/".join(c.countries),
        "audience_countries": lambda c: "main audience " + "/".join(c.audience_countries),
        "audience_ages": lambda c: "audience age " + "/".join(c.audience_ages),
        "min_subscribers": lambda c: f">= {c.min_subscribers:,} subscribers",
        "max_subscribers": lambda c: f"<= {c.max_subscribers:,} subscribers",
        "min_avg_views": lambda c: f">= {c.min_avg_views:,} avg views",
        "min_median_views": lambda c: f">= {c.min_median_views:,} median views",
        "min_engagement_rate": lambda c: f">= {c.min_engagement_rate:.1%} engagement",
        "min_uploads_30d": lambda c: f">= {c.min_uploads_30d} uploads in 30 days",
        "active_within_days": lambda c: f"uploaded in the last {c.active_within_days} days",
        "require_email": lambda c: "has a contact email",
        "require_sponsorship_history": lambda c: "has done sponsorships",
    }
    return ", ".join(parts[a](criteria) for a in applied if a in parts)
