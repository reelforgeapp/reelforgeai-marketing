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
  * mainLanguage is unreliable for bilingual channels; content screening has
    to confirm language.
Values inside one filter are ORed; filters are ANDed.
"""

from __future__ import annotations

from typing import Any, Optional

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
RELEVANCE_FIELDS = ("keywords", "topics", "niches")

_GATES: dict[str, Any] = {
    "languages": ["en"],
    # Where the viewers are, not where the creator lives (often blank).
    "audience_countries": ["US", "CA", "GB", "AU"],
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
}
DEFAULT_PRESET = "quality"


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
