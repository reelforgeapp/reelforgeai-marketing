"""
ReelForge niche taxonomy (versioned).

Changing labels, weights or keyword rules requires bumping TAXONOMY_VERSION so
stored classifications stay comparable and evaluation results stay tied to a
specific rule set.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

TAXONOMY_VERSION = "2026-10-25.2"


@dataclass(frozen=True)
class Label:
    key: str
    name: str
    reelforge_relevance: float  # prior weight of this topic for ReelForge
    description: str
    patterns: tuple[str, ...]


LABELS: tuple[Label, ...] = (
    Label(
        "faceless_youtube_automation", "Faceless YouTube automation", 1.0,
        "Running channels without appearing on camera; cash-cow / automation channels.",
        (r"\bfaceless\b", r"cash\s*cow", r"youtube\s+automation", r"automated?\s+(youtube\s+)?channels?",
         r"without\s+showing\s+(your\s+)?face", r"no[\s-]face\s+channel"),
    ),
    Label(
        "ai_shorts_creation", "AI Shorts creation", 1.0,
        "Producing YouTube Shorts / TikToks / Reels with AI tools.",
        (r"\bai\s+(youtube\s+)?shorts\b", r"shorts\s+(with|using)\s+ai", r"\bai\s+(tiktoks?|reels)\b",
         r"short[\s-]form\s+(video|content)\s+(with|using)\s+ai", r"viral\s+shorts?\s+(with|using)\s+ai",
         r"(text|script|prompt)\s+to\s+(shorts?|reels?)"),
    ),
    Label(
        "ai_video_tools", "AI video tools", 0.9,
        "Reviews, tutorials and comparisons of AI video generation/editing tools.",
        (r"\bai\s+video\s+(generator|maker|editor|editing|tool)s?\b", r"text[\s-]to[\s-]video",
         r"\b(invideo|pictory|fliki|opus\s*clip|zebracat|revid|crayo|autoshorts|submagic|vizard|klap|heygen|synthesia)\b",
         # Generators and workflows named in live CreatorDB results (2026-10-25).
         r"\b(seedance|kling|higgsfield|hailuo|pixverse|vidu|dreamina|kaiber|runway\s*ml|luma\s+(ai|dream\s+machine)|"
         r"veo\s*\d|sora\s*\d?|wan\s*2|flova|nano\s+banana)\b",
         r"\bai\s+(videos?|films?|filmmaking|animations?|movies?|music\s+videos?|cartoons?|documentar(y|ies))\b",
         r"(animation|videos?|documentar(y|ies)|cartoons?)\s+(with|using)\s+ai\b"),
    ),
    Label(
        "ai_voice_generation", "AI voice generation", 0.7,
        "AI voiceovers, text-to-speech and voice cloning.",
        (r"\bai\s+voice(over)?s?\b", r"text[\s-]to[\s-]speech", r"voice\s+clon", r"\belevenlabs?\b", r"eleven\s+labs"),
    ),
    Label(
        "social_media_content_automation", "Social media content automation", 0.8,
        "Scheduling, auto-posting and repurposing content across platforms.",
        (r"auto[\s-]?post", r"schedul\w+\s+(posts?|content|videos?)", r"repurpos", r"content\s+automation",
         r"automat\w+\s+(social\s+media|instagram|tiktok)"),
    ),
    Label(
        "creator_monetization", "Creator monetization", 0.6,
        "Earning money from content: YPP, RPM, affiliate income, side hustles.",
        (r"monetiz", r"\brpm\b", r"partner\s+program", r"passive\s+income", r"make\s+money\s+(on|with|from)\s+youtube",
         r"side\s+hustle"),
    ),
    Label(
        "general_ai_news", "General AI news", 0.35,
        "AI industry news and general AI tool roundups.",
        (r"\bai\s+news\b", r"\b(gpt|chatgpt|openai|gemini|claude|llm)\b", r"\bai\s+tools?\b"),
    ),
    Label(
        "unrelated_technical", "Unrelated technical topics", 0.05,
        "Programming, IT, hardware and other technical content outside video creation.",
        (r"\b(python|javascript|kubernetes|docker|sql|linux|networking|cybersecurity)\b",),
    ),
    Label(
        "entertainment", "Entertainment content", 0.05,
        "Gaming, vlogs, comedy, music and other entertainment.",
        (r"\b(gameplay|let'?s\s+play|vlog|prank|reaction|music\s+video|minecraft|fortnite)\b",),
    ),
    Label("other", "Other", 0.1, "Anything else.", ()),
)

LABEL_KEYS = tuple(l.key for l in LABELS)
LABEL_BY_KEY = {l.key: l for l in LABELS}
RELEVANT_LABELS = {l.key for l in LABELS if l.reelforge_relevance >= 0.6}

COMPILED = {l.key: [re.compile(p, re.IGNORECASE) for p in l.patterns] for l in LABELS}

BUYER_INTENT_CLASSES = (
    "explicit_product_shopping_interest",
    "specific_workflow_pain_point",
    "general_educational_interest",
    "curiosity_without_purchase_intent",
    "negative_or_unsuitable_fit",
)

AUDIENCE_PROBLEMS = (
    "difficulty_creating_videos_consistently",
    "expensive_video_editing",
    "high_production_time",
    "limited_technical_skills",
    "difficulty_producing_vertical_content",
    "need_automated_publishing",
    "voiceover_problems",
    "operating_multiple_channels",
    "need_original_content",
    "software_pricing_or_alternatives",
)

# ReelForge capabilities that address audience problems. Based on features
# present in the reelforge-ai codebase (video generation, scheduling, social
# publishing, voice clones; plan prices from backend/config/tiers.js).
# Review whenever the product changes; outreach drafts may only cite these.
PRODUCT_CAPABILITIES = {
    "difficulty_creating_videos_consistently": "Generates complete short-form videos from a prompt or niche",
    "expensive_video_editing": "Automated editing, captions and assembly without an editor",
    "high_production_time": "Video produced in minutes instead of hours",
    "limited_technical_skills": "No editing skills required",
    "difficulty_producing_vertical_content": "Native vertical Shorts/Reels/TikTok output",
    "need_automated_publishing": "Scheduling and social publishing",
    "voiceover_problems": "Built-in AI voiceovers and voice clones",
    "operating_multiple_channels": "Series and scheduling for multiple channels",
    "need_original_content": "Original scripts and visuals per video",
    "software_pricing_or_alternatives": "Plans from $17/month",
}
