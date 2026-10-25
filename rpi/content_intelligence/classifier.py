"""
Claude niche intelligence and buyer-intent analysis.

Pipeline per creator:
  1. Free keyword prefilter (rpi.content_intelligence.prefilter). Clear
     results skip Claude entirely.
  2. Fast model labels each supplied video (multi-label taxonomy, audience
     problems, buyer-intent class, cited evidence).
  3. Escalate to the deep model only when the fast result is uncertain and
     the creator is far enough down the funnel to justify the cost.

The creator-level relevance used in scoring is computed deterministically
from per-video labels and taxonomy weights. The model's holistic relevance is
kept as a separate, clearly labeled AI judgment.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional

from rpi.administration.budget import Priority
from rpi.compliance.untrusted import DATA_HANDLING_RULES, wrap_items
from rpi.content_intelligence.claude_client import LLM, ModelPrice
from rpi.content_intelligence.prefilter import PrefilterResult, prefilter_creator
from rpi.content_intelligence.taxonomy import (
    AUDIENCE_PROBLEMS,
    BUYER_INTENT_CLASSES,
    LABEL_BY_KEY,
    LABEL_KEYS,
    LABELS,
    PRODUCT_CAPABILITIES,
    RELEVANT_LABELS,
    TAXONOMY_VERSION,
)

PROMPT_VERSION = "niche-v1"
MAX_VIDEOS_PER_CALL = 20
DESCRIPTION_CHARS = 700
TRANSCRIPT_CHARS = 2500


@dataclass
class VideoInput:
    ref: str  # rpi_creator_content.id or platform content id
    title: Optional[str]
    description: Optional[str]
    transcript_excerpt: Optional[str] = None
    url: Optional[str] = None


@dataclass
class VideoLabel:
    ref: str
    labels: list[str]
    relevance: float
    audience_problems: list[str]
    buyer_intent: Optional[str]
    note: str
    promotional: bool


@dataclass
class CreatorAnalysis:
    method: str  # keyword_prefilter | claude_fast | claude_deep
    model: Optional[str]
    taxonomy_version: str
    prompt_version: Optional[str]
    videos: list[VideoLabel]
    computed_relevance: float  # deterministic, used by scoring
    model_relevance: Optional[float]  # AI judgment, displayed separately
    primary_labels: list[str]
    audience_problem: Optional[str]
    product_capabilities: list[str]
    confidence: float
    uncertainty_reasons: list[str]
    evidence: list[dict]
    injection_flags: dict[str, list[str]] = field(default_factory=dict)
    cost_usd: Decimal = Decimal("0")
    dropped_references: list[str] = field(default_factory=list)
    error: Optional[str] = None


def analysis_tool() -> dict:
    return {
        "name": "report_creator_analysis",
        "description": "Report the niche classification of the supplied videos. This is the only output channel.",
        "input_schema": {
            "type": "object",
            "properties": {
                "videos": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string", "description": "id attribute of the untrusted_content block"},
                            "labels": {"type": "array", "items": {"type": "string", "enum": list(LABEL_KEYS)}},
                            "relevance": {"type": "number", "minimum": 0, "maximum": 1},
                            "audience_problems": {"type": "array", "items": {"type": "string", "enum": list(AUDIENCE_PROBLEMS)}},
                            "buyer_intent": {"type": "string", "enum": list(BUYER_INTENT_CLASSES)},
                            "promotional": {"type": "boolean"},
                            "note": {"type": "string", "description": "One short sentence grounded in the block text."},
                        },
                        "required": ["id", "labels", "relevance", "buyer_intent", "promotional", "note"],
                    },
                },
                "creator_summary": {
                    "type": "object",
                    "properties": {
                        "primary_labels": {"type": "array", "items": {"type": "string", "enum": list(LABEL_KEYS)}},
                        "reelforge_relevance": {"type": "number", "minimum": 0, "maximum": 1},
                        "audience_problem": {"type": "string"},
                        "addressable_problems": {"type": "array", "items": {"type": "string", "enum": list(AUDIENCE_PROBLEMS)}},
                        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                        "uncertainty_reasons": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["primary_labels", "reelforge_relevance", "confidence", "uncertainty_reasons"],
                },
            },
            "required": ["videos", "creator_summary"],
        },
    }


def system_prompt() -> str:
    taxonomy = "\n".join(f"- {l.key}: {l.description}" for l in LABELS)
    return (
        "You classify YouTube creator content for ReelForge AI, an AI short-form video creation product "
        "(faceless channels, AI Shorts, automated editing, voiceovers and scheduled publishing).\n\n"
        f"Taxonomy (multi-label):\n{taxonomy}\n\n"
        "Buyer-intent classes describe what the content suggests about the audience's commercial interest: "
        + ", ".join(BUYER_INTENT_CLASSES)
        + ". Positive tone is not purchase intent. A tutorial for beginners is usually general_educational_interest "
        "unless the content is about choosing or paying for tools.\n\n"
        "Rules: label only from the text supplied. If a video's text is too thin to judge, use label 'other', low "
        "relevance and say so in the note. Do not infer audience demographics or any sensitive personal "
        "characteristic. Confidence reflects how much text you had and how consistent the channel is.\n\n"
        + DATA_HANDLING_RULES
    )


def _clamp(v: Any, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        return max(lo, min(hi, float(v)))
    except (TypeError, ValueError):
        return lo


def computed_relevance(videos: list[VideoLabel]) -> float:
    """Deterministic creator relevance: mean over videos of the best taxonomy
    weight among each video's labels."""
    if not videos:
        return 0.0
    total = 0.0
    for v in videos:
        weights = [LABEL_BY_KEY[l].reelforge_relevance for l in v.labels if l in LABEL_BY_KEY]
        total += max(weights) if weights else 0.0
    return round(total / len(videos), 3)


def validate_report(report: dict, supplied_ids: list[str]) -> tuple[list[VideoLabel], dict, list[str]]:
    supplied = set(supplied_ids)
    videos: list[VideoLabel] = []
    dropped: list[str] = []
    seen: set[str] = set()
    for item in report.get("videos") or []:
        if not isinstance(item, dict):
            continue
        ref = str(item.get("id", ""))
        if ref not in supplied or ref in seen:
            dropped.append(ref)
            continue
        seen.add(ref)
        labels = [l for l in item.get("labels") or [] if l in LABEL_KEYS] or ["other"]
        intent = item.get("buyer_intent") if item.get("buyer_intent") in BUYER_INTENT_CLASSES else None
        problems = [p for p in item.get("audience_problems") or [] if p in AUDIENCE_PROBLEMS]
        videos.append(
            VideoLabel(
                ref=ref,
                labels=labels[:4],
                relevance=_clamp(item.get("relevance")),
                audience_problems=problems[:4],
                buyer_intent=intent,
                note=str(item.get("note", ""))[:300],
                promotional=bool(item.get("promotional", False)),
            )
        )
    summary = report.get("creator_summary") or {}
    clean_summary = {
        "primary_labels": [l for l in summary.get("primary_labels") or [] if l in LABEL_KEYS][:3],
        "reelforge_relevance": _clamp(summary.get("reelforge_relevance")),
        "audience_problem": str(summary.get("audience_problem") or "")[:300] or None,
        "addressable_problems": [p for p in summary.get("addressable_problems") or [] if p in AUDIENCE_PROBLEMS],
        "confidence": _clamp(summary.get("confidence")),
        "uncertainty_reasons": [str(r)[:200] for r in (summary.get("uncertainty_reasons") or [])][:6],
    }
    return videos, clean_summary, dropped


def from_prefilter(pre: PrefilterResult, videos: list[VideoInput]) -> CreatorAnalysis:
    labels = []
    by_ref = {v.ref: v for v in videos}
    for m in pre.videos:
        keys = list(m.labels.keys()) or ["other"]
        labels.append(VideoLabel(m.content_ref, keys, 0.0, [], None, "keyword match: " + ", ".join(
            t for terms in m.labels.values() for t in terms[:2]) if m.labels else "no keyword match", False))
    evidence = [
        {"kind": "content", "ref": m.content_ref, "url": by_ref.get(m.content_ref).url if by_ref.get(m.content_ref) else None,
         "excerpt": (by_ref[m.content_ref].title or "")[:140] if m.content_ref in by_ref else None, "labels": list(m.labels)}
        for m in pre.videos if set(m.labels) & RELEVANT_LABELS
    ][:6]
    primary = [k for k, _ in pre.label_counts.most_common(3)]
    return CreatorAnalysis(
        method="keyword_prefilter",
        model=None,
        taxonomy_version=TAXONOMY_VERSION,
        prompt_version=None,
        videos=labels,
        computed_relevance=computed_relevance(labels),
        model_relevance=None,
        primary_labels=primary,
        audience_problem=None,
        product_capabilities=[],
        confidence=0.75 if pre.decision != "needs_llm" else 0.3,
        uncertainty_reasons=["keyword rules only; no semantic analysis"] + pre.reasons,
        evidence=evidence,
    )


async def analyze_creator(
    llm: Optional[LLM],
    videos: list[VideoInput],
    *,
    fast: ModelPrice,
    deep: Optional[ModelPrice] = None,
    allow_deep: bool = False,
    force_llm: bool = False,
    job_id: Optional[str] = None,
    creator_id: Optional[str] = None,
    priority: Priority = Priority.NORMAL,
    escalate_below_confidence: float = 0.6,
) -> CreatorAnalysis:
    videos = videos[:MAX_VIDEOS_PER_CALL]
    pre = prefilter_creator([(v.ref, v.title, v.description) for v in videos])
    if llm is None or (pre.decision != "needs_llm" and not force_llm):
        result = from_prefilter(pre, videos)
        if llm is None and pre.decision == "needs_llm":
            result.uncertainty_reasons.append("Claude not configured; semantic analysis skipped")
        return result

    analysis = await _llm_pass(llm, videos, fast, "claude_fast", job_id, creator_id, priority)
    if analysis.error:
        fallback = from_prefilter(pre, videos)
        fallback.uncertainty_reasons.append(f"Claude analysis unavailable: {analysis.error}")
        fallback.cost_usd = analysis.cost_usd
        return fallback

    disagreement = (
        abs((analysis.model_relevance or 0) - analysis.computed_relevance) if analysis.model_relevance is not None else 0
    )
    if deep and allow_deep and (analysis.confidence < escalate_below_confidence or disagreement > 0.3):
        deeper = await _llm_pass(llm, videos, deep, "claude_deep", job_id, creator_id, priority)
        if not deeper.error:
            deeper.cost_usd += analysis.cost_usd
            deeper.uncertainty_reasons.append(
                f"escalated from fast model (confidence {analysis.confidence:.2f}, disagreement {disagreement:.2f})"
            )
            return deeper
        analysis.uncertainty_reasons.append(f"deep analysis unavailable: {deeper.error}")
        analysis.cost_usd += deeper.cost_usd
    return analysis


async def _llm_pass(llm, videos, price, method, job_id, creator_id, priority) -> CreatorAnalysis:
    items = []
    for v in videos:
        parts = [f"TITLE: {v.title or ''}", f"DESCRIPTION: {(v.description or '')[:DESCRIPTION_CHARS]}"]
        if v.transcript_excerpt:
            parts.append(f"TRANSCRIPT EXCERPT (machine-generated, may contain errors): {v.transcript_excerpt[:TRANSCRIPT_CHARS]}")
        items.append((v.ref, "\n".join(parts)))
    wrapped = wrap_items(items)
    user = (
        f"Classify these {len(videos)} videos from one creator. Report every video id.\n\n" + wrapped.text
    )
    result = await llm.call_tool(
        price=price,
        system=system_prompt(),
        user=user,
        tool=analysis_tool(),
        max_tokens=4000,
        job_id=job_id,
        creator_id=creator_id,
        priority=priority,
    )
    if not result.ok or result.data is None:
        return _error_analysis(method, price.model, result.error or "unknown error", result.cost_usd)

    labels, summary, dropped = validate_report(result.data, wrapped.ids)
    by_ref = {v.ref: v for v in videos}
    evidence = [
        {"kind": "content", "ref": vl.ref, "url": by_ref[vl.ref].url, "excerpt": (by_ref[vl.ref].title or "")[:140],
         "labels": vl.labels, "note": vl.note}
        for vl in sorted(labels, key=lambda x: x.relevance, reverse=True)
        if set(vl.labels) & RELEVANT_LABELS
    ][:6]
    reasons = list(summary["uncertainty_reasons"])
    if len(labels) < len(videos):
        reasons.append(f"model reported {len(labels)} of {len(videos)} videos")
    if dropped:
        reasons.append(f"discarded {len(dropped)} references to ids that were not supplied")
    if wrapped.injection_flags:
        reasons.append(f"instruction-like text found in {len(wrapped.injection_flags)} item(s); treated as data")
    capabilities = [PRODUCT_CAPABILITIES[p] for p in summary["addressable_problems"] if p in PRODUCT_CAPABILITIES]
    return CreatorAnalysis(
        method=method,
        model=price.model,
        taxonomy_version=TAXONOMY_VERSION,
        prompt_version=PROMPT_VERSION,
        videos=labels,
        computed_relevance=computed_relevance(labels),
        model_relevance=summary["reelforge_relevance"],
        primary_labels=summary["primary_labels"],
        audience_problem=summary["audience_problem"],
        product_capabilities=capabilities,
        confidence=summary["confidence"],
        uncertainty_reasons=reasons,
        evidence=evidence,
        injection_flags=wrapped.injection_flags,
        cost_usd=result.cost_usd,
        dropped_references=dropped,
    )


def _error_analysis(method: str, model: str, error: str, cost: Decimal) -> CreatorAnalysis:
    return CreatorAnalysis(
        method=method, model=model, taxonomy_version=TAXONOMY_VERSION, prompt_version=PROMPT_VERSION, videos=[],
        computed_relevance=0.0, model_relevance=None, primary_labels=[], audience_problem=None,
        product_capabilities=[], confidence=0.0, uncertainty_reasons=[error], evidence=[], cost_usd=cost, error=error,
    )


def buyer_intent_summary(videos: list[VideoLabel]) -> dict:
    """Qualitative distribution of buyer-intent classes across content.
    Content-inferred; describes what creators discuss, not measured viewer intent."""
    counts = {c: 0 for c in BUYER_INTENT_CLASSES}
    for v in videos:
        if v.buyer_intent:
            counts[v.buyer_intent] += 1
    return {
        "basis": "content_inferred",
        "counts": counts,
        "caveat": "Classified from creator content. Commenters and topics are a biased proxy for the audience; "
        "validate against actual campaign conversions before relying on this.",
    }
