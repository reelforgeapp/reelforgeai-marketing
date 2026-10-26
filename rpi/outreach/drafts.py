"""
Outreach draft generation.

Drafts are built only from verified research: specific video titles stored in
rpi_creator_content, the niche classification, the configured affiliate
program terms and the product capability list. Claude writes the prose
through a forced tool call; a deterministic template is used when Claude is
unavailable. Every draft is validated and stored with its grounding so the
approver can see exactly which facts it rests on. Generation never sends.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Optional

from rpi.compliance.untrusted import DATA_HANDLING_RULES, wrap_items
from rpi.content_intelligence.claude_client import LLM, ModelPrice
from rpi.content_intelligence.taxonomy import LABEL_BY_KEY
from rpi.core.audit import audit
from rpi.outreach.validator import INCOME_PATTERNS, MENTION_RE, NUMBER_RE, Grounding, hidden_characters, validate_draft

AFFILIATE_INFO_URL = "https://www.reelforgeai.io/become-affiliate"
PRODUCT_URL = "https://reelforgeai.io"


@dataclass
class DraftContext:
    creator_id: str
    creator_name: Optional[str]
    videos: list[dict]  # {ref, title, url}
    primary_labels: list[str]
    capabilities: list[str]
    commission_rate_pct: str
    commission_duration_text: str
    proposition: str  # e.g. "paid sponsorship" | "affiliate partnership"
    request_audience_data: bool
    sender_name: str
    # Direct competitors with a recent paid deal; the email asks about exclusivity.
    competitor_deals: tuple[str, ...] = ()
    # Competitor names and aliases; videos about them are not quoted when others exist.
    avoid_title_terms: tuple[str, ...] = ()
    # Month of the last Partner Scout email ("February 2026"); drafts then read
    # as a follow-up. Partner Scout does not record replies, so drafts never
    # claim the creator did not answer.
    prior_contact_month: Optional[str] = None


def build_grounding(ctx: DraftContext, sender_email: Optional[str]) -> Grounding:
    numbers: set[str] = set()
    for v in ctx.videos:
        numbers.update(NUMBER_RE.findall(v.get("title") or ""))
    numbers.update(NUMBER_RE.findall(ctx.commission_duration_text))
    numbers.update(NUMBER_RE.findall(ctx.creator_name or ""))  # the greeting uses the channel name
    numbers.update({"17"})  # lowest paid plan price (tiers.js)
    return Grounding(
        content_titles=[v["title"] for v in ctx.videos if v.get("title")],
        allowed_numbers=numbers,
        allowed_urls={AFFILIATE_INFO_URL, PRODUCT_URL},
        commission_rate_pct=ctx.commission_rate_pct,
        sender_email=sender_email,
    )


def citable_videos(ctx: DraftContext) -> list[dict]:
    """Videos the email may quote: never a title with an income claim (quoting
    it reads as endorsing it), one that names another channel or account, or one
    with hidden formatting characters; and, when possible, not one about a competitor."""
    clean = [v for v in ctx.videos if v.get("title")
             and not any(re.search(p, v["title"], re.IGNORECASE) for p in INCOME_PATTERNS)
             and not MENTION_RE.search(v["title"]) and not hidden_characters(v["title"])]
    avoid = [re.compile(rf"(?<![\w]){re.escape(t)}(?![\w])", re.IGNORECASE) for t in ctx.avoid_title_terms if t]
    neutral = [v for v in clean if not any(p.search(v["title"]) for p in avoid)]
    return neutral or clean


def _topic(ctx: DraftContext) -> str:
    key = ctx.primary_labels[0] if ctx.primary_labels else None
    name = LABEL_BY_KEY[key].name if key in LABEL_BY_KEY else "AI video creation"
    first, _, rest = name.partition(" ")
    return (first if first.isupper() else first.lower()) + (" " + rest if rest else "")


def template_draft(ctx: DraftContext) -> tuple[str, str]:
    # Channel names are not personal names, so greet the channel as named.
    name = (ctx.creator_name or "").replace("[SYNTHETIC]", "").strip() or "there"
    videos = citable_videos(ctx)
    video = videos[0] if videos else None
    subject = f"Partnership idea for your {_topic(ctx)} content"
    # One idea per paragraph, separated by blank lines; the HTML version
    # (rpi.outreach.sender.render_html) keeps the same paragraphs.
    paragraphs = [f"Hi {name},"]
    if ctx.prior_contact_month:
        paragraphs.append(f"We emailed you about ReelForge AI in {ctx.prior_contact_month}, so this is one short "
                          "follow-up with specific terms.")
    intro = []
    if video and ctx.prior_contact_month:
        intro.append(f"Your video \"{video['title']}\" is closely aligned with what ReelForge AI does.")
    elif video:
        intro.append(f"Your video \"{video['title']}\" is closely aligned with what ReelForge AI does, which is why I'm reaching out.")
    intro.append("ReelForge AI turns a prompt or niche into finished short-form videos, including script, visuals, "
                 "voiceover, captions and scheduled publishing.")
    paragraphs.append(" ".join(intro))
    if ctx.proposition == "affiliate partnership":
        offer = [f"Our affiliate program pays {ctx.commission_rate_pct}% recurring commission {ctx.commission_duration_text}."]
    else:
        offer = [f"We'd like to discuss a sponsored integration, and our affiliate program "
                 f"({ctx.commission_rate_pct}% recurring {ctx.commission_duration_text}) can be part of it."]
    offer += [f"One of your recent videos was sponsored by {brand}. Do you have a category exclusivity with "
              f"{brand}, and if so, when does it end?" for brand in ctx.competitor_deals]
    paragraphs.append(" ".join(offer))
    if ctx.request_audience_data:
        paragraphs.append("If you're open to it, a recent screenshot of your audience geography and age breakdown "
                          "would help us propose fair terms.")
    paragraphs += ["Would you be open to a short call or a reply with your current sponsorship rates?",
                   f"Program details: {AFFILIATE_INFO_URL}", ctx.sender_name]
    return subject, "\n\n".join(paragraphs)


def draft_tool() -> dict:
    return {
        "name": "write_outreach_email",
        "description": "Return the outreach email. This is the only output channel.",
        "input_schema": {
            "type": "object",
            "properties": {
                "subject": {"type": "string", "maxLength": 110},
                "body": {"type": "string", "description": "Plain text, under 200 words."},
            },
            "required": ["subject", "body"],
        },
    }


def draft_system_prompt() -> str:
    return (
        "You write first-contact partnership emails from ReelForge AI to YouTube creators. Plain text, under "
        "180 words, warm and specific, no hype.\n"
        "Hard rules (drafts that break them are rejected automatically):\n"
        "- Do not claim the sender watched, follows or is a fan of the creator. Refer to their videos factually "
        "(\"Your video 'X' covers ...\").\n"
        "- Mention at least one supplied video title verbatim.\n"
        "- Do not state income, earnings, results or guarantees.\n"
        "- Only use numbers, percentages and URLs given in FACTS.\n"
        "- Do not include any email address.\n"
        "- Sign with the sender name given in FACTS.\n"
        "- If FACTS lists ask_about_exclusivity_with, say that a recent video was sponsored by that brand and ask "
        "whether the creator has a category exclusivity with it and when it ends.\n"
        "- If FACTS gives previous_contact, open by saying briefly that we wrote before and this is a short follow-up. "
        "Never present the email as a first contact.\n\n" + DATA_HANDLING_RULES
    )


async def generate(llm: Optional[LLM], price: Optional[ModelPrice], ctx: DraftContext,
                   *, sender_email: Optional[str]) -> dict:
    grounding = build_grounding(ctx, sender_email)
    generator = "template"
    subject, body = template_draft(ctx)
    llm_error = None
    if llm is not None and price is not None:
        facts = {
            "sender_name": ctx.sender_name,
            "proposition": ctx.proposition,
            "commission": f"{ctx.commission_rate_pct}% recurring {ctx.commission_duration_text}",
            "urls": [AFFILIATE_INFO_URL, PRODUCT_URL],
            "product_capabilities": ctx.capabilities or [
                "Generates complete short-form videos from a prompt or niche",
                "Automated editing, captions and AI voiceovers",
                "Scheduling and social publishing",
            ],
            "creator_topics": [LABEL_BY_KEY[l].name for l in ctx.primary_labels if l in LABEL_BY_KEY],
            "request_audience_data": ctx.request_audience_data,
            "ask_about_exclusivity_with": list(ctx.competitor_deals),
            "previous_contact": (f"ReelForge AI emailed this creator before, last in {ctx.prior_contact_month}; "
                                 "whether they replied is unknown, so do not say they did not") if ctx.prior_contact_month else None,
        }
        wrapped = wrap_items([(v["ref"], f"VIDEO TITLE: {v['title']}") for v in (citable_videos(ctx) or ctx.videos)[:5]])
        user = "FACTS (trusted, from ReelForge):\n" + json.dumps(facts, indent=2) + \
               "\n\nCreator videos (untrusted titles):\n" + wrapped.text
        result = await llm.call_tool(price=price, system=draft_system_prompt(), user=user, tool=draft_tool(),
                                     max_tokens=700, creator_id=ctx.creator_id)
        if result.ok and result.data:
            cand_subject = str(result.data.get("subject", ""))[:120]
            cand_body = str(result.data.get("body", ""))[:3000]
            check = validate_draft(cand_subject, cand_body, grounding)
            if check.ok:
                subject, body, generator = cand_subject, cand_body, f"claude:{price.model}"
            else:
                llm_error = "claude draft failed validation: " + "; ".join(check.errors[:3])
        else:
            llm_error = result.error
    validation = validate_draft(subject, body, grounding)
    return {
        "subject": subject,
        "body": body,
        "generator": generator,
        "validation": validation.as_dict() | ({"llm_note": llm_error} if llm_error else {}),
        "grounding": {
            "videos": ctx.videos[:5],
            "allowed_numbers": sorted(grounding.allowed_numbers),
            "allowed_urls": sorted(grounding.allowed_urls),
            "commission_rate_pct": grounding.commission_rate_pct,
        },
    }


def draft_key(creator_id, subject: str, body: str) -> str:
    """Idempotency key of a generated draft. Edits keep the stored key, so a
    draft whose text no longer matches its key was changed by a person."""
    return hashlib.sha256(f"{creator_id}:{subject}:{body}".encode()).hexdigest()


async def save_draft(conn, creator_id, contact_id, draft: dict, actor: str) -> str:
    key = draft_key(creator_id, draft["subject"], draft["body"])
    draft_id = await conn.fetchval(
        """INSERT INTO rpi_outreach_drafts
           (creator_id, contact_id, subject, body_text, grounding, validation, generator, status, created_by, idempotency_key)
           VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,'draft',$8,$9)
           ON CONFLICT (idempotency_key) DO UPDATE SET validation = EXCLUDED.validation
           RETURNING id""",
        creator_id, contact_id, draft["subject"], draft["body"], json.dumps(draft["grounding"], default=str),
        json.dumps(draft["validation"]), draft["generator"], actor, key,
    )
    await audit(conn, actor, "outreach.draft_created", "outreach_draft", draft_id,
                {"creator_id": str(creator_id), "generator": draft["generator"], "valid": draft["validation"]["ok"]})
    return str(draft_id)


def regrounding_from_row(row: dict, sender_email: Optional[str]) -> Grounding:
    g = row["grounding"] if isinstance(row["grounding"], dict) else json.loads(row["grounding"])
    return Grounding(
        content_titles=[v.get("title") for v in g.get("videos", []) if v.get("title")],
        allowed_numbers=set(g.get("allowed_numbers", [])),
        allowed_urls=set(g.get("allowed_urls", [])),
        commission_rate_pct=str(g.get("commission_rate_pct", "30")),
        sender_email=sender_email,
    )


def strip_unsafe(text: str) -> str:
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
