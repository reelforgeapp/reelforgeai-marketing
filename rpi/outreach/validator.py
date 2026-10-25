"""
Outreach draft validator.

Blocks the failure modes that matter for creator outreach:
  * fabricated personal familiarity ("I just watched...", "big fan")
  * unverified income or results claims ("creators earn $1,000+/month")
  * commission terms other than the configured program terms
  * numbers that do not appear in the grounding facts
  * email addresses or URLs that were not supplied
  * drafts that reference no specific, verified piece of the creator's content

The validator runs at draft creation, at approval and again immediately
before sending.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

FAMILIARITY_PATTERNS = [
    r"\bI(?:'ve| have)? (?:just |recently )?(?:watched|seen|binged)\b",
    r"\bI(?:'ve| have) been (?:following|watching|a fan)",
    r"\b(?:big|huge|long[- ]?time) fan\b",
    r"\blong[- ]?time (?:viewer|subscriber)\b",
    r"\bI (?:love|loved|enjoy|enjoyed) (?:your|watching)\b",
    r"\bas (?:a|one of your) (?:subscriber|viewer)s?\b",
]
INCOME_PATTERNS = [
    r"\$\s?\d[\d,]*(?:\.\d+)?\s?(?:k\b)?\+?\s?(?:/|per|a)\s?(?:mo|month|week|year)",
    r"\b(?:creators?|partners?|affiliates?)\s+(?:are\s+)?(?:earning|making|generating|earn|make)\b",
    r"\bpassive income\b",
    r"\bguarantee[ds]?\b",
]
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
URL_RE = re.compile(r"https?://[^\s)>\]]+")
NUMBER_RE = re.compile(r"(?<![\w.])\$?\d[\d,]*(?:\.\d+)?%?")
PERCENT_RE = re.compile(r"(\d+(?:\.\d+)?)\s?%")


@dataclass
class ValidationResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {"ok": self.ok, "errors": self.errors, "warnings": self.warnings}


@dataclass
class Grounding:
    content_titles: list[str]
    allowed_numbers: set[str]
    allowed_urls: set[str]
    commission_rate_pct: str  # e.g. "30"
    sender_email: str | None = None


def _norm_number(token: str) -> str:
    return token.replace("$", "").replace(",", "").rstrip("%").rstrip(".")


def validate_draft(subject: str, body: str, grounding: Grounding, *, max_words: int = 220) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []
    text = f"{subject}\n{body}"

    for p in FAMILIARITY_PATTERNS:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            errors.append(f"claims personal familiarity: '{m.group(0)}'")
    for p in INCOME_PATTERNS:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            errors.append(f"unverified income or results claim: '{m.group(0)}'")

    for m in PERCENT_RE.finditer(text):
        if _norm_number(m.group(1)) != _norm_number(grounding.commission_rate_pct):
            errors.append(f"percentage {m.group(0)} differs from program commission {grounding.commission_rate_pct}%")

    allowed = {_norm_number(n) for n in grounding.allowed_numbers} | {_norm_number(grounding.commission_rate_pct)}
    for m in NUMBER_RE.finditer(text):
        token = _norm_number(m.group(0))
        if not token or token in allowed:
            continue
        if re.fullmatch(r"20[2-3]\d", token):  # years
            continue
        if len(token) == 1:  # "1 video", "2 ideas"
            continue
        errors.append(f"number '{m.group(0)}' is not in the verified facts")

    for email in EMAIL_RE.findall(text):
        if not grounding.sender_email or email.lower() != grounding.sender_email.lower():
            errors.append(f"contains an email address that was not supplied: {email}")
    for url in URL_RE.findall(text):
        if url.rstrip(".,") not in grounding.allowed_urls:
            errors.append(f"contains a URL that was not supplied: {url}")

    lowered = text.lower()
    if grounding.content_titles and not any(t and t.lower()[:40] in lowered for t in grounding.content_titles):
        errors.append("does not reference any specific verified video of the creator")

    words = len(body.split())
    if words > max_words:
        errors.append(f"body is {words} words (max {max_words})")
    if not subject.strip():
        errors.append("subject is empty")
    if len(subject) > 120:
        warnings.append("subject is long")
    return ValidationResult(not errors, errors, warnings)
