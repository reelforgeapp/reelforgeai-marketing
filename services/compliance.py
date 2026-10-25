"""
ReelForge Marketing Engine - Outreach compliance (CAN-SPAM / FTC)

Every outreach email must carry the sender's identity, a valid physical postal
address and a working way to opt out, and must not make unverifiable income
claims or pretend the sender personally watched the creator's content.

- compliance_problems(): reasons sending must be refused (fail closed)
- finalize_email(): appends the footer and returns List-Unsubscribe headers
- unsubscribe_token()/verify_unsubscribe_token(): signed per-prospect links
- content_violations(): rejects AI-written copy that makes forbidden claims
"""

import hashlib
import hmac
import html
import re
from typing import Optional

DEFAULT_ADMIN_API_KEY = "change-me-in-production"

FAMILIARITY_PATTERNS = [
    r"\bI(?:'ve| have)? (?:just |recently )?(?:watched|seen|binged)\b",
    r"\bI(?:'ve| have) been (?:following|watching|a fan)",
    r"\b(?:big|huge|long[- ]?time) fan\b",
    r"\bI (?:love|loved|enjoy|enjoyed) (?:your|watching)\b",
    r"\b(?:really |truly )?(?:love|loved|enjoy|enjoyed) (?:what you(?:'re| are)? (?:doing|making|creating)|your (?:content|videos?|channel|style))",
    r"\bgenuinely impressed\b",
]
INCOME_PATTERNS = [
    r"\$\s?\d[\d,]*(?:\.\d+)?\s?k?\+?\s*(?:/\s*|per\s+|a\s+|an\s+|each\s+)?"
    r"(?:mo\b|month|monthly|week|weekly|year|yearly|annually)",
    r"\b(?:seeing|making|earning|earn|make|generated|generating|hit)\s+(?:over\s+|up\s+to\s+|more\s+than\s+)?\$\s?\d",
    r"\b(?:creators?|partners?|affiliates?)\s+(?:are\s+|have\s+)?(?:earning|making|generating|generated|earned|made)\b",
    r"\bpassive income\b",
    r"\bguarantee[ds]?\b",
    r"\bspots? (?:are )?filling up\b",
]


def _signing_key(settings) -> Optional[bytes]:
    secret = settings.unsubscribe_secret or ""
    if not secret:
        admin = settings.admin_api_key or ""
        if admin and admin != DEFAULT_ADMIN_API_KEY:
            secret = admin
    if len(secret) < 16:
        return None
    # Domain-separated so the admin key itself is never usable as a link token.
    return hmac.new(secret.encode(), b"partner-scout-unsubscribe-v1", hashlib.sha256).digest()


def unsubscribe_token(settings, prospect_id: str) -> Optional[str]:
    key = _signing_key(settings)
    if key is None:
        return None
    return hmac.new(key, str(prospect_id).encode(), hashlib.sha256).hexdigest()[:40]


def verify_unsubscribe_token(settings, prospect_id: str, token: str) -> bool:
    expected = unsubscribe_token(settings, prospect_id)
    return bool(expected and token) and hmac.compare_digest(expected, token)


def unsubscribe_url(settings, prospect_id: str) -> Optional[str]:
    token = unsubscribe_token(settings, prospect_id)
    if token is None or not settings.public_base_url:
        return None
    return f"{settings.public_base_url.rstrip('/')}/unsubscribe/{prospect_id}?t={token}"


def compliance_problems(settings) -> list[str]:
    problems = []
    if len((settings.company_postal_address or "").strip()) < 10:
        problems.append("COMPANY_POSTAL_ADDRESS is not set (CAN-SPAM requires a valid physical postal address)")
    if _signing_key(settings) is None:
        problems.append("no unsubscribe signing key (set UNSUBSCRIBE_SECRET or ADMIN_API_KEY, 16+ chars)")
    if not settings.public_base_url:
        problems.append("PUBLIC_BASE_URL is not set (needed for unsubscribe links)")
    return problems


def content_violations(*texts: str) -> list[str]:
    text = "\n".join(t for t in texts if t)
    found = []
    for pattern in FAMILIARITY_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            found.append(f"claims personal familiarity: '{m.group(0)}'")
    for pattern in INCOME_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            found.append(f"unverified earnings or urgency claim: '{m.group(0)}'")
    return found


def finalize_email(settings, prospect_id: str, html_body: str, text_body: Optional[str]) -> tuple[str, str, dict]:
    """Append the compliance footer. Callers must check compliance_problems() first."""
    url = unsubscribe_url(settings, prospect_id)
    sender = settings.brevo_sender_name or "ReelForge AI"
    company = settings.company_legal_name
    address = settings.company_postal_address.strip()
    footer_text = (
        f"\n\n--\n{sender}, {company}\n{address}\n"
        f"You received this because you publish content about AI video tools. "
        f"To stop hearing from us, unsubscribe here: {url} or reply \"unsubscribe\"."
    )
    footer_html = (
        '<hr style="border:none;border-top:1px solid #ddd;margin:24px 0 12px">'
        '<p style="color:#666;font-size:12px;line-height:1.5">'
        f"{html.escape(sender)}, {html.escape(company)}<br>{html.escape(address).replace(chr(10), '<br>')}<br>"
        "You received this because you publish content about AI video tools. "
        f'<a href="{html.escape(url)}">Unsubscribe</a> or reply &quot;unsubscribe&quot; and we won\'t contact you again.</p>'
    )
    text = (text_body or re.sub(r"<[^>]+>", "", html_body.replace("<br>", "\n").replace("</p>", "\n\n"))).strip()
    headers = {
        "List-Unsubscribe": f"<{url}>",
        "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    }
    return html_body + footer_html, text + footer_text, headers
