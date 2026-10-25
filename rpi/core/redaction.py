"""
Secret redaction for logs and stored request metadata.

Provider keys must never reach logs, audit rows, prompts or client bundles.
`redact_mapping` is applied to headers/params before they are persisted and
`structlog_redactor` can be added to the structlog processor chain.
"""

from __future__ import annotations

import re
from typing import Any, Mapping

SENSITIVE_KEY_PATTERN = re.compile(
    r"(api[-_]?key|apikey|token|secret|password|authorization|signature|cookie|x-api-key|bearer|credential)",
    re.IGNORECASE,
)

# Value-shaped secrets that can leak through free text (error messages, URLs).
SECRET_VALUE_PATTERNS = [
    re.compile(r"sk-ant-[A-Za-z0-9_\-]{8,}"),
    re.compile(r"xkeysib-[A-Za-z0-9\-]{8,}"),
    re.compile(r"AIza[0-9A-Za-z_\-]{20,}"),
    re.compile(r"whsec_[A-Za-z0-9]{8,}"),
    re.compile(r"sk_(live|test)_[A-Za-z0-9]{8,}"),
    re.compile(r"(?i)bearer\s+[A-Za-z0-9._\-]{8,}"),
    re.compile(r"(?i)([?&](api_key|apikey|key|token)=)[^&\s]+"),
]

REDACTED = "[REDACTED]"


def redact_text(text: str, extra_secrets: tuple[str, ...] = ()) -> str:
    if not text:
        return text
    out = text
    for secret in extra_secrets:
        if secret and len(secret) >= 6:
            out = out.replace(secret, REDACTED)
    for pattern in SECRET_VALUE_PATTERNS:
        if pattern.groups >= 2:
            out = pattern.sub(lambda m: f"{m.group(1)}{REDACTED}", out)
        else:
            out = pattern.sub(REDACTED, out)
    return out


def redact_mapping(data: Mapping[str, Any] | None) -> dict[str, Any]:
    if not data:
        return {}
    clean: dict[str, Any] = {}
    for key, value in data.items():
        if SENSITIVE_KEY_PATTERN.search(str(key)):
            clean[key] = REDACTED
        elif isinstance(value, Mapping):
            clean[key] = redact_mapping(value)
        elif isinstance(value, str):
            clean[key] = redact_text(value)
        else:
            clean[key] = value
    return clean


def structlog_redactor(_logger, _method, event_dict: dict) -> dict:
    """structlog processor that scrubs sensitive keys and secret-shaped values."""
    for key in list(event_dict.keys()):
        value = event_dict[key]
        if key != "event" and SENSITIVE_KEY_PATTERN.search(key):
            event_dict[key] = REDACTED
        elif isinstance(value, str):
            event_dict[key] = redact_text(value)
        elif isinstance(value, Mapping):
            event_dict[key] = redact_mapping(value)
    return event_dict
