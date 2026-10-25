"""
Stripe webhook signature verification (no SDK dependency).

Stripe signs `"{timestamp}.{raw_body}"` with HMAC-SHA256 using the endpoint
secret and sends `Stripe-Signature: t=<ts>,v1=<hex>[,v1=<hex>...]`. A request
is accepted only when a v1 signature matches and the timestamp is within the
tolerance window (replay protection). Event-id uniqueness in
rpi_billing_events provides the second replay barrier.

RPI uses its own Stripe webhook endpoint and secret, separate from the
product's /api/billing/webhook, so the two never interfere.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from typing import Optional


class SignatureError(ValueError):
    pass


def parse_header(header: str) -> tuple[Optional[int], list[str]]:
    timestamp, sigs = None, []
    for part in (header or "").split(","):
        key, _, value = part.strip().partition("=")
        if key == "t":
            try:
                timestamp = int(value)
            except ValueError:
                timestamp = None
        elif key == "v1" and value:
            sigs.append(value)
    return timestamp, sigs


def compute_signature(secret: str, timestamp: int, payload: bytes) -> str:
    signed = f"{timestamp}.".encode() + payload
    return hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()


def verify(payload: bytes, header: str, secret: str, tolerance_seconds: int = 300, now: Optional[int] = None) -> int:
    if not secret:
        raise SignatureError("webhook secret not configured")
    timestamp, sigs = parse_header(header)
    if timestamp is None or not sigs:
        raise SignatureError("malformed Stripe-Signature header")
    expected = compute_signature(secret, timestamp, payload)
    if not any(hmac.compare_digest(expected, s) for s in sigs):
        raise SignatureError("signature mismatch")
    now = int(time.time()) if now is None else now
    if abs(now - timestamp) > tolerance_seconds:
        raise SignatureError("timestamp outside tolerance (possible replay)")
    return timestamp
