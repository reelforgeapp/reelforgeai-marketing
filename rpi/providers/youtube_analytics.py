"""
YouTube Analytics (creator-authorized) — OAuth plumbing.

Status: implemented but DISABLED by default (rpi_youtube_analytics_enabled).
YouTube Developer Policies III.E.3(b) limit display of authorized data to the
authorizing user; showing a creator's analytics to the ReelForge team needs
legal review first (KNOWN_LIMITATIONS.md). Not tested against Google: no
OAuth client credentials were available during the build.

What is implemented:
  * consent URL with a signed, single-use, expiring `state` bound to the creator
  * code exchange; refresh token encrypted at rest (Fernet)
  * revocation (Google revoke endpoint) with authorized-data deletion
    scheduled within 7 days (III.D.2); 30-day re-confirmation (III.E.4(b))
  * scopes limited to yt-analytics.readonly + youtube.readonly

Creators who decline OAuth can submit analytics manually (notes kind
'manual_analytics'), stored as manual_submitted and never shown as verified.
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlencode

import httpx

SCOPES = (
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
)
AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
REVOKE_URL = "https://oauth2.googleapis.com/revoke"
PROVIDER = "youtube_analytics_api"


class AnalyticsDisabled(RuntimeError):
    pass


def _fernet(key: str):
    from cryptography.fernet import Fernet

    if not key:
        raise AnalyticsDisabled("RPI_TOKEN_ENCRYPTION_KEY is not set")
    return Fernet(key.encode())


def encrypt_token(token: str, key: str) -> str:
    return _fernet(key).encrypt(token.encode()).decode()


def decrypt_token(blob: str, key: str) -> str:
    return _fernet(key).decrypt(blob.encode()).decode()


def _check_enabled(settings) -> None:
    if not settings.rpi_youtube_analytics_enabled:
        raise AnalyticsDisabled("YouTube Analytics OAuth is disabled pending legal review (rpi_youtube_analytics_enabled)")
    if not (settings.google_oauth_client_id and settings.google_oauth_client_secret and settings.google_oauth_redirect_uri):
        raise AnalyticsDisabled("Google OAuth client is not configured")


async def consent_url(conn, settings, creator_id) -> str:
    _check_enabled(settings)
    state = secrets.token_urlsafe(32)
    await conn.execute(
        "INSERT INTO rpi_oauth_states (state_hash, creator_id, expires_at) VALUES ($1, $2, NOW() + INTERVAL '30 minutes')",
        hashlib.sha256(state.encode()).hexdigest(), creator_id,
    )
    return AUTH_URL + "?" + urlencode({
        "client_id": settings.google_oauth_client_id,
        "redirect_uri": settings.google_oauth_redirect_uri,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "false",
        "state": state,
    })


async def handle_callback(conn, settings, code: str, state: str, *, transport=None) -> str:
    _check_enabled(settings)
    row = await conn.fetchrow(
        """UPDATE rpi_oauth_states SET used_at = NOW()
           WHERE state_hash = $1 AND used_at IS NULL AND expires_at > NOW() RETURNING creator_id""",
        hashlib.sha256(state.encode()).hexdigest(),
    )
    if not row:
        raise PermissionError("invalid, expired or reused OAuth state")
    async with httpx.AsyncClient(timeout=20.0, transport=transport) as client:
        resp = await client.post(TOKEN_URL, data={
            "code": code, "client_id": settings.google_oauth_client_id,
            "client_secret": settings.google_oauth_client_secret,
            "redirect_uri": settings.google_oauth_redirect_uri, "grant_type": "authorization_code",
        })
    if resp.status_code != 200:
        raise PermissionError(f"token exchange failed: HTTP {resp.status_code}")
    payload = resp.json()
    refresh = payload.get("refresh_token")
    if not refresh:
        raise PermissionError("no refresh token returned")
    granted = tuple(payload.get("scope", "").split())
    await conn.execute(
        """INSERT INTO rpi_oauth_grants (creator_id, provider, scopes, encrypted_refresh_token, reconfirm_by)
           VALUES ($1, $2, $3, $4, NOW() + INTERVAL '30 days')
           ON CONFLICT (creator_id, provider) DO UPDATE SET scopes = EXCLUDED.scopes,
             encrypted_refresh_token = EXCLUDED.encrypted_refresh_token, granted_at = NOW(),
             reconfirm_by = EXCLUDED.reconfirm_by, revoked_at = NULL, revocation_source = NULL, data_delete_by = NULL""",
        row["creator_id"], PROVIDER, list(granted), encrypt_token(refresh, settings.rpi_token_encryption_key),
    )
    return str(row["creator_id"])


async def revoke(conn, settings, creator_id, *, source: str = "operator", transport=None) -> dict:
    grant = await conn.fetchrow(
        "SELECT encrypted_refresh_token FROM rpi_oauth_grants WHERE creator_id = $1 AND provider = $2 AND revoked_at IS NULL",
        creator_id, PROVIDER,
    )
    if not grant:
        return {"revoked": False, "reason": "no active grant"}
    google_status: Optional[int] = None
    if grant["encrypted_refresh_token"] and settings.rpi_token_encryption_key:
        token = decrypt_token(grant["encrypted_refresh_token"], settings.rpi_token_encryption_key)
        async with httpx.AsyncClient(timeout=15.0, transport=transport) as client:
            resp = await client.post(REVOKE_URL, data={"token": token})
        google_status = resp.status_code
    await conn.execute(
        """UPDATE rpi_oauth_grants SET revoked_at = NOW(), revocation_source = $3, encrypted_refresh_token = '',
               data_delete_by = NOW() + INTERVAL '7 days' WHERE creator_id = $1 AND provider = $2""",
        creator_id, PROVIDER, source,
    )
    return {"revoked": True, "google_status": google_status,
            "data_delete_by": (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()}
