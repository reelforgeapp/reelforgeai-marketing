"""
Partner Scout endpoint hardening: admin key on operational endpoints and
authenticated Brevo webhooks. Runs against the real FastAPI app without the
lifespan (no database, Redis or Celery broker needed).
"""

import base64
import hashlib
import hmac
import json

import httpx
import pytest

from app import main

ADMIN_KEY = "test-admin-key-0123456789abcdef"
SECRET = "brevo-webhook-secret-0123456789"
EVENT = json.dumps({"event": "delivered", "email": "x@example.com"}).encode()  # no message-id → no DB access


@pytest.fixture
async def client(monkeypatch):
    monkeypatch.setattr(main.settings, "admin_api_key", ADMIN_KEY)
    monkeypatch.setattr(main.settings, "brevo_webhook_secret", SECRET)
    monkeypatch.setattr(main.settings, "environment", "production")
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class _FakeTask:
    id = "task-123"


@pytest.fixture
def mocked_discovery(monkeypatch):
    from tasks import discovery_tasks

    calls = []
    monkeypatch.setattr(discovery_tasks.run_youtube_discovery, "delay", lambda *a, **k: calls.append(1) or _FakeTask())
    return calls


PROTECTED = [
    ("POST", "/trigger/youtube-discovery"), ("POST", "/trigger/email-extraction"),
    ("POST", "/trigger/email-verification"), ("POST", "/trigger/sequence-processing"),
    ("POST", "/trigger/auto-enroll"), ("POST", "/trigger/brevo-sync"), ("POST", "/trigger/brevo-sync-full"),
    ("POST", "/trigger/deliverability-check"), ("POST", "/trigger/keyword-trends"),
    ("GET", "/keywords"), ("POST", "/keywords"), ("PATCH", "/keywords/abc"), ("DELETE", "/keywords/abc"),
    ("GET", "/keywords/trends/ai"), ("POST", "/keywords/compare"), ("GET", "/tasks/abc"),
]


@pytest.mark.parametrize("method,path", PROTECTED)
async def test_missing_and_wrong_key_rejected(client, method, path):
    assert (await client.request(method, path)).status_code == 401
    assert (await client.request(method, path, headers={"X-Admin-Key": "wrong"})).status_code == 401


async def test_placeholder_or_empty_admin_key_fails_closed(client, monkeypatch, mocked_discovery):
    for configured in (main.DEFAULT_ADMIN_API_KEY, ""):
        monkeypatch.setattr(main.settings, "admin_api_key", configured)
        r = await client.post("/trigger/youtube-discovery", headers={"X-Admin-Key": configured})
        assert r.status_code == 401
    assert mocked_discovery == []


async def test_correct_key_reaches_trigger(client, mocked_discovery):
    r = await client.post("/trigger/youtube-discovery", headers={"X-Admin-Key": ADMIN_KEY})
    assert r.status_code == 200 and r.json() == {"status": "triggered", "task_id": "task-123"}
    assert mocked_discovery == [1]


async def test_correct_key_reaches_keyword_listing(client, monkeypatch):
    class FakePool:
        async def fetch(self, *a):
            return []

        async def close(self):
            pass

    async def fake_db():
        return FakePool()

    monkeypatch.setattr(main, "get_database_async", fake_db)
    r = await client.get("/keywords", headers={"X-Admin-Key": ADMIN_KEY})
    assert r.status_code == 200 and r.json()["total"] == 0


async def test_public_endpoints_stay_public(client):
    assert (await client.get("/health")).status_code == 200


# --------------------------------------------------------------------------
# Brevo webhook
# --------------------------------------------------------------------------


async def test_webhook_rejected_without_credentials(client):
    assert (await client.post("/webhooks/brevo", content=EVENT)).status_code == 401
    r = await client.post("/webhooks/brevo", content=EVENT, headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401


async def test_webhook_rejected_when_secret_missing_in_production(client, monkeypatch):
    monkeypatch.setattr(main.settings, "brevo_webhook_secret", "")
    r = await client.post("/webhooks/brevo", content=EVENT, headers={"Authorization": f"Bearer {SECRET}"})
    assert r.status_code == 401


async def test_webhook_allowed_without_secret_only_in_development(client, monkeypatch):
    monkeypatch.setattr(main.settings, "brevo_webhook_secret", "")
    monkeypatch.setattr(main.settings, "environment", "development")
    r = await client.post("/webhooks/brevo", content=EVENT)
    assert r.status_code == 200 and r.json()["status"] == "skipped"


@pytest.mark.parametrize("headers", [
    {"Authorization": f"Bearer {SECRET}"},
    {"Authorization": "Basic " + base64.b64encode(f"brevo:{SECRET}".encode()).decode()},
    {"X-Brevo-Webhook-Token": SECRET},
    {"X-Sib-Signature": hmac.new(SECRET.encode(), EVENT, hashlib.sha256).hexdigest()},
])
async def test_webhook_accepts_each_supported_credential(client, headers):
    r = await client.post("/webhooks/brevo", content=EVENT, headers=headers)
    assert r.status_code == 200 and r.json()["status"] == "skipped"


async def test_webhook_rejects_malformed_basic_auth(client):
    r = await client.post("/webhooks/brevo", content=EVENT, headers={"Authorization": "Basic !!!not-base64"})
    assert r.status_code == 401


async def test_root_redirects_to_the_rpi_dashboard(client):
    r = await client.get("/", follow_redirects=False)
    assert r.status_code == 307 and r.headers["location"] == "/rpi/ui"
    assert (await client.get("/health")).status_code != 307  # Render's health check is unaffected
