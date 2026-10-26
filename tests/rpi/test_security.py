"""Security tests: authorization, sending controls, budget abuse, webhooks, leakage, retention."""

import json
import time
from datetime import datetime, timedelta, timezone

import pytest

from rpi.attribution.stripe_signature import compute_signature

pytestmark = pytest.mark.db
V1 = "/rpi/api/v1"


async def key_for(api, role):
    r = await api.post(f"{V1}/admin/principals", headers=api.admin, json={"name": f"t-{role}", "role": role})
    return {"X-RPI-Key": r.json()["key"]}


async def test_requests_without_or_with_bad_keys_rejected(api):
    assert (await api.get(f"{V1}/creators")).status_code == 401
    assert (await api.get(f"{V1}/creators", headers={"X-RPI-Key": "rpi_wrong"})).status_code == 401
    # The UI shell is public but carries no data.
    ui = await api.get("/rpi/ui")
    assert ui.status_code == 200 and "X-RPI-Key" in ui.text


async def test_role_boundaries(api):
    viewer, analyst = await key_for(api, "viewer"), await key_for(api, "analyst")
    assert (await api.get(f"{V1}/creators", headers=viewer)).status_code == 200
    assert (await api.post(f"{V1}/discovery/runs", headers=viewer, json={"inline": True})).status_code == 403
    assert (await api.put(f"{V1}/outreach/settings", headers=analyst, json={
        "sending_enabled": True, "daily_send_limit": 1, "min_days_between_contacts": 1})).status_code == 403
    assert (await api.post(f"{V1}/admin/principals", headers=analyst, json={"name": "x", "role": "admin"})).status_code == 403
    assert (await api.get(f"{V1}/admin/audit", headers=analyst)).status_code == 403


async def test_analyst_cannot_approve_or_send(api):
    analyst = await key_for(api, "analyst")
    async with api.ctx.pool.acquire() as conn:
        cid = await conn.fetchval("INSERT INTO rpi_creators (display_name, pipeline_status) VALUES ('X', 'recommended') RETURNING id")
        did = await conn.fetchval(
            """INSERT INTO rpi_outreach_drafts (creator_id, subject, body_text, generator, created_by, idempotency_key, validation)
               VALUES ($1, 's', 'b', 'template', 'a', 'k1', '{"ok": true}') RETURNING id""", cid)
    assert (await api.post(f"{V1}/outreach/drafts/{did}/approve", headers=analyst)).status_code == 403
    assert (await api.post(f"{V1}/outreach/drafts/{did}/send", headers=analyst)).status_code == 403
    assert (await api.post(f"{V1}/crm/creators/{cid}/transition", headers=analyst,
                           json={"status": "outreach_approved"})).status_code == 403


async def test_budget_increases_need_an_admin_and_are_audited(api):
    budgets = (await api.get(f"{V1}/admin/budgets", headers=api.admin)).json()["budgets"]
    monthly = next(b for b in budgets if b["scope"] == "global" and b["period"] == "monthly")
    analyst = await key_for(api, "analyst")
    req = await api.post(f"{V1}/admin/budgets/{monthly['id']}/change-requests", headers=analyst,
                         json={"requested_limit_usd": "5000", "reason": "big research push"})
    assert req.json()["status"] == "pending_approval"  # an analyst can only ask
    assert (await api.post(f"{V1}/admin/budget-requests/{req.json()['request_id']}", headers=analyst,
                           json={"approve": True})).status_code == 403
    ok = await api.post(f"{V1}/admin/budget-requests/{req.json()['request_id']}", headers=api.admin, json={"approve": True})
    assert ok.status_code == 200
    direct = await api.post(f"{V1}/admin/budgets/{monthly['id']}/change-requests", headers=api.admin,
                            json={"requested_limit_usd": "6000", "reason": "admin raises it directly"})
    assert direct.json() == {"applied": True}  # single-admin control (owner decision, 2026-10-26)
    async with api.ctx.pool.acquire() as conn:
        assert await conn.fetchval("SELECT limit_usd FROM rpi_budgets WHERE id = $1", monthly["id"]) == 6000
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_audit_log WHERE action = 'budget.increased'") == 1


async def test_over_budget_enrichment_is_refused(api):
    async with api.ctx.pool.acquire() as conn:
        await conn.execute("UPDATE rpi_budgets SET limit_usd = 0.01 WHERE scope = 'global' AND period = 'daily'")
    r = await api.post(f"{V1}/discovery/runs", headers=api.admin, json={"preset": "none", "strategies": ["niche", "emerging"], "inline": True,
                                                                        "max_candidates": 200})
    assert r.status_code == 200
    async with api.ctx.pool.acquire() as conn:
        spent = await conn.fetchval("SELECT COALESCE(SUM(actual_usd), 0) FROM rpi_spend_ledger WHERE status = 'committed'")
        denied = await conn.fetchval("SELECT COUNT(*) FROM rpi_spend_ledger WHERE status = 'denied'")
    assert float(spent) <= 0.01 and denied >= 1  # limits are whole cents (NUMERIC(12,2))
    assert r.json()["status"] == "paused_budget"


async def test_webhook_rejects_unsigned_forged_and_replayed(api):
    api.ctx.settings = api.ctx.settings.model_copy(update={"rpi_stripe_webhook_secret": "whsec_sec_123456"})
    event = json.dumps({"id": "evt_x", "type": "invoice.paid", "created": int(time.time()), "data": {"object": {}}}).encode()
    assert (await api.post(f"{V1}/webhooks/stripe", content=event)).status_code == 400
    forged = f"t={int(time.time())},v1={compute_signature('whsec_wrong', int(time.time()), event)}"
    assert (await api.post(f"{V1}/webhooks/stripe", content=event, headers={"stripe-signature": forged})).status_code == 400
    old = int(time.time()) - 3600
    replay = f"t={old},v1={compute_signature('whsec_sec_123456', old, event)}"
    assert (await api.post(f"{V1}/webhooks/stripe", content=event, headers={"stripe-signature": replay})).status_code == 400
    async with api.ctx.pool.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_billing_events") == 0


async def test_webhook_fails_closed_without_secret(api):
    api.ctx.settings = api.ctx.settings.model_copy(update={"rpi_stripe_webhook_secret": ""})
    r = await api.post(f"{V1}/webhooks/stripe", content=b"{}", headers={"stripe-signature": "t=1,v1=abc"})
    assert r.status_code == 400


async def test_unsubscribe_link_requires_valid_token(api):
    async with api.ctx.pool.acquire() as conn:
        cid = await conn.fetchval("INSERT INTO rpi_creators (display_name) VALUES ('U') RETURNING id")
        did = await conn.fetchval(
            """INSERT INTO rpi_outreach_drafts (creator_id, subject, body_text, generator, created_by, idempotency_key)
               VALUES ($1, 's', 'b', 'template', 'a', 'k-u') RETURNING id""", cid)
    assert (await api.get(f"/rpi/public/unsubscribe/{did}?t=forged")).status_code == 400
    from rpi.outreach.sender import unsubscribe_token

    ok = await api.get(f"/rpi/public/unsubscribe/{did}?t={unsubscribe_token(str(did))}")
    assert ok.status_code == 200
    async with api.ctx.pool.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_suppression_list WHERE kind = 'creator'") == 0  # no accounts yet
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_outreach_events WHERE event_type = 'unsubscribed'") == 1


async def test_audit_log_never_contains_secrets(api):
    await api.post(f"{V1}/admin/principals", headers=api.admin, json={"name": "leak-check", "role": "viewer"})
    async with api.ctx.pool.acquire() as conn:
        rows = await conn.fetch("SELECT detail::text FROM rpi_audit_log")
    blob = " ".join(r["detail"] for r in rows)
    assert "rpi_" not in blob.replace("rpi_audit", "")  # issued keys are never logged
    assert "bootstrap-admin-key" not in blob


async def test_creator_deletion_cascades_and_suppresses(api):
    await api.post(f"{V1}/discovery/runs", headers=api.admin, json={"preset": "none", "strategies": ["niche"], "inline": True, "max_candidates": 5})
    creators = (await api.get(f"{V1}/creators", headers=api.admin)).json()
    cid = creators[0]["id"]
    analyst = await key_for(api, "analyst")
    assert (await api.delete(f"{V1}/creators/{cid}?reason=request", headers=analyst)).status_code == 403
    r = await api.delete(f"{V1}/creators/{cid}?reason=creator%20request", headers=api.admin)
    assert r.status_code == 200
    async with api.ctx.pool.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_creators WHERE id = $1", cid) == 0
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_creator_profile_snapshots WHERE creator_id = $1", cid) == 0
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_suppression_list WHERE kind = 'creator'") == 1


async def test_retention_deletes_expired_provider_data(api):
    await api.post(f"{V1}/discovery/runs", headers=api.admin, json={"preset": "none", "strategies": ["niche"], "inline": True, "max_candidates": 5})
    async with api.ctx.pool.acquire() as conn:
        before = await conn.fetchval("SELECT COUNT(*) FROM rpi_creator_profile_snapshots")
        await conn.execute("UPDATE rpi_data_provenance SET expires_at = NOW() - INTERVAL '1 minute'")
    dry = (await api.post(f"{V1}/admin/retention/run?dry_run=true", headers=api.admin)).json()
    assert dry["dry_run"] and dry["expired_provenance"]
    res = (await api.post(f"{V1}/admin/retention/run?dry_run=false", headers=api.admin)).json()
    async with api.ctx.pool.acquire() as conn:
        after = await conn.fetchval("SELECT COUNT(*) FROM rpi_creator_profile_snapshots")
    assert before > 0 and after == 0 and res["provenance_deleted"] >= before


async def test_fixture_provider_refused_in_production():
    from app.config import get_settings
    from rpi.providers.registry import FixtureInProductionError, build_registry

    s = get_settings().model_copy(update={"environment": "production", "rpi_enable_fixture_provider": True})
    with pytest.raises(FixtureInProductionError):
        build_registry(s)
