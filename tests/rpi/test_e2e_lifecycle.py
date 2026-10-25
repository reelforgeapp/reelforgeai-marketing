"""
End-to-end lifecycle over HTTP against the real FastAPI app and database,
with the synthetic fixture provider, a fake Claude client and a fake email
sender:

Discovery → Qualification → Recommendation → Outreach approval → Partnership
→ Referral → Subscription → Profit report.
"""

import json
import time
from datetime import datetime, timedelta, timezone

import pytest

from rpi.attribution.stripe_signature import compute_signature

pytestmark = pytest.mark.db

V1 = "/rpi/api/v1"
WEBHOOK_SECRET = "whsec_e2e_secret_value"


async def test_full_lifecycle(api):
    admin = api.admin
    api.ctx.settings = api.ctx.settings.model_copy(update={"rpi_stripe_webhook_secret": WEBHOOK_SECRET})

    # 1. Discovery (fixture provider), with cost estimate first
    est = await api.post(f"{V1}/discovery/estimate", json={"preset": "none", "strategies": ["niche"], "max_candidates": 40}, headers=admin)
    assert est.status_code == 200 and est.json()["primary_provider"] == "fixture"
    run = await api.post(f"{V1}/discovery/runs", headers=admin, json={
        "preset": "none", "strategies": ["niche", "competitor_sponsorship", "emerging", "lookalike"], "max_candidates": 40, "inline": True,
        "criteria": {"min_subscribers": 1000, "max_subscribers": 1000000}})
    assert run.status_code == 200, run.text
    body = run.json()
    assert body["status"] == "succeeded"
    runs = (await api.get(f"{V1}/discovery/runs", headers=admin)).json()
    detail = (await api.get(f"{V1}/discovery/runs/{runs[0]['id']}", headers=admin)).json()
    assert detail["run"]["duplicate_count"] >= 1  # strategies overlap; duplicates counted, not re-created
    assert any("lookalike" in w for w in detail["run"]["warnings"])
    assert all(c["inclusion_reasons"] for c in detail["candidates"])
    creators = (await api.get(f"{V1}/creators?limit=500", headers=admin)).json()
    ids = [c["id"] for c in creators]
    assert len(ids) == len(set(ids)) > 5  # no duplicate creator records

    # 2. Stage-2 screening for everyone discovered
    screen = await api.post(f"{V1}/research", headers=admin, json={"creator_ids": ids, "stage": "screen", "inline": True})
    assert screen.status_code == 200 and screen.json()["status"] == "succeeded", screen.text
    results = screen.json()["progress"]["results"]
    assert "reject_niche_mismatch" in results.values()  # gaming fixture creators rejected on content
    qualified = [cid for cid, rec in results.items() if rec in ("recommend", "investigate")]
    assert qualified

    # 3. Deep qualification of the best candidates
    deep = await api.post(f"{V1}/research", headers=admin, json={"creator_ids": qualified[:3], "stage": "deep", "inline": True})
    assert deep.status_code == 200, deep.text
    recommended = [cid for cid, rec in deep.json()["progress"]["results"].items() if rec == "recommend"]
    assert recommended, deep.json()
    cid = recommended[0]
    profile = (await api.get(f"{V1}/creators/{cid}", headers=admin)).json()
    assert profile["creator"]["pipeline_status"] == "recommended"
    assert profile["qualification"]["recommendation"] == "recommend"
    assert profile["contacts"], "contacts are fetched only after qualification"
    assert profile["sponsorships"]  # fixture descriptions contain sponsor/affiliate evidence
    assert all(a["measurement"] == "third_party_modeled" for a in profile["audience"])

    # Rejected creators never had demographics or contacts bought for them
    rejected = [c for c, rec in results.items() if rec == "reject_niche_mismatch"][0]
    rej_profile = (await api.get(f"{V1}/creators/{rejected}", headers=admin)).json()
    assert rej_profile["audience"] == [] and rej_profile["contacts"] == []

    # 4. Draft → approve → send refused while sending disabled → enable → send
    draft = await api.post(f"{V1}/outreach/creators/{cid}/drafts", headers=admin, json={})
    assert draft.status_code == 200, draft.text
    d = draft.json()
    assert d["validation"]["ok"], d["validation"]
    approve = await api.post(f"{V1}/outreach/drafts/{d['id']}/approve", headers=admin)
    assert approve.status_code == 200, approve.text
    refused = await api.post(f"{V1}/outreach/drafts/{d['id']}/send", headers=admin)
    assert refused.status_code == 403 and "disabled" in refused.text
    assert api.ctx.send_fn.sent == []
    settings = await api.put(f"{V1}/outreach/settings", headers=admin, json={
        "sending_enabled": True, "daily_send_limit": 5, "min_days_between_contacts": 30, "sender_name": "Jr",
        "sender_email": "jr@reelforgeai.io", "postal_address": "100 Example Ave, Austin, TX 78701"})
    assert settings.status_code == 200
    sent = await api.post(f"{V1}/outreach/drafts/{d['id']}/send", headers=admin)
    assert sent.status_code == 200 and sent.json()["sent"], sent.text
    assert len(api.ctx.send_fn.sent) == 1
    again = await api.post(f"{V1}/outreach/drafts/{d['id']}/send", headers=admin)
    assert again.status_code in (403, 409) and len(api.ctx.send_fn.sent) == 1

    # 5. Negotiation → agreed deal → campaign
    for status in ("replied", "negotiating", "agreement_pending"):
        t = await api.post(f"{V1}/crm/creators/{cid}/transition", headers=admin, json={"status": status})
        assert t.status_code == 200, t.text
    no_contract = await api.post(f"{V1}/deals", headers=admin, json={
        "creator_id": cid, "kind": "fixed_plus_recurring", "upfront_fee": "400", "commission_rate": "0.30", "status": "agreed"})
    assert no_contract.status_code == 422
    deal = await api.post(f"{V1}/deals", headers=admin, json={
        "creator_id": cid, "kind": "fixed_plus_recurring", "upfront_fee": "400", "commission_rate": "0.30",
        "status": "agreed", "contract_reference": "DOC-2026-001"})
    assert deal.status_code == 200, deal.text
    camp = await api.post(f"{V1}/campaigns", headers=admin, json={
        "creator_id": cid, "deal_id": deal.json()["id"], "name": "E2E campaign", "referral_code": "E2E30"})
    assert camp.status_code == 200, camp.text
    camp_id = camp.json()["id"]
    fee = await api.post(f"{V1}/campaigns/{camp_id}/costs", headers=admin, json={
        "entry_type": "influencer_fee", "amount": "400", "occurred_on": "2026-10-01", "reference": "INV-77"})
    assert fee.json()["recorded"]
    assert (await api.post(f"{V1}/crm/creators/{cid}/transition", headers=admin, json={"status": "active_partnership"})).status_code == 200

    # 6. Referral → signup (ingest principal) → paid subscription via signed Stripe webhook
    ingest = await api.post(f"{V1}/admin/principals", headers=admin, json={"name": "reelforge-backend", "role": "ingest"})
    ingest_key = {"X-RPI-Key": ingest.json()["key"]}
    now = datetime.now(timezone.utc)
    touch = await api.post(f"{V1}/attribution/touch", headers=ingest_key, json={
        "anonymous_id": "anon-e2e", "referral_code": "E2E30", "consent_state": "granted",
        "touched_at": (now - timedelta(days=1)).isoformat(), "idempotency_key": "touch-e2e"})
    assert touch.status_code == 200 and touch.json()["campaign_id"] == camp_id
    ident = await api.post(f"{V1}/attribution/identify", headers=ingest_key, json={
        "customer_ref": "user-e2e", "signed_up_at": now.isoformat(), "anonymous_id": "anon-e2e",
        "billing_customer_id": "cus_e2e", "consent_state": "granted"})
    assert ident.status_code == 200 and ident.json()["last_touch_campaign_id"] == camp_id
    assert ident.json()["posthog"]["sent"] is False  # no PostHog key configured in tests
    # The ingest role cannot read data.
    assert (await api.get(f"{V1}/creators", headers=ingest_key)).status_code == 403

    event = {"id": "evt_e2e_1", "type": "invoice.paid", "created": int(time.time()), "livemode": False,
             "data": {"object": {"id": "in_e2e_1", "customer": "cus_e2e", "amount_paid": 3700, "tax": 0, "currency": "usd",
                                 "billing_reason": "subscription_create", "status_transitions": {"paid_at": int(time.time())},
                                 "lines": {"data": [{"price": {"id": "price_hustler_m", "recurring": {"interval": "month"}}}]}}}}
    raw = json.dumps(event).encode()
    ts = int(time.time())
    sig = {"stripe-signature": f"t={ts},v1={compute_signature(WEBHOOK_SECRET, ts, raw)}", "content-type": "application/json"}
    wh = await api.post(f"{V1}/webhooks/stripe", content=raw, headers=sig)
    assert wh.status_code == 200 and wh.json()["status"] == "processed", wh.text
    dup = await api.post(f"{V1}/webhooks/stripe", content=raw, headers=sig)
    assert dup.json()["status"] == "duplicate"

    # 7. Profit report from observed ledger data
    report = (await api.get(f"{V1}/reports/partners", headers=admin)).json()
    row = next(r for r in report if r["campaign_id"] == camp_id)
    assert row["paying_customers"] == 1 and row["net_revenue_usd"] == "37.00"
    assert row["acquisition_costs_usd"]["influencer_fee"] == "-400.00"
    assert row["acquisition_costs_usd"]["affiliate_commission"] == "-11.10"
    assert row["complete"] is False  # render costs not yet recorded → profit explicitly not exact
    forecast = await api.post(f"{V1}/profit/forecast", headers=admin, json={"deal": {"kind": "fixed", "upfront_fee": "400"},
                                                                           "compare_structures": True})
    assert forecast.status_code == 200 and "deal_comparison" in forecast.json()
    dash = (await api.get(f"{V1}/dashboard/executive", headers=admin)).json()
    assert dash["active_partnerships"] == 1 and dash["profit_reports_complete"] is False
    recon = (await api.get(f"{V1}/reports/reconciliation", headers=admin)).json()
    assert recon["checks"]["usd_invoice_totals_match"]
    costs = (await api.get(f"{V1}/dashboard/costs", headers=admin)).json()
    assert costs["by_provider"] and costs["cost_per_discovered_creator_usd"] is not None
