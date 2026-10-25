"""Billing ingestion, idempotency, attribution, commissions and reconciliation (database)."""

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D

import pytest

from rpi.attribution import touches
from rpi.financial_ledger import ledger, reports

pytestmark = pytest.mark.db

T0 = datetime(2026, 9, 1, 12, tzinfo=timezone.utc)


def invoice_event(eid, inv, customer, paid, *, tax=0, discount=0, reason="subscription_create", at=T0, price="price_creator_m",
                  currency="usd"):
    return {
        "id": eid, "type": "invoice.paid", "created": int(at.timestamp()), "livemode": False,
        "data": {"object": {
            "id": inv, "customer": customer, "amount_paid": paid, "tax": tax, "currency": currency,
            "billing_reason": reason, "status_transitions": {"paid_at": int(at.timestamp())},
            "total_discount_amounts": [{"amount": discount}] if discount else [],
            "lines": {"data": [{"price": {"id": price, "recurring": {"interval": "month"}}}]},
        }},
    }


async def ingest(conn, event):
    raw = json.dumps(event).encode()
    row, new = await ledger.store_billing_event(conn, event, raw, signature_verified=True)
    if not new:
        return "duplicate"
    out = await ledger.process_stripe_event(conn, event, row, price_map={"price_creator_m": "creator"},
                                            fee_rate=D("0.029"), fee_fixed_minor=30)
    await conn.execute("UPDATE rpi_billing_events SET processing_status = 'processed' WHERE id = $1", row)
    return out


async def make_campaign(conn, *, code="SAM20", window=30, commission="0.30", months=None):
    cid = await conn.fetchval("INSERT INTO rpi_creators (display_name) VALUES ('Sam') RETURNING id")
    deal = await conn.fetchval(
        """INSERT INTO rpi_deals (creator_id, structure, upfront_fee_usd, commission_rate, commission_months, status, created_by)
           VALUES ($1, 'fixed_plus_recurring', 400, $2, $3, 'agreed', 't') RETURNING id""", cid, D(commission), months)
    camp = await conn.fetchval(
        """INSERT INTO rpi_campaigns (creator_id, deal_id, name, referral_code, utm_campaign, attribution_window_days)
           VALUES ($1, $2, 'Sam Oct', $3, $3, $4) RETURNING id""", cid, deal, code, window)
    return cid, camp


async def test_invoice_with_tax_and_discount_and_idempotency(pool):
    async with pool.acquire() as conn:
        ev = invoice_event("evt_1", "in_1", "cus_1", paid=1870, tax=170, discount=300)
        assert (await ingest(conn, ev)).startswith("recorded")
        assert await ingest(conn, ev) == "duplicate"
        # Same invoice via the other event type must not double count.
        ev2 = dict(ev, id="evt_1b", type="invoice.payment_succeeded")
        await ingest(conn, ev2)
        rows = {r["entry_type"]: r["amount_minor"] for r in await conn.fetch(
            "SELECT entry_type, amount_minor FROM rpi_ledger_entries WHERE customer_ref = 'stripe:cus_1'")}
    # net (excl. tax) = 1870 - 170 = 1700; gross pre-discount = 2000
    assert rows["subscription_charge"] == 2000 and rows["discount"] == -300 and rows["tax"] == 170
    assert rows["payment_fee"] == -(round(1870 * 0.029) + 30)


async def test_refunds_are_cumulative_without_double_counting(pool):
    async with pool.acquire() as conn:
        await ingest(conn, invoice_event("evt_i", "in_r", "cus_r", 1700))
        base = {"type": "charge.refunded", "created": int(T0.timestamp()) + 100}
        await ingest(conn, base | {"id": "evt_r1", "data": {"object": {"id": "ch_1", "customer": "cus_r", "currency": "usd", "amount_refunded": 500}}})
        await ingest(conn, base | {"id": "evt_r2", "data": {"object": {"id": "ch_1", "customer": "cus_r", "currency": "usd", "amount_refunded": 1700}}})
        await ingest(conn, base | {"id": "evt_r3", "data": {"object": {"id": "ch_1", "customer": "cus_r", "currency": "usd", "amount_refunded": 1700}}})
        total = await conn.fetchval("SELECT SUM(amount_minor) FROM rpi_ledger_entries WHERE entry_type = 'refund'")
    assert total == -1700


async def test_missing_fx_is_flagged_not_guessed(pool):
    async with pool.acquire() as conn:
        await ingest(conn, invoice_event("evt_eur", "in_eur", "cus_eur", 1500, currency="eur"))
        row = await conn.fetchrow("SELECT amount_usd, fx_source FROM rpi_ledger_entries WHERE entry_type = 'subscription_charge'")
        assert row["fx_source"] == "MISSING" and row["amount_usd"] == 0
        recon = await reports.reconciliation(conn)
    assert recon["checks"]["no_missing_fx"] is False


async def test_attribution_first_last_touch_and_lock(pool):
    async with pool.acquire() as conn:
        _, camp_a = await make_campaign(conn, code="AAA")
        _, camp_b = await make_campaign(conn, code="BBB")
        signup = T0
        for code, days, key in (("AAA", 20, "t1"), ("BBB", 3, "t2"), ("BBB", 60, "t-old")):
            await touches.record_touch(conn, {"anonymous_id": "anon-1", "referral_code": code, "consent_state": "granted",
                                              "touched_at": signup - timedelta(days=days), "idempotency_key": key})
        res = await touches.identify(conn, {"customer_ref": "user-1", "signed_up_at": signup, "anonymous_id": "anon-1",
                                            "billing_customer_id": "cus_u1"})
        assert res["first_touch_campaign_id"] == str(camp_a) and res["last_touch_campaign_id"] == str(camp_b)
        await ingest(conn, invoice_event("evt_p", "in_p", "cus_u1", 1700, at=signup + timedelta(days=1)))
        locked = await conn.fetchval("SELECT attribution_locked_at FROM rpi_customers WHERE customer_ref = 'user-1'")
        assert locked is not None
        # A later touch cannot move revenue after lock.
        await touches.record_touch(conn, {"anonymous_id": "anon-1", "referral_code": "AAA", "consent_state": "granted",
                                          "touched_at": signup + timedelta(days=2), "idempotency_key": "t3"})
        res2 = await touches.identify(conn, {"customer_ref": "user-1", "signed_up_at": signup, "anonymous_id": "anon-1"})
        assert res2["locked"] and res2["last_touch_campaign_id"] == str(camp_b)


async def test_denied_consent_stores_no_identifier(pool):
    async with pool.acquire() as conn:
        await make_campaign(conn, code="CCC")
        r = await touches.record_touch(conn, {"anonymous_id": "anon-x", "referral_code": "CCC", "consent_state": "denied",
                                              "touched_at": T0, "idempotency_key": "d1"})
        assert r["recorded"] and not r["identifier_stored"]


async def test_billing_before_identify_merges_placeholder(pool):
    async with pool.acquire() as conn:
        await make_campaign(conn, code="DDD")
        await ingest(conn, invoice_event("evt_early", "in_early", "cus_early", 1700))
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_customers WHERE customer_ref = 'stripe:cus_early'") == 1
        await touches.identify(conn, {"customer_ref": "user-early", "signed_up_at": T0 - timedelta(hours=1),
                                      "billing_customer_id": "cus_early", "signup_referral_code": "DDD"})
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_customers WHERE customer_ref = 'stripe:cus_early'") == 0
        assert await conn.fetchval("SELECT COUNT(*) FROM rpi_ledger_entries WHERE customer_ref = 'user-early'") >= 2
        assert await conn.fetchval("SELECT attribution_locked_at FROM rpi_customers WHERE customer_ref = 'user-early'") is not None


async def test_commission_accrual_duration_clawback_and_partner_report(pool):
    async with pool.acquire() as conn:
        _, camp = await make_campaign(conn, code="EEE", months=2)
        await touches.identify(conn, {"customer_ref": "user-e", "signed_up_at": T0 - timedelta(days=1),
                                      "billing_customer_id": "cus_e", "signup_referral_code": "EEE"})
        await ingest(conn, invoice_event("e1", "in_e1", "cus_e", 1700, at=T0))
        await ingest(conn, invoice_event("e2", "in_e2", "cus_e", 1700, at=T0 + timedelta(days=31), reason="subscription_cycle"))
        await ingest(conn, invoice_event("e3", "in_e3", "cus_e", 1700, at=T0 + timedelta(days=62), reason="subscription_cycle"))
        await ingest(conn, {"id": "e4", "type": "charge.refunded", "created": int((T0 + timedelta(days=70)).timestamp()),
                            "data": {"object": {"id": "ch_e3", "customer": "cus_e", "currency": "usd", "amount_refunded": 1700}}})
        n1 = await ledger.accrue_commissions(conn)
        n2 = await ledger.accrue_commissions(conn)
        commissions = [r["amount_minor"] for r in await conn.fetch(
            "SELECT amount_minor FROM rpi_ledger_entries WHERE entry_type = 'affiliate_commission' ORDER BY occurred_at")]
        await ledger.add_entry(conn, entry_type="influencer_fee", amount_minor=-40000, currency="usd", occurred_at=T0,
                               idempotency_key="fee-e", source="operator", campaign_id=camp)
        report = (await reports.partner_report(conn))[0]
    assert n2 == 0  # idempotent
    # months 0 and 1 earn commission (510 each); month 2 is outside the 2-month term; refund at day 70 > 60-day clawback window.
    assert commissions == [-510, -510] and n1 == 2
    assert report["paying_customers"] == 1
    assert report["complete"] is False and any("render_cost" in m for m in report["missing"])
    assert D(report["net_revenue_usd"]) == D("34.00")  # 3 × 17.00 − 17.00 refund
    assert report["acquisition_costs_usd"]["influencer_fee"] == "-400.00"


async def test_reconciliation_ok_for_clean_usd_data(pool):
    async with pool.acquire() as conn:
        await ingest(conn, invoice_event("ok1", "in_ok1", "cus_ok", 1870, tax=170, discount=300))
        recon = await reports.reconciliation(conn)
    assert recon["checks"]["usd_invoice_totals_match"] and recon["checks"]["every_paid_invoice_has_entry"]
    assert recon["customers_not_linked_to_product_user"] == 1


async def test_cohort_retention_from_charges(pool):
    async with pool.acquire() as conn:
        for i in range(4):
            await ingest(conn, invoice_event(f"c{i}", f"in_c{i}", f"cus_c{i}", 1700, at=T0))
        await ingest(conn, invoice_event("c0b", "in_c0b", "cus_c0", 1700, at=T0 + timedelta(days=31), reason="subscription_cycle"))
        result = await reports.cohorts(conn)
    cohort = result["cohorts"][0]
    assert cohort["customers"] == 4 and cohort["retention"][0] == 1.0 and cohort["retention"][1] == 0.25
