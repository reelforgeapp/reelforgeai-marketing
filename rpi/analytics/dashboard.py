"""Executive dashboard and research-cost metrics (all observed values)."""

from __future__ import annotations

from decimal import Decimal

from rpi.financial_ledger.reports import partner_report

Q = Decimal("0.01")


def _per(total: Decimal, n: int):
    return str((total / n).quantize(Decimal("0.0001"))) if n else None


async def research_costs(conn) -> dict:
    total = Decimal(await conn.fetchval(
        "SELECT COALESCE(SUM(actual_usd), 0) FROM rpi_spend_ledger WHERE status = 'committed'"))
    by_provider = await conn.fetch(
        """SELECT provider, COALESCE(SUM(actual_usd), 0) AS usd, COUNT(*) FILTER (WHERE status = 'committed') AS calls,
                  COUNT(*) FILTER (WHERE status = 'denied') AS denied
           FROM rpi_spend_ledger GROUP BY provider ORDER BY provider""")
    month = Decimal(await conn.fetchval(
        "SELECT COALESCE(SUM(actual_usd), 0) FROM rpi_spend_ledger WHERE status = 'committed' AND created_at >= date_trunc('month', NOW())"))
    counts = await conn.fetchrow(
        """SELECT COUNT(*) AS discovered,
                  COUNT(*) FILTER (WHERE pipeline_status NOT IN ('discovered', 'rejected', 'do_not_contact')
                                   OR funnel_stage >= 2) AS qualified,
                  COUNT(*) FILTER (WHERE pipeline_status IN ('recommended', 'contact_identified', 'outreach_approved')) AS outreach_ready,
                  COUNT(*) FILTER (WHERE pipeline_status IN ('active_partnership', 'campaign_completed', 'under_performance_review')) AS partnerships
           FROM rpi_creators WHERE merged_into_id IS NULL""")
    paying = await conn.fetchval(
        "SELECT COUNT(*) FROM rpi_customers WHERE first_paid_at IS NOT NULL AND last_touch_campaign_id IS NOT NULL")
    return {
        "total_research_spend_usd": str(total.quantize(Q)),
        "month_to_date_spend_usd": str(month.quantize(Q)),
        "by_provider": [{"provider": r["provider"], "usd": str(Decimal(r["usd"]).quantize(Q)), "calls": r["calls"],
                         "denied": r["denied"]} for r in by_provider],
        "cost_per_discovered_creator_usd": _per(total, counts["discovered"]),
        "cost_per_qualified_prospect_usd": _per(total, counts["qualified"]),
        "cost_per_outreach_ready_prospect_usd": _per(total, counts["outreach_ready"]),
        "cost_per_secured_partnership_usd": _per(total, counts["partnerships"]),
        "research_cost_per_paying_customer_usd": _per(total, paying),
        "counts": dict(counts) | {"attributed_paying_customers": paying},
        "note": "Research spend only (data providers + AI inference). Influencer fees and commissions are in partner reports.",
    }


async def executive(conn) -> dict:
    pipeline = await conn.fetch(
        "SELECT pipeline_status, COUNT(*) AS n FROM rpi_creators WHERE merged_into_id IS NULL GROUP BY pipeline_status")
    by_status = {r["pipeline_status"]: r["n"] for r in pipeline}
    month_partner_spend = Decimal(await conn.fetchval(
        """SELECT COALESCE(-SUM(amount_usd), 0) FROM rpi_ledger_entries
           WHERE entry_type IN ('influencer_fee', 'affiliate_commission', 'other_campaign_cost')
             AND occurred_at >= date_trunc('month', NOW())"""))
    new_paying = await conn.fetchval(
        """SELECT COUNT(*) FROM rpi_customers WHERE first_paid_at >= date_trunc('month', NOW())
           AND last_touch_campaign_id IS NOT NULL""")
    partners = await partner_report(conn)
    top = sorted(partners, key=lambda p: Decimal(p["campaign_profit_usd"]), reverse=True)[:5]
    alerts = await conn.fetch(
        "SELECT kind, severity, message, created_at FROM rpi_alerts WHERE acknowledged_at IS NULL ORDER BY created_at DESC LIMIT 20")
    pending_drafts = await conn.fetchval("SELECT COUNT(*) FROM rpi_outreach_drafts WHERE status = 'draft'")
    review = await conn.fetchval("SELECT COUNT(*) FROM rpi_creator_identity_matches WHERE decision = 'pending_review'")
    followups = await conn.fetchval(
        "SELECT COUNT(*) FROM rpi_creators WHERE next_action_due IS NOT NULL AND next_action_due <= CURRENT_DATE")
    return {
        "total_discovered": sum(by_status.values()),
        "qualified_prospects": sum(v for k, v in by_status.items() if k not in ("discovered", "rejected", "do_not_contact")),
        "active_negotiations": by_status.get("negotiating", 0) + by_status.get("agreement_pending", 0),
        "active_partnerships": by_status.get("active_partnership", 0),
        "pipeline": by_status,
        "monthly_partnership_spend_usd": str(month_partner_spend.quantize(Q)),
        "new_paying_subscribers_this_month": new_paying,
        "campaign_contribution_profit_usd": str(sum((Decimal(p["campaign_profit_usd"]) for p in partners), Decimal("0")).quantize(Q)),
        "profit_reports_complete": all(p["complete"] for p in partners) if partners else None,
        "top_partners": top,
        "alerts": [dict(a) for a in alerts],
        "action_items": {"drafts_awaiting_review": pending_drafts, "identity_matches_to_review": review,
                         "follow_ups_due": followups},
        "research_costs": await research_costs(conn),
    }
