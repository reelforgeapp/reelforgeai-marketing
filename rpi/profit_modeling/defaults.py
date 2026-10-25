"""
Default ReelForge economics used until ledger actuals exist.

Every value is labeled ESTIMATE (with the repository document it came from)
or ASSUMPTION. None of these are measured results. `rpi.profit_modeling.actuals`
replaces them with ACTUAL values once the billing ledger has data.

Sources (reelforge-ai repository, read 2026-10-25):
  * backend/config/tiers.js: plan prices (monthly / annual) and video limits
  * docs/ENG-PROPOSAL-CUT-PREMIUM-COGS.md: estimated cost per reel by tier
  * affiliate terms: 30% recurring for 12 months (confirmed by ReelForge 2026-10-25), 60-day window
"""

from __future__ import annotations

from decimal import Decimal

from rpi.profit_modeling.model import (
    Basis,
    CampaignInputs,
    Funnel,
    Input,
    PaymentFees,
    PlanEconomics,
    Range,
)

TIERS_SRC = "reelforge-ai backend/config/tiers.js"
COGS_SRC = "reelforge-ai docs/ENG-PROPOSAL-CUT-PREMIUM-COGS.md (per-reel estimate x assumed 40% quota usage)"

# Estimated cost per reel by tier and video quota per month.
_REEL_COST = {"creator": Decimal("0.21"), "hustler": Decimal("0.27"), "mogul": Decimal("0.43"), "empire": Decimal("0.43")}
_QUOTA = {"creator": 30, "hustler": 75, "mogul": 150, "empire": 500}
_PRICE = {"creator": Decimal("17"), "hustler": Decimal("37"), "mogul": Decimal("67"), "empire": Decimal("147")}
# Assumed share of the monthly video quota an average paying customer uses.
QUOTA_USAGE_ASSUMPTION = Decimal("0.40")
# Assumed new-customer plan mix for referred customers (no data yet).
_MIX = {"creator": Decimal("0.55"), "hustler": Decimal("0.30"), "mogul": Decimal("0.12"), "empire": Decimal("0.03")}


def variable_cost(tier: str) -> Input:
    monthly = (_REEL_COST[tier] * _QUOTA[tier] * QUOTA_USAGE_ASSUMPTION).quantize(Decimal("0.01"))
    return Input(monthly, Basis.ESTIMATE, COGS_SRC)


def default_plans() -> tuple[PlanEconomics, ...]:
    churn = Range.of("0.08", "0.12", "0.18", Basis.ASSUMPTION, "assumed monthly churn; replace with ledger cohorts")
    return tuple(
        PlanEconomics(
            plan=tier,
            billing_period="monthly",
            price=Input(_PRICE[tier], Basis.ESTIMATE, TIERS_SRC + " (list price, before discounts and tax)"),
            mix=Input(_MIX[tier], Basis.ASSUMPTION, "assumed plan mix for referred customers"),
            variable_cost_monthly=variable_cost(tier),
            monthly_churn=churn,
            first_payment_refund_rate=Input(Decimal("0.03"), Basis.ASSUMPTION, "assumed first-payment refund rate"),
        )
        for tier in ("creator", "hustler", "mogul", "empire")
    )


def default_fees() -> PaymentFees:
    return PaymentFees(
        rate=Input(Decimal("0.029"), Basis.ASSUMPTION, "standard card processing rate; confirm against Stripe balance transactions"),
        fixed=Input(Decimal("0.30"), Basis.ASSUMPTION, "standard card processing fixed fee"),
    )


def default_funnel(expected_views: int = 20000) -> Funnel:
    return Funnel(
        expected_views=Range.of(int(expected_views * 0.5), expected_views, int(expected_views * 1.5)),
        click_through_rate=Range.of("0.004", "0.01", "0.02", Basis.ASSUMPTION, "assumed sponsored-link CTR"),
        visitor_to_paid_rate=Range.of("0.01", "0.03", "0.06", Basis.ASSUMPTION, "assumed visit-to-paid conversion"),
    )


def default_inputs(expected_views: int = 20000) -> CampaignInputs:
    return CampaignInputs(plans=default_plans(), payment_fees=default_fees(), funnel=default_funnel(expected_views))
