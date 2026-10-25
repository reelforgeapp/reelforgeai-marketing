"""Profit engine: cohort arithmetic, commissions, break-even, payback, scenarios."""

from decimal import Decimal as D

import pytest

from rpi.profit_modeling.deals import ProgramTerms, candidate_structures, compare_deals
from rpi.profit_modeling.defaults import default_inputs
from rpi.profit_modeling.model import (
    Basis,
    BonusTerms,
    CampaignInputs,
    CommissionTerms,
    DealKind,
    DealStructure,
    Funnel,
    Input,
    PaymentFees,
    PlanEconomics,
    Range,
    Scenario,
    evaluate,
    forecast,
    plan_cashflow,
)


def simple_plan(price="20", churn="0.10", variable="2", refund="0", period="monthly", renewal=None):
    return PlanEconomics(
        plan="p", billing_period=period, price=Input.of(price), mix=Input.of("1"),
        variable_cost_monthly=Input.of(variable),
        monthly_churn=Range.of(churn, churn, churn) if period == "monthly" else None,
        annual_renewal_rate=Range.of(renewal, renewal, renewal) if renewal else None,
        first_payment_refund_rate=Input.of(refund),
    )


def inputs(plan=None, customers=10, fee_rate="0", fee_fixed="0", horizon=12, incr="1", other="0"):
    views = D(customers) * 100  # ctr 0.1, conv 0.1 → customers
    return CampaignInputs(
        plans=(plan or simple_plan(),),
        payment_fees=PaymentFees(Input.of(fee_rate), Input.of(fee_fixed)),
        funnel=Funnel(Range.of(views, views, views), Range.of("0.1", "0.1", "0.1"), Range.of("0.1", "0.1", "0.1")),
        other_campaign_costs=Input.of(other),
        incrementality=Input.of(incr),
        horizon_months=horizon,
    )


def test_twelve_month_contribution_matches_closed_form():
    # contribution per month = 20 - 2 = 18; survival 0.9^m; Σ_{m<12} 0.9^m = (1 - 0.9^12) / 0.1
    flow = plan_cashflow(simple_plan(), PaymentFees(Input.of("0"), Input.of("0")), 12)
    expected = D("18") * (1 - D("0.9") ** 12) / D("0.1")
    assert abs(flow.expected_contribution() - expected) < D("0.000001")


def test_campaign_profit_formula_fixed_fee():
    inp = inputs(customers=10)
    r = evaluate(inp, DealStructure(DealKind.FIXED, D("500")))
    per_customer = D("18") * (1 - D("0.9") ** 12) / D("0.1")
    assert r.customers == D("10")
    assert abs(r.campaign_profit - (10 * per_customer - 500)) < D("0.0001")
    assert r.commission_total == 0
    assert r.creator_payout == D("500")


def test_commission_is_not_double_counted_in_margin():
    inp = inputs(customers=10)
    fixed = evaluate(inp, DealStructure(DealKind.FIXED, D("0")))
    aff = evaluate(inp, DealStructure(DealKind.AFFILIATE_ONLY, D("0"), CommissionTerms(D("0.30"), None)))
    # Same gross contribution; commission appears once, as acquisition cost.
    assert fixed.gross_contribution == aff.gross_contribution
    revenue_pc = D("20") * (1 - D("0.9") ** 12) / D("0.1")
    assert abs(aff.commission_total - D("0.30") * 10 * revenue_pc) < D("0.0001")
    assert abs((fixed.campaign_profit - aff.campaign_profit) - aff.commission_total) < D("0.0001")


def test_commission_duration_limits_months():
    inp = inputs(customers=1)
    lifetime = evaluate(inp, DealStructure(DealKind.AFFILIATE_ONLY, D(0), CommissionTerms(D("0.3"), None)))
    three = evaluate(inp, DealStructure(DealKind.AFFILIATE_ONLY, D(0), CommissionTerms(D("0.3"), 3)))
    expected_three = D("0.3") * D("20") * (1 + D("0.9") + D("0.81"))
    assert abs(three.commission_total - expected_three) < D("0.0001")
    assert three.commission_total < lifetime.commission_total


def test_payment_fees_and_refunds_reduce_contribution():
    fees = PaymentFees(Input.of("0.029"), Input.of("0.30"))
    flow = plan_cashflow(simple_plan(refund="0.05", churn="0"), fees, 2)
    fee = D("20") * D("0.029") + D("0.30")
    assert flow.contribution[0] == D("20") - D("1") - fee - D("2")  # 5% of first payment refunded
    assert flow.contribution[1] == D("20") - fee - D("2")


def test_annual_plan_charges_once_and_renews_at_month_12():
    plan = simple_plan(price="120", variable="1", period="annual", renewal="0.5")
    flow = plan_cashflow(plan, PaymentFees(Input.of("0"), Input.of("0")), 24)
    assert flow.contribution[0] == D("119")
    assert flow.contribution[5] == D("-1")  # service cost continues, no new charge
    assert flow.survival[11] == 1 and flow.survival[12] == D("0.5")
    assert flow.contribution[12] == D("119")


def test_break_even_and_payback():
    inp = inputs(customers=10)
    deal = DealStructure(DealKind.FIXED, D("500"))
    r = evaluate(inp, deal)
    per_customer = D("18") * (1 - D("0.9") ** 12) / D("0.1")
    assert abs(r.break_even_customers - (D("500") / per_customer)) < D("0.01")
    # 10 customers × 18 × 0.9^m: cumulative 180, 342, 487.8, 619.0 → crosses 500 at month index 3.
    assert r.payback_month == 3


def test_no_break_even_when_each_customer_loses_money():
    plan = simple_plan(price="5", variable="10")
    r = evaluate(inputs(plan=plan), DealStructure(DealKind.FIXED, D("100")))
    assert r.break_even_customers is None
    assert r.payback_month is None
    assert r.campaign_profit < 0


def test_max_justified_fee_with_required_roi():
    inp = inputs(customers=10)
    r0 = evaluate(inp, DealStructure(DealKind.FIXED, D("0")), required_roi=D("0"))
    r1 = evaluate(inp, DealStructure(DealKind.FIXED, D("0")), required_roi=D("1"))
    assert abs(r0.max_justified_upfront_fee - r0.gross_contribution) < D("0.0001")
    assert abs(r1.max_justified_upfront_fee - r0.gross_contribution / 2) < D("0.0001")
    at_max = evaluate(inp, DealStructure(DealKind.FIXED, r0.max_justified_upfront_fee))
    assert abs(at_max.campaign_profit) < D("0.0001")


def test_incrementality_scales_contribution_but_not_commission():
    inp = inputs(customers=10, incr="0.5")
    r = evaluate(inp, DealStructure(DealKind.AFFILIATE_ONLY, D(0), CommissionTerms(D("0.3"), None)))
    full = evaluate(inputs(customers=10), DealStructure(DealKind.AFFILIATE_ONLY, D(0), CommissionTerms(D("0.3"), None)))
    assert abs(r.gross_contribution - full.gross_contribution / 2) < D("0.0001")
    assert r.commission_total == full.commission_total


def test_bonus_terms_threshold_and_cap():
    bonus = BonusTerms(threshold_customers=D("5"), per_customer_above_threshold_usd=D("40"), cap_usd=D("100"))
    assert bonus.amount(D("4")) == 0
    assert bonus.amount(D("6")) == D("40")
    assert bonus.amount(D("50")) == D("100")


def test_scenarios_are_ordered():
    f = forecast(default_inputs(20000), DealStructure(DealKind.FIXED, D("500")), simulate=False)
    p = D(f["scenarios"]["pessimistic"]["campaign_profit"])
    b = D(f["scenarios"]["base"]["campaign_profit"])
    o = D(f["scenarios"]["optimistic"]["campaign_profit"])
    assert p < b < o
    assert f["basis"]["is_forecast"] is True
    assert "Not a guaranteed return" in f["basis"]["statement"]


def test_simulation_is_labeled_and_deterministic():
    inp = default_inputs()
    deal = DealStructure(DealKind.FIXED, D("300"))
    a = forecast(inp, deal)["assumption_simulation"]
    b = forecast(inp, deal)["assumption_simulation"]
    assert a == b
    assert a["label"] == "assumption_derived"


def test_validation_rejects_bad_inputs():
    plan = simple_plan()
    bad = CampaignInputs(plans=(PlanEconomics("x", "monthly", Input.of("10"), Input.of("0.5"), Input.of("1"),
                                              monthly_churn=Range.of("0.1", "0.1", "0.1")),),
                         payment_fees=PaymentFees(Input.of("0"), Input.of("0")), funnel=inputs().funnel)
    with pytest.raises(ValueError, match="mix must sum to 1"):
        evaluate(bad, DealStructure(DealKind.FIXED))
    with pytest.raises(ValueError):
        Range.of("0.5", "0.2", "0.9")
    assert plan  # silence


def test_sensitivity_grid_shape():
    f = forecast(inputs(), DealStructure(DealKind.FIXED, D("100")), simulate=False)
    grid = f["sensitivity"]["campaign_profit"]
    assert len(grid) == 5 and all(len(row) == 5 for row in grid)
    # More churn → less profit at fixed conversion.
    assert D(grid[0][2]) > D(grid[4][2])
    # More conversion → more profit at fixed churn.
    assert D(grid[2][4]) > D(grid[2][0])


def test_deal_comparison_flags_nonstandard_terms():
    program = ProgramTerms(D("0.30"), None)
    result = compare_deals(default_inputs(), candidate_structures(D("500"), program), program)
    by_kind = {r["deal"]["kind"]: r for r in result["ranking"]}
    assert by_kind["affiliate_only"]["requires_documented_agreement"] is False
    assert by_kind["reduced_fixed_limited_commission"]["requires_documented_agreement"] is True
    assert by_kind["fixed_plus_bonus"]["requires_documented_agreement"] is True
    assert result["recommended"] == result["ranking"][0]


def test_basis_summary_flags_assumption_heavy_forecasts():
    f = forecast(default_inputs(), DealStructure(DealKind.FIXED, D("500")), simulate=False)
    assert f["basis"]["share_actual"] == 0
    assert "Most inputs are assumptions" in f["basis"]["statement"]
    assert f["basis"]["inputs"]["creator.price"] == Basis.ESTIMATE.value
