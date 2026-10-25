"""
Commercial profit engine.

Deterministic, Decimal-based cohort economics for an influencer campaign.
No LLM arithmetic. Every input carries a `Basis` (actual / estimate /
assumption) and the output reports which basis each result rests on.

Definitions (see docs/rpi/PROFIT_MODEL.md):

  per-customer monthly contribution (before acquisition costs)
      = revenue − refunds − payment fees − variable delivery costs
  The deal-specific affiliate commission is NOT inside the margin; it is an
  acquisition cost applied per deal, so it is never double counted.

  12_month_contribution = Σ_m survival[m] × contribution[m]   (per customer)
  campaign_profit       = N × incrementality × (contribution − commission)
                          − bonus − upfront_fee − other_campaign_costs

  primary metric: retained contribution per acquisition dollar
      = (N × incrementality × contribution) / (upfront + commission + bonus + other)
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field, replace
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from typing import Optional

ZERO = Decimal("0")
ONE = Decimal("1")
CENT = Decimal("0.01")


class Basis(str, Enum):
    ACTUAL = "actual"  # from the billing ledger / measured campaign results
    ESTIMATE = "estimate"  # derived from documented internal estimates
    ASSUMPTION = "assumption"  # operator planning input


@dataclass(frozen=True)
class Input:
    value: Decimal
    basis: Basis = Basis.ASSUMPTION
    source: str = "operator input"

    @staticmethod
    def of(value, basis: Basis = Basis.ASSUMPTION, source: str = "operator input") -> "Input":
        return Input(Decimal(str(value)), basis, source)


@dataclass(frozen=True)
class Range:
    """Pessimistic / base / optimistic values for one uncertain input."""

    low: Decimal
    base: Decimal
    high: Decimal
    basis: Basis = Basis.ASSUMPTION
    source: str = "operator input"

    @staticmethod
    def of(low, base, high, basis: Basis = Basis.ASSUMPTION, source: str = "operator input") -> "Range":
        lo, b, hi = Decimal(str(low)), Decimal(str(base)), Decimal(str(high))
        if not (lo <= b <= hi):
            raise ValueError(f"range must satisfy low <= base <= high (got {lo}, {b}, {hi})")
        return Range(lo, b, hi, basis, source)

    def pick(self, scenario: "Scenario", higher_is_better: bool = True) -> Decimal:
        if scenario == Scenario.BASE:
            return self.base
        good = self.high if higher_is_better else self.low
        bad = self.low if higher_is_better else self.high
        return good if scenario == Scenario.OPTIMISTIC else bad


class Scenario(str, Enum):
    PESSIMISTIC = "pessimistic"
    BASE = "base"
    OPTIMISTIC = "optimistic"


@dataclass(frozen=True)
class PlanEconomics:
    plan: str
    billing_period: str  # "monthly" | "annual"
    price: Input  # per billing period, net of tax and typical discounts
    mix: Input  # share of new paying customers on this plan (sums to 1)
    variable_cost_monthly: Input  # render + storage + delivery + AI per active customer-month
    monthly_churn: Optional[Range] = None  # monthly plans
    survival_curve: Optional[tuple[Decimal, ...]] = None  # measured; overrides churn
    survival_basis: Basis = Basis.ASSUMPTION
    annual_renewal_rate: Optional[Range] = None  # annual plans
    first_payment_refund_rate: Input = field(default_factory=lambda: Input.of("0"))


@dataclass(frozen=True)
class PaymentFees:
    rate: Input
    fixed: Input


@dataclass(frozen=True)
class Funnel:
    expected_views: Range
    click_through_rate: Range  # views → referred visits
    visitor_to_paid_rate: Range  # visits → paying customers in the attribution window


@dataclass(frozen=True)
class CommissionTerms:
    rate: Decimal
    # None = lifetime of the customer (applies for the whole horizon).
    duration_months: Optional[int] = None


@dataclass(frozen=True)
class BonusTerms:
    threshold_customers: Decimal = ZERO
    per_customer_above_threshold_usd: Decimal = ZERO
    flat_bonus_at_threshold_usd: Decimal = ZERO
    cap_usd: Optional[Decimal] = None

    def amount(self, customers: Decimal) -> Decimal:
        if customers < self.threshold_customers or self.threshold_customers < ZERO:
            return ZERO
        total = self.flat_bonus_at_threshold_usd + self.per_customer_above_threshold_usd * (
            customers - self.threshold_customers
        )
        if self.cap_usd is not None:
            total = min(total, self.cap_usd)
        return max(total, ZERO)


class DealKind(str, Enum):
    FIXED = "fixed"
    AFFILIATE_ONLY = "affiliate_only"
    FIXED_PLUS_RECURRING = "fixed_plus_recurring"
    REDUCED_FIXED_LIMITED_COMMISSION = "reduced_fixed_limited_commission"
    FIXED_PLUS_BONUS = "fixed_plus_bonus"


@dataclass(frozen=True)
class DealStructure:
    kind: DealKind
    upfront_fee: Decimal = ZERO
    commission: Optional[CommissionTerms] = None
    bonus: Optional[BonusTerms] = None
    label: str = ""


@dataclass(frozen=True)
class CampaignInputs:
    plans: tuple[PlanEconomics, ...]
    payment_fees: PaymentFees
    funnel: Funnel
    other_campaign_costs: Input = field(default_factory=lambda: Input.of("0"))
    # Share of attributed customers who would not have converted anyway.
    incrementality: Input = field(
        default_factory=lambda: Input.of("1", Basis.ASSUMPTION, "no holdout data: all attributed customers assumed incremental")
    )
    horizon_months: int = 12

    def validate(self) -> list[str]:
        problems = []
        if not self.plans:
            problems.append("at least one plan is required")
        mix_total = sum((p.mix.value for p in self.plans), ZERO)
        if self.plans and abs(mix_total - ONE) > Decimal("0.001"):
            problems.append(f"plan mix must sum to 1 (got {mix_total})")
        for p in self.plans:
            if p.billing_period not in ("monthly", "annual"):
                problems.append(f"{p.plan}: billing_period must be monthly or annual")
            if p.billing_period == "monthly" and p.monthly_churn is None and p.survival_curve is None:
                problems.append(f"{p.plan}: monthly plan needs monthly_churn or survival_curve")
            if p.billing_period == "annual" and p.annual_renewal_rate is None and self.horizon_months > 12:
                problems.append(f"{p.plan}: annual plan needs annual_renewal_rate for horizons over 12 months")
            if p.price.value < ZERO or p.variable_cost_monthly.value < ZERO:
                problems.append(f"{p.plan}: price and variable cost must be non-negative")
            if p.monthly_churn and not (ZERO <= p.monthly_churn.low and p.monthly_churn.high <= ONE):
                problems.append(f"{p.plan}: monthly churn must be within [0, 1]")
        if not (1 <= self.horizon_months <= 60):
            problems.append("horizon_months must be between 1 and 60")
        if not (ZERO <= self.incrementality.value <= ONE):
            problems.append("incrementality must be within [0, 1]")
        return problems


# --------------------------------------------------------------------------
# Per-customer cash flows
# --------------------------------------------------------------------------


@dataclass
class PlanCashflow:
    plan: str
    survival: list[Decimal]
    contribution: list[Decimal]  # per surviving customer, before commission
    commissionable_revenue: list[Decimal]  # net revenue the commission applies to

    def expected_contribution(self) -> Decimal:
        return sum((s * c for s, c in zip(self.survival, self.contribution)), ZERO)

    def expected_commission(self, terms: Optional[CommissionTerms]) -> Decimal:
        if not terms or terms.rate <= ZERO:
            return ZERO
        months = len(self.survival) if terms.duration_months is None else min(terms.duration_months, len(self.survival))
        return sum(
            (terms.rate * self.survival[m] * self.commissionable_revenue[m] for m in range(months)), ZERO
        )

    def monthly_net(self, terms: Optional[CommissionTerms]) -> list[Decimal]:
        out = []
        for m, (s, c, r) in enumerate(zip(self.survival, self.contribution, self.commissionable_revenue)):
            commission = ZERO
            if terms and terms.rate > ZERO and (terms.duration_months is None or m < terms.duration_months):
                commission = terms.rate * r
            out.append(s * (c - commission))
        return out


def _survival(plan: PlanEconomics, horizon: int, scenario: Scenario, churn_multiplier: Decimal) -> list[Decimal]:
    if plan.billing_period == "annual":
        renewal = plan.annual_renewal_rate.pick(scenario) if plan.annual_renewal_rate else ZERO
        if churn_multiplier != ONE:
            renewal = max(ZERO, ONE - (ONE - renewal) * churn_multiplier)
        return [renewal ** (m // 12) for m in range(horizon)]
    if plan.survival_curve:
        curve = list(plan.survival_curve[:horizon])
        if len(curve) < horizon:
            # Extend with the last observed month-over-month retention.
            step = curve[-1] / curve[-2] if len(curve) >= 2 and curve[-2] > ZERO else ONE
            while len(curve) < horizon:
                curve.append(curve[-1] * step)
        if churn_multiplier != ONE:
            k = float(churn_multiplier)
            curve = [Decimal(str(float(s) ** k)) if s > ZERO else ZERO for s in curve]
        return curve
    assert plan.monthly_churn is not None
    churn = plan.monthly_churn.pick(scenario, higher_is_better=False) * churn_multiplier
    churn = min(max(churn, ZERO), ONE)
    return [(ONE - churn) ** m for m in range(horizon)]


def plan_cashflow(
    plan: PlanEconomics,
    fees: PaymentFees,
    horizon: int,
    scenario: Scenario = Scenario.BASE,
    churn_multiplier: Decimal = ONE,
) -> PlanCashflow:
    survival = _survival(plan, horizon, scenario, churn_multiplier)
    price = plan.price.value
    refund_rate = plan.first_payment_refund_rate.value
    contribution, commissionable = [], []
    for m in range(horizon):
        charged = plan.billing_period == "monthly" or m % 12 == 0
        revenue = price if charged else ZERO
        refunds = price * refund_rate if m == 0 else ZERO
        fee = (price * fees.rate.value + fees.fixed.value) if charged else ZERO
        variable = plan.variable_cost_monthly.value
        contribution.append(revenue - refunds - fee - variable)
        commissionable.append(revenue - refunds)
    return PlanCashflow(plan.plan, survival, contribution, commissionable)


# --------------------------------------------------------------------------
# Campaign evaluation
# --------------------------------------------------------------------------


@dataclass
class CampaignResult:
    scenario: str
    deal_kind: str
    customers: Decimal
    incremental_customers: Decimal
    contribution_per_customer: Decimal
    commission_per_customer: Decimal
    gross_contribution: Decimal
    commission_total: Decimal
    bonus_total: Decimal
    upfront_fee: Decimal
    other_costs: Decimal
    acquisition_spend: Decimal
    campaign_profit: Decimal
    contribution_per_acquisition_dollar: Optional[Decimal]
    payback_month: Optional[int]
    break_even_customers: Optional[Decimal]
    max_justified_upfront_fee: Decimal
    creator_payout: Decimal
    monthly_cumulative: list[Decimal]

    def as_dict(self) -> dict:
        def q(v):
            if v is None:
                return None
            if isinstance(v, Decimal):
                return str(v.quantize(CENT, rounding=ROUND_HALF_UP))
            return v
        d = {k: q(v) for k, v in self.__dict__.items() if k != "monthly_cumulative"}
        d["customers"] = str(self.customers.quantize(Decimal("0.01")))
        d["incremental_customers"] = str(self.incremental_customers.quantize(Decimal("0.01")))
        if self.contribution_per_acquisition_dollar is not None:
            d["contribution_per_acquisition_dollar"] = str(
                self.contribution_per_acquisition_dollar.quantize(Decimal("0.001"))
            )
        d["monthly_cumulative"] = [q(v) for v in self.monthly_cumulative]
        return d


def expected_customers(funnel: Funnel, scenario: Scenario) -> Decimal:
    return (
        funnel.expected_views.pick(scenario)
        * funnel.click_through_rate.pick(scenario)
        * funnel.visitor_to_paid_rate.pick(scenario)
    )


def _blend(inputs: CampaignInputs, scenario: Scenario, churn_multiplier: Decimal):
    flows = [
        (p.mix.value, plan_cashflow(p, inputs.payment_fees, inputs.horizon_months, scenario, churn_multiplier))
        for p in inputs.plans
    ]
    return flows


def evaluate(
    inputs: CampaignInputs,
    deal: DealStructure,
    scenario: Scenario = Scenario.BASE,
    *,
    customers_override: Optional[Decimal] = None,
    churn_multiplier: Decimal = ONE,
    required_roi: Decimal = ZERO,
) -> CampaignResult:
    problems = inputs.validate()
    if problems:
        raise ValueError("; ".join(problems))

    flows = _blend(inputs, scenario, churn_multiplier)
    customers = customers_override if customers_override is not None else expected_customers(inputs.funnel, scenario)
    incr = inputs.incrementality.value
    n = customers * incr

    contribution_pc = sum((mix * f.expected_contribution() for mix, f in flows), ZERO)
    commission_pc = sum((mix * f.expected_commission(deal.commission) for mix, f in flows), ZERO)
    # Commission is owed on every attributed customer, incremental or not.
    gross = n * contribution_pc
    commission_total = customers * commission_pc
    bonus_total = deal.bonus.amount(customers) if deal.bonus else ZERO
    other = inputs.other_campaign_costs.value
    upfront = deal.upfront_fee
    spend = upfront + commission_total + bonus_total + other
    profit = gross - commission_total - bonus_total - upfront - other

    per_dollar = (gross / spend) if spend > ZERO else None

    # Cumulative cash position by month (fees and bonus paid at month 0).
    cumulative = []
    running = -(upfront + other + bonus_total)
    for m in range(inputs.horizon_months):
        month_gross = sum((mix * f.survival[m] * f.contribution[m] for mix, f in flows), ZERO) * n
        month_commission = customers * sum(
            (mix * f.survival[m] * _commission_at(deal.commission, f, m) for mix, f in flows), ZERO
        )
        running += month_gross - month_commission
        cumulative.append(running)
    payback = next((m for m, v in enumerate(cumulative) if v >= ZERO), None)

    break_even = _break_even_customers(contribution_pc * incr, commission_pc, deal, other)
    max_fee = (gross / (ONE + required_roi)) - (commission_total + bonus_total + other)

    return CampaignResult(
        scenario=scenario.value,
        deal_kind=deal.kind.value,
        customers=customers,
        incremental_customers=n,
        contribution_per_customer=contribution_pc,
        commission_per_customer=commission_pc,
        gross_contribution=gross,
        commission_total=commission_total,
        bonus_total=bonus_total,
        upfront_fee=upfront,
        other_costs=other,
        acquisition_spend=spend,
        campaign_profit=profit,
        contribution_per_acquisition_dollar=per_dollar,
        payback_month=payback,
        break_even_customers=break_even,
        max_justified_upfront_fee=max_fee,
        creator_payout=upfront + commission_total + bonus_total,
        monthly_cumulative=cumulative,
    )


def _commission_at(terms: Optional[CommissionTerms], flow: PlanCashflow, m: int) -> Decimal:
    if not terms or terms.rate <= ZERO:
        return ZERO
    if terms.duration_months is not None and m >= terms.duration_months:
        return ZERO
    return terms.rate * flow.commissionable_revenue[m]


def _break_even_customers(
    net_contribution_pc: Decimal, commission_pc: Decimal, deal: DealStructure, other: Decimal
) -> Optional[Decimal]:
    """Smallest attributed-customer count with profit >= 0, or None when each
    customer loses money (so no volume breaks even)."""
    unit = net_contribution_pc - commission_pc
    fixed = deal.upfront_fee + other

    def profit(n: Decimal) -> Decimal:
        bonus = deal.bonus.amount(n) if deal.bonus else ZERO
        return n * unit - fixed - bonus

    if fixed <= ZERO and profit(ZERO) >= ZERO and not deal.bonus:
        return ZERO
    if unit <= ZERO:
        return None
    lo, hi = ZERO, Decimal("1")
    while profit(hi) < ZERO:
        hi *= 2
        if hi > Decimal("10000000"):
            return None
    for _ in range(80):
        mid = (lo + hi) / 2
        if profit(mid) >= ZERO:
            hi = mid
        else:
            lo = mid
    return hi.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# --------------------------------------------------------------------------
# Forecast bundle: scenarios, sensitivity, assumption-derived probability
# --------------------------------------------------------------------------


def sensitivity_grid(
    inputs: CampaignInputs,
    deal: DealStructure,
    churn_multipliers: tuple[Decimal, ...] = (Decimal("0.5"), Decimal("0.75"), ONE, Decimal("1.5"), Decimal("2")),
    conversion_multipliers: tuple[Decimal, ...] = (Decimal("0.25"), Decimal("0.5"), ONE, Decimal("1.5"), Decimal("2")),
) -> dict:
    base_customers = expected_customers(inputs.funnel, Scenario.BASE)
    rows = []
    for cm in churn_multipliers:
        row = []
        for vm in conversion_multipliers:
            r = evaluate(inputs, deal, Scenario.BASE, customers_override=base_customers * vm, churn_multiplier=cm)
            row.append(str(r.campaign_profit.quantize(CENT)))
        rows.append(row)
    return {
        "churn_multipliers": [str(c) for c in churn_multipliers],
        "conversion_multipliers": [str(c) for c in conversion_multipliers],
        "campaign_profit": rows,
        "note": "rows: churn multiplier; columns: conversion multiplier; base scenario otherwise",
    }


def assumption_simulation(
    inputs: CampaignInputs, deal: DealStructure, *, runs: int = 2000, seed: int = 7
) -> dict:
    """Triangular sampling over each Range. The result describes the
    operator's stated assumption ranges, not a calibrated probability."""
    rng = random.Random(seed)
    f = inputs.funnel

    def tri(r: Range) -> Decimal:
        if r.low == r.high:
            return r.base
        return Decimal(str(rng.triangular(float(r.low), float(r.high), float(r.base))))

    profits = []
    for _ in range(runs):
        customers = tri(f.expected_views) * tri(f.click_through_rate) * tri(f.visitor_to_paid_rate)
        churn_mult = Decimal(str(rng.triangular(0.75, 1.5, 1.0)))
        r = evaluate(inputs, deal, Scenario.BASE, customers_override=customers, churn_multiplier=churn_mult)
        profits.append(r.campaign_profit)
    profits.sort()

    def pct(p: float) -> str:
        return str(profits[min(len(profits) - 1, int(p * len(profits)))].quantize(CENT))

    positive = sum(1 for p in profits if p > ZERO)
    return {
        "label": "assumption_derived",
        "warning": "Derived from operator-entered ranges, not from observed campaign outcomes. "
        "Not a statistically calibrated probability.",
        "runs": runs,
        "share_of_runs_profitable": round(positive / runs, 3),
        "p10": pct(0.10),
        "p50": pct(0.50),
        "p90": pct(0.90),
    }


def basis_summary(inputs: CampaignInputs) -> dict:
    items = {
        "expected_views": inputs.funnel.expected_views.basis,
        "click_through_rate": inputs.funnel.click_through_rate.basis,
        "visitor_to_paid_rate": inputs.funnel.visitor_to_paid_rate.basis,
        "incrementality": inputs.incrementality.basis,
        "payment_fees": inputs.payment_fees.rate.basis,
        "other_campaign_costs": inputs.other_campaign_costs.basis,
    }
    for p in inputs.plans:
        items[f"{p.plan}.price"] = p.price.basis
        items[f"{p.plan}.mix"] = p.mix.basis
        items[f"{p.plan}.variable_cost"] = p.variable_cost_monthly.basis
        if p.survival_curve:
            items[f"{p.plan}.retention"] = p.survival_basis
        elif p.monthly_churn:
            items[f"{p.plan}.retention"] = p.monthly_churn.basis
        elif p.annual_renewal_rate:
            items[f"{p.plan}.retention"] = p.annual_renewal_rate.basis
    values = {k: v.value for k, v in items.items()}
    actual_share = sum(1 for v in items.values() if v == Basis.ACTUAL) / max(len(items), 1)
    return {
        "inputs": values,
        "share_actual": round(actual_share, 2),
        "is_forecast": True,
        "statement": "Forecast from the inputs above. Not a guaranteed return."
        + ("" if actual_share >= 0.5 else " Most inputs are assumptions, not measured results."),
    }


def forecast(inputs: CampaignInputs, deal: DealStructure, *, required_roi: Decimal = ZERO, simulate: bool = True) -> dict:
    scenarios = {
        s.value: evaluate(inputs, deal, s, required_roi=required_roi).as_dict()
        for s in (Scenario.PESSIMISTIC, Scenario.BASE, Scenario.OPTIMISTIC)
    }
    out = {
        "deal": _deal_dict(deal),
        "scenarios": scenarios,
        "sensitivity": sensitivity_grid(inputs, deal),
        "basis": basis_summary(inputs),
        "horizon_months": inputs.horizon_months,
        "required_roi": str(required_roi),
    }
    if simulate:
        out["assumption_simulation"] = assumption_simulation(inputs, deal)
    return out


def _deal_dict(deal: DealStructure) -> dict:
    return {
        "kind": deal.kind.value,
        "label": deal.label,
        "upfront_fee": str(deal.upfront_fee),
        "commission": None
        if not deal.commission
        else {"rate": str(deal.commission.rate), "duration_months": deal.commission.duration_months},
        "bonus": None
        if not deal.bonus
        else {
            "threshold_customers": str(deal.bonus.threshold_customers),
            "per_customer_above_threshold_usd": str(deal.bonus.per_customer_above_threshold_usd),
            "flat_bonus_at_threshold_usd": str(deal.bonus.flat_bonus_at_threshold_usd),
            "cap_usd": None if deal.bonus.cap_usd is None else str(deal.bonus.cap_usd),
        },
    }


def with_upfront(deal: DealStructure, fee: Decimal) -> DealStructure:
    return replace(deal, upfront_fee=fee)
