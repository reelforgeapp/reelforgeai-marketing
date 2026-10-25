"""
Deal structure optimizer.

Compares fixed, affiliate-only, fixed + recurring, reduced fixed + limited
commission and fixed + bonus structures across the three scenarios and ranks
them by risk-adjusted profit. Any commission that differs from the published
ReelForge affiliate program terms is flagged as requiring a documented
agreement; the optimizer never assumes such terms are already agreed.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from rpi.profit_modeling.model import (
    CENT,
    ZERO,
    BonusTerms,
    CampaignInputs,
    CommissionTerms,
    DealKind,
    DealStructure,
    Scenario,
    evaluate,
)

# Weights for the risk-adjusted ranking. Downside losses are penalized on top
# of the weighted mean so a structure that loses badly when the campaign
# underperforms ranks below one with a similar mean but a smaller tail.
SCENARIO_WEIGHTS = {Scenario.PESSIMISTIC: Decimal("0.3"), Scenario.BASE: Decimal("0.5"), Scenario.OPTIMISTIC: Decimal("0.2")}
DOWNSIDE_PENALTY = Decimal("0.5")


@dataclass(frozen=True)
class ProgramTerms:
    """Affiliate program terms. ReelForge confirmed 30% recurring for 12
    months on 2026-10-25 (rpi_affiliate_commission_months; None = lifetime).
    The /affiliate-terms page still says customer lifetime and a 60-day
    window, and /affiliates says 90 days; see docs/rpi/KNOWN_LIMITATIONS.md."""

    rate: Decimal = Decimal("0.30")
    duration_months: Optional[int] = None
    attribution_window_days: int = 60

    def standard_commission(self) -> CommissionTerms:
        return CommissionTerms(self.rate, self.duration_months)

    def is_standard(self, terms: Optional[CommissionTerms]) -> bool:
        if terms is None:
            return True
        return terms.rate == self.rate and terms.duration_months == self.duration_months


def candidate_structures(
    proposed_fee: Decimal,
    program: ProgramTerms,
    *,
    reduced_fee_ratio: Decimal = Decimal("0.5"),
    limited_commission_months: int = 6,
    bonus: Optional[BonusTerms] = None,
) -> list[DealStructure]:
    standard = program.standard_commission()
    reduced = (proposed_fee * reduced_fee_ratio).quantize(CENT)
    structures = [
        DealStructure(DealKind.FIXED, proposed_fee, None, None, "Fixed sponsorship fee"),
        DealStructure(DealKind.AFFILIATE_ONLY, ZERO, standard, None, "Standard affiliate program only"),
        DealStructure(DealKind.FIXED_PLUS_RECURRING, proposed_fee, standard, None, "Fixed fee + standard recurring commission"),
        DealStructure(
            DealKind.REDUCED_FIXED_LIMITED_COMMISSION,
            reduced,
            CommissionTerms(program.rate, limited_commission_months),
            None,
            f"Reduced fee ({int(reduced_fee_ratio * 100)}%) + {limited_commission_months}-month commission",
        ),
    ]
    structures.append(
        DealStructure(
            DealKind.FIXED_PLUS_BONUS,
            reduced,
            None,
            bonus
            or BonusTerms(
                threshold_customers=Decimal("5"),
                per_customer_above_threshold_usd=Decimal("40"),
                cap_usd=proposed_fee,
            ),
            "Reduced fee + performance bonus per paying customer above 5",
        )
    )
    return structures


def compare_deals(inputs: CampaignInputs, structures: list[DealStructure], program: ProgramTerms) -> dict:
    rows = []
    for deal in structures:
        results = {s: evaluate(inputs, deal, s) for s in SCENARIO_WEIGHTS}
        weighted = sum((SCENARIO_WEIGHTS[s] * r.campaign_profit for s, r in results.items()), ZERO)
        downside = min(results[Scenario.PESSIMISTIC].campaign_profit, ZERO)
        risk_adjusted = weighted + DOWNSIDE_PENALTY * downside
        rows.append(
            {
                "deal": {
                    "kind": deal.kind.value,
                    "label": deal.label,
                    "upfront_fee": str(deal.upfront_fee),
                    "commission_rate": str(deal.commission.rate) if deal.commission else None,
                    "commission_months": deal.commission.duration_months if deal.commission else None,
                    "bonus": bool(deal.bonus),
                },
                "requires_documented_agreement": not program.is_standard(deal.commission) or deal.bonus is not None,
                "profit": {s.value: str(r.campaign_profit.quantize(CENT)) for s, r in results.items()},
                "creator_payout_base": str(results[Scenario.BASE].creator_payout.quantize(CENT)),
                "payback_month_base": results[Scenario.BASE].payback_month,
                "break_even_customers": str(results[Scenario.BASE].break_even_customers)
                if results[Scenario.BASE].break_even_customers is not None
                else None,
                "risk_adjusted_profit": str(risk_adjusted.quantize(CENT)),
                "_score": risk_adjusted,
            }
        )
    rows.sort(key=lambda r: r["_score"], reverse=True)
    for r in rows:
        r.pop("_score")
    best = rows[0] if rows else None
    return {
        "ranking": rows,
        "recommended": best,
        "method": "risk_adjusted = 0.3*pessimistic + 0.5*base + 0.2*optimistic + 0.5*min(pessimistic, 0)",
        "caveat": "Ranks deal structures under the stated assumptions. Any structure flagged "
        "requires_documented_agreement needs a signed agreement before it is offered.",
    }
