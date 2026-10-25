# Profit model

Code: `rpi/profit_modeling/` (`model.py`, `deals.py`, `defaults.py`, `actuals.py`). API:
`POST /rpi/api/v1/profit/forecast`. UI: Profitability Lab. All arithmetic is `Decimal`; no LLM arithmetic.

## Primary metric

**Retained contribution per acquisition dollar** =
`(N × incrementality × contribution per customer) / (upfront fee + commissions + bonus + other campaign costs)`.

A value above 1.0 means the campaign returns more contribution than it costs. Net campaign profit is reported
next to it. "Incremental" depends on the `incrementality` input; there is no holdout data yet, so it defaults to
1.0 and is labeled as an assumption.

## Per-customer contribution (before acquisition costs)

For each plan and month `m` of the horizon (default 12):

```
revenue_m     = price if charged in month m (monthly plans every month; annual plans at m % 12 == 0)
refunds_0     = price × first_payment_refund_rate        (month 0 only)
fees_m        = price × fee_rate + fee_fixed               (when charged)
variable_m    = render + storage + delivery + AI cost per active customer-month
contribution_m = revenue_m − refunds − fees_m − variable_m
12-month contribution = Σ_m survival_m × contribution_m
```

Survival is `(1 − monthly_churn)^m` for monthly plans, `renewal_rate^(m // 12)` for annual plans, or an observed
curve from the ledger. The plan mix weights the per-plan results.

**No double counting:** the affiliate commission is *not* part of the contribution margin. It is a deal-specific
acquisition cost applied to commissionable net revenue (`revenue − refunds`) for the commission duration.
Variable costs are explicit per customer-month and never also applied as a margin percentage.

## Campaign

```
N                 = expected_views × click_through_rate × visitor_to_paid_rate
gross_contribution = N × incrementality × contribution_per_customer
commission_total   = N × commission_per_customer          (owed on every attributed customer)
campaign_profit    = gross_contribution − commission_total − bonus(N) − upfront_fee − other_costs
```

## Outputs

| Output | Definition |
|---|---|
| Scenarios | Pessimistic (low views/CTR/conversion, high churn), base, optimistic |
| Break-even customers | Smallest attributed-customer count with profit ≥ 0 (bisection; handles bonuses). `None` if each customer loses money |
| Payback month | First month index where cumulative cash (fees and bonus paid at month 0) ≥ 0, else "not within horizon" |
| Max justified upfront fee | `gross / (1 + required_roi) − (commission + bonus + other)` |
| Sensitivity grid | Profit for churn × {0.5, 0.75, 1, 1.5, 2} and conversion × {0.25, 0.5, 1, 1.5, 2} |
| Assumption simulation | 2,000 seeded triangular draws over the stated ranges; labeled `assumption_derived`, explicitly not a calibrated probability |
| Basis summary | Basis (actual / estimate / assumption) of every input and the share that are actual |

## Deal optimizer

Compares fixed fee, standard affiliate only, fixed + standard recurring, reduced fee (50%) + 6-month commission,
and reduced fee + performance bonus (per customer above 5, capped at the proposed fee). Ranked by
`0.3·pessimistic + 0.5·base + 0.2·optimistic + 0.5·min(pessimistic, 0)`. Any structure whose commission differs
from the published program terms, or that includes a bonus, is flagged `requires_documented_agreement`;
the API refuses to mark such a deal `agreed` without a `contract_reference`.

## Default inputs (until ledger actuals exist)

| Input | Value | Basis / source |
|---|---|---|
| Plan prices | Creator $17, Hustler $37, Mogul $67, Empire $147 per month | Estimate: reelforge-ai `backend/config/tiers.js` (list price) |
| Variable cost | cost per reel × quota × 40% usage → $2.52 / $8.10 / $25.80 / $86.00 per month | Estimate: `docs/ENG-PROPOSAL-CUT-PREMIUM-COGS.md` plus assumed usage |
| Plan mix | 55 / 30 / 12 / 3 % | Assumption |
| Monthly churn | 8% / 12% / 18% | Assumption |
| First-payment refunds | 3% | Assumption |
| Card fees | 2.9% + $0.30 | Assumption (confirm against Stripe) |
| CTR | 0.4% / 1% / 2% | Assumption |
| Visit → paid | 1% / 3% / 6% | Assumption |
| Program terms | 30% recurring for 12 months, 60-day window | Duration confirmed by ReelForge 2026-10-25; window unconfirmed (`/affiliate-terms` 60 days, `/affiliates` 90) |

With these defaults a $500 fixed sponsorship at 20,000 expected views gives about 6 customers in the base case,
about $144 contribution per customer and about $363 base profit, with a pessimistic loss of about $456. Treat these
numbers as illustrations of the model, not forecasts.

## Ledger actuals

`inputs_from_ledger` replaces price and mix with ACTUAL values once a plan has ≥ 30 paying customers, and uses
observed monthly retention once at least three cohort months each cover ≥ 30 customers. Otherwise defaults stay
and the response says why.

## Observed profitability (reports)

`GET /reports/partners` computes from the ledger, per campaign and attribution model: signups, paying and
active customers, net revenue, variable costs (estimated share shown), contribution, acquisition costs by type,
campaign profit, contribution per acquisition dollar and cost per paying customer. A report is `complete` only
when payment fees and render costs exist for its customers, an acquisition cost is recorded, and no FX rate is
missing. Otherwise it states that profit is not exact.
