# Operations runbook

Base path `https://<host>/rpi/api/v1`, header `X-RPI-Key`. UI at `https://<host>/rpi/ui`.

## Weekly research cycle

1. **Estimate**: Discovery → choose strategies → *Estimate cost* (`POST /discovery/estimate`).
2. **Discover**: *Run discovery* with a job budget. Runs of ≤200 candidates run inline; larger runs are queued
   (Celery `discovery` queue). Review the run's warnings, duplicates and "why included".
3. **Screen** (stage 2): select candidates → *Screen selected*, or `POST /research/funnel` with `screen`/`analyze`
   sizes (defaults: 200 / 50). Rejected-for-niche creators never receive paid enrichment afterwards.
4. **Deep analysis** (stages 3–4): demographics, provider sponsorship history, selective transcripts, contacts.
5. **Review**: creator profile → evidence, provenance tags, missing data, policy exclusions, risk screen.
6. **Compare** shortlisted creators, then model the economics in the **Profitability Lab** with the creator's
   expected views and requested fee (add the fee as a `sponsorship_request` note so scoring can use it).
7. **Outreach**: *Draft outreach* → edit → approver *Approve* → *Send* (only if sending is enabled).
8. **Negotiate**: CRM transitions + `negotiation` notes; create the deal; mark it `agreed` only with a contract
   reference when required.
9. **Launch**: create the campaign (referral code + UTM), record the influencer fee as a campaign cost with the
   invoice reference, move the creator to `active_partnership`.
10. **Measure**: Campaigns → partner profitability (switch first/last touch) and cohorts.

## Daily checks (5 minutes)

* Executive dashboard alerts (budget thresholds, reconciliation failures).
* Administration → Jobs: resume `paused_budget` jobs only after deciding the extra spend is worth it.
* Outreach drafts awaiting review; follow-ups due.

## Budgets

* View: Administration → Budgets (`GET /admin/budgets`).
* Decrease: applies immediately. Increase: creates a request that **another** admin must approve.
* At 80% of any budget, low-priority jobs are refused; at 100% everything is refused.
* Per-creator cap: `RPI_MAX_COST_PER_CREATOR_USD` (default $1.50); per-job budget per request.

## Monthly

* `POST /ledger/allocate-variable-costs {"month": "YYYY-MM-01"}` until the product sends real per-render costs
  (entries are flagged estimates).
* Add FX rates for any non-USD currency (`PUT /ledger/fx-rates`) with their source.
* Review `GET /reports/reconciliation`; reprocess failed billing events with `POST /ledger/reprocess-failed`.
* Review provider spend vs value: `GET /dashboard/costs`, discovery run `strategy_breakdown` (audience-first
  unique finds).

## Incidents

| Symptom | Action |
|---|---|
| Provider circuit open / many errors | Administration → Providers; check the provider status page; jobs pause or record errors and can be resumed later |
| `credits_exhausted` | Top up with the provider, or lower usage; nothing retries automatically |
| Unexpected spend | Lower the global daily budget to 0 (immediate); inspect `GET /admin/spend` and `/admin/requests` |
| Wrong message approved | Reject the draft before sending; after sending, mark the creator `do_not_contact` if requested |
| Creator deletion request | `DELETE /creators/{id}?reason=...` (admin). If campaigns exist, deletion is refused; anonymize manually and keep financial records |
| Revoked YouTube Analytics access | Revocation endpoint; data is deleted by the daily retention task within 7 days |
| Webhook failures | `rpi_billing_events.processing_status = 'failed'` → fix the cause → `POST /ledger/reprocess-failed` |

## Rate limits and concurrency

Token buckets are per process. With the worker's `--concurrency=2`, the effective provider ceiling is 2× the
configured rate (CreatorDB default 5 req/s per process; SparkToro 0.9 req/s, under its 60/min paid limit).
