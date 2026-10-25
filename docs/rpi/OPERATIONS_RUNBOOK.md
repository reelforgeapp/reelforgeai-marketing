# Operations runbook

Base path `https://<host>/rpi/api/v1`, header `X-RPI-Key`. UI at `https://<host>/rpi/ui`.

## Weekly research cycle

1. **Estimate**: Discovery → choose strategies → *Estimate cost* (`POST /discovery/estimate`). The estimate shows
   credits and the final criteria after the preset.
2. **Discover**: *Run discovery* with the `quality` preset (default) and a job budget. Runs of ≤200 candidates run
   inline; larger runs are queued (Celery `discovery` queue). Review the run's warnings, duplicates, "why
   included" and `matching_in_provider` (how many creators pass every filter).
3. **Screen** (stage 2): select candidates → *Screen selected*, or `POST /research/funnel` with `screen`/`analyze`
   sizes (defaults: 25 / 10). The funnel screens only creators an RPI discovery run preliminarily qualified;
   creators imported from Partner Scout or added by hand are screened only when selected explicitly.
   Rejected-for-niche creators never receive paid enrichment afterwards.
4. **Deep analysis** (stages 3–4): provider sponsorship history, selective transcripts, and a contact lookup for
   recommended creators only. Audience demographics only when `RPI_FETCH_AUDIENCE_DEMOGRAPHICS=true`.
5. **Review**: creator profile → evidence, provenance tags, missing data, policy exclusions, risk screen.
6. **Compare** shortlisted creators, then model the economics in the **Profitability Lab** with the creator's
   expected views and requested fee (add the fee as a `sponsorship_request` note so scoring can use it).
7. **Outreach**: *Draft outreach* → edit → *Send test to me* (only to `RPI_TEST_RECIPIENTS`; the creator is not
   emailed) → approver *Approve* → *Send* (only if sending is enabled). If Partner
   Scout already emailed the creator, the creator page and the draft say so, the draft is written as a follow-up,
   and sending waits until `min_days_between_contacts` after Partner Scout's last email. Partner Scout does not
   record replies; check the inbox before following up.
8. **Negotiate**: CRM transitions + `negotiation` notes; create the deal; mark it `agreed` only with a contract
   reference when required.
9. **Launch**: create the campaign (referral code + UTM), record the influencer fee as a campaign cost with the
   invoice reference, move the creator to `active_partnership`.
10. **Measure**: Campaigns → partner profitability (switch first/last touch) and cohorts.

## Credit-efficient discovery (CreatorDB)

CreatorDB charges 1 credit per search page of up to 100 creators for up to 10 filters, and 3–15 credits per
creator for enrichment. RPI therefore filters first and enriches last:

| Step | Credits | Rule |
|---|---|---|
| Search (`quality` preset) | 1 per run | 10 filters: educator hashtags, AI / content-creation / video-editing topics, English, main audience US/CA/GB/AU, 10k–150k subscribers, median views ≥ 2,000, engagement ≥ 2%, ≥ 2 uploads in 30 days, has a contact email. Sorted by median views; only `max_candidates` (default 25) are stored |
| Preliminary screen | 0 | Provider-enforced filters count as met; names mostly in non-Latin script are held back |
| Screen (recent videos) | 3 per creator | Top 25 by preliminary score, and only when the provider's data may be used for scoring |
| Deep analysis | 5 per creator | Top 10 screened; sponsorship history |
| Contact | 15 per creator | Only `recommend`; charged only when an email exists |
| Audience demographics | 10 per creator | Off by default; search already filters the main audience country |

Paid calls whose data the score may not use are skipped: with `RPI_CREATORDB_SCORING_PERMITTED=false` no
CreatorDB screening runs at all, because it could not change a recommendation. Cached and recently fetched
results are never re-bought (freshness windows in `rpi/enrichment/value_gate.py`).

Search relevance limits, from live tests on 2026-10-25: CreatorDB niches are hashtag-derived and noisy, the
`mainLanguage` label misses many bilingual channels, and hashtags such as `#facelessyoutube` also match channels
that publish AI content rather than teach it. Screening the recent videos is what separates them. Use
`quality_broad` (topics + niches, including income niches) for a wider pool, or `preset: "none"` to search
with only the fields you send.

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
