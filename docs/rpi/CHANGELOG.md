# RPI changelog

## 0.1.2 — 2026-10-25

### Targeted, credit-efficient discovery
* Discovery presets (`rpi/discovery/presets.py`). The default `quality` preset puts every rule CreatorDB can enforce
  into one 10-filter search (1 credit): educator hashtags AND AI / content-creation / video-editing topics, English,
  main audience US/CA/GB/AU, 10k–150k subscribers, median views ≥ 2,000, engagement ≥ 2%, ≥ 2 uploads in 30 days,
  contact email on file; sorted by median views. Tuned with live searches (niches proved noisy). `quality_broad`
  and `none` are available; explicitly sent fields always win.
* New search criteria: audience countries and ages, median views, engagement rate, uploads in 30 days, contact
  email, sponsorship history, sort order. Search results report the enforced filters and the matching pool size.
* The preliminary screen counts provider-enforced filters as met (labeled), keeps the provider's ranking, and
  holds back names mostly in non-Latin script.
* Paid enrichment is skipped when its data may not be used for scoring (no CreatorDB screening while
  `RPI_CREATORDB_SCORING_PERMITTED=false`). Contacts (15 credits) only for `recommend`. Audience demographics
  (10 credits) off by default (`RPI_FETCH_AUDIENCE_DEMOGRAPHICS`). Funnel defaults lowered to discover 100,
  screen 25, analyze 10.
* Dashboard: preset selector, main-audience countries, credit estimates, subscribers on the run page.

## 0.1.1 — 2026-10-25

### CreatorDB adapter matched to the official API
* Endpoints, request bodies, response fields, error codes and credit costs now follow CreatorDB's OpenAPI 3.1
  spec and credit pages. Fixed: content path (`/youtube/content-detail`, was `/youtube/videos`), search body
  (`filters`/`pageSize`, was free-form keywords/limit), every profile field name, audience and sponsorship
  shapes, pagination (`hasNextPage`/`nextOffset`).
* Added `/youtube/performance` (views, engagement, uploads in 90 days) and free `/usage` (credits remaining;
  used by the health check).
* Costs settle from the provider-reported `creditsUsed`; not-found lookups and contact calls with no email cost 0.
* `QUOTA_EXCEEDED` is treated as credits exhausted and never retried; `RATE_LIMIT_EXCEEDED` is retried.
* Countries and languages are converted between CreatorDB's ISO alpha-3 / 639-3 and RPI's alpha-2 / 639-1.
* Brand-sponsorship search returns UNSUPPORTED instead of unrelated creators until `/sponsor/creators` is added.
* `scripts/rpi_verify_creatordb.py` rewritten: free usage check, one profile by default, `--probe` for every
  endpoint, `--niches` to list niche ids; reads the key from a mode-600 file.

## 0.1.0 — 2026-10-25

### Audit (Milestone 0)
* Audited this repository (Partner Scout v4.13), reelforge-ai (product), outreach and affiliate-scanner
  (read-only). Findings and ten risks in IMPLEMENTATION_PLAN.md.
* API verification: YouTube Developer Policies, SparkToro API docs, CreatorDB docs index/authentication page,
  Supadata homepage. A safety-check outage blocked other pages; those items are marked unverified.

### Implemented
* Migration 005 (45 `rpi_*` tables, additive) with rollback, tested up → down → up on PostgreSQL 14.
* Provider layer: normalized contract with explicit UNSUPPORTED; resilient HTTP (full-jitter retries,
  Retry-After, circuit breaker, token bucket, timeouts, structured errors, secret redaction); declarative field
  mapping; gateway with policy-bounded caching, budget reservations and request metering.
* Adapters: CreatorDB (profile verified; others gated), SparkToro, Supadata, YouTube Data (verification only),
  YouTube Analytics OAuth (disabled), synthetic fixture provider (refused in production).
* Spending controller: global/provider daily/monthly budgets, job and per-creator caps, low-priority pausing,
  alerts, two-person budget increases.
* Identity resolution, discovery engine (niche, audience-first, competitor sponsorship, emerging), enrichment
  value gate, research pipeline, resumable jobs, funnel orchestration.
* Content intelligence: versioned taxonomy, keyword prefilter, Claude fast/deep classification via forced tool,
  prompt-injection defenses, deterministic relevance, buyer-intent summary, evaluation harness.
* Sponsorship detection (ported affiliate-scanner rules plus disclosures and promo codes), audience fit (3 ICPs,
  no imputation), risk screening, deterministic qualification scoring with policy exclusions.
* Profit engine: cohort contribution, scenarios, break-even, payback, max fee, sensitivity, labeled simulation,
  deal optimizer, ledger actuals.
* CRM (16 statuses, audit trail, notes, follow-ups), suppression (hashed; includes Partner Scout opt-outs),
  outreach drafts with validator, approval workflow, gated exactly-once sending with CAN-SPAM footer and signed
  unsubscribe, Brevo event handling.
* Attribution (touch/identify/milestone, first/last touch, lock at first paid, consent-aware hashing), Stripe
  webhook (own endpoint, HMAC + replay window), ledger (tax/discount/refund/chargeback, FX provenance, commission
  accrual with duration and clawback), partner reports, cohorts, reconciliation, PostHog person-property bridge.
* Executive dashboard, cost per stage, 9-screen UI, admin endpoints, retention enforcement, compliant deletion.
* Celery: `run_rpi_job`, daily retention, daily ledger checks (no paid calls on schedules).
* Scripts: migrate/seed/import, create key, CreatorDB verification, benchmark import/report.
* Changes to existing code (additive): RPI routers mounted in `app/main.py`; log redaction processor; RPI
  handling appended to the Brevo webhook; RPI tasks in `celery_config.py`; settings in `app/config.py`;
  `cryptography` dependency; env vars in `.env.example` and `render.yaml`; `.gitignore` added.

### Tests performed
* `pytest`: **122 passed** in ~5 s against local PostgreSQL 14 (details in TEST_STRATEGY.md).
* Bugs found by tests and fixed: provider error bodies echoing API keys were not redacted.
* Manual: ran the app locally with synthetic data. Discovery found 32 candidates (28 unique, 4 duplicates
  across strategies); screening gave 18 recommend / 3 investigate / 3 hold / 4 rejected on niche; 4 creators
  deep-analyzed to `recommended`; total synthetic spend $0.43. Checked the executive dashboard, creators, creator
  profile, Profitability Lab, CRM board and administration in a browser. Fixed a JavaScript syntax error, a CSP
  conflict with inline styles, and negative-currency formatting.

### Not done / blockers
See KNOWN_LIMITATIONS.md. Main blockers: CreatorDB access and endpoint verification, provider terms reviews,
product-side attribution integration, manual benchmark labels, affiliate-term decision.
