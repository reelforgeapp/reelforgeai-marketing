# RPI — Audit, Gap Analysis and Implementation Plan

Status date: 2026-10-25. This document covers deliverables **A** (existing architecture assessment),
**C** (gap analysis) and **D** (dependency-ordered plan) from the directive. The API verification
matrix (**B**) is in [API_INTEGRATIONS.md](API_INTEGRATIONS.md) and the production architecture (**E**)
in [ARCHITECTURE.md](ARCHITECTURE.md). What was built is in [CHANGELOG.md](CHANGELOG.md).

## A. Existing architecture assessment

### This repository: ReelForge Marketing Engine ("Partner Scout"), v4.13

| Area | What exists | Reuse decision |
|---|---|---|
| Runtime | FastAPI (`app/main.py`), Celery worker + beat in one Render worker (`celery_config.py`), Redis broker | **Reused**: RPI is mounted in the same FastAPI app; RPI tasks run on the same worker |
| Database | PostgreSQL via asyncpg, raw SQL, hand-applied SQL migrations (`database/`) | **Reused**: same DB, same style; RPI tables are `rpi_*` (migration 005) |
| Discovery | YouTube Data API `search.list` over `competitor_keywords`; stores channels in `marketing_prospects` | **Kept unchanged**. Prospects are imported into the RPI identity graph by `scripts/rpi_migrate.py` |
| Contact discovery | `HybridEmailExtractor` scrapes `youtube.com/channel/<id>/about` and websites (HTTP + Playwright) | **Not reused** (see risk 5) |
| Email verification | Bouncer / Clearout / Hunter clients | Available for reuse; not yet wired into RPI (KNOWN_LIMITATIONS) |
| Outreach | Brevo transactional send, 3-step sequences, auto-enrollment every 3h, sending every 15 min, Brevo webhook | **Reused**: `BrevoClient.send_email` as RPI's sender; Brevo webhook extended (additive) to process RPI message events |
| AI | Raw-HTTP Claude call for email personalization | Pattern reused (raw httpx); RPI uses its own client with forced tool output and budget metering |
| Compliance | 180-day purge, idempotency table, unsubscribe/bounce statuses | RPI checks Partner Scout `unsubscribed`/`bounced` statuses before any RPI send |

**Do not change without an explicit decision** (none of these were modified): the live Partner Scout
sending pipeline, its templates, its tables, and its beat schedule.

### Sibling repositories (read-only audit)

* **reelforge-ai** (product): Node 20 / Express / Prisma / Next.js 14. Stripe is the only billing provider
  (`POST /api/billing/webhook`, `constructEvent` verification, event-id dedupe). Plans (verified in
  `backend/config/tiers.js`): Creator $17/$153, Hustler $37/$333, Mogul $67/$603, Empire $147/$1,323
  (monthly/annual). PostHog events are snake_case with `distinct_id` = user UUID. Referrals go through
  Reditus; `users.referred_by_code` and `affiliate_click_id` are stored at signup. There is no per-render cost tracking;
  the estimated cost per reel is in `docs/ENG-PROPOSAL-CUT-PREMIUM-COGS.md`.
* **outreach**: multi-tenant marketing-ops app (FastAPI + SQLAlchemy + React/Clerk). Patterns reviewed; no code
  imported (its approval link sends on GET, has no token expiry, and its webhook check fails open).
* **affiliate-scanner**: affiliate-link detection rules (`app/detection.py`, verified by reading) **ported** into
  `rpi/sponsorship_intelligence/detection.py`. The competitor domain seed list was also ported.

No implementation named "Partner Scout" exists outside this repository; this repo is treated as Partner Scout.

### Risks found in existing systems (reported, not changed)

| # | Finding | Where | Severity | Recommendation |
|---|---|---|---|---|
| 1 | `/trigger/*`, `/keywords` (incl. DELETE) and `/tasks/{id}` had **no authentication**; anyone could trigger outreach runs or delete keywords | `app/main.py` | High | **Fixed**: `X-Admin-Key` required, fails closed when `ADMIN_API_KEY` is unset |
| 2 | Brevo webhook accepted **unauthenticated** requests when `BREVO_WEBHOOK_SECRET` was unset, and its HMAC check could never pass because Brevo does not sign payloads | `app/main.py:validate_brevo_webhook` | High | **Fixed**: accepts the secret as bearer token, basic-auth password or `X-Brevo-Webhook-Token`; fails closed outside development |
| 3 | Partner Scout **sends autonomously** (auto-enroll + sequence processing) with no human approval | `tasks/outreach_tasks.py` | Medium | Decide whether to keep; RPI never sends without approval |
| 4 | Templates contain **unverified income claims** ("creators … $500–$2,000/month", "one partner hit $1,500") and **fabricated familiarity** ("I just watched your video on …"); no postal address | `database/schema.sql` seed templates, `services/ai_personalization.py` | High (FTC / CAN-SPAM) | **Fixed** (PR #2): copy rewritten, AI copy checked, footer with postal address + signed unsubscribe; sending refused until `COMPANY_POSTAL_ADDRESS` is set |
| 5 | Email extraction **scrapes YouTube pages** with Playwright | `discovery/hybrid_email_extractor.py` | High (YouTube ToS) | **Fixed** (PR #2): YouTube pages are never fetched; only creators' own websites |
| 6 | YouTube API data kept up to 180 days | `tasks/maintenance_tasks.py` purge | Medium (Dev Policies III.E.4: 30 days) | **Fixed** (PR #2): daily scrub clears YouTube statistics after 30 days |
| 7 | `ANTHROPIC_MODEL` default `claude-3-5-haiku-20241022` is very likely retired, so personalization may silently fall back to templates | `app/config.py` | Low | **Fixed** (PR #2): default is now `claude-haiku-5-5` |
| 8 | Affiliate terms disagree: `/affiliate-terms` = 30% for customer lifetime, 60-day last click; `/affiliates` = 30% for 12 months, 90-day cookie | reelforge-ai frontend | Medium (contractual) | **Duration decided**: 12 months (2026-10-25), RPI default. `/affiliate-terms` still says lifetime and must be corrected; cookie window (60 vs 90 days) undecided |
| 9 | Product deploy runs `prisma db push --accept-data-loss` | reelforge-ai `Dockerfile:61` | High for any shared DB | RPI must never use the product database (it does not) |
| 10 | Product has no analytics/affiliate consent mechanism (PostHog + Reditus load for everyone) | reelforge-ai `layout.jsx` (audit report) | Medium | Add consent; RPI's PostHog bridge stays silent until it exists |

Items 9 and the plan prices were verified directly. Item 10 and other product details come from a read-only
sub-agent audit and are not re-verified.

## C. Gap analysis

| Directive module | Status | Notes |
|---|---|---|
| providers (contract, resilience, metering) | **Built** | Normalized contract, explicit UNSUPPORTED, retries with jitter, Retry-After, circuit breaker, token bucket, timeouts, schema validation, cost metering, cache, request log |
| CreatorDB adapter | **Built to the official spec, live check pending** | All used endpoints, fields and credit costs match CreatorDB's OpenAPI spec; `scripts/rpi_verify_creatordb.py` confirms them with the key |
| SparkToro adapter | **Built, live-unverified** | Endpoints/credits verified from docs; response field names unverified |
| YouTube Data adapter | **Built** (verification only) | Never feeds scores |
| YouTube Analytics OAuth | **Built, disabled** | Pending legal review of Dev Policies III.E.3(b); report retrieval not implemented |
| Supadata adapter | **Built, live-unverified** | Endpoint path provisional |
| identity graph | **Built** | Exact id match, conflict review, cross-platform strong-evidence merge, manual review endpoint |
| discovery (A niche, B audience-first, C competitor, E emerging) | **Built** | Lookalike (D) **deferred** to Milestone 7 (needs outcome data) |
| enrichment funnel + value gate | **Built** | Configurable stage sizes; resumable jobs |
| content intelligence (taxonomy, prefilter, Claude, eval harness) | **Built** | Labeled benchmark data **not yet created** (needs manual review) |
| buyer intent | **Built (content-level)** | Comment-level analysis deferred (YouTube comment data restrictions) |
| audience fit (3 ICPs) | **Built** | Never imputes; completeness reported |
| audience quality / risk | **Built** | Screening labels only |
| sponsorship intelligence | **Built** | Conservative 4-class taxonomy with evidence |
| qualification scoring | **Built** | Deterministic, versioned, policy-gated |
| profit modeling + deal optimizer | **Built** | Scenarios, break-even, payback, max fee, sensitivity, labeled simulation, ledger actuals |
| CRM | **Built** | 16 statuses, transitions, notes, follow-ups, suppression, audit |
| outreach | **Built** | Drafts + validator + approval + gated sending; sending **off** by default |
| attribution | **Built** | Touch/identify/milestone ingest, first/last touch, lock at first paid, consent-aware |
| financial ledger | **Built** | Stripe ingestion, tax/discount/refund/chargeback, commission accrual, FX provenance, reports, reconciliation |
| analytics / dashboard | **Built** | Executive, cost per stage |
| compliance | **Built** | Policy registry, retention enforcement, deletion, untrusted-content handling |
| administration | **Built** | Roles, keys, budgets with two-person increases, jobs, audit |
| UI (9 screens) | **Built** | Vanilla JS SPA served by FastAPI |
| Product-side integration (calls to `/attribution/*`) | **Not started — other repo** | See ATTRIBUTION_SPECIFICATION.md |
| Commercial SaaS features | **Deferred by design** | `workspace_id` columns keep the door open |

## D. Implementation plan and milestone status

| Milestone | Scope | Status | Definition of done → evidence |
|---|---|---|---|
| 0 Audit | Architecture, reuse, API matrix, risks | **Done** (API matrix partly unverified) | This document + API_INTEGRATIONS.md |
| 1 Core discovery | Schema, CreatorDB adapter, metering, search, identity, dedupe, prelim filter, dashboard | **Code done; live use blocked** | Works end to end on the fixture provider (tests). Real search needs the CreatorDB key and a live check |
| 2 AI research & qualification | Claude classification, evidence, audience suitability, confidence, sponsorships, transcript fallback | **Code done; live Claude unverified** | Fake-model tests pass; needs `ANTHROPIC_API_KEY` and a labeled benchmark |
| 3 Audience intelligence | SparkToro, audience-first, provenance, provider value comparison | **Code done; live unverified** | `strategy_breakdown.audience_first.not_found_by_other_strategies` per run |
| 4 Commercial engine | Pricing scenarios, contribution model, break-even, deals, CRM, drafts, approvals | **Done** | Tests + Profitability Lab |
| 5 Attribution & revenue | Referral attribution, PostHog, billing webhook, ledger, cost allocation, cohorts, dashboards | **RPI side done; product integration pending** | Signed-webhook E2E test; product must call `/attribution/identify` |
| 6 Controlled rollout | ≥100 real partners discovered, pilots, reconciled outcomes | **Not started** (needs keys + business decisions) | — |
| 7 Optimization | Lookalike, calibration, cohorts, more adapters | **Not started** | Needs ≥ a handful of completed campaigns |

### Next steps (dependency order)

1. **Decisions** (owner: ReelForge): affiliate commission duration; whether Partner Scout keeps autonomous sending;
   fixes for risks 1, 2, 4 and 5.
2. **Provision**: CreatorDB key → run `scripts/rpi_verify_creatordb.py` → fix any mapping drift. Review the
   CreatorDB customer agreement → set `RPI_CREATORDB_SCORING_PERMITTED`.
3. **Deploy** migration 005 (DEPLOYMENT_RUNBOOK.md), create principals, set budgets.
4. **Label** 50–100 benchmark channels (`docs/rpi/benchmark_template.csv`) → `scripts/rpi_benchmark.py --report`.
5. **Product integration** (reelforge-ai): call `/attribution/identify` at signup and `/attribution/milestone`
   at first render/export; register the RPI Stripe webhook endpoint; optionally send per-render costs.
6. **Pilot**: discover ≥100, screen, deep-qualify, approve outreach, enable sending with a small daily limit.
7. Milestone 7 once outcomes exist.
