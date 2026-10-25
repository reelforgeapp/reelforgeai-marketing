# RPI architecture

A modular monolith inside the existing ReelForge Marketing Engine: same FastAPI process, same Celery worker,
same PostgreSQL database (separate `rpi_*` tables). No microservices.

```
                ┌──────────────────────── FastAPI (app/main.py) ────────────────────────┐
 Browser ──────▶│ /rpi/ui  (static SPA, no data)                                         │
 (X-RPI-Key)    │ /rpi/api/v1/*  ── auth (roles) ── routes_* ──┐                         │
 reelforge-ai ─▶│ /rpi/api/v1/attribution/* (ingest role)      │                         │
 Stripe ───────▶│ /rpi/api/v1/webhooks/stripe (signature)      │                         │
 Brevo ────────▶│ /webhooks/brevo (legacy + RPI events)        │                         │
                └──────────────────────────────────────────────┼─────────────────────────┘
                                                               ▼
   discovery ─▶ provider gateway ─▶ adapters (CreatorDB, SparkToro, Supadata, YouTube, fixture)
       │            │  value gate · budget reserve/settle · cache · request log
       ▼            ▼
   identity ─▶ enrichment service ─▶ content intelligence (prefilter → Claude fast → deep)
                    │                 sponsorship detection · audience fit · risk screen
                    ▼
               qualification (deterministic) ─▶ CRM ─▶ outreach (drafts → approval → gated send)
                                                         │
   attribution (touches, identify, lock) ◀── product ────┘
   financial ledger (Stripe events, commissions, costs) ─▶ reports · cohorts · reconciliation
   profit modeling (scenarios, deals, actuals) ◀── ledger actuals
                                   PostgreSQL (rpi_* tables)          Celery: jobs, retention, ledger checks
```

## Package layout (`rpi/`)

| Package | Responsibility |
|---|---|
| `core` | Provenance types, audit log, secret redaction |
| `providers` | Normalized contract (`base`), resilient HTTP (`http`), adapters, declarative mapping, gateway (budget + cache + metering), registry |
| `compliance` | Provider policy registry, retention enforcement, untrusted-content wrapping |
| `administration` | Budget controller, auth/roles, research jobs |
| `identity` | Resolver (pure) and persistence of match decisions |
| `discovery` | Strategies and run bookkeeping |
| `enrichment` | Value gate and the per-creator research pipeline |
| `content_intelligence` | Taxonomy, keyword prefilter, Claude client, classifier, evaluation metrics |
| `sponsorship_intelligence` | Link/disclosure/promo-code detection, competitor seed list |
| `audience_intelligence` | ICP fit and risk screening |
| `qualification` | Deterministic scoring |
| `profit_modeling` | Cohort economics, deal optimizer, defaults, ledger actuals |
| `crm` | Pipeline state machine, suppression |
| `outreach` | Drafts, validator, sender gates, delivery events |
| `attribution` | Stripe signatures, touches/identify, PostHog bridge |
| `financial_ledger` | Ledger writes, Stripe event processing, reports, reconciliation |
| `analytics` | Executive dashboard, cost per stage |
| `storage` | Creator repository (provenance-linked writes, profile reads, compliant deletion) |
| `api` | Context wiring, dependencies, routes, router mounting |
| `ui` | Static dashboard |

## Key design decisions

1. **Provider independence**: business code only sees `rpi.providers.base` models. Provider field names live in
   one spec per adapter. Unsupported operations return `UNSUPPORTED`.
2. **Every paid call goes through the gateway**: capability check → cache (policy TTL) → budget reservation
   (serialized with a Postgres advisory lock) → call → settle/release → request log.
3. **Deterministic numbers, AI judgments separate**: scores and money are Python `Decimal` arithmetic; Claude
   returns labels through a forced tool, and the creator relevance used for scoring is recomputed from labels.
4. **Policy enforcement in code**: `compliance.policies.scoring_allowed()` gates what can feed scores;
   YouTube API data never can; CreatorDB data can only after a terms review flag.
5. **Human control of outbound actions**: drafts need an approver; sending is disabled by default and gated by
   ten checks in `outreach/sender.py`.
6. **Idempotency everywhere money or messages move**: ledger idempotency keys, Stripe event uniqueness,
   draft status compare-and-swap, unique in-flight draft per creator.
7. **Future multi-tenancy**: root tables carry `workspace_id` (single default workspace today). Data licensing
   per tenant is a commercialization prerequisite (COMMERCIALIZATION_READINESS.md).

## Runtime processes

* Web (Render web service): FastAPI incl. `/rpi`.
* Worker (Render worker): Celery `run_rpi_job` (queue `discovery`, no auto-retry; jobs resume from progress),
  `rpi_enforce_retention` (daily 04:30), `rpi_daily_ledger_checks` (daily 05:15); times are in the Celery
  timezone (America/New_York). No scheduled task calls a paid provider.
