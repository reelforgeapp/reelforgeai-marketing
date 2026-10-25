# Commercialization readiness

**Status: not ready, and not intended to be yet.** RPI is an internal tool. Commercial SaaS work is deferred
until internal results justify it.

## What the architecture already allows

* `workspace_id` on root tables (creators, principals, budgets, runs, jobs, searches, campaigns, audit) with
  one default workspace.
* Provider adapters behind a normalized contract, so data sources can differ per customer.
* Role-based principals, per-workspace budgets and an audit trail.

## Gates before any commercial launch

| Gate | Evidence required | Status |
|---|---|---|
| Internal validation | Benchmark beats CreatorDB-only ranking on topical relevance; campaigns show positive contribution per acquisition dollar with reconciled ledger data | Not started |
| Data licensing | Written confirmation that CreatorDB, SparkToro, Supadata (and any other) data may be used for third-party customers, including storage, derived scoring and display | Not reviewed. YouTube API data cannot be resold or redistributed (III.G.1, III.I.15) |
| Demand | Interviews or LOIs from outside SaaS companies | None |
| Unit economics | Per-customer data and inference cost vs price | Unknown |
| Tenant isolation | Workspace scoping enforced in every query (or Postgres RLS), per-tenant keys and budgets, tenant-aware caches | **Not implemented**: queries are not workspace-filtered today |
| Security | External penetration test, SOC 2 readiness, API rate limiting, SSO | Not started |
| Support and acquisition plan | Onboarding, docs, pricing, billing | Deferred (no public billing, white-labeling, self-service onboarding or data APIs) |

## Specific constraints

* SparkToro: do not use its data to build a competing audience-intelligence product, and surface TAM rationale.
* YouTube: no resale of API data or API access; one API project per client; 30-day storage. A multi-tenant
  product would need its own compliance audit.
* Creator personal data (contacts) would need a lawful basis and DPA per customer.
