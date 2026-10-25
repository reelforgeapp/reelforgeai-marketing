# RPI database schema

Migration: `database/migrations/005_rpi_core.sql` (additive). Rollback: `005_rpi_core_down.sql`.
Apply with `python -m scripts.rpi_migrate`. All tables are prefixed `rpi_`; Partner Scout tables are untouched.
Money is `NUMERIC`. Ledger amounts are signed integers in currency minor units plus a USD amount with FX source.

## Identity graph

| Table | Purpose | Key constraints |
|---|---|---|
| `rpi_creators` | One row per real-world creator: CRM status, funnel stage, `merged_into_id`, `legacy_prospect_id` (Partner Scout), next action | `workspace_id` |
| `rpi_creator_social_accounts` | Platform accounts | `UNIQUE (platform, platform_account_id)`, normalized handle index |
| `rpi_creator_identity_matches` | Auto-merge / pending-review decisions with signals | `UNIQUE (creator_a, creator_b)`, `CHECK a <> b` |
| `rpi_creator_profile_snapshots` | Time series of provider profile values (subscribers, views, activity) | → provenance (cascade) |
| `rpi_audience_estimates` | Audience buckets with measurement type | share ∈ [0,1]; → provenance |
| `rpi_creator_content` | Videos (title, description, stats, transcript excerpt) | `UNIQUE (platform, platform_content_id)` |
| `rpi_content_classifications` | Niche labels, relevance (deterministic), audience problem, buyer intent, evidence, model, prompt version, cost | |
| `rpi_sponsorship_relationships` | Brand relationships with type and evidence | type ∈ confirmed_paid / disclosed_affiliate / possible_promotional / ordinary_mention |
| `rpi_competitors` | Editable competitor registry | `UNIQUE (name)` |
| `rpi_audience_quality_signals` | Risk-screening signals | severity ∈ info / review / elevated |
| `rpi_creator_contacts` | Contacts with source and verification status | `UNIQUE (creator_id, type, value_normalized)` |

## Provenance, providers, spend

| Table | Purpose |
|---|---|
| `rpi_data_provenance` | Provider, operation, source record, retrieved/observed time, measurement type, completeness, retention policy, commercial use, `derived_metrics_prohibited`, `expires_at` |
| `rpi_data_retention_rules` | Per-provider retention (YouTube 30 d, CreatorDB 90 d placeholder, …) |
| `rpi_provider_requests` | Every provider call: status, cost units/USD, duration, attempts, error code |
| `rpi_provider_cache` | Policy-bounded response cache |
| `rpi_budgets`, `rpi_budget_change_requests` | Global/provider daily/monthly limits; increase approvals |
| `rpi_spend_ledger` | Reservations → committed/released/denied |
| `rpi_alerts` | Deduplicated alerts |

## Workflow

`rpi_icp_profiles` (reserved for overrides; ICPs currently in code), `rpi_saved_searches`, `rpi_research_jobs`
(status, budget, spent, progress for resume), `rpi_discovery_runs`, `rpi_discovery_candidates` (strategy,
reasons, missing fields, duplicate flag), `rpi_qualification_scores` (components, evidence, missing data,
excluded sources).

## CRM and outreach

`rpi_pipeline_events` (full transition history), `rpi_notes` (note / sponsorship_request / negotiation / call /
manual_analytics), `rpi_suppression_list` (salted hashes only), `rpi_outreach_settings` (sending disabled by
default), `rpi_outreach_drafts` (grounding, validation, approval, idempotency key; partial unique index allows
one approved/sending message per creator), `rpi_outreach_events`.

## Commercial

`rpi_deals` (structure, fee, commission, bonus, contract reference, forecast snapshot), `rpi_campaigns`
(unique referral code and UTM campaign, attribution window), `rpi_attribution_touches` (hashed anonymous id,
consent state, idempotency key), `rpi_customers` (product user ↔ billing customer, first/last touch, lock),
`rpi_billing_events` (raw signed events, unique per provider event id), `rpi_fx_rates`, `rpi_ledger_entries`.

## Access, audit, OAuth, evaluation

`rpi_workspaces`, `rpi_principals` (SHA-256 key hashes, roles), `rpi_audit_log`, `rpi_oauth_grants`
(Fernet-encrypted refresh tokens, re-confirm and deletion deadlines), `rpi_oauth_states` (hashed, single use),
`rpi_benchmark_labels`.

## Deletion semantics

Deleting a creator cascades to accounts, snapshots, content, classifications, sponsorships, contacts, scores,
drafts and notes; orphaned provenance rows are removed; the creator's accounts and emails are added to the
suppression list (hashed). Creators with campaigns (financial history) cannot be deleted; anonymize instead.
