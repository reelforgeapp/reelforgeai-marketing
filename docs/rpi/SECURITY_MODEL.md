# Security model

## Authentication and roles

* All `/rpi/api/v1/*` routes except the Stripe webhook require `X-RPI-Key`. Keys are 32 random bytes and are
  stored as SHA-256 hashes (`rpi_principals`). They are shown once at creation and never logged.
* Roles: `viewer` < `analyst` < `approver` < `admin`, plus `ingest` (machine role: attribution and cost ingest
  only, cannot read data).

| Action | Minimum role |
|---|---|
| Read creators, reports, dashboard | viewer |
| Run discovery / research, edit drafts, CRM moves, notes, suppression | analyst |
| Approve / reject / send outreach, move to `outreach_approved`, campaigns, deal status, campaign costs | approver |
| Outreach settings (enable sending), budgets decisions, principals, audit log, deletion, retention, FX | admin |
| Attribution touch / identify / milestone, cost feed | ingest (or admin) |

* `RPI_BOOTSTRAP_ADMIN_KEY` (≥ 24 chars, compared in constant time) exists only to create the first principals.
  Remove it afterwards.
* The UI shell is public and data-free; the key lives in `sessionStorage` and is sent only to the same origin.
  The page uses a restrictive CSP, `X-Frame-Options: DENY` and no-referrer, and renders all data with `textContent`
  (creator content is never interpreted as HTML).

## Application-level controls (not prompt-based)

| Risk | Control |
|---|---|
| Unauthorized sending | Sending disabled by default; admin-only toggle; approver-only approval and send; every gate in `outreach/sender.py` re-checked at send time (enabled, approved, re-validated, stored contact, suppression incl. Partner Scout opt-outs, CRM status, contact spacing, daily limit, CAN-SPAM identity, signing salt); status compare-and-swap; unique in-flight draft per creator |
| Over-budget spending | Every paid call reserves budget under a Postgres advisory lock; per-provider, global daily/monthly, per-job and per-creator caps; low-priority work paused at the alert threshold; budget increases require a second admin |
| Prompt injection | Untrusted content wrapped with per-request delimiters, closing tags neutralized, "treat as data" rules, forced report-only tool, schema validation, discarded unsupplied ids, deterministic recomputation of relevance; the model has no tools that act |
| Fabricated outreach | Validator blocks familiarity claims, income claims, other commission rates, unverified numbers, unsupplied emails/URLs; re-validated at approval and at send |
| Credential leakage | Secrets only in environment variables; never in prompts or client bundles; `structlog_redactor` scrubs keys/tokens in logs; provider errors redacted against the adapter's own secrets; audit details redacted |
| Webhook forgery/replay | Stripe HMAC with 300-second tolerance, fail-closed without a secret, event-id uniqueness, idempotent ledger keys |
| Duplicate billing events | Unique `(provider, provider_event_id)`; per-invoice idempotency keys; refunds via cumulative delta under an advisory lock |
| Unsubscribe forgery | Unsubscribe links are HMAC-signed per draft |
| Unauthorized creator-data access | Role checks on every route; ingest keys cannot read |
| Malicious provider responses | Typed coercion rejects garbage, out-of-range ratios are dropped, non-JSON is an error, missing ids cause a schema error |
| OAuth tokens | Fernet-encrypted refresh tokens; hashed, single-use, 30-minute state; revocation endpoint; deletion deadlines |
| SQL injection | Parameterized asyncpg queries only; dynamic SQL limited to fixed column whitelists |

## Known gaps (see KNOWN_LIMITATIONS.md)

* Rate limiting of the HTTP API itself is not implemented (keys are few and internal).
* The legacy Partner Scout endpoints `/trigger/*`, `/keywords*` and `/tasks/*` use a single shared `X-Admin-Key`
  (no per-person keys or roles), and `/status` is public.
* Token buckets are per process; with N worker processes the provider rate ceiling is N × the configured rate.
* Backups and restore drills depend on Render PostgreSQL backups (DEPLOYMENT_RUNBOOK.md).
