# Deployment runbook

RPI deploys with the existing Render services (`reelforge-marketing` web, `reelforge-worker`). No new services.
**Never point RPI at the reelforge-ai product database**: the product's deploy runs
`prisma db push --accept-data-loss`, which can drop tables it does not know about.

## 1. Pre-flight

* [ ] Back up the marketing database (Render → PostgreSQL → Backups → "Create backup"), and note the backup id.
* [ ] Confirm `DATABASE_URL` is the marketing database (`reelforge-marketing-db`).
* [ ] Generate secrets locally:
  ```bash
  python -c "import secrets; print(secrets.token_urlsafe(32))"   # RPI_BOOTSTRAP_ADMIN_KEY
  python -c "import secrets; print(secrets.token_urlsafe(24))"   # RPI_ATTRIBUTION_HASH_SALT (never change later)
  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"  # RPI_TOKEN_ENCRYPTION_KEY
  ```

## 2. Environment (both web and worker)

Set in Render (`render.yaml` declares the keys with `sync: false`): `RPI_BOOTSTRAP_ADMIN_KEY`,
`RPI_ATTRIBUTION_HASH_SALT`, `RPI_PUBLIC_BASE_URL`, `ANTHROPIC_API_KEY`, `CREATORDB_API_KEY`,
`CREATORDB_USD_PER_CREDIT`, `CREATORDB_VERIFIED_ENDPOINTS`, `RPI_CREATORDB_SCORING_PERMITTED`,
`SPARKTORO_API_KEY`, `SPARKTORO_USD_PER_CREDIT`, `SUPADATA_API_KEY`, `SUPADATA_USD_PER_REQUEST`,
`RPI_STRIPE_WEBHOOK_SECRET`, `RPI_STRIPE_PRICE_PLAN_MAP`, `POSTHOG_API_KEY`. See `.env.example` for all
options. Do **not** set `RPI_ENABLE_FIXTURE_PROVIDER` in production: RPI requests and jobs then fail with
`FixtureInProductionError` by design.

## 3. Migrate

```bash
DATABASE_URL=<marketing db url> python -m scripts.rpi_migrate
```

Idempotent: it creates `rpi_*` tables, seeds the competitor registry and budgets, and imports Partner Scout
YouTube prospects as RPI creators (ids, handles and names only; no emails). Run it before the code deploy goes live.
Verify:

```sql
SELECT version, applied_at FROM rpi_schema_migrations;          -- 005_rpi_core, 006_..., 007_rpi_autopilot
SELECT scope, period, provider, limit_usd FROM rpi_budgets;      -- $200/mo global, $25/day, per-provider
```

## 4. Deploy

Push the branch / merge to `main`; Render rebuilds both services. Check:

```bash
curl https://<host>/health                                     # {"status":"healthy",...}
curl -H "X-RPI-Key: $RPI_BOOTSTRAP_ADMIN_KEY" https://<host>/rpi/api/v1/admin/providers
```

## 5. Create principals, then retire the bootstrap key

```bash
DATABASE_URL=... python -m scripts.rpi_create_key --name larry --role admin
DATABASE_URL=... python -m scripts.rpi_create_key --name reviewer --role approver
DATABASE_URL=... python -m scripts.rpi_create_key --name reelforge-backend --role ingest
```

Store the keys in a password manager. Remove `RPI_BOOTSTRAP_ADMIN_KEY` from Render and redeploy. Budget increases
require two different admins, so create at least two admin principals if you need to raise limits.

## 6. Provider verification (before relying on discovery)

```bash
python -m scripts.rpi_verify_creatordb --channel UC...              # /usage (free) + 1 profile (2 credits)
python -m scripts.rpi_verify_creatordb --channel UC... --probe --yes  # every endpoint once (<= 39 credits)
```

The key comes from `CREATORDB_API_KEY` or `~/.config/creatordb/api_key` (mode 600). If a response's keys or
missing fields differ from `CREATORDB_FIELDS` / `PERFORMANCE_FIELDS` in `rpi/providers/creatordb.py`, fix the
mapping before running discovery. Review your CreatorDB customer agreement, then set
`RPI_CREATORDB_SCORING_PERMITTED=true` if derived scoring and AI analysis are permitted.

## 7. Stripe

In the Stripe dashboard (live mode) → Developers → Webhooks → *Add endpoint*:

* URL `https://reelforgeai-marketing.onrender.com/rpi/api/v1/webhooks/stripe`
* Events: `invoice.paid`, `invoice.payment_succeeded`, `charge.refunded`, `charge.dispute.created`,
  `charge.dispute.closed`, `customer.subscription.created`, `customer.subscription.updated`,
  `customer.subscription.deleted`

This is a second endpoint next to the product's own; nothing in the product changes. Copy the endpoint's signing
secret into `RPI_STRIPE_WEBHOOK_SECRET` on the **reelforgeai-marketing** web service. Set
`RPI_STRIPE_PRICE_PLAN_MAP` (JSON price id → plan name, from the product's `STRIPE_*_PRICE_ID` variables, e.g.
`{"price_123": "creator"}`). Send a test event from the Stripe dashboard and check
`GET /rpi/api/v1/reports/reconciliation`.

The product side needs `RPI_BASE_URL` and `RPI_INGEST_KEY` (an RPI `ingest` principal) on **reelforge-api**.

## 8. Outreach (only when ready)

`PUT /rpi/api/v1/outreach/settings` (admin) with sender name, sender email, physical postal address, a small
`daily_send_limit` (e.g. 5) and `sending_enabled: true`. `RPI_PUBLIC_BASE_URL` must be set for unsubscribe links.

## Rollback

1. Code: redeploy the previous commit in Render. RPI tables are inert without the code.
2. Schema (destructive, only if needed): export `rpi_ledger_entries` and `rpi_billing_events` first, then
   `python -m scripts.rpi_migrate --down` (asks for typed confirmation). Partner Scout tables are not touched.
3. Restore from the pre-flight backup if anything else went wrong.

## Backups and recovery testing

Render PostgreSQL daily backups cover RPI. Quarterly: restore the latest backup to a scratch database, run
`python -m scripts.rpi_benchmark --report` and `GET /reports/reconciliation` against it, and record the result.
