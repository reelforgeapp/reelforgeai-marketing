# ReelForge Partner Intelligence (RPI)

Influencer discovery, qualification, partnership management and profit measurement for ReelForge AI, optimized
for **retained contribution per dollar of influencer acquisition spend** rather than follower counts.

RPI runs inside this repository's FastAPI app and Celery worker, next to the existing Partner Scout pipeline,
with its own `rpi_*` tables.

## Status (2026-10-25)

| | |
|---|---|
| Code | Milestones 1–5 implemented (discovery through attribution and ledger); lookalike discovery deferred |
| Tests | 122 passing (unit, provider contract, Postgres integration, HTTP end-to-end, security) |
| Live data | **Not yet**: needs a CreatorDB key, endpoint verification and a terms review (KNOWN_LIMITATIONS.md) |
| Sending | **Off** by default (research + drafts only) |

## Quick start (local, synthetic data)

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
createdb rpi_dev_local && psql -d rpi_dev_local -f database/schema.sql
DATABASE_URL=postgresql:///rpi_dev_local .venv/bin/python -m scripts.rpi_migrate
DATABASE_URL=postgresql:///rpi_dev_local ENVIRONMENT=development RPI_ENABLE_FIXTURE_PROVIDER=true \
  RPI_BOOTSTRAP_ADMIN_KEY=<24+ random chars> RPI_ATTRIBUTION_HASH_SALT=<16+ chars> \
  .venv/bin/uvicorn app.main:app --port 8765
```

Open http://localhost:8765/rpi/ui and enter the bootstrap key. The fixture provider produces clearly labeled
synthetic creators (`[SYNTHETIC] …`, channel ids `UCFIXTURE…`).

Tests: `createdb rpi_test && .venv/bin/pytest`.

## Documentation

| Document | Contents |
|---|---|
| [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) | Audit of existing systems, gap analysis, milestone status, next steps |
| [API_INTEGRATIONS.md](API_INTEGRATIONS.md) | Provider verification matrix |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Modules, data flow, design decisions |
| [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md) | Tables and deletion semantics |
| [DATA_LICENSING_AND_COMPLIANCE.md](DATA_LICENSING_AND_COMPLIANCE.md) | Provider policies and enforcement |
| [AI_SCORING_AND_EVALUATION.md](AI_SCORING_AND_EVALUATION.md) | Taxonomy, Claude pipeline, scoring, benchmark |
| [PROFIT_MODEL.md](PROFIT_MODEL.md) | Contribution model, scenarios, deal optimizer |
| [ATTRIBUTION_SPECIFICATION.md](ATTRIBUTION_SPECIFICATION.md) | Attribution rules, ingest API, product changes |
| [SECURITY_MODEL.md](SECURITY_MODEL.md) | Roles and controls |
| [TEST_STRATEGY.md](TEST_STRATEGY.md) | What is tested and what is not |
| [DEPLOYMENT_RUNBOOK.md](DEPLOYMENT_RUNBOOK.md) | Deploy, verify, rollback |
| [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md) | Weekly cycle, budgets, incidents |
| [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md) | Blockers and gaps |
| [COMMERCIALIZATION_READINESS.md](COMMERCIALIZATION_READINESS.md) | Gates before any SaaS launch |
| [CHANGELOG.md](CHANGELOG.md) | Work completed, tests, results |
