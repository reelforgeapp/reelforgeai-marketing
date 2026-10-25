# Test strategy

Run: `pip install -r requirements-dev.txt && pytest` (needs a local PostgreSQL database whose name contains
"test"; default `postgresql:///rpi_test`, override with `RPI_TEST_DATABASE_URL`). The suite rebuilds the
schema once per session from `database/schema.sql` + migration 005 and truncates `rpi_*` tables per test.
DB tests skip, rather than fake, when PostgreSQL is unreachable.

**Result on 2026-10-25: 122 passed, 0 failed, 0 skipped** (Python 3.11.14, PostgreSQL 14, ~5 s).

| File | Covers |
|---|---|
| `test_profit_model.py` | Closed-form cohort contribution, fixed-fee profit, commission not double-counted, commission duration, fees/refunds, annual plans, break-even, payback, max fee with ROI, incrementality, bonus caps, scenario ordering, deterministic simulation, input validation, sensitivity monotonicity, non-standard deal flags, basis labeling |
| `test_budget.py` | Limits, provider scoping, low-priority pausing, job/creator caps, negative amounts, reserve/settle/release; **30 concurrent reservations against Postgres never exceed the limit**; alert dedupe |
| `test_identity.py` | Same id, same-handle conflict → review, name-only never merges, one-way link → review, bidirectional link and two strong signals → auto-merge, generic link domains ignored, normalizers |
| `test_sponsorship_detection.py` | Mention vs paid disclosure vs undisclosed/disclosed affiliate, promo codes, platform paid flag, unknown brands, ported link rules |
| `test_content_intelligence.py` | Prefilter clear cases skip Claude, deterministic relevance vs model judgment, escalation rules, report validation (unsupplied ids, clamping, unknown labels), delimiter neutralization, **compromised-model prompt-injection test**, failure fallback |
| `test_qualification_fit_risk.py` | No imputation of audience data, measured > modeled precedence, risk labeling, soft size preference, policy-excluded providers, confident niche rejection only, elevated risk blocks recommendation |
| `test_providers_contract.py` | CreatorDB request shape, normalization, missing fields, schema mismatch, garbage responses, auth failure (no retry, **key redacted**), credit exhaustion, Retry-After, 5xx retries, circuit breaker, unverified endpoints disabled, pagination, unapplied criteria; SparkToro 202/402 credit semantics |
| `test_gateway_gate_misc.py` | Caching and metering, contacts never cached, budget denial, unsupported/not configured, value-gate rules, log redaction, Stripe signatures (valid/forged/replay/missing secret), attribution windows |
| `test_outreach.py` | Validator blocks familiarity, income claims, other commission %, unverified numbers, foreign emails/URLs, ungrounded drafts; template validity; fabricated Claude draft rejected; **sending disabled by default**; unapproved never sent; compliance footer; **5 concurrent sends → exactly one**; suppression and Partner Scout opt-outs; daily limit; same-address dedupe; tampered drafts re-validated |
| `test_ledger_attribution.py` | Tax/discount split, duplicate events, invoice.paid + payment_succeeded dedupe, cumulative refunds, missing FX flagged, first/last touch + lock, denied consent, billing-before-identify merge, commission duration + clawback window, partner report completeness, reconciliation, cohort retention |
| `test_e2e_lifecycle.py` | **Full HTTP lifecycle**: discovery → screen → deep → recommend → draft → approve → refused while disabled → enable → send once → negotiation → contract-gated deal → campaign → fee → touch → identify → signed Stripe invoice (+ duplicate) → partner report → forecast → dashboard → reconciliation |
| `test_security.py` | Missing/invalid keys, role boundaries, analyst cannot approve/send, two-person budget increases, over-budget runs pause, webhook unsigned/forged/replayed/fail-closed, unsubscribe token, no secrets in audit log, deletion cascade + suppression, retention deletion, fixture refused in production |
| `test_evaluation.py` | Benchmark metric arithmetic |

## What the tests do not prove

* **Live provider behavior.** Contract tests use fixtures shaped after the documented requests. CreatorDB
  fixtures follow its official OpenAPI spec; SparkToro/Supadata field names are unverified. Live checks:
  `scripts/rpi_verify_creatordb.py`.
* **Claude output quality.** A fake model is used. Classification accuracy needs the labeled benchmark.
* **Stripe payload variations across API versions** beyond the fields used.
* **The product integration**, which lives in another repository.
* **UI behavior.** Verified manually in a browser on 2026-10-25 (executive, creators, creator profile,
  Profitability Lab, CRM, administration) against a locally running app with synthetic data; no automated UI tests.

## Adding tests

New provider adapters need a fixture file in `tests/rpi/fixtures/` and contract tests covering success, missing
fields, auth failure, credit exhaustion, rate limiting and garbage responses. New money paths need idempotency and
duplicate-event tests.
