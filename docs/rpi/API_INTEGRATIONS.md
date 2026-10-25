# API verification matrix and integrations

Verified on 2026-10-25 from official pages where noted. During the build, a safety-check outage blocked most
documentation fetches, so many items are **UNVERIFIED**. Code treats unverified items conservatively:
endpoints are disabled, field mappings are isolated, and missing data is reported, never filled in.

Legend: ✅ verified (source given) · ⚠️ unverified · ⛔ blocked/disabled in code

## CreatorDB — primary discovery

| Item | Status | Detail |
|---|---|---|
| Base URL | ✅ | `https://apiv3.creatordb.app` (docs index) |
| Auth | ✅ / ⚠️ | Example uses header `api-key: <key>`; the same page also says "authorization header". Adapter sends `api-key`; the verify script tries both |
| Profile | ✅ | `POST /youtube/profile` body `{"channelId": "UC…"}` (authentication page example) |
| Methods / body | ✅ | GET and POST; POST requires a JSON body |
| Pagination | ✅ | ≤100 results per call, `offset` in body (check off-by-one live) |
| Rate limit | ✅ | Up to 6,000 calls/minute, depends on credits |
| Search, audience, sponsorship, videos, subtitles, contacts | ⚠️ ⛔ | Paths provisional (`/youtube/search`, …); **disabled** until listed in `CREATORDB_VERIFIED_ENDPOINTS` |
| Response field names | ⚠️ | Mapped in `CREATORDB_FIELDS` (only `channelId` verified) |
| Credits per endpoint | ⚠️ | Assumed 1 credit/call; set `CREATORDB_USD_PER_CREDIT` from your plan |
| Pricing | ⚠️ | Purchased in the API dashboard; no amounts in the docs index |
| Terms (storage, AI analysis, derived scoring) | ⚠️ ⛔ | Not reviewed. Scoring from CreatorDB data is **off** until `RPI_CREATORDB_SCORING_PERMITTED=true` |

Access requirement: CreatorDB account + API credits. **Verification task (≈5 min):**
`CREATORDB_API_KEY=… python -m scripts.rpi_verify_creatordb --channel UC…` (1 call). Add
`--probe-unverified --yes` to probe the provisional endpoints (may use credits).

## SparkToro — selective audience-first research

| Item | Status | Detail |
|---|---|---|
| Base / auth | ✅ | `https://api.sparktoro.com`, `Authorization: Bearer <key>` (https://sparktoro.com/api/docs) |
| Endpoints used | ✅ | `POST /v3/describe/create` (10 credits), `GET /v3/youtube` (1), `GET /v3/account/credits` (free) |
| Other endpoints | ✅ | demographics 1, bios 5, websites 2, social 2, networks 1, podcasts 1, reddit 2, press 2, keywords 5, tam 3, personas 25 (not used) |
| Billing semantics | ✅ | Each (endpoint, report_id) charged once per billing period; 5xx refunded; 402 not charged (body has `credits_required`, `credits_remaining`); trial 200 credits |
| Limits | ✅ | 10 req/min trial, 60 req/min paid; 429 with `Retry-After` |
| Async sections | ✅ | 202 = still building |
| Response field names | ⚠️ | Isolated in `SPARKTORO_FIELDS` |
| Dollar pricing | ⚠️ | Not on the docs page |
| Terms (storage, training, competing products, redistribution) | ⚠️ | Terms page not readable at build time; training and redistribution treated as prohibited; TAM rationale must be surfaced |

Used only when an operator selects `audience_first` for a discovery run. Its incremental value is recorded per
run (`not_found_by_other_strategies`).

## YouTube Data API v3 — verification only

| Item | Status | Detail |
|---|---|---|
| Storage | ✅ | Dev Policies III.E.4(c)/(d): ≤30 days, then refresh or delete; (e)/(f) keep data current |
| Derived metrics | ✅ | III.E.4(h): no new/derived data or metrics (example: a score using likes/views) |
| Projects/keys | ✅ | III.D.1(c): exactly one API project per client; quota increases via audit (III.D.3) |
| Scraping | ✅ | III.E.6 prohibits it |
| Redistribution | ✅ | III.G.1, III.I.15 |
| Quota costs (10,000/day; search 100, channels 1) | ⚠️ | Quota page not fetched |
| commentThreads | ⚠️ | Not verified; comment analysis not implemented |

Source: https://developers.google.com/youtube/terms/developer-policies. RPI calls only `channels.list`
(verification), marks records `derived_metrics_prohibited`, expires them at 30 days, and scoring ignores them.

## YouTube Analytics API — creator-authorized

| Item | Status | Detail |
|---|---|---|
| Display restriction | ✅ | III.E.3(b): authorized data shown only to the authorizing user |
| Re-confirmation | ✅ | III.E.4(b): every 30 days |
| Revocation | ✅ | III.D.2: revoke immediately, delete within 7 days (30 if via Google settings) |
| Scopes, dimensions (ageGroup, gender, country), privacy thresholds | ⚠️ | Not fetched |

⛔ Disabled by default (`RPI_YOUTUBE_ANALYTICS_ENABLED=false`) because III.E.3(b) may prohibit showing a
creator's analytics to ReelForge staff. OAuth lifecycle is implemented; report retrieval is not.
Fallback: creators submit analytics manually (notes kind `manual_analytics`, labeled unverified).

## Supadata — optional transcript fallback

| Item | Status | Detail |
|---|---|---|
| Free tier | ✅ | 100 requests, no card (https://supadata.ai/) |
| Endpoint, auth, credits, limits, terms | ⚠️ | Path `/v1/youtube/transcript` and `x-api-key` header are provisional |

Used only through the value gate (stage ≥3, uncertain classification, ≤3 per creator).

## Anthropic Claude

| Item | Status | Detail |
|---|---|---|
| Messages API | — | Raw httpx, `anthropic-version: 2023-06-01`, forced `tool_choice` |
| Models | ⚠️ | Defaults `claude-haiku-5-5` (fast) and `claude-sonnet-5-5` (deep), configurable |
| Prices | ⚠️ | Defaults $1/$5 and $3/$15 per MTok are **placeholders**; set the `RPI_CLAUDE_*_USD_PER_MTOK` values from current pricing before trusting spend reports |

## Stripe — billing (RPI's own webhook endpoint)

Signature scheme (`t=…,v1=…`, HMAC-SHA256 over `"{t}.{body}"`, tolerance window) implemented without the SDK.
Events used: `invoice.paid` / `invoice.payment_succeeded` (deduplicated per invoice), `charge.refunded`
(cumulative), `charge.dispute.created` / `.closed` (won), `customer.subscription.created|updated|deleted`.
Register `https://<host>/rpi/api/v1/webhooks/stripe` as an **additional** endpoint in Stripe; the product's
existing endpoint is untouched.

## PostHog

Person properties `partner_*` and the single event `partner_attribution_assigned` via `POST /capture/`.
Nothing is sent unless `consent_state == granted` (default). See ATTRIBUTION_SPECIFICATION.md.

## Brevo (existing)

RPI sends through the existing `BrevoClient`. The existing `/webhooks/brevo` handler now also forwards
events for RPI messages (additive, wrapped in try/except).

## Not integrated, by design

Modash, HypeAuditor and Influencers.club are not dependencies.
