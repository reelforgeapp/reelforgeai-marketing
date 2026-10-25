# API verification matrix and integrations

Verified on 2026-10-25 from official pages where noted. During the build, a safety-check outage blocked most
documentation fetches, so many items are **UNVERIFIED**. Code treats unverified items conservatively:
endpoints are disabled, field mappings are isolated, and missing data is reported, never filled in.

Legend: ✅ verified (source given) · ⚠️ unverified · ⛔ blocked/disabled in code

## CreatorDB — primary discovery

Verified 2026-10-25 against CreatorDB's official OpenAPI 3.1 spec (`docs.creatordb.app/api-v3.yaml`) and the
credit, field-cost and search-filter pages, then **checked live the same day** (`--probe` on one channel: all
seven endpoints returned the mapped fields, 39 credits, matching the documented costs). Responses also carry
fields the spec omits (`hasEmail`, `isAgencyRepresented`, `isPodcast`, `joinedDate`, `totalViews`;
`videoPrice`/`shortsPrice` media-value estimates; `detectedBrandNamesByVideo`), not used yet.

| Item | Status | Detail |
|---|---|---|
| Base URL / auth | ✅ | `https://apiv3.creatordb.app`, header `api-key: <key>`. No sandbox: every call uses real credits |
| `GET /usage` | ✅ | Free. Credits available, plan vs purchased, the key's spend limit. Used by the health check |
| `POST /youtube/search` | ✅ | Body `{filters:[{filterName, op, value}], pageSize ≤100, offset, sortBy, desc}`; response `data.creatorList`, `hasNextPage`, `nextOffset`. **1 credit per 10 filters per page, charged even with no matches.** Results carry only `channelId`, `uniqueId`, `displayName`, `avatarUrl`, `totalSubscribers` |
| `POST /youtube/profile` | ✅ | 2 credits. `bio`, `country` (ISO alpha-3), `mainLanguage` (ISO 639-3), `totalSubscribers`, `totalContents`, `lastPublishTime` (ms), `categoryBreakdown`, `topics`, `niches`, `hashtags`, `otherLinks` |
| `POST /youtube/performance` | ✅ | 3 credits. `videosPerformanceRecent` (last 20 videos: views avg/median, engagement), `contentCountByDays` |
| `POST /youtube/content-detail` | ✅ | 3 credits. `recentVideos[]` (with `isSponsored`) and `recentShorts[]`: `contentId`, `title`, `description`, `publishTime`, `length`, views/likes/comments |
| `POST /youtube/audience` | ✅ | 10 credits. `audienceLocations[{country, share}]`, `audienceGender{maleRatio, femaleRatio}`, `audienceAgeBreakdown[{ageRange, share}]`, `audienceAvgAge` |
| `POST /youtube/sponsorship` | ✅ | 5 credits. `sponsorList[{brandName, brandId (site URL or id), sponsoredVideos[]}]` |
| `POST /youtube/contact` | ✅ | 15 credits, **charged only when at least one email is returned**. `emails[]` |
| Not found | ✅ | A direct query that returns nothing costs 0 credits |
| Errors | ✅ | 400 `VALIDATION_ERROR`; 403 `FRACTIONAL_CALLS_NOT_ENABLED`; 429 `RATE_LIMIT_EXCEEDED` (retried) or `QUOTA_EXCEEDED` (credits exhausted, not retried); 200 with `success:false` is an error |
| Costs | ✅ | Each response reports `creditsUsed`, which is settled against the budget. Set `CREATORDB_USD_PER_CREDIT` from your plan price |
| Not used yet | — | Fractional calls (per-field billing; must be enabled on the key), search enrichment, `/youtube/performance-history`, `/youtube/subtitles/*`, `/youtube/niches` + `/youtube/topics` (1 credit each; the verify script can list niches), `/sponsor/creators` (15 credits; would power competitor-sponsorship discovery) |
| Terms (storage, AI analysis, derived scoring) | ⚠️ ⛔ | CreatorDB publishes no API terms (the privacy policy refers to Terms of Service that are not online). Your customer agreement decides. Scoring from CreatorDB data is **off** until `RPI_CREATORDB_SCORING_PERMITTED=true` |

Search translation (`build_search_body`): keywords → `hashtags in ["#aitools", …]`; topics and niches →
`topics`/`niches` ids; languages → `mainLanguage` (ISO 639-3); countries → `country` and audience countries →
`mainAudienceLocation` (alpha-3); audience ages → `mainAudienceAge`; subscriber, view, engagement and activity
bounds → `totalSubscribers`, `avgRecentVideosViews`, `medianRecentVideosViews`, `avgRecentVideosEngagementRate`,
`contentsIn30Days`, `lastPublishTime`; `hasEmail`, `hasSponsors`. Codes not in `rpi/providers/iso_codes.py` are
reported as unapplied; enforced criteria are returned as `applied_criteria`, and `totalResults` as
`total_available`. Results sort by `medianRecentVideosViews` (the API default is alphabetical). Brand-sponsorship
criteria return UNSUPPORTED rather than unrelated creators. Presets and their tuning: `rpi/discovery/presets.py`
and OPERATIONS_RUNBOOK.md → Credit-efficient discovery.

**Verification:** `python -m scripts.rpi_verify_creatordb --channel UC…` calls `/usage` (free) and one profile
(2 credits). `--probe --yes` calls every endpoint once (≤ 39 credits). `--niches ai,video --yes` lists niche ids
(1 credit).

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
