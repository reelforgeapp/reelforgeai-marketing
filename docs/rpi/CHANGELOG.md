# RPI changelog

## 0.2.9 — 2026-10-26

* **Uniform-engagement check.** The risk screen flags creators whose videos all land on about the same views and
  likes (`uniform_engagement`, severity `elevated`: under 2× apart on views and 2.5× on likes across 10+ videos),
  the pattern of views and likes bought per video that the flat-engagement check cannot judge because views barely
  vary. Flagged creators are rejected as `reject_suspect_engagement`. Tested on the 55 creators scored so far: it
  flags NyesWorldTV and Tech Explorer AI and nothing else. No extra credits.

## 0.2.8 — 2026-10-26

* **Autopilot crash fixed.** The 07:30 ET run on 2026-10-26 stopped in deep analysis with "'str' object has no
  attribute 'get'": Claude returned part of its classification as JSON text instead of an object. It had spent 86
  credits, drafted nothing, and left Trainer1300 half-analyzed.
  * The classifier reads JSON-text objects and arrays from Claude. A response with no usable video labels counts
    as a failed call, so keyword rules decide instead of a confident zero-relevance verdict.
  * One creator's error no longer stops a research job: it is recorded as `failed: ...`, listed in the autopilot
    summary with an alert, and the job continues.
  * Saving progress after each creator keeps the job's other stages; before, a deep-stage save overwrote the
    discovery progress, so a failure lost the page it had searched.
  * The funnel retries deep analysis for creators left in `deep_analysis` with only a screening score.

## 0.2.7 — 2026-10-26

* **Spoken-language gate** (`rpi.qualification.language_gate`). CreatorDB labels channels by their titles, so
  Hindi-speaking channels with English titles passed its English filter; Hindi AI Gyaan was recommended and
  auto-approved. RPI now reads the spoken language of each channel's 10 newest videos from the YouTube Data API
  (2 quota units, no CreatorDB credits) right after each search, and again before screening and deep analysis.
  Channels whose videos mostly declare another language are disqualified (`non_english_audio`) before any paid
  research. The autopilot no longer drafts for disqualified creators. `POST /research/language-check` re-checks
  chosen creators for free.
* Re-checked on 2026-10-26: 6 of the 9 creators waiting to be screened and 2 recommended creators (Hindi AI
  Gyaan, Chad Grow) speak another language. Hindi AI Gyaan's scheduled email was held.

## 0.2.6 — 2026-10-26

* **Flat-engagement check (scoring qual-2026-10-26.1).** The risk screen flags creators whose likes stay the same
  whatever a video's views (`flat_engagement`, severity `elevated`), the pattern of engagement bought per video.
  Found on @BeerMoneyForum (a Partner Scout prospect): about 2,500 likes on every video from 33k to 160k views.
  A flagged creator is rejected as `reject_suspect_engagement` (`disqualified_reason = suspect_engagement`)
  before deep analysis, so the funnel and the autopilot spend nothing more on them. Re-checked against the 15
  creators already scored in production: none flagged. No extra credits.

## 0.2.5 — 2026-10-26

* **Security housekeeping.**
  * The production database's IP allow list is now empty (Render services only). Migrations run as Render jobs
    (DEPLOYMENT_RUNBOOK §3).
  * `GET /outreach/drafts/{id}/events` returns one email's delivery events, so delivery checks no longer read the
    database directly.
  * Budget increases by an admin apply immediately and are audited. An analyst's increase still needs an admin.
    Single-admin control replaces two-person approval, at the owner's request.
  * A point-in-time restore was tested and compared with production.

## 0.2.4 — 2026-10-26

* **Product attribution.** reelforge-backend now reports signups, the `?ref=` referral code, the Stripe customer and
  first render/export to RPI (`backend/services/rpi-attribution.js`).
* **Campaign tracking.** The creator page can start a campaign with the creator's Reditus `?ref=` code. Creating a
  campaign attributes earlier signups that used the code (`attribute_existing_signups`), but never takes a paid
  customer from another partner. Referral codes are matched case-insensitively and may be 2–60 characters.
* The partnership-bot conversation notice no longer repeats the email address.
* Plan docs refreshed: milestone status, decisions taken, next steps.

## 0.2.3 — 2026-10-26

* **Partnership bot conversations** (migration `009_rpi_external_conversations`, `rpi.crm.external_conversations`).
  The partnership bot in reelforge-backend reads and answers the marketing@reelforgeai.io inbox. After each run it
  reports every sender to `POST /crm/external-contacts` (ingest role): address, name, YouTube handles or channel
  ids, and message dates, with no message content. RPI uses that in three places:
  * The send gate refuses, and the autopilot does not draft for, a creator the bot exchanged email with in the last
    `RPI_EXTERNAL_CONVERSATION_DAYS` days (default 180). Matching is by any stored email, the draft's address, a
    YouTube handle or a channel id. This came up after the bot and RPI were both about to email zapiwala ai and
    NyesWorldTV.
  * A creator who writes to the inbox after an RPI email gets a `replied` event on that email and moves from
    `contacted` to `replied`.
  * The creator page shows the conversation.

## 0.2.2 — 2026-10-26

* **Outside-range discovery presets** (migration `008_rpi_outside_range_presets`): `outside_range_small`
  (3,000–9,999 subscribers, median views ≥ 1,000, engagement ≥ 4%) and `outside_range_large` (150,001–500,000
  subscribers, median views ≥ 10,000, engagement ≥ 3%). Both search only the core segment hashtags
  (`CORE_HASHTAGS`) together with CreatorDB's topic classification, and both cost one credit per page. The
  autopilot searches them after `quality` and `quality_broad` are exhausted, within the same weekly allowance.
  Scoring still treats size as a soft preference, which lowers an outside-range creator's score slightly but never
  excludes them. The migration appends the new presets to existing settings once; re-running migrations keeps an
  admin's later choice.

## 0.2.1 — 2026-10-26

* A creator with recent paid deals from several direct competitors gets one exclusivity question naming all of
  them, instead of one repeated question per brand.
* Drafts no longer quote a video title that names another channel or account (`@handle`) or contains hidden
  formatting characters (Unicode Cf, such as text-direction controls). The validator rejects both anywhere in a
  draft, so a draft that has them fails re-validation at send.
* Competitor title avoidance matches whole words ("Vidu" no longer matches "individual").
* Competitor registry adds Higgsfield, Topview and Flova (direct), and DeeVid, Kling, Vidu and OpenArt
  (adjacent). `scripts/rpi_migrate.py` `seed_competitors` adds missing registry entries and links sponsorships
  recorded before their brand was listed, so exclusivity questions see them.

## 0.2.0 — 2026-10-25

* **Autopilot** (migration `007_rpi_autopilot`, `rpi.administration.autopilot`): a daily worker job
  (`rpi_autopilot`, 07:30 America/New_York) that drafts outreach for recommended creators, approves its own valid
  template drafts when allowed, schedules approved drafts into free send days, and researches more creators when
  fewer than the target number of messages are waiting. Off by default; admins configure it on the new *Autopilot*
  page (`GET/PUT /autopilot`, `POST /autopilot/run`). It never reads replies or sends follow-ups.
* **Per-job credit caps**: `CallContext.unit_caps` / job param `unit_caps` make the provider gateway refuse a call
  when the job's committed provider units plus the call's documented worst case would pass the cap, independent of
  USD prices. Autopilot jobs get what is left of the weekly allowance.
* Discovery runs return `next_page_tokens`; autopilot keeps a per-preset cursor so it never pays for the same page
  twice.
* Draft creation and approval moved to `rpi.outreach.service`, shared by the API and the autopilot. A draft's
  default contact is now the first email not marked invalid or undeliverable.
* Fix: a job that raised no longer overwrites the progress it had saved, so resuming it does not pay for the same
  creators again.

## 0.1.9 — 2026-10-25

* **Scheduled sends** (migration `006_rpi_scheduled_sends`): approved drafts get a *send at* time (dashboard
  *Schedule*, `POST /outreach/drafts/{id}/schedule`). The worker job `rpi_send_scheduled` runs every 5 minutes and
  sends due drafts inside the send window (`RPI_SEND_TIMEZONE`, `RPI_SEND_WINDOW`, `RPI_SEND_WEEKDAYS_ONLY`) through
  `send_draft`, so every gate still applies. A full daily limit or disabled sending keeps the schedule without audit
  noise; any other refusal cancels it and records the reason. `scripts/rpi_migrate.py` now applies 005 and 006.

## 0.1.8 — 2026-10-25

* Outreach email format: the template puts one idea per paragraph (same wording), and the HTML version is a single
  readable column (system font, 600px, no images) with a linked "Program details" and a small grey footer with an
  "opt out here" link. All HTML text is now escaped; video titles in drafts come from creators.
* Email signature: the RF monogram (served from reelforgeai.io), the sender's name, `RPI_SIGNATURE_TITLE` with
  "ReelForge AI", and the website and reply address; a plain-text version for the text part. It replaces the
  plain sign-off name and keeps a closing such as "Best,".

## 0.1.7 — 2026-10-25

* **Test sends:** *Send test to me* (`POST /outreach/drafts/{id}/test-send`) delivers a draft exactly as the creator
  would get it, subject prefixed `[TEST]`, but only to addresses in `RPI_TEST_RECIPIENTS`. It works before approval
  and while sending is off. Its unsubscribe link is inactive, and it never changes the draft, the CRM record or
  contact history.
* RPI emails now go out under the sender name and email in Outreach settings (reply-to the same address) instead of
  Partner Scout's Brevo sender, so the From line and the footer always agree.
* RFC 8058 one-click unsubscribe: `List-Unsubscribe` and `List-Unsubscribe-Post` headers, plus a POST handler on
  the unsubscribe link.

## 0.1.6 — 2026-10-25

* RPI accounts for Partner Scout's email history (`rpi/crm/contact_history.py`), matched by linked prospect,
  YouTube channel or email. The contact-frequency send gate counts Partner Scout's last email, the creator page
  and the draft panel show the history, and drafts for previously emailed creators open as a short follow-up.
  Partner Scout does not record replies, so drafts never claim the creator did not answer.

## 0.1.5 — 2026-10-25

* The automatic research funnel screens only creators an RPI discovery run preliminarily qualified. The 1,822
  Partner Scout prospects imported by migration 005 (and creators added by hand) are no longer eligible, so the
  funnel cannot spend 3 CreatorDB credits each screening them; they can still be screened when selected.

## 0.1.4 — 2026-10-25

* Affiliate commission default is now 30% recurring for 12 months, as confirmed by ReelForge
  (`RPI_AFFILIATE_COMMISSION_MONTHS=12`). Outreach drafts and profit forecasts use it. The product's
  `/affiliate-terms` page still says customer lifetime and needs correcting; the cookie window is undecided.
* Outreach drafts ask about category exclusivity when a creator has a recent paid direct-competitor deal, skip
  video titles with income claims or about competitors, and greet the channel by name.
* CreatorDB-reported sponsorships count as paid and are linked to the competitor registry.

## 0.1.3 — 2026-10-25

### Competitor promotions and sponsor saturation (scoring qual-2026-10-25.2)
* A recent paid direct-competitor promotion no longer lowers the score. It raises a "check exclusivity before
  outreach" flag with the brand and date, shown on the creator page.
* New commercial-fit signals from the stored recent videos: share of long-form videos that are paid integrations
  (saturation) and sponsored-video median views versus the creator's other videos. No extra credits.
* Taxonomy 2026-10-25.2 recognizes current AI video generators (Seedance, Kling, Higgsfield, …); the activity score
  falls back to stored content dates when the search snapshot has none.

## 0.1.2 — 2026-10-25

### Targeted, credit-efficient discovery
* Discovery presets (`rpi/discovery/presets.py`). The default `quality` preset puts every rule CreatorDB can enforce
  into one 10-filter search (1 credit): educator hashtags AND AI / content-creation / video-editing topics, English,
  main audience US/CA/GB/AU, 10k–150k subscribers, median views ≥ 2,000, engagement ≥ 2%, ≥ 2 uploads in 30 days,
  contact email on file; sorted by median views. Tuned with live searches (niches proved noisy). `quality_broad`
  and `none` are available; explicitly sent fields always win.
* New search criteria: audience countries and ages, median views, engagement rate, uploads in 30 days, contact
  email, sponsorship history, sort order. Search results report the enforced filters and the matching pool size.
* The preliminary screen counts provider-enforced filters as met (labeled), keeps the provider's ranking, and
  holds back names mostly in non-Latin script.
* Paid enrichment is skipped when its data may not be used for scoring (no CreatorDB screening while
  `RPI_CREATORDB_SCORING_PERMITTED=false`). Contacts (15 credits) only for `recommend`. Audience demographics
  (10 credits) off by default (`RPI_FETCH_AUDIENCE_DEMOGRAPHICS`). Funnel defaults lowered to discover 100,
  screen 25, analyze 10.
* Dashboard: preset selector, main-audience countries, credit estimates, subscribers on the run page.

## 0.1.1 — 2026-10-25

### CreatorDB adapter matched to the official API
* Endpoints, request bodies, response fields, error codes and credit costs now follow CreatorDB's OpenAPI 3.1
  spec and credit pages. Fixed: content path (`/youtube/content-detail`, was `/youtube/videos`), search body
  (`filters`/`pageSize`, was free-form keywords/limit), every profile field name, audience and sponsorship
  shapes, pagination (`hasNextPage`/`nextOffset`).
* Added `/youtube/performance` (views, engagement, uploads in 90 days) and free `/usage` (credits remaining;
  used by the health check).
* Costs settle from the provider-reported `creditsUsed`; not-found lookups and contact calls with no email cost 0.
* `QUOTA_EXCEEDED` is treated as credits exhausted and never retried; `RATE_LIMIT_EXCEEDED` is retried.
* Countries and languages are converted between CreatorDB's ISO alpha-3 / 639-3 and RPI's alpha-2 / 639-1.
* Brand-sponsorship search returns UNSUPPORTED instead of unrelated creators until `/sponsor/creators` is added.
* `scripts/rpi_verify_creatordb.py` rewritten: free usage check, one profile by default, `--probe` for every
  endpoint, `--niches` to list niche ids; reads the key from a mode-600 file.

## 0.1.0 — 2026-10-25

### Audit (Milestone 0)
* Audited this repository (Partner Scout v4.13), reelforge-ai (product), outreach and affiliate-scanner
  (read-only). Findings and ten risks in IMPLEMENTATION_PLAN.md.
* API verification: YouTube Developer Policies, SparkToro API docs, CreatorDB docs index/authentication page,
  Supadata homepage. A safety-check outage blocked other pages; those items are marked unverified.

### Implemented
* Migration 005 (45 `rpi_*` tables, additive) with rollback, tested up → down → up on PostgreSQL 14.
* Provider layer: normalized contract with explicit UNSUPPORTED; resilient HTTP (full-jitter retries,
  Retry-After, circuit breaker, token bucket, timeouts, structured errors, secret redaction); declarative field
  mapping; gateway with policy-bounded caching, budget reservations and request metering.
* Adapters: CreatorDB (profile verified; others gated), SparkToro, Supadata, YouTube Data (verification only),
  YouTube Analytics OAuth (disabled), synthetic fixture provider (refused in production).
* Spending controller: global/provider daily/monthly budgets, job and per-creator caps, low-priority pausing,
  alerts, two-person budget increases.
* Identity resolution, discovery engine (niche, audience-first, competitor sponsorship, emerging), enrichment
  value gate, research pipeline, resumable jobs, funnel orchestration.
* Content intelligence: versioned taxonomy, keyword prefilter, Claude fast/deep classification via forced tool,
  prompt-injection defenses, deterministic relevance, buyer-intent summary, evaluation harness.
* Sponsorship detection (ported affiliate-scanner rules plus disclosures and promo codes), audience fit (3 ICPs,
  no imputation), risk screening, deterministic qualification scoring with policy exclusions.
* Profit engine: cohort contribution, scenarios, break-even, payback, max fee, sensitivity, labeled simulation,
  deal optimizer, ledger actuals.
* CRM (16 statuses, audit trail, notes, follow-ups), suppression (hashed; includes Partner Scout opt-outs),
  outreach drafts with validator, approval workflow, gated exactly-once sending with CAN-SPAM footer and signed
  unsubscribe, Brevo event handling.
* Attribution (touch/identify/milestone, first/last touch, lock at first paid, consent-aware hashing), Stripe
  webhook (own endpoint, HMAC + replay window), ledger (tax/discount/refund/chargeback, FX provenance, commission
  accrual with duration and clawback), partner reports, cohorts, reconciliation, PostHog person-property bridge.
* Executive dashboard, cost per stage, 9-screen UI, admin endpoints, retention enforcement, compliant deletion.
* Celery: `run_rpi_job`, daily retention, daily ledger checks (no paid calls on schedules).
* Scripts: migrate/seed/import, create key, CreatorDB verification, benchmark import/report.
* Changes to existing code (additive): RPI routers mounted in `app/main.py`; log redaction processor; RPI
  handling appended to the Brevo webhook; RPI tasks in `celery_config.py`; settings in `app/config.py`;
  `cryptography` dependency; env vars in `.env.example` and `render.yaml`; `.gitignore` added.

### Tests performed
* `pytest`: **122 passed** in ~5 s against local PostgreSQL 14 (details in TEST_STRATEGY.md).
* Bugs found by tests and fixed: provider error bodies echoing API keys were not redacted.
* Manual: ran the app locally with synthetic data. Discovery found 32 candidates (28 unique, 4 duplicates
  across strategies); screening gave 18 recommend / 3 investigate / 3 hold / 4 rejected on niche; 4 creators
  deep-analyzed to `recommended`; total synthetic spend $0.43. Checked the executive dashboard, creators, creator
  profile, Profitability Lab, CRM board and administration in a browser. Fixed a JavaScript syntax error, a CSP
  conflict with inline styles, and negative-currency formatting.

### Not done / blockers
See KNOWN_LIMITATIONS.md. Main blockers: CreatorDB access and endpoint verification, provider terms reviews,
product-side attribution integration, manual benchmark labels, affiliate-term decision.
