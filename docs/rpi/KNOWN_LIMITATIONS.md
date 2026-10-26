# Known limitations and blockers

## External blockers (need access or a decision)

1. **CreatorDB search relevance unmeasured.** Live since 2026-10-25. Keywords are matched as hashtags, which may
   miss creators who don't tag, and niches are noisy (presets use hashtags AND topics). Competitor-sponsorship
   discovery needs `/sponsor/creators`, which is not called yet.
2. **CreatorDB publishes no API terms.** ReelForge confirmed its agreement permits internal scoring;
   `RPI_CREATORDB_SCORING_PERMITTED=true` in production.
3. **SparkToro and Supadata terms unread; response shapes unverified.**
4. **Claude prices unverified.** Spend reports use the configured per-token prices.
5. **YouTube Analytics display restriction** (Dev Policies III.E.3(b)) needs legal review; the integration is off.
   Analytics report retrieval is not implemented.
6. **Affiliate cookie window undecided** (60 vs 90 days); RPI uses 60. The commission terms pages were corrected to
   30% for 12 months on 2026-10-25.
7. **Stripe → RPI endpoint not registered yet.** The product reports signups and milestones (reelforge-backend,
   2026-10-26), but payments only reach RPI once the owner adds RPI's Stripe endpoint and secret. Signups before
   the product integration went live are not in RPI. Landing-page touches are not sent (the signup `?ref=` code
   is the touch), and no per-render cost feed exists.
8. **No labeled benchmark.** No accuracy claim is possible until 50–100 channels are manually labeled.
9. **The documentation fetch outage** during the build left items in API_INTEGRATIONS.md marked unverified.

## Functional limitations

* Lookalike discovery (strategy D) is not implemented; it needs outcome data from completed campaigns.
* Comment-level buyer-intent analysis is not implemented. Buyer intent is classified from creator content.
* A creator whose deep analysis keeps failing is retried on every run (a sponsorship lookup is cached for 14
  days, so a retry mostly costs Claude tokens) until a person looks at the alert.
* The spoken-language gate trusts the language uploaders declare on their videos. Channels that declare none
  pass unchecked, and the gate fails open (no YouTube key, quota exhausted, API error). Its quota is shared with
  Partner Scout's daily YouTube discovery.
* Bought engagement is detected only when likes stay flat while views vary (`flat_engagement`). Likes bought in
  proportion to views, bought views alone, and generated comment text are not detected; comments are not read.
* Email verification (Bouncer/Clearout/Hunter, already in this repo) is not yet wired into RPI contacts;
  contacts show `unverified` unless a hard bounce marks them undeliverable.
* Replies are detected through the partnership bot, which reads the inbox once a day (14:00 UTC). A creator is
  marked `replied` when they write in after an RPI email. That shows they wrote, not what they said, and anything
  they send before the next bot run is not yet visible. The autopilot sends no follow-ups; the bot answers people
  who write in.
* Autopilot discovery stops when every preset has been paged through (an alert says so); it does not invent new
  search criteria. CreatorDB adds creators over time, so clearing `discovery_cursor` later can find new ones.
* Payment fees are estimated (2.9% + $0.30) and dispute fees are estimated ($15); balance-transaction fees are
  not fetched. External reconciliation against Stripe payouts is not implemented.
* Chargebacks are recorded without a customer when the dispute payload only carries a charge id.
* Render/infra cost per customer is an allocation estimate until the product sends real per-render costs.
* Commission accruals are estimates to reconcile against Reditus payouts; Reditus is not integrated.
* ICP definitions live in code (`DEFAULT_ICPS`); `rpi_icp_profiles` is reserved for overrides and not yet read.
* Provider-sourced sponsorships have no content id, so repeated deep analysis can store duplicate rows for them.
* The preliminary (stage-1) screen is deliberately inclusive: language, activity and size usually suffice, because
  topical relevance comes from the search itself.
* Identity resolution across platforms only has data when providers return linked accounts; RPI is YouTube-first.
* Token buckets are per process (not shared across workers).
* No API rate limiting on `/rpi` itself.
* The UI has no automated tests and is plain JavaScript (no design-system reuse from the product).

## Partner Scout issues found in the audit

See IMPLEMENTATION_PLAN.md, "Risks found in existing systems". Fixed in PRs #1 and #2: unauthenticated
trigger/keyword endpoints, rejected Brevo webhooks, unverified claims and fabricated familiarity in templates,
missing postal address and unsubscribe link, YouTube page scraping, retention of YouTube statistics, outdated
Claude model id. Still open: Partner Scout sends without human approval (owner decision).
