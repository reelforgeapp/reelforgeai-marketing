# Attribution specification

Journey: **partner referral → landing → signup → first successful render → export/publish → paid conversion →
renewal → retention**.

## Sources of truth

| Fact | Source |
|---|---|
| Revenue, refunds, disputes, plan, cancellation | Stripe webhook events received by RPI (authoritative) |
| Referral touch | Product/website calls `POST /rpi/api/v1/attribution/touch` |
| Signup and identity link | Product calls `POST /rpi/api/v1/attribution/identify` |
| First render / first export | Product calls `POST /rpi/api/v1/attribution/milestone` |
| Behavioral funnels | PostHog (product's existing events), broken down by RPI-set person properties |

PostHog is never used as a financial ledger.

## Rules

1. A **touch** is a visit carrying a campaign referral code (`?red=` Reditus slug or `?ref=`) or a campaign `utm_campaign`. Campaigns have
   unique referral codes and UTM campaign values.
2. Only touches **within the campaign's attribution window before signup** count (default 60 days, the
   `/affiliate-terms` cookie window).
3. A referral code recorded by the product at signup (`users.referred_by_code`) counts as a touch at signup time.
4. **First-touch** and **last-touch** campaigns are stored separately on `rpi_customers`; reports can use either.
   Commission accrual uses **last touch** (the program's last-click rule).
5. Attribution is **locked at the first paid charge** (or when identify arrives after the first payment). Later visits
   cannot move revenue between partners.
6. If a billing event arrives before identify, a placeholder customer `stripe:<cus_id>` is created and merged into
   the product user when identify supplies the `billing_customer_id`.
7. **Consent**: with `consent_state = denied`, the touch is counted without any identifier. Otherwise the
   anonymous id is stored only as a salted SHA-256 hash.

## Ingest API (role `ingest`, server-to-server only)

```http
POST /rpi/api/v1/attribution/touch
{"anonymous_id": "<posthog distinct id or cookie id>", "referral_code": "SAM20",
 "utm_source": "youtube", "utm_medium": "influencer", "utm_campaign": "SAM20",
 "landing_page_variant": "faceless-v2", "consent_state": "granted|denied|unknown",
 "touched_at": "2026-10-20T15:00:00Z", "idempotency_key": "<unique per visit>"}

POST /rpi/api/v1/attribution/identify
{"customer_ref": "<product user UUID>", "signed_up_at": "...", "anonymous_id": "...",
 "billing_customer_id": "cus_...", "signup_referral_code": "SAM20", "consent_state": "..."}

POST /rpi/api/v1/attribution/milestone
{"customer_ref": "<uuid>", "milestone": "first_render|first_export", "at": "..."}
```

## Reditus slug ↔ campaign

Affiliate links are `reelforgeai.io/?red=SLUG` (Reditus). Since reelforge-backend PR #499 (2026-11-14) the
product stores `SLUG` as `users.referred_by_code` at every signup path, including accounts created at checkout,
sends it with `identify`, and reports the signup to Reditus with the user id as `uid`. (A bare `?ref=CODE` is
also stored, for creator codes RPI hands out, but it is not reported to Reditus.) A campaign's `referral_code` is
the creator's Reditus slug, matched case-insensitively. The Reditus sync starts tracking by itself for an
accepted affiliate it links to a creator; otherwise start it from the creator page (*Campaign tracking*) or
`POST /campaigns`. Creating the campaign also attributes signups that used the code earlier: unlocked customers
are re-attributed, and customers who already paid without any campaign get this one. A paid customer is never
moved from another partner.

## Reditus sync (rpi.attribution.reditus)

Reditus pays affiliates: it records the referral (click → signup) and its Stripe connection credits each
payment of a referred customer as commission. RPI reads the program once a day (read only) and checks the two
hand-offs that have failed before:

* **Signup reached Reditus**: every RPI customer since 2026-11-14 whose signup code is an accepted affiliate's
  slug must have a Reditus referral with their user id within a day; otherwise an alert says how many are
  missing (their affiliates would not be paid).
* **Payment credited**: a referred customer with a first payment more than two days old whose Reditus referral
  is not `paid` raises a critical alert (check Reditus' Stripe connection). Needs RPI's Stripe endpoint.

## Product integration (reelforge-backend `backend/services/rpi-attribution.js`, live 2026-10-26)

Implemented: `identify` after email, Google and checkout signups and whenever a Stripe customer is linked;
`milestone first_render` where `first_value_reached` fires; `milestone first_export` on the first download or
publish; the referral code (Reditus `?red=` slug, or `?ref=`) carried through public checkout. Server-to-server, fire-and-forget, user id only (no
email or name). Not implemented: landing-page `touch` calls (rule 3 makes the signup code the touch) and the
per-render cost feed.

## Product changes originally specified

1. On landing with `ref`/UTM: server-side `touch` call (or proxy from the frontend through the backend; never
   expose the ingest key to browsers).
2. In `routes/auth.js` signup (and checkout-created users): `identify` with user id, signup time,
   `referred_by_code`, PostHog distinct id and, once known, `stripe_customer_id`.
3. In `services/video-generation.js` where `first_value_reached` fires: `milestone first_render`. On first download or
   publish: `milestone first_export`.
4. In Stripe: add the endpoint `https://<marketing-host>/rpi/api/v1/webhooks/stripe` with events `invoice.paid`,
   `invoice.payment_succeeded`, `charge.refunded`, `charge.dispute.created`, `charge.dispute.closed`,
   `customer.subscription.created|updated|deleted`; put its signing secret in `RPI_STRIPE_WEBHOOK_SECRET`.
5. Optional: per-render cost events to `POST /rpi/api/v1/ledger/costs` (role `ingest`). Until then, run
   `POST /ledger/allocate-variable-costs` monthly (estimates).

## PostHog naming

The product already uses snake_case events with `distinct_id` = user UUID (`user_signed_up`,
`first_value_reached`, `video_rendered`, `social_post_published`, `subscription_created`, …). The directive's
proposed `partner_signup`, `partner_first_render_success` and similar events would duplicate these, so RPI does
**not** emit them. Instead:

* RPI sets person properties `partner_first_touch_campaign_id`, `partner_last_touch_campaign_id`,
  `partner_creator_id`, `partner_referral_code`, `partner_attribution_model`. All existing events can then be
  segmented by partner.
* RPI emits one new event: `partner_attribution_assigned` (properties: `campaign_id`, `attribution_model`, …).
* `partner_referral_visit` should be captured client-side by the website, because only the browser sees the visit.
  Use properties `partner_id`, `campaign_id`, `creator_id`, `referral_code`, `utm_*`, `landing_page_variant`.

No emails, names or secrets are sent. With the default `RPI_POSTHOG_REQUIRES_CONSENT=true`, nothing is sent
unless the product supplies `consent_state=granted`. The product currently has no consent mechanism.

## Commission accrual

For each revenue, discount or refund entry of a customer whose last-touch campaign has an **agreed** deal with a
commission: commission = rate × net amount, for the commission duration (months since first payment). Refunds
within 60 days of the first payment reverse commission (clawback). Accruals are idempotent per ledger entry and
flagged as estimates. The Campaigns page shows them next to the commissions Reditus actually recorded for each
affiliate (pending and paid); Reditus' figures are what is paid.
