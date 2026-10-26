# Data licensing and compliance

The enforceable version of this document is `rpi/compliance/policies.py` (shown at
`GET /rpi/api/v1/admin/compliance`). Unverified permissions default to the most restrictive reasonable
interpretation and are marked `verified: false`.

## Provider policy summary

| Provider | Verified | Derived scoring | AI analysis | Training | Max storage | Notes |
|---|---|---|---|---|---|---|
| YouTube Data API | ✅ | **Prohibited** (III.E.4(h)) | not addressed → not used for scoring | not addressed → not used | 30 days | One project; no scraping; no redistribution |
| YouTube Analytics (OAuth) | ✅ | Prohibited | — | Prohibited | 30 days | Display only to the authorizing user (III.E.3(b)); integration **disabled** pending legal review |
| CreatorDB | ❌ | Off until `RPI_CREATORDB_SCORING_PERMITTED=true` | same flag | Unknown → not used | 90 days (placeholder) | Review terms before production scoring |
| SparkToro | ❌ (docs only) | Unknown | Unknown | Treated as prohibited | 30 days | Audience-level only; surface TAM rationale; do not build a competing audience-intelligence product |
| Supadata | ❌ | Unknown | Unknown | Unknown | 30 days | Transcripts stored only as ≤1,500-char excerpts |
| First-party (CRM, ledger, attribution) | ✅ | Allowed | Allowed | Allowed | — | Subject to privacy deletion |
| Manual creator submissions | ✅ | Allowed | Allowed | Not used | 365 days | Labeled unverified |
| Fixture (synthetic) | ✅ | Allowed | Allowed | — | 7 days | Refused in production |

## How enforcement works

* **Scoring gate**: `scoring_allowed(provider)` decides whether a provider's fields may feed qualification scores,
  audience fit, risk screening or classification. Excluded inputs are listed in `excluded_sources` on every score.
* **YouTube**: the adapter marks records `derived_metrics_prohibited` and calls `channels.list`, plus
  `playlistItems.list` and `videos.list` for the spoken-language gate (`rpi.qualification.language_gate`). The gate
  reads each recent video's `defaultAudioLanguage` and uses it only to decide whether a creator is researched at
  all: no score, metric or ranking is built from it (III.E.4(h)), the raw values stay in the 30-day cache, and only
  the decision is stored on the creator. Treating a raw-field filter as outside III.E.4(h) is the operator's
  judgment (2026-10-26), not a reviewed legal opinion. Partner Scout's existing YouTube discovery is separate and
  unchanged (see IMPLEMENTATION_PLAN risk 6).
* **Retention**: `rpi_data_provenance.expires_at` is set from the policy at write time; the daily retention task
  deletes expired provenance and cascades to the facts. Expired data is not refreshed automatically, because
  refreshing costs money.
* **Caching**: allowed only within each policy's `cache_ttl_days`; contacts are never cached.
* **Model training**: RPI trains no models today. `training_allowed(provider)` must be checked before any
  future model uses provider data (AI_SCORING_AND_EVALUATION.md).

## Privacy

* Creator contact data comes only from providers, the creator, or an operator, with the source recorded. Emails
  are never guessed or scraped by RPI.
* Visitor identifiers are stored as salted hashes, and not at all when consent is denied.
* PostHog receives only campaign/creator ids and referral codes, never emails or names, and only with consent.
* Deletion: `DELETE /rpi/api/v1/creators/{id}` (admin) removes the creator and provider-derived data and
  suppresses the contact (hashed) so re-discovery cannot lead to outreach.
* Opt-outs: unsubscribe link (signed), Brevo unsubscribe/complaint/hard-bounce events, manual suppression, and
  Partner Scout `unsubscribed`/`bounced` statuses all block sending.

## Outreach compliance (CAN-SPAM)

Sending is blocked unless a sender name, sender email and **physical postal address** are configured. Every
message carries a working one-click unsubscribe link and reply-to-unsubscribe instructions. Drafts cannot
contain income claims, fabricated familiarity, unsupplied URLs/emails or commission terms other than the program's.

## Commercialization caveat

Commercializing the software does not license the data feeding it. Each provider's terms must be reviewed for
multi-customer use before any SaaS launch (COMMERCIALIZATION_READINESS.md).
