# AI scoring and evaluation

## Niche taxonomy (`TAXONOMY_VERSION = 2026-10-25.1`)

| Label | ReelForge weight |
|---|---|
| faceless_youtube_automation | 1.0 |
| ai_shorts_creation | 1.0 |
| ai_video_tools | 0.9 |
| social_media_content_automation | 0.8 |
| ai_voice_generation | 0.7 |
| creator_monetization | 0.6 |
| general_ai_news | 0.35 |
| other | 0.1 |
| unrelated_technical | 0.05 |
| entertainment | 0.05 |

Labels with weight ≥ 0.6 count as "relevant". Changing labels, weights or keyword rules requires a version bump.

## Classification pipeline

1. **Keyword prefilter** (free): title + first 600 description characters against per-label patterns.
   * `clear_relevant`: ≥ 60% of ≥ 5 videos relevant → no Claude call.
   * `clear_irrelevant`: weighted relevance ≤ 0.1 over ≥ 8 videos → no Claude call.
   * otherwise `needs_llm`.
2. **Fast model** (`claude-haiku-5-5` by default): up to 20 videos per call, each wrapped as untrusted content.
   Output only via the forced `report_creator_analysis` tool: labels (enum), relevance, audience problems
   (enum), buyer-intent class (enum), promotional flag, one-sentence note, and a creator summary with
   confidence and uncertainty reasons.
3. **Deep model** (`claude-sonnet-5-5`): only when the fast confidence is < 0.6 or the model's own relevance
   differs from the computed relevance by > 0.3, **and** the creator is at stage ≥ 3.
4. **Validation**: unknown labels dropped, numbers clamped, references to video ids that were not supplied
   discarded (and counted), duplicate ids ignored, truncated responses rejected.
5. **Deterministic relevance**: `computed_relevance` = mean over videos of the best label weight. This is what
   scoring uses. The model's `reelforge_relevance` is stored as `model_relevance`, a labeled AI judgment.

Prompt version `niche-v1`. Model, prompt version, taxonomy version and cost are stored on every
classification row.

## Buyer intent

Classes: explicit product-shopping interest, specific workflow pain point, general educational interest,
curiosity without purchase intent, negative/unsuitable fit. Today these are classified from **creator content**
(content-inferred), not from viewer comments, and are reported as a qualitative distribution with an explicit
caveat. Comment analysis is not implemented because of YouTube API data restrictions. Predictive usefulness must
be validated against campaign outcomes before it influences ranking.

## Spoken-language gate

RPI targets English-speaking channels. CreatorDB's `mainLanguage` follows a channel's titles, so channels that title in
English but speak Hindi passed its filter (Hindi AI Gyaan, Chad Grow), and its search can include languages but not
exclude them. After every CreatorDB search, and again before screening and deep analysis, RPI reads the spoken
language each of the channel's 10 newest uploads declares (YouTube `defaultAudioLanguage`, 2 free quota units).
When at least 3 videos declare one and fewer than half of those declare English, the creator is disqualified
(`non_english_audio`, recommendation `reject_non_english`) before any CreatorDB credit is spent on them. Fewer than 3
declared languages, no YouTube key or an API error: the creator continues unchecked. `POST /research/language-check`
runs the check alone for chosen creators.

On 2026-10-26 the check found 6 of the 9 creators waiting to be screened, and 2 of 8 recommended creators, speaking
Hindi, Ukrainian or Malayalam. 6 of the 24 creators checked declared no language and would pass unchecked.

## Qualification score (`qual-2026-10-26.1`)

| Component | Weight | Source |
|---|---|---|
| niche_relevance | 0.40 | computed relevance (content-inferred) |
| audience_fit | 0.20 | best ICP fit (see below) |
| activity | 0.15 | upload recency + uploads in 90 days |
| commercial_fit | 0.15 | relevant software promotions (+), sponsor saturation (−), sponsored-video view retention (±), requested fee vs range |
| size_fit | 0.10 | 10k–150k subscribers = 1.0, outside → soft penalty with a floor of 0.4 |

* The overall score is a weighted mean over **available** components; missing or policy-excluded components drop
  out and are listed.
* Risk penalty: −0.05 for `review_recommended`, −0.15 for `elevated_review`.
* Commercial fit starts at 0.5. +0.15 per confirmed paid or disclosed affiliate software promotion (max +0.3),
  including competitors: a creator who has sold a competing tool has an in-market audience and a format that
  converts. Sponsor saturation over the stored recent long-form videos (CreatorDB `isSponsored` or a confirmed paid
  detection; judged only with ≥ 5 videos): > 30% sponsored −0.1, > 50% −0.2. Sponsored videos' median views versus
  the other recent videos: < 60% −0.1, ≥ 90% +0.05. These thresholds are judgment calls to recalibrate against
  campaign outcomes.
* A paid promotion of a **direct** competitor in the last 90 days does not change the score. It adds a flag,
  "check exclusivity before outreach: <brand> (paid, <date>)", shown on the creator page, because sponsorship
  contracts often carry a 30–90 day category exclusivity.
* Confidence = completeness × (0.5 + 0.5 × classification confidence) × (0.6 + 0.4 × ICP completeness).
* Recommendation: `reject_suspect_engagement` (any `elevated` risk signal, checked first) · `recommend` (overall ≥
  0.7, niche ≥ 0.6, risk not elevated) · `investigate` (≥ 0.5) · `hold` · `reject_niche_mismatch` (niche < 0.25
  with classification confidence ≥ 0.6) · `needs_data`. Both rejections set `rpi_creators.disqualified_reason`
  (`suspect_engagement`, `niche_mismatch`), so the funnel and the autopilot skip the creator; a person can still
  draft for them.

## Audience fit

Three ICPs (faceless entrepreneurs, small-business marketers, content agencies), scored separately. Components:
content topics, audience problems, geography (US/CA/GB/AU), language, age. When sources conflict, measured
first-party data is preferred over third-party modeled estimates, then manual submissions, then content inference.
Missing data is never imputed; `completeness` shows how much of the profile was evaluable.

## Risk screening

Signals: view concentration (best/median ≥ 20×), low views per subscriber (< 0.5%), engagement inconsistency or
extremes, inactivity (> 60 days), irregular posting, subscriber growth spikes or drops between snapshots, niche
drift. Output is labeled "risk screening result (not a forensic determination)".

**Flat engagement** (`flat_engagement`, severity `elevated`): likes that stay the same whatever a video's views,
the pattern of a fixed package of likes and comments bought per video. Over the stored long-form videos at least
3 days old (≥ 6 of them, and only when the 90th-percentile video has ≥ 2× the views of the 10th), it fits
log(likes) against log(views); a slope below 0.3 flags. On real channels likes keep pace with views: the 15
creators scored in production on 2026-10-26 had slopes of 0.59–1.19. @BeerMoneyForum had about 2,500 likes and
290 comments on every video between 33k and 160k views (slope 0.02), and none of the other signals caught it.
Any `elevated` signal makes the risk level `elevated_review` and rejects the creator. It uses the per-video
stats screening already buys, so it costs no credits.

**Uniform engagement** (`uniform_engagement`, severity `elevated`): the case flat engagement cannot judge, where views
are bought too and every video lands on about the same views and likes. Over at least 10 of the same videos, it flags
when the 90th-percentile video has under 2× the views of the 10th and under 2.5× the likes. NyesWorldTV (views
within 1.8×, likes within 1.3×; on YouTube, views that stopped growing after about four days) and Tech Explorer AI
(1.8× and 2.2×) were flagged; both drew comments from the same comment farm. Of the 55 creators scored by
2026-10-26 no other had views under 2× and likes under 3×. Like flat engagement it rejects the creator
(`reject_suspect_engagement`).

**Recent switch.** Both engagement checks also run on the newest 15 judged videos, for channels that started buying
partway through their history (Ari Thinks: 7–17K varied views until mid-August 2026, then 36–48K on every video).
A signal found this way says "newest 15 videos:". Of the 55 scored creators it added APEX_001, NexcopeAI and
Trainer1300, each confirmed by hand (fixed view levels, farmed or numbered comments).

**Views below subscribers** (`views_below_subscribers`, severity `elevated`): fewer lifetime channel views than
subscribers, the mark of bought subscribers. Deep analysis first fetches the CreatorDB profile (2 credits, reused
for 7 days) for `totalViews`; when views are below subscribers it skips demographics, sponsorship history and
transcripts and rejects the creator. Real channels have tens to hundreds of lifetime views per subscriber (11–634
across the 55 scored creators, median 67); Ari Thinks had 0.97.

## Evaluation benchmark

* Labels: `rpi_benchmark_labels`, imported from a manually reviewed CSV (`docs/rpi/benchmark_template.csv`) with
  `python -m scripts.rpi_benchmark --labels file.csv --labeler <name>`.
* Report: `python -m scripts.rpi_benchmark --report` compares `provider_order` (CreatorDB-only baseline),
  `plus_content`, `plus_audience_first` and `full` on precision@20, false-positive rate, recall within the labeled
  set, critical-field completeness and research cost per relevant creator.
* Targets (engineering targets, not demonstrated performance): ≥ 90% topical relevance in the top 20; ≥ 80%
  critical-field availability; no uncontrolled spend; no duplicate creators; no unapproved messages; reconciled
  ledger.
* **Status: no labels exist yet.** No accuracy claim can be made until 50–100 channels are reviewed.

## Continuous learning (not started)

No statistical model is trained. Once enough campaigns have outcomes: regularized or hierarchical models on
features with `training_allowed(provider) == True` only, out-of-sample evaluation, features frozen at
recommendation time (no outcome leakage), and ranking on expected incremental contribution.
