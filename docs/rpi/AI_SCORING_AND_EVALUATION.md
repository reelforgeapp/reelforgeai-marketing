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

## Qualification score (`qual-2026-10-25.1`)

| Component | Weight | Source |
|---|---|---|
| niche_relevance | 0.40 | computed relevance (content-inferred) |
| audience_fit | 0.20 | best ICP fit (see below) |
| activity | 0.15 | upload recency + uploads in 90 days |
| commercial_fit | 0.15 | relevant software promotions (+), active direct-competitor sponsorship (−), requested fee vs range |
| size_fit | 0.10 | 10k–150k subscribers = 1.0, outside → soft penalty with a floor of 0.4 |

* The overall score is a weighted mean over **available** components; missing or policy-excluded components drop
  out and are listed.
* Risk penalty: −0.05 for `review_recommended`, −0.15 for `elevated_review`.
* Confidence = completeness × (0.5 + 0.5 × classification confidence) × (0.6 + 0.4 × ICP completeness).
* Recommendation: `recommend` (overall ≥ 0.7, niche ≥ 0.6, risk not elevated) · `investigate` (≥ 0.5) · `hold` ·
  `reject_niche_mismatch` (niche < 0.25 with classification confidence ≥ 0.6) · `needs_data`.

## Audience fit

Three ICPs (faceless entrepreneurs, small-business marketers, content agencies), scored separately. Components:
content topics, audience problems, geography (US/CA/GB/AU), language, age. When sources conflict, measured
first-party data is preferred over third-party modeled estimates, then manual submissions, then content inference.
Missing data is never imputed; `completeness` shows how much of the profile was evaluable.

## Risk screening

Signals: view concentration (best/median ≥ 20×), low views per subscriber (< 0.5%), engagement inconsistency or
extremes, inactivity (> 60 days), irregular posting, subscriber growth spikes or drops between snapshots, niche
drift. Output is labeled "risk screening result (not a forensic determination)".

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
