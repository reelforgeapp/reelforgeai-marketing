"""
Evaluation benchmark metrics (directive §26).

Compares ranking variants against a manually labeled benchmark of relevant
and irrelevant channels (rpi_benchmark_labels; template in
docs/rpi/benchmark_template.csv). Variants:

  provider_order       order returned by the discovery provider (baseline)
  plus_content         + RPI content classification (niche relevance)
  plus_audience_first  + SparkToro-sourced candidates
  full                 complete qualification score

Metrics: precision@k (topical relevance in the top k), false-positive rate,
recall within the labeled set, critical-field completeness, research cost per
relevant creator found. Labels must come from human review; nothing here
generates labels.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

CRITICAL_FIELDS = ("subscribers", "language", "last_upload_at", "content_evidence", "contact")


@dataclass
class RankedCandidate:
    key: str  # "youtube:UC..."
    score: Optional[float]
    fields_present: set[str]
    cost_usd: Decimal = Decimal("0")


@dataclass
class VariantResult:
    variant: str
    k: int
    precision_at_k: Optional[float]
    false_positive_rate: Optional[float]
    recall: Optional[float]
    labeled_in_top_k: int
    critical_field_completeness: Optional[float]
    cost_per_relevant_found_usd: Optional[str]
    unlabeled_in_top_k: int


def evaluate_variant(name: str, ranked: list[RankedCandidate], labels: dict[str, bool], k: int = 20) -> VariantResult:
    ordered = sorted(ranked, key=lambda c: (c.score is None, -(c.score or 0)))
    top = ordered[:k]
    labeled_top = [c for c in top if c.key in labels]
    relevant_top = [c for c in labeled_top if labels[c.key]]
    relevant_total = sum(1 for v in labels.values() if v)
    irrelevant_total = sum(1 for v in labels.values() if not v)
    found_relevant = {c.key for c in top if labels.get(c.key)}
    false_pos = sum(1 for c in top if c.key in labels and not labels[c.key])
    completeness = None
    if top:
        completeness = sum(len(c.fields_present & set(CRITICAL_FIELDS)) for c in top) / (len(top) * len(CRITICAL_FIELDS))
    cost = sum((c.cost_usd for c in ranked), Decimal("0"))
    return VariantResult(
        variant=name,
        k=k,
        precision_at_k=round(len(relevant_top) / len(labeled_top), 3) if labeled_top else None,
        false_positive_rate=round(false_pos / irrelevant_total, 3) if irrelevant_total else None,
        recall=round(len(found_relevant) / relevant_total, 3) if relevant_total else None,
        labeled_in_top_k=len(labeled_top),
        critical_field_completeness=round(completeness, 3) if completeness is not None else None,
        cost_per_relevant_found_usd=str((cost / len(found_relevant)).quantize(Decimal("0.0001"))) if found_relevant else None,
        unlabeled_in_top_k=len(top) - len(labeled_top),
    )


def acceptance(result: VariantResult) -> dict:
    """Directive §26 engineering targets (not proven performance)."""
    return {
        "top20_topical_relevance_>=_90%": result.precision_at_k is not None and result.precision_at_k >= 0.9,
        "critical_fields_>=_80%": result.critical_field_completeness is not None and result.critical_field_completeness >= 0.8,
        "enough_labels_in_top_k": result.labeled_in_top_k >= min(result.k, 15),
    }


async def load_variants(conn, k: int = 20) -> dict[str, list[RankedCandidate]]:
    rows = await conn.fetch(
        """SELECT a.platform || ':' || a.platform_account_id AS key, c.id,
                  (SELECT MIN(d.created_at) FROM rpi_discovery_candidates d WHERE d.creator_id = c.id) AS first_seen,
                  (SELECT MAX(d.preliminary_score) FROM rpi_discovery_candidates d WHERE d.creator_id = c.id) AS prelim,
                  (SELECT bool_or(d.strategy = 'audience_first') FROM rpi_discovery_candidates d WHERE d.creator_id = c.id) AS via_af,
                  q.niche_relevance, q.overall,
                  s.subscribers, s.language, s.last_upload_at,
                  EXISTS (SELECT 1 FROM rpi_content_classifications x WHERE x.creator_id = c.id) AS has_evidence,
                  EXISTS (SELECT 1 FROM rpi_creator_contacts k WHERE k.creator_id = c.id) AS has_contact,
                  COALESCE((SELECT SUM(actual_usd) FROM rpi_spend_ledger l WHERE l.creator_id = c.id AND l.status = 'committed'), 0) AS cost
           FROM rpi_creators c
           JOIN LATERAL (SELECT platform, platform_account_id FROM rpi_creator_social_accounts z WHERE z.creator_id = c.id LIMIT 1) a ON TRUE
           LEFT JOIN LATERAL (SELECT niche_relevance, overall FROM rpi_qualification_scores q WHERE q.creator_id = c.id ORDER BY computed_at DESC LIMIT 1) q ON TRUE
           LEFT JOIN LATERAL (SELECT subscribers, language, last_upload_at FROM rpi_creator_profile_snapshots s WHERE s.creator_id = c.id ORDER BY observed_at DESC LIMIT 1) s ON TRUE
           WHERE c.merged_into_id IS NULL""")

    def fields(r) -> set[str]:
        present = set()
        for f, ok in (("subscribers", r["subscribers"] is not None), ("language", r["language"] is not None),
                      ("last_upload_at", r["last_upload_at"] is not None), ("content_evidence", r["has_evidence"]),
                      ("contact", r["has_contact"])):
            if ok:
                present.add(f)
        return present

    base = [r for r in rows if not r["via_af"]]
    return {
        "provider_order": [RankedCandidate(r["key"], r["prelim"], fields(r), Decimal(r["cost"])) for r in base],
        "plus_content": [RankedCandidate(r["key"], r["niche_relevance"], fields(r), Decimal(r["cost"])) for r in base],
        "plus_audience_first": [RankedCandidate(r["key"], r["niche_relevance"], fields(r), Decimal(r["cost"])) for r in rows],
        "full": [RankedCandidate(r["key"], r["overall"], fields(r), Decimal(r["cost"])) for r in rows],
    }
