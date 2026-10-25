"""Benchmark metric arithmetic (labels here are test inputs, not real judgments)."""

from decimal import Decimal

from rpi.content_intelligence.evaluation import RankedCandidate, acceptance, evaluate_variant


def test_precision_recall_fpr_and_cost():
    labels = {"a": True, "b": True, "c": False, "d": False, "e": True}
    ranked = [RankedCandidate("a", 0.9, {"subscribers", "language"}, Decimal("0.10")),
              RankedCandidate("c", 0.8, set(), Decimal("0.10")),
              RankedCandidate("b", 0.7, {"subscribers"}, Decimal("0.10")),
              RankedCandidate("x", 0.6, set(), Decimal("0.10")),  # unlabeled
              RankedCandidate("e", None, set(), Decimal("0.10"))]
    r = evaluate_variant("v", ranked, labels, k=4)
    assert r.precision_at_k == round(2 / 3, 3)  # a, b relevant; c not; x unlabeled
    assert r.false_positive_rate == 0.5  # c out of {c, d}
    assert r.recall == round(2 / 3, 3)
    assert r.unlabeled_in_top_k == 1
    assert r.cost_per_relevant_found_usd == "0.2500"
    assert acceptance(r)["top20_topical_relevance_>=_90%"] is False
