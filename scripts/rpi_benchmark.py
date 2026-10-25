"""
Load manual benchmark labels and score the ranking variants.

    python -m scripts.rpi_benchmark --labels docs/rpi/benchmark_template.csv --labeler larry   # import labels
    python -m scripts.rpi_benchmark --report                                                 # compute metrics

CSV columns: platform,platform_account_id,segment,relevant,labels,notes
`relevant` is true/false as judged by a human reviewer. Rows with an empty
`relevant` are skipped, never defaulted.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rpi.content_intelligence.evaluation import acceptance, evaluate_variant, load_variants  # noqa: E402


async def import_labels(conn, path: str, labeler: str) -> int:
    n = 0
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            rel = (row.get("relevant") or "").strip().lower()
            if rel not in ("true", "false") or not row.get("platform_account_id"):
                continue
            await conn.execute(
                """INSERT INTO rpi_benchmark_labels (platform, platform_account_id, segment, relevant, labels, labeled_by, notes)
                   VALUES ($1,$2,$3,$4,$5,$6,$7)
                   ON CONFLICT (platform, platform_account_id) DO UPDATE SET relevant = EXCLUDED.relevant,
                     segment = EXCLUDED.segment, labels = EXCLUDED.labels, labeled_by = EXCLUDED.labeled_by,
                     notes = EXCLUDED.notes, labeled_at = NOW()""",
                row.get("platform") or "youtube", row["platform_account_id"].strip(), row.get("segment") or "unspecified",
                rel == "true", [l.strip() for l in (row.get("labels") or "").split(";") if l.strip()], labeler, row.get("notes"))
            n += 1
    return n


async def report(conn, k: int) -> dict:
    labels = {f"{r['platform']}:{r['platform_account_id']}": r["relevant"]
              for r in await conn.fetch("SELECT platform, platform_account_id, relevant FROM rpi_benchmark_labels")}
    if not labels:
        return {"error": "no benchmark labels; import a reviewed CSV first"}
    variants = await load_variants(conn, k)
    out = {"labels": len(labels), "relevant": sum(labels.values()), "variants": []}
    for name, ranked in variants.items():
        res = evaluate_variant(name, ranked, labels, k)
        out["variants"].append(asdict(res) | {"acceptance": acceptance(res)})
    return out


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--labels")
    ap.add_argument("--labeler", default="operator")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--k", type=int, default=20)
    ap.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    args = ap.parse_args()
    conn = await asyncpg.connect(args.database_url)
    try:
        if args.labels:
            print(f"imported {await import_labels(conn, args.labels, args.labeler)} labels")
        if args.report:
            print(json.dumps(await report(conn, args.k), indent=2, default=str))
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
