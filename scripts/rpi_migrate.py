"""
Apply the RPI schema and seed reference data (idempotent).

    python -m scripts.rpi_migrate                 # uses DATABASE_URL
    python -m scripts.rpi_migrate --down          # rollback (destructive; asks to confirm)

Seeds the competitor registry from rpi.sponsorship_intelligence.detection
and imports existing Partner Scout YouTube prospects as RPI creators
(status 'discovered', legacy_prospect_id set) so the two systems share one
identity per channel. Import copies only ids/handles/names, not emails.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import asyncpg

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rpi.sponsorship_intelligence.detection import DEFAULT_COMPETITORS  # noqa: E402

MIGRATIONS = ROOT / "database" / "migrations"
# Applied in order; each file is idempotent.
UP = [MIGRATIONS / "005_rpi_core.sql", MIGRATIONS / "006_rpi_scheduled_sends.sql", MIGRATIONS / "007_rpi_autopilot.sql"]
DOWN = [MIGRATIONS / "007_rpi_autopilot_down.sql", MIGRATIONS / "006_rpi_scheduled_sends_down.sql",
        MIGRATIONS / "005_rpi_core_down.sql"]


async def seed(conn) -> dict:
    n = 0
    for c in DEFAULT_COMPETITORS:
        res = await conn.execute(
            """INSERT INTO rpi_competitors (name, domains, aliases, category, overlap) VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (name) DO NOTHING""",
            c.name, list(c.domains), list(c.aliases), c.category, c.overlap)
        n += res.endswith("1")
    imported = 0
    if await conn.fetchval("SELECT to_regclass('public.marketing_prospects') IS NOT NULL"):
        rows = await conn.fetch(
            """SELECT id, youtube_channel_id, youtube_handle, full_name FROM marketing_prospects
               WHERE youtube_channel_id LIKE 'UC%'
                 AND NOT EXISTS (SELECT 1 FROM rpi_creator_social_accounts a
                                 WHERE a.platform = 'youtube' AND a.platform_account_id = marketing_prospects.youtube_channel_id)""")
        for r in rows:
            async with conn.transaction():
                cid = await conn.fetchval(
                    "INSERT INTO rpi_creators (display_name, legacy_prospect_id) VALUES ($1, $2) RETURNING id",
                    r["full_name"], r["id"])
                await conn.execute(
                    """INSERT INTO rpi_creator_social_accounts (creator_id, platform, platform_account_id, handle, handle_normalized, url)
                       VALUES ($1, 'youtube', $2, $3, lower(ltrim($3, '@')), 'https://www.youtube.com/channel/' || $2)
                       ON CONFLICT DO NOTHING""",
                    cid, r["youtube_channel_id"], r["youtube_handle"])
                await conn.execute(
                    "INSERT INTO rpi_pipeline_events (creator_id, to_status, actor, reason) VALUES ($1, 'discovered', 'migration', 'imported from Partner Scout')",
                    cid)
            imported += 1
    return {"competitors_added": n, "partner_scout_imported": imported}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--down", action="store_true")
    ap.add_argument("--no-import", action="store_true", help="skip importing Partner Scout prospects")
    ap.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    args = ap.parse_args()
    if not args.database_url:
        sys.exit("DATABASE_URL is required")
    conn = await asyncpg.connect(args.database_url)
    try:
        if args.down:
            if input("This drops every rpi_* table, including the ledger. Type DROP RPI to continue: ") != "DROP RPI":
                sys.exit("aborted")
            for path in DOWN:
                await conn.execute(path.read_text())
            print("rolled back", ", ".join(p.stem.removesuffix("_down") for p in DOWN))
            return
        for path in UP:
            await conn.execute(path.read_text())
        result = await seed(conn) if not args.no_import else {"seeded": "skipped"}
        print("applied", ", ".join(p.stem for p in UP), result)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
