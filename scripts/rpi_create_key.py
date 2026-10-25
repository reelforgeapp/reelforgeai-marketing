"""
Create an RPI API principal and print its key once.

    python -m scripts.rpi_create_key --name larry --role admin
    python -m scripts.rpi_create_key --name reelforge-backend --role ingest
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rpi.administration.auth import create_principal  # noqa: E402


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--role", required=True, choices=["viewer", "analyst", "approver", "admin", "ingest"])
    ap.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    args = ap.parse_args()
    conn = await asyncpg.connect(args.database_url)
    try:
        pid, key = await create_principal(conn, args.name, args.role)
        await conn.execute(
            "INSERT INTO rpi_audit_log (actor, action, entity_type, entity_id, detail) VALUES ('cli', 'principal.created', 'principal', $1, $2::jsonb)",
            pid, f'{{"name": "{args.name}", "role": "{args.role}"}}')
    finally:
        await conn.close()
    print(f"principal {pid} ({args.role}) created. Key (shown once, store it in a password manager):\n{key}")


if __name__ == "__main__":
    asyncio.run(main())
