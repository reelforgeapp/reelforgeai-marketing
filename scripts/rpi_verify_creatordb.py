"""
Verify the CreatorDB adapter against the live API.

    python -m scripts.rpi_verify_creatordb --channel UC83AILrMkQSzeGbN0rJV7JA
    python -m scripts.rpi_verify_creatordb --channel UC... --probe-unverified --yes

Default run: ONE POST /youtube/profile call (the endpoint documented in the
CreatorDB authentication page). It prints the response's top-level keys and
which CREATORDB_FIELDS mappings matched, so the mapping in
rpi/providers/creatordb.py can be corrected and marked verified.

--probe-unverified also calls each provisional endpoint once. These calls may
consume credits; they run only with --yes. Endpoints that return 2xx with the
expected shape can then be listed in CREATORDB_VERIFIED_ENDPOINTS.

Output never includes the API key. Responses are not saved to disk.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rpi.providers.creatordb import CREATORDB_FIELDS, ENDPOINTS  # noqa: E402
from rpi.providers.mapping import extract  # noqa: E402


def summarize(payload) -> dict:
    record = payload.get("data", payload) if isinstance(payload, dict) else payload
    if isinstance(record, list):
        record = record[0] if record else {}
    values, missing = extract(record if isinstance(record, dict) else {}, CREATORDB_FIELDS)
    return {
        "top_level_keys": sorted(payload.keys()) if isinstance(payload, dict) else type(payload).__name__,
        "record_keys": sorted(record.keys())[:60] if isinstance(record, dict) else None,
        "mapped_fields_found": sorted(values),
        "mapped_fields_missing": missing,
    }


async def call(client, base, key, header, endpoint, body):
    spec = ENDPOINTS[endpoint]
    resp = await client.request(spec.method, base + spec.path, json=body, headers={header: key, "content-type": "application/json"})
    out = {"endpoint": endpoint, "path": spec.path, "status": resp.status_code}
    try:
        payload = resp.json()
        out["summary"] = summarize(payload) if resp.status_code < 300 else {"error_body_keys": sorted(payload)[:10] if isinstance(payload, dict) else None}
    except json.JSONDecodeError:
        out["summary"] = {"non_json": resp.text[:120]}
    return out


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", required=True)
    ap.add_argument("--probe-unverified", action="store_true")
    ap.add_argument("--yes", action="store_true", help="confirm calls that may consume credits")
    ap.add_argument("--base-url", default=os.environ.get("CREATORDB_BASE_URL", "https://apiv3.creatordb.app"))
    args = ap.parse_args()
    key = os.environ.get("CREATORDB_API_KEY")
    if not key:
        sys.exit("CREATORDB_API_KEY is not set")
    async with httpx.AsyncClient(timeout=30) as client:
        results = []
        for header in ("api-key", "Authorization"):
            r = await call(client, args.base_url, key if header == "api-key" else f"Bearer {key}", header, "profile",
                           {"channelId": args.channel})
            r["auth_header"] = header
            results.append(r)
            if r["status"] < 300:
                break
        if args.probe_unverified:
            if not args.yes:
                sys.exit("--probe-unverified may consume credits; re-run with --yes to confirm")
            header = results[-1]["auth_header"]
            hv = key if header == "api-key" else f"Bearer {key}"
            for ep in [e for e, s in ENDPOINTS.items() if not s.verified]:
                body = {"channelId": args.channel} if ep != "search" else {"keywords": ["faceless youtube"], "limit": 5, "offset": 0}
                r = await call(client, args.base_url, hv, header, ep, body)
                results.append(r)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
