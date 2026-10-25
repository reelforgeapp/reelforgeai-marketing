"""
Verify the CreatorDB adapter against the live API.

    python -m scripts.rpi_verify_creatordb --channel UC...                   # /usage (free) + 1 profile (2 credits)
    python -m scripts.rpi_verify_creatordb --channel UC... --probe --yes     # every endpoint once (<= 39 credits)
    python -m scripts.rpi_verify_creatordb --niches ai,video,youtube --yes   # list matching niche ids (1 credit)

The key is read from CREATORDB_API_KEY, or from the file named by
--key-file / CREATORDB_API_KEY_FILE (default ~/.config/creatordb/api_key).

Each call goes through CreatorDBAdapter, so this checks the real request
bodies and field mappings. Output shows response keys, mapped fields found or
missing, and creditsUsed / creditsAvailable. It never prints the key or
contact emails, and saves nothing to disk.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rpi.providers.base import CreatorSearchCriteria  # noqa: E402
from rpi.providers.creatordb import CreatorDBAdapter, topic_label  # noqa: E402

DEFAULT_KEY_FILE = Path.home() / ".config" / "creatordb" / "api_key"
PROBE_CREDITS = {"profile": 2, "performance": 3, "content": 3, "audience": 10, "sponsorship": 5,
                 "contact": 15, "search": 1}


def load_key(key_file: str | None) -> str:
    key = os.environ.get("CREATORDB_API_KEY", "").strip()
    if key:
        return key
    path = Path(key_file or os.environ.get("CREATORDB_API_KEY_FILE") or DEFAULT_KEY_FILE).expanduser()
    if not path.exists():
        sys.exit(f"no key: set CREATORDB_API_KEY or create {path}")
    if path.stat().st_mode & 0o077:
        sys.exit(f"{path} is readable by other users; run: chmod 600 {path}")
    return path.read_text().strip()


class Capture:
    """Keep each raw response so its keys can be compared with the mapping."""

    def __init__(self, adapter: CreatorDBAdapter):
        self.raw: dict[str, dict] = {}
        self._orig = adapter.http.request
        adapter.http.request = self.request

    async def request(self, method, path, **kw):
        response = await self._orig(method, path, **kw)
        try:
            self.raw[path] = response.json()
        except ValueError:
            self.raw[path] = {}
        return response


def describe(name: str, result, raw: dict | None, *, hide_values: bool = False) -> dict:
    data = (raw or {}).get("data")
    out = {
        "endpoint": name,
        "status": result.status.value,
        "error": result.error_code and f"{result.error_code}: {result.error}",
        "credits_used": (raw or {}).get("creditsUsed"),
        "credits_available": (raw or {}).get("creditsAvailable"),
        "response_data_keys": sorted(data)[:60] if isinstance(data, dict) else type(data).__name__,
        "missing_fields": result.missing_fields,
        "unapplied_criteria": result.unapplied_criteria or None,
    }
    value = result.data
    if hide_values:
        out["items"] = len(value or [])
    elif isinstance(value, list):
        out["items"] = len(value)
        out["first_item"] = value[0].model_dump(mode="json", exclude={"provenance"}) if value else None
    elif hasattr(value, "model_dump"):
        out["normalized"] = value.model_dump(mode="json", exclude={"provenance"})
    elif value is not None:
        out["normalized"] = value
    return {k: v for k, v in out.items() if v not in (None, [], "")}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", help="YouTube channel id (UC...) to look up")
    ap.add_argument("--probe", action="store_true", help="call every endpoint once (uses credits)")
    ap.add_argument("--niches", help="comma-separated words; list niche ids containing them (1 credit)")
    ap.add_argument("--yes", action="store_true", help="confirm calls that use credits")
    ap.add_argument("--key-file")
    ap.add_argument("--base-url", default=os.environ.get("CREATORDB_BASE_URL", "https://apiv3.creatordb.app"))
    args = ap.parse_args()
    if (args.probe or args.niches) and not args.yes:
        sys.exit("--probe and --niches use credits; re-run with --yes to confirm")

    adapter = CreatorDBAdapter(load_key(args.key_file), base_url=args.base_url, rate_per_second=2)
    cap = Capture(adapter)
    report = []

    usage = await adapter.get_usage_costs()
    report.append(describe("usage (free)", usage, cap.raw.get("/usage")))
    if not usage.ok:
        print(json.dumps(report, indent=2, default=str))
        sys.exit(1)

    if args.channel:
        calls = [("profile", adapter.get_creator_profile, "/youtube/profile")]
        if args.probe:
            calls += [("performance", adapter.get_creator_performance, "/youtube/performance"),
                      ("content", adapter.get_creator_content, "/youtube/content-detail"),
                      ("audience", adapter.get_audience_demographics, "/youtube/audience"),
                      ("sponsorship", adapter.get_sponsorship_history, "/youtube/sponsorship"),
                      ("contact", adapter.get_creator_contacts, "/youtube/contact")]
        for name, fn, path in calls:
            result = await fn(args.channel)
            report.append(describe(f"{name} (<= {PROBE_CREDITS[name]} credits)", result, cap.raw.get(path),
                                   hide_values=name == "contact"))
    if args.probe:
        result = await adapter.search_creators(CreatorSearchCriteria(
            keywords=["faceless youtube", "youtube automation"], min_subscribers=10_000, max_subscribers=150_000,
            limit=5))
        report.append(describe("search (1 credit)", result, cap.raw.get("/youtube/search")))

    if args.niches:
        words = [w.strip().lower() for w in args.niches.split(",") if w.strip()]
        resp = await adapter.http.request("GET", "/youtube/niches")
        payload = resp.json()
        rows = [n for n in payload.get("data") or [] if isinstance(n, dict)]
        hits = [{"id": n.get("id"), "label": topic_label(n.get("id", "")), "name": n.get("name"),
                 "category": n.get("category"), "channels": n.get("channelCount")}
                for n in rows if any(w in f"{n.get('id', '')} {n.get('name', '')}".lower() for w in words)]
        hits.sort(key=lambda n: -(n["channels"] or 0))
        report.append({"endpoint": "niches (1 credit)", "credits_used": payload.get("creditsUsed"),
                       "total_niches": len(rows), "matches": hits[:80]})

    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())
