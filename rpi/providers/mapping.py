"""
Declarative response mapping helpers.

Provider field names live in one spec per provider, each entry flagged
verified/unverified, so a schema change or an unverified assumption is a
one-line fix instead of a hunt through business logic. Missing fields are
reported, never filled in.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass(frozen=True)
class FieldSpec:
    path: str  # dotted path into the provider record, e.g. "stats.subscribers"
    kind: str = "str"  # str | int | float | ratio | percent | datetime | list | bool
    verified: bool = False
    critical: bool = False


@dataclass(frozen=True)
class EndpointSpec:
    method: str
    path: str
    verified: bool
    cost_units: float  # documented credits per call (0 if unknown -> estimate from config)
    cost_verified: bool = False
    notes: str = ""


def get_path(record: Any, path: str) -> Any:
    cur = record
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list) and part.isdigit():
            idx = int(part)
            cur = cur[idx] if idx < len(cur) else None
        else:
            return None
        if cur is None:
            return None
    return cur


def coerce(value: Any, kind: str) -> Any:
    if value is None:
        return None
    try:
        if kind == "int":
            return int(float(value))
        if kind == "float":
            return float(value)
        if kind == "ratio":  # provider already returns a 0..1 fraction
            v = float(value)
            return v if 0 <= v <= 1 else None
        if kind == "percent":  # provider returns 0..100; stored as a fraction
            v = float(value)
            return v / 100 if 0 <= v <= 100 else None
        if kind == "datetime":
            if isinstance(value, (int, float)):
                # epoch seconds or milliseconds
                ts = value / 1000 if value > 10_000_000_000 else value
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            text = str(value).replace("Z", "+00:00")
            dt = datetime.fromisoformat(text)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        if kind == "list":
            return list(value) if isinstance(value, (list, tuple)) else [value]
        if kind == "bool":
            return bool(value)
        return str(value)
    except (TypeError, ValueError):
        return None


def extract(record: dict, spec: dict[str, FieldSpec]) -> tuple[dict[str, Any], list[str]]:
    """Return (values, missing_field_names)."""
    values: dict[str, Any] = {}
    missing: list[str] = []
    for name, fs in spec.items():
        v = coerce(get_path(record, fs.path), fs.kind)
        if v is None or v == "":
            missing.append(name)
        else:
            values[name] = v
    return values, missing


def critical_missing(spec: dict[str, FieldSpec], missing: list[str]) -> list[str]:
    return [m for m in missing if spec[m].critical]


def unverified_fields(spec: dict[str, FieldSpec]) -> list[str]:
    return [k for k, v in spec.items() if not v.verified]


def first_list(payload: Any, *candidate_paths: str) -> Optional[list]:
    for p in candidate_paths:
        v = get_path(payload, p) if p else payload
        if isinstance(v, list):
            return v
    return None
