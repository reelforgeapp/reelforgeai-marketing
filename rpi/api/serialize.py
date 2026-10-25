"""JSON-safe conversion that keeps money exact (Decimal → string)."""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any


def clean(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "model_dump"):
        return clean(value.model_dump(mode="json"))
    if hasattr(value, "items"):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [clean(v) for v in value]
    if hasattr(value, "__dict__"):
        return clean(vars(value))
    return str(value)


def jsonb(value: Any) -> Any:
    """asyncpg returns jsonb as str unless a codec is set; decode defensively."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value
