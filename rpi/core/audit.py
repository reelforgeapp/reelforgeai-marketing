"""Append-only audit trail (rpi_audit_log)."""

from __future__ import annotations

import json
from typing import Any, Optional

from rpi.core.redaction import redact_mapping


async def audit(conn, actor: str, action: str, entity_type: Optional[str] = None, entity_id: Optional[str] = None,
                detail: Optional[dict[str, Any]] = None) -> None:
    await conn.execute(
        "INSERT INTO rpi_audit_log (actor, action, entity_type, entity_id, detail) VALUES ($1, $2, $3, $4, $5::jsonb)",
        actor, action, entity_type, str(entity_id) if entity_id else None,
        json.dumps(redact_mapping(detail or {}), default=str),
    )
