"""
Role-based API authorization for /rpi.

Principals authenticate with an `X-RPI-Key` header. Keys are random 32-byte
tokens stored only as SHA-256 hashes. Roles are ordered
viewer < analyst < approver < admin; `ingest` is a separate machine role for
the product's server-to-server attribution calls and cannot read data.

A bootstrap admin key (RPI_BOOTSTRAP_ADMIN_KEY) allows creating the first
principals; rotate it out once real keys exist.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
from dataclasses import dataclass
from typing import Optional

ROLE_RANK = {"viewer": 1, "analyst": 2, "approver": 3, "admin": 4}


@dataclass(frozen=True)
class Principal:
    id: Optional[str]
    name: str
    role: str

    def has(self, role: str) -> bool:
        if role == "ingest":
            return self.role in ("ingest", "admin")
        if self.role == "ingest":
            return False
        return ROLE_RANK.get(self.role, 0) >= ROLE_RANK[role]

    @property
    def actor(self) -> str:
        return f"{self.role}:{self.name}"


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def generate_key() -> str:
    return "rpi_" + secrets.token_urlsafe(32)


async def authenticate(conn, key: Optional[str], bootstrap_key: str) -> Optional[Principal]:
    if not key:
        return None
    if bootstrap_key and len(bootstrap_key) >= 24 and hmac.compare_digest(key, bootstrap_key):
        return Principal(None, "bootstrap", "admin")
    row = await conn.fetchrow(
        "UPDATE rpi_principals SET last_used_at = NOW() WHERE key_hash = $1 AND active RETURNING id, name, role",
        hash_key(key),
    )
    if not row:
        return None
    return Principal(str(row["id"]), row["name"], row["role"])


async def create_principal(conn, name: str, role: str) -> tuple[str, str]:
    if role not in (*ROLE_RANK, "ingest"):
        raise ValueError(f"unknown role {role}")
    key = generate_key()
    pid = await conn.fetchval(
        "INSERT INTO rpi_principals (name, role, key_hash, key_prefix) VALUES ($1, $2, $3, $4) RETURNING id",
        name, role, hash_key(key), key[:10],
    )
    return str(pid), key
