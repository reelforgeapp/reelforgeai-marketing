"""FastAPI dependencies: context, authentication and role checks."""

from __future__ import annotations

from typing import Callable

from fastapi import Depends, Header, HTTPException, Request

from rpi.administration.auth import Principal, authenticate
from rpi.api.context import RPIContext, build_context

_context: RPIContext | None = None


def set_context(ctx: RPIContext | None) -> None:
    global _context
    _context = ctx


async def get_ctx(request: Request) -> RPIContext:
    global _context
    if _context is None:
        from app import database
        from app.config import get_settings

        if database._app_pool is None:
            raise HTTPException(503, "database not initialized")
        _context = build_context(database._app_pool, get_settings())
    return _context


async def get_principal(ctx: RPIContext = Depends(get_ctx), x_rpi_key: str | None = Header(default=None)) -> Principal:
    async with ctx.pool.acquire() as conn:
        principal = await authenticate(conn, x_rpi_key, ctx.settings.rpi_bootstrap_admin_key)
    if principal is None:
        raise HTTPException(401, "missing or invalid X-RPI-Key")
    return principal


def require(role: str) -> Callable:
    async def checker(principal: Principal = Depends(get_principal)) -> Principal:
        if not principal.has(role):
            raise HTTPException(403, f"requires role {role}")
        return principal

    return checker
