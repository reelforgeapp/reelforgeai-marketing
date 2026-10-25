"""Assembles the /rpi routers and static dashboard."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from rpi.api import routes_admin, routes_attribution, routes_commercial, routes_research

UI_DIR = Path(__file__).resolve().parent.parent / "ui"


def api_router() -> APIRouter:
    router = APIRouter(prefix="/rpi/api/v1")
    for module in (routes_research, routes_commercial, routes_attribution, routes_admin):
        router.include_router(module.router)
    return router


def mount(app: FastAPI) -> None:
    app.include_router(api_router())
    app.include_router(routes_admin.public, prefix="/rpi/public")
    app.mount("/rpi/ui/static", StaticFiles(directory=UI_DIR / "static"), name="rpi-static")

    @app.get("/rpi", include_in_schema=False)
    @app.get("/rpi/ui", include_in_schema=False)
    async def rpi_ui():
        # The shell contains no data; every data call requires an X-RPI-Key.
        return FileResponse(UI_DIR / "index.html", headers={
            "Content-Security-Policy": "frame-ancestors 'none'",
            "X-Frame-Options": "DENY",
            "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff",
        })
