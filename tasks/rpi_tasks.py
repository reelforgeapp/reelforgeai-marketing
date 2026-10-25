"""
ReelForge Partner Intelligence - Celery tasks

Scheduled tasks are limited to free, idempotent maintenance (retention,
commission accrual, reconciliation alerts). Paid research runs only when an
operator starts a job; nothing on the beat schedule spends provider credits.
"""
import sys
sys.path.insert(0, '/app')

import asyncio
from datetime import date

import structlog

from celery_config import celery_app
from app.config import get_settings
from app.database import get_database_async

logger = structlog.get_logger()


@celery_app.task(bind=True, max_retries=0, queue='discovery', acks_late=True)
def run_rpi_job(self, job_id: str):
    # No automatic retry: jobs are resumable from their recorded progress, and a
    # blind retry could repeat paid work. Resume via POST /rpi/api/v1/jobs/{id}/resume.
    return asyncio.run(_run_job_async(job_id))


async def _run_job_async(job_id: str) -> dict:
    from rpi.administration.jobs import run_job
    from rpi.api.context import build_context

    pool = await get_database_async()
    try:
        ctx = build_context(pool, get_settings())
        return await run_job(pool, job_id, discovery=ctx.discovery, research=ctx.research, stages=ctx.stages)
    finally:
        await pool.close()


@celery_app.task(bind=True, max_retries=2, queue='default')
def rpi_enforce_retention(self):
    return asyncio.run(_retention_async())


async def _retention_async() -> dict:
    from rpi.compliance.retention import enforce

    pool = await get_database_async()
    try:
        async with pool.acquire() as conn:
            return await enforce(conn)
    finally:
        await pool.close()


@celery_app.task(bind=True, max_retries=2, queue='default')
def rpi_daily_ledger_checks(self):
    return asyncio.run(_ledger_checks_async())


async def _ledger_checks_async() -> dict:
    from rpi.financial_ledger.ledger import accrue_commissions
    from rpi.financial_ledger.reports import reconciliation

    pool = await get_database_async()
    try:
        async with pool.acquire() as conn:
            accrued = await accrue_commissions(conn)
            recon = await reconciliation(conn)
            if not recon["ok"]:
                failed = [k for k, v in recon["checks"].items() if not v]
                await conn.execute(
                    """INSERT INTO rpi_alerts (kind, severity, message, dedupe_key)
                       VALUES ('reconciliation', 'warning', $1, $2) ON CONFLICT (dedupe_key) DO NOTHING""",
                    "Ledger reconciliation checks failing: " + ", ".join(failed),
                    "recon:" + ",".join(failed) + ":" + date.today().isoformat(),
                )
            logger.info("rpi_ledger_checks", accrued=accrued, ok=recon["ok"])
            return {"commissions_accrued": accrued, "reconciliation_ok": recon["ok"]}
    finally:
        await pool.close()
