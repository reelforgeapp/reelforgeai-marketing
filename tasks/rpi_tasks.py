"""
ReelForge Partner Intelligence - Celery tasks

Scheduled tasks: free, idempotent maintenance (retention, commission accrual,
reconciliation alerts), sending scheduled approved drafts, and the autopilot.
The autopilot is the only scheduled task that can spend provider credits, and
only after an admin enables it with a weekly credit allowance
(rpi.administration.autopilot); with it off, paid research runs only when an
operator starts a job.
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


@celery_app.task(bind=True, max_retries=0, queue='default')
def rpi_send_scheduled(self):
    # No retry: a draft that was not sent stays scheduled (or is cancelled with a
    # reason) and the next run picks it up; sends are exactly-once regardless.
    return asyncio.run(_send_scheduled_async())


async def _send_scheduled_async() -> dict:
    from outreach.brevo_client import BrevoClient
    from rpi.outreach.sender import send_due_drafts

    settings = get_settings()
    if not settings.brevo_api_key or not settings.rpi_public_base_url:
        return {"status": "not_configured"}
    pool = await get_database_async()
    try:
        result = await send_due_drafts(pool, BrevoClient().send_email, public_base_url=settings.rpi_public_base_url)
    finally:
        await pool.close()
    if result.get("sent") or result.get("cancelled"):
        logger.info("RPI scheduled sends", sent=len(result["sent"]), cancelled=len(result["cancelled"]))
    return result


@celery_app.task(bind=True, max_retries=0, queue='discovery', acks_late=True)
def rpi_autopilot(self):
    # No retry: a cycle is safe to rerun (it skips work already done), and the
    # next scheduled run picks up anything this one could not finish.
    return asyncio.run(_autopilot_async())


async def _autopilot_async() -> dict:
    from rpi.administration.autopilot import run_cycle
    from rpi.api.context import build_context

    pool = await get_database_async()
    try:
        ctx = build_context(pool, get_settings())
        return await run_cycle(pool, ctx)
    finally:
        await pool.close()
