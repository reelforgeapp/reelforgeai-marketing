"""
RPI autopilot: the unattended research-to-send loop.

Runs once a day (Celery beat `rpi-autopilot`) and on demand, and only while an
admin has it enabled in rpi_autopilot_settings. Each cycle:

1. Drafts outreach (the deterministic template, free) for every recommended
   creator with a stored email who has never had an RPI draft.
2. Approves those drafts when auto-approval is on and they pass validation.
3. Schedules approved, unscheduled drafts into the next free days at
   `send_time`, at most `daily_send_limit` a day.
4. When fewer than `target_queue` messages are waiting, researches more
   creators: one page of the current discovery preset (continuing where the
   last page ended), then screening and deep analysis, as an `autopilot` job.
   The provider gateway caps that job's CreatorDB credits at what is left of
   `weekly_credit_cap` over the last 7 days; USD budgets still apply as usual.
5. Repeats steps 1-3 for creators the research just recommended.

Sending stays with tasks.rpi_tasks.rpi_send_scheduled and every send rule
(sending enabled, daily limit, suppression, contact spacing, CAN-SPAM,
re-validation, exactly-once). The autopilot never reads or answers replies,
never sends follow-ups and never negotiates: every creator gets at most one
RPI email from it, and replies are handled by a person.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time as dtime, timedelta, timezone
from decimal import Decimal
from typing import Optional
from zoneinfo import ZoneInfo

import structlog

from rpi.administration.jobs import FAILED, create_job, run_job
from rpi.api.serialize import jsonb
from rpi.core.audit import audit
from rpi.crm.contact_history import partner_scout_history
from rpi.crm.external_conversations import conversation as external_conversation
from rpi.crm.suppression import is_suppressed
from rpi.outreach import service as outreach
from rpi.outreach.drafts import draft_key
from rpi.outreach.sender import AUTOPILOT_ACTOR
from rpi.outreach.service import USABLE_EMAIL_SQL, OutreachRefused

logger = structlog.get_logger()

# pg_advisory_lock key: one cycle at a time across web and worker.
AUTOPILOT_LOCK_KEY = 7_340_118_222
# Several sends on one day go out this far apart, starting at send_time.
SLOT_SPACING = timedelta(minutes=20)
# Smallest allowance worth starting research with (one screening call).
MIN_RESEARCH_CREDITS = 3
CURSOR_END = "end"
# A scheduled draft still unsent this long after its time was held back (daily
# limit, sending off) and is given a new day before new drafts are placed.
OVERDUE_AFTER = timedelta(minutes=30)


def window_bounds(window: str) -> tuple[dtime, dtime]:
    start, end = (dtime.fromisoformat(part.strip()) for part in window.split("-"))
    return start, end


def next_slots(count: int, *, now: datetime, taken: dict[date, int], per_day: int, tz: str, send_time: dtime,
               window_end: dtime, weekdays_only: bool, horizon_days: int = 120) -> list[datetime]:
    """The next `count` free send times (UTC): `send_time` on each allowed day,
    then every SLOT_SPACING after it up to the window end, skipping days that
    already have `per_day` messages sent or scheduled and days whose
    send_time has passed."""
    zone = ZoneInfo(tz)
    local_now = now.astimezone(zone)
    out: list[datetime] = []
    day = local_now.date()
    for _ in range(horizon_days):
        if len(out) >= count or per_day <= 0:
            break
        base = datetime.combine(day, send_time, tzinfo=zone)
        if (not weekdays_only or day.weekday() < 5) and base > local_now:
            used = taken.get(day, 0)
            while used < per_day and len(out) < count:
                slot = base + SLOT_SPACING * used
                if slot.time() > window_end or slot.date() != day:
                    break
                out.append(slot.astimezone(timezone.utc))
                used += 1
        day += timedelta(days=1)
    return out


def current_preset(row: dict) -> tuple[Optional[str], Optional[str]]:
    """(preset, page token) to search next; (None, None) once every preset is exhausted."""
    cursor = jsonb(row["discovery_cursor"]) or {}
    for preset in row["presets"] or []:
        token = cursor.get(preset)
        if token != CURSOR_END:
            return preset, token
    return None, None


def provider_name(ctx) -> str:
    primary = ctx.discovery.primary()
    return primary.name if primary else "creatordb"


async def load_settings(conn) -> dict:
    return dict(await conn.fetchrow("SELECT * FROM rpi_autopilot_settings LIMIT 1"))


async def credits_used(conn, provider: str, now: datetime) -> Decimal:
    """Credits the autopilot's own research jobs committed in the last 7 days."""
    v = await conn.fetchval(
        """SELECT COALESCE(SUM(l.actual_units), 0) FROM rpi_spend_ledger l
           JOIN rpi_research_jobs j ON j.id = l.job_id AND j.job_type = 'autopilot'
           WHERE l.provider = $1 AND l.status = 'committed' AND l.created_at > $2""",
        provider, now - timedelta(days=7))
    return Decimal(v)


async def queue_depth(conn) -> int:
    """Messages waiting to go out: scheduled approved drafts, plus autopilot
    drafts still waiting for a person's approval."""
    return await conn.fetchval(
        """SELECT COUNT(*) FROM rpi_outreach_drafts
           WHERE (status = 'approved' AND scheduled_send_at IS NOT NULL)
              OR (status = 'draft' AND created_by = $1 AND (validation->>'ok')::boolean)""", AUTOPILOT_ACTOR)


async def _alert(conn, message: str, key: str, now: datetime, severity: str = "warning") -> None:
    await conn.execute(
        """INSERT INTO rpi_alerts (kind, severity, message, dedupe_key) VALUES ('autopilot', $1, $2, $3)
           ON CONFLICT (dedupe_key) DO NOTHING""", severity, message, f"autopilot:{key}:{now.date().isoformat()}")


# --------------------------------------------------------------------------
# Free steps: draft, approve, schedule
# --------------------------------------------------------------------------


async def _draft_new(conn, ctx, now: datetime, summary: dict) -> None:
    creators = await conn.fetch(
        """SELECT c.id, c.display_name FROM rpi_creators c
           WHERE c.pipeline_status IN ('recommended', 'contact_identified')
             AND c.disqualified_reason IS NULL
             AND (SELECT q.recommendation FROM rpi_qualification_scores q WHERE q.creator_id = c.id
                  ORDER BY q.computed_at DESC LIMIT 1) = 'recommend'
             AND EXISTS (SELECT 1 FROM rpi_creator_contacts k WHERE k.creator_id = c.id AND k.contact_type = 'email'
                         AND COALESCE(k.verification_status, '') NOT IN ('invalid', 'undeliverable'))
             AND NOT EXISTS (SELECT 1 FROM rpi_outreach_drafts d WHERE d.creator_id = c.id)
           ORDER BY c.updated_at""")
    days = await conn.fetchval("SELECT min_days_between_contacts FROM rpi_outreach_settings LIMIT 1") or 30
    for c in creators:
        name = c["display_name"] or str(c["id"])
        contact = await conn.fetchrow(USABLE_EMAIL_SQL, c["id"])
        email = contact["value"]
        reason = await is_suppressed(conn, email=email, creator_id=c["id"])
        if reason:
            summary["skipped"].append(f"{name}: {reason}")
            continue
        talking = await external_conversation(conn, c["id"], email=email, now=now,
                                              days=ctx.settings.rpi_external_conversation_days)
        if talking:
            summary["skipped"].append(f"{name}: {talking.summary()}")
            continue
        prior = await partner_scout_history(conn, c["id"], email=email)
        if prior and prior.last_contacted_at and prior.last_contacted_at > now - timedelta(days=days):
            summary["skipped"].append(f"{name}: Partner Scout emailed them on {prior.last_contacted_at.date()}; "
                                      f"waiting for the {days}-day spacing")
            continue
        try:
            # The template, not Claude: the same wording a person reviewed, and
            # no generated text goes out unreviewed.
            draft = await outreach.create_draft(conn, c["id"], program=ctx.program, llm=None, price=None,
                                                actor=AUTOPILOT_ACTOR, contact_id=str(contact["id"]))
        except OutreachRefused as e:
            summary["skipped"].append(f"{name}: {e.detail}")
            continue
        summary["drafted"].append(name)
        if not draft["validation"]["ok"]:
            summary["skipped"].append(f"{name}: draft fails validation and waits for a person: "
                                      + "; ".join(draft["validation"].get("errors", [])[:2]))


async def _approve_drafts(conn, summary: dict) -> None:
    drafts = await conn.fetch(
        """SELECT d.id, d.creator_id, d.subject, d.body_text, d.idempotency_key, c.display_name
           FROM rpi_outreach_drafts d JOIN rpi_creators c ON c.id = d.creator_id
           WHERE d.status = 'draft' AND d.created_by = $1 AND d.generator = 'template'
             AND (d.validation->>'ok')::boolean AND d.contact_id IS NOT NULL
           ORDER BY d.created_at""", AUTOPILOT_ACTOR)
    for d in drafts:
        if d["idempotency_key"] != draft_key(d["creator_id"], d["subject"], d["body_text"]):
            # A person edited it: only an approver may approve their text.
            summary["skipped"].append(f"{d['display_name']}: edited by a person; waits for an approver")
            continue
        try:
            await outreach.approve(conn, d["id"], AUTOPILOT_ACTOR)
            summary["approved"].append(d["display_name"])
        except OutreachRefused as e:
            summary["skipped"].append(f"{d['display_name']}: not approved: {e.detail}")


async def _schedule(conn, ctx, now: datetime, summary: dict, row: dict) -> None:
    """Give approved drafts a send day: first those held back past their time
    (oldest first), then unscheduled ones in approval order. A day is full when
    its sent messages (by sent_at) plus its scheduled ones reach the daily limit."""
    s = ctx.settings
    drafts = await conn.fetch(
        """SELECT d.id, c.display_name, d.scheduled_send_at FROM rpi_outreach_drafts d
           JOIN rpi_creators c ON c.id = d.creator_id
           WHERE d.status = 'approved' AND d.failure_reason IS NULL
             AND (d.scheduled_send_at IS NULL OR d.scheduled_send_at < $1)
           ORDER BY d.scheduled_send_at NULLS LAST, d.approved_at NULLS LAST, d.created_at""", now - OVERDUE_AFTER)
    if not drafts:
        return
    per_day = await conn.fetchval("SELECT daily_send_limit FROM rpi_outreach_settings LIMIT 1") or 0
    taken = {r["day"]: r["n"] for r in await conn.fetch(
        """SELECT day, COUNT(*) AS n FROM (
               SELECT (COALESCE(sent_at, $3) AT TIME ZONE $1)::date AS day FROM rpi_outreach_drafts
                WHERE status IN ('sent', 'sending') AND COALESCE(sent_at, $3) > $2
               UNION ALL
               SELECT (scheduled_send_at AT TIME ZONE $1)::date FROM rpi_outreach_drafts
                WHERE status = 'approved' AND scheduled_send_at >= $4) t
           GROUP BY day""", s.rpi_send_timezone, now - timedelta(days=2), now, now - OVERDUE_AFTER)}
    _, window_end = window_bounds(s.rpi_send_window)
    slots = next_slots(len(drafts), now=now, taken=taken, per_day=per_day, tz=s.rpi_send_timezone,
                       send_time=dtime.fromisoformat(row["send_time"]), window_end=window_end,
                       weekdays_only=s.rpi_send_weekdays_only)
    for d, slot in zip(drafts, slots):
        await conn.execute("UPDATE rpi_outreach_drafts SET scheduled_send_at = $2 WHERE id = $1 AND status = 'approved'",
                           d["id"], slot)
        held = d["scheduled_send_at"]
        await audit(conn, AUTOPILOT_ACTOR, "outreach.rescheduled" if held else "outreach.scheduled", "outreach_draft",
                    d["id"], {"send_at": slot.isoformat(), **({"was": held.isoformat()} if held else {})})
        summary["scheduled"].append({"creator": d["display_name"], "send_at": slot.isoformat(),
                                     **({"held_since": held.isoformat()} if held else {})})
    if len(slots) < len(drafts):
        summary["skipped"].append(f"{len(drafts) - len(slots)} approved draft(s) not scheduled: no free send day "
                                  "(daily send limit is 0?)")


async def _prepare(pool, ctx, row: dict, now: datetime, summary: dict) -> None:
    async with pool.acquire() as conn:
        await _draft_new(conn, ctx, now, summary)
        if row["auto_approve"]:
            await _approve_drafts(conn, summary)
        await _schedule(conn, ctx, now, summary, row)


# --------------------------------------------------------------------------
# Paid step: research
# --------------------------------------------------------------------------


async def _research(pool, ctx, row: dict, now: datetime, summary: dict) -> None:
    provider = provider_name(ctx)
    async with pool.acquire() as conn:
        queued = await queue_depth(conn)
        used = await credits_used(conn, provider, now)
    allowance = Decimal(row["weekly_credit_cap"]) - used
    summary["queue"] = queued
    summary["credits_used_7d"] = str(used)
    if queued >= row["target_queue"]:
        summary["research"] = f"not needed: {queued} message(s) waiting (target {row['target_queue']})"
        return
    if row["weekly_credit_cap"] <= 0:
        summary["research"] = "off: weekly credit allowance is 0"
        return
    if allowance < MIN_RESEARCH_CREDITS:
        summary["research"] = f"paused: weekly allowance of {row['weekly_credit_cap']} credits used ({used} in 7 days)"
        async with pool.acquire() as conn:
            await _alert(conn, f"Autopilot research paused: the weekly allowance of {row['weekly_credit_cap']} CreatorDB "
                               f"credits is used. It resumes as credits age out of the 7-day window.", "allowance", now)
        return
    preset, token = current_preset(row)
    params = {"unit_caps": {provider: str(allowance)}, "preset": preset, "page_token": token,
              "discover": row["discover_per_run"], "screen": row["screen_per_run"], "analyze": row["analyze_per_run"]}
    async with pool.acquire() as conn:
        job_id = await create_job(conn, "autopilot", params, AUTOPILOT_ACTOR,
                                  budget_usd=Decimal(str(ctx.settings.rpi_default_job_budget_usd)))
    try:
        result = await run_job(pool, job_id, discovery=ctx.discovery, research=ctx.research, stages=ctx.stages,
                               autopilot=True)
    except Exception as exc:  # recorded on the job; the free steps still run
        summary["research"] = f"job {job_id} failed: {str(exc)[:200]}"
        async with pool.acquire() as conn:
            # Never resumed: its cap belonged to this cycle. Keep the page it already searched.
            await conn.execute("UPDATE rpi_research_jobs SET status = 'canceled', finished_at = NOW() "
                               "WHERE id = $1 AND status = 'failed'", job_id)
            saved = jsonb(await conn.fetchval("SELECT progress FROM rpi_research_jobs WHERE id = $1", job_id)) or {}
            await _advance_cursor(conn, row, preset, saved.get("discovery") or {}, now)
            await _alert(conn, f"Autopilot research job failed: {str(exc)[:200]}", "failed", now)
        return
    progress = result.get("progress") or {}
    disc = progress.get("discovery") or {}
    async with pool.acquire() as conn:
        await _advance_cursor(conn, row, preset, disc, now)
        spent = await credits_used(conn, provider, now)
    summary["research"] = {
        "job_id": job_id, "status": result.get("status"), "preset": preset, "page_token": token,
        "discovery": {k: disc.get(k) for k in ("skipped", "total_found", "new_creators", "preliminary_qualified", "non_english")
                      if k in disc},
        "screened": len((progress.get("screen") or {}).get("done", [])),
        "deep_analyzed": len((progress.get("deep") or {}).get("done", [])),
        "contacts_retried": len((progress.get("contacts") or {}).get("done", [])),
        "recommended": sum(1 for v in (progress.get("deep") or {}).get("results", {}).values() if v == "recommend"),
        "credits": str(spent - used), "stopped": progress.get("stopped"),
    }
    summary["credits_used_7d"] = str(spent)
    failed = [f"{cid}: {v}" for stage in ("screen", "deep", "contacts")
              for cid, v in ((progress.get(stage) or {}).get("results") or {}).items() if str(v).startswith(FAILED)]
    if failed:
        summary["research"]["failed"] = failed[:10]
        async with pool.acquire() as conn:
            await _alert(conn, f"Autopilot research skipped {len(failed)} creator(s) after an error ({failed[0][:160]}); "
                               "deep analysis retries them on the next run.", "creator_failed", now)


async def _advance_cursor(conn, row: dict, preset: Optional[str], disc: dict, now: datetime) -> None:
    """Continue after the page a search returned; a search that failed leaves the cursor alone."""
    if not (disc.get("searched") and preset):
        return
    cursor = jsonb(row["discovery_cursor"]) or {}
    cursor[preset] = disc.get("next_page_token") or CURSOR_END
    await conn.execute("UPDATE rpi_autopilot_settings SET discovery_cursor = $1::jsonb", json.dumps(cursor))
    row["discovery_cursor"] = cursor
    if current_preset(row)[0] is None:
        await _alert(conn, "Autopilot has searched every page of its discovery presets. Add a preset or "
                           "widen the criteria to find new creators.", "exhausted", now)


# --------------------------------------------------------------------------
# Cycle and status
# --------------------------------------------------------------------------


async def run_cycle(pool, ctx, *, now: Optional[datetime] = None) -> dict:
    """One autopilot cycle; returns (and stores) a summary of what it did."""
    now = now or datetime.now(timezone.utc)
    async with pool.acquire() as lock:
        if not await lock.fetchval("SELECT pg_try_advisory_lock($1)", AUTOPILOT_LOCK_KEY):
            return {"status": "already_running"}
        try:
            row = await load_settings(lock)
            if not row["enabled"]:
                return {"status": "disabled"}
            summary: dict = {"status": "ok", "at": now.isoformat(), "drafted": [], "approved": [], "scheduled": [],
                             "skipped": []}
            await _prepare(pool, ctx, row, now, summary)
            await _research(pool, ctx, row, now, summary)
            if isinstance(summary.get("research"), dict):
                row = await load_settings(lock)
                if row["enabled"]:  # not turned off while research ran
                    await _prepare(pool, ctx, row, now, summary)
            await lock.execute("UPDATE rpi_autopilot_settings SET last_run_at = $1, last_run = $2::jsonb", now,
                               json.dumps(summary, default=str))
            await audit(lock, AUTOPILOT_ACTOR, "autopilot.run", "autopilot", None,
                        {k: summary[k] for k in ("drafted", "approved", "scheduled", "research") if k in summary})
            logger.info("rpi_autopilot", drafted=len(summary["drafted"]), approved=len(summary["approved"]),
                        scheduled=len(summary["scheduled"]), research=str(summary.get("research"))[:200])
            return summary
        finally:
            await lock.execute("SELECT pg_advisory_unlock($1)", AUTOPILOT_LOCK_KEY)


async def status(conn, ctx, now: Optional[datetime] = None) -> dict:
    now = now or datetime.now(timezone.utc)
    row = await load_settings(conn)
    provider = provider_name(ctx)
    used = await credits_used(conn, provider, now)
    preset, token = current_preset(row)
    upcoming = await conn.fetch(
        """SELECT d.id, c.display_name, d.scheduled_send_at, d.approved_by FROM rpi_outreach_drafts d
           JOIN rpi_creators c ON c.id = d.creator_id
           WHERE d.status = 'approved' AND d.scheduled_send_at IS NOT NULL ORDER BY d.scheduled_send_at LIMIT 20""")
    from rpi.administration.jobs import SCREEN_QUEUE_SQL

    return {
        **{k: row[k] for k in ("enabled", "auto_approve", "weekly_credit_cap", "target_queue", "send_time",
                               "discover_per_run", "screen_per_run", "analyze_per_run", "presets", "updated_by",
                               "updated_at", "last_run_at")},
        "discovery_cursor": jsonb(row["discovery_cursor"]) or {},
        "last_run": jsonb(row["last_run"]) or {},
        "provider": provider,
        "credits_used_7d": str(used),
        "credits_left_7d": str(max(Decimal(row["weekly_credit_cap"]) - used, Decimal(0))),
        "queue": await queue_depth(conn),
        "next_search": {"preset": preset, "page_token": token} if preset else None,
        "candidates_waiting": await conn.fetchval(f"SELECT COUNT(*) {SCREEN_QUEUE_SQL}"),
        "upcoming": [{"draft_id": str(r["id"]), "creator": r["display_name"], "send_at": r["scheduled_send_at"],
                      "approved_by": r["approved_by"]} for r in upcoming],
        "send_window": {"timezone": ctx.settings.rpi_send_timezone, "window": ctx.settings.rpi_send_window,
                        "weekdays_only": ctx.settings.rpi_send_weekdays_only},
    }
