"""The automatic funnel screens only creators an RPI discovery run qualified."""

from rpi.administration.jobs import StageSizes, _funnel, create_job
from rpi.providers.gateway import CallContext


class RecordingResearch:
    def __init__(self):
        self.screened: list[str] = []

    async def screen(self, creator_id, ctx, actor):
        self.screened.append(str(creator_id))
        return {"qualification": {"recommendation": "hold"}, "notes": []}

    async def deep(self, creator_id, ctx, actor):
        raise AssertionError("no creator should reach deep analysis here")


async def test_funnel_skips_imported_and_unqualified_creators(pool):
    async with pool.acquire() as conn:
        async def creator(name, legacy=False):
            return await conn.fetchval(
                "INSERT INTO rpi_creators (display_name, legacy_prospect_id) VALUES ($1, CASE WHEN $2 THEN gen_random_uuid() END) "
                "RETURNING id", name, legacy)
        imported = await creator("Imported from Partner Scout", legacy=True)
        manual = await creator("Added by hand")
        held_back = await creator("Held back by preliminary screen")
        qualified = await creator("Qualified by discovery")
        run = await conn.fetchval(
            "INSERT INTO rpi_discovery_runs (criteria, strategies, created_by) VALUES ('{}', ARRAY['niche'], 'test') RETURNING id")
        for cid, ok, score in ((held_back, False, 0.4), (qualified, True, 0.9)):
            await conn.execute(
                """INSERT INTO rpi_discovery_candidates (run_id, creator_id, strategy, source_provider, inclusion_reasons,
                       missing_fields, was_duplicate, preliminary_score, preliminary_qualified)
                   VALUES ($1, $2, 'niche', 'creatordb', '[]', '[]', false, $3, $4)""", run, cid, score, ok)
        job_id = await create_job(conn, "funnel", {"screen": 10, "analyze": 5}, "test")

    research = RecordingResearch()
    status, _ = await _funnel(pool, research, CallContext(job_id=job_id), {}, StageSizes(), "test", job_id,
                              {"screen": 10, "analyze": 5})
    assert status == "succeeded"
    assert research.screened == [str(qualified)]
    assert {str(imported), str(manual), str(held_back)}.isdisjoint(research.screened)


async def test_funnel_retries_interrupted_deep_analysis(pool):
    class RecordingDeep(RecordingResearch):
        def __init__(self):
            super().__init__()
            self.deep_ids: list[str] = []

        async def deep(self, creator_id, ctx, actor):
            self.deep_ids.append(str(creator_id))
            return {"qualification": {"recommendation": "recommend"}, "notes": []}

    async with pool.acquire() as conn:
        async def scored(name, status, stage, recommendation):
            cid = await conn.fetchval(
                "INSERT INTO rpi_creators (display_name, pipeline_status) VALUES ($1, $2) RETURNING id", name, status)
            await conn.execute("""INSERT INTO rpi_qualification_scores (creator_id, scoring_version, stage, overall,
                                      recommendation) VALUES ($1, 'test', $2, 0.8, $3)""", cid, stage, recommendation)
            return cid
        waiting = await scored("Screened, waiting", "preliminary_qualified", 2, "recommend")
        interrupted = await scored("Crashed mid-analysis", "deep_analysis", 2, "recommend")  # e.g. Trainer1300
        await scored("Analyzed, not recommended", "deep_analysis", 3, "investigate")
        job_id = await create_job(conn, "funnel", {}, "test")
    research = RecordingDeep()
    status, _ = await _funnel(pool, research, CallContext(job_id=job_id), {}, StageSizes(), "test", job_id,
                              {"screen": 10, "analyze": 5})
    assert status == "succeeded" and sorted(research.deep_ids) == sorted([str(waiting), str(interrupted)])
