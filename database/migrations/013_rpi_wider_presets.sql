-- RPI 013: wider-relevance discovery presets for the autopilot
-- (rpi.discovery.presets english_based_broad and english_based_topics). The three
-- english_based presets had run out by 2026-11-10, after about 89 creators.
-- They go after the presets already in the order, once only: an admin who later
-- changes the order keeps that choice when migrations re-run.
-- A finished preset's cursor now records the day of its last page ("end:YYYY-MM-DD")
-- and is searched again 30 days later (RPI_PRESET_RESEARCH_DAYS). Cursors written
-- before say only "end"; they get today's date, so the new presets are searched
-- first and the finished ones again in a month.
-- Deploy the code first: older code rejects these preset names and would send a
-- dated cursor to CreatorDB as a page offset.
-- (012 is left free for the presenter-format migration still being written.)
ALTER TABLE rpi_autopilot_settings ALTER COLUMN presets
    SET DEFAULT ARRAY['english_based', 'english_based_small', 'english_based_large',
                      'english_based_broad', 'english_based_topics'];
UPDATE rpi_autopilot_settings
SET presets = ARRAY(SELECT p FROM unnest(presets) WITH ORDINALITY AS t(p, i)
                    WHERE p NOT IN ('english_based_broad', 'english_based_topics') ORDER BY i)
    || ARRAY['english_based_broad', 'english_based_topics']
WHERE NOT EXISTS (SELECT 1 FROM rpi_schema_migrations WHERE version = '013_rpi_wider_presets');
UPDATE rpi_autopilot_settings
SET discovery_cursor = (SELECT jsonb_object_agg(k, CASE WHEN v = '"end"'::jsonb
                                                       THEN to_jsonb('end:' || CURRENT_DATE::text) ELSE v END)
                        FROM jsonb_each(discovery_cursor) AS t(k, v))
WHERE EXISTS (SELECT 1 FROM jsonb_each(discovery_cursor) AS t(k, v) WHERE v = '"end"'::jsonb);
INSERT INTO rpi_schema_migrations (version) VALUES ('013_rpi_wider_presets') ON CONFLICT DO NOTHING;
