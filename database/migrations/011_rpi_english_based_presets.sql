-- RPI 011: the autopilot searches only creators who live in an English-speaking
-- country (rpi.discovery.presets english_based*). The presets it searched before
-- check only the largest audience country, which let through channels with most
-- of their audience elsewhere; they stay available for manual discovery runs.
ALTER TABLE rpi_autopilot_settings ALTER COLUMN presets
    SET DEFAULT ARRAY['english_based', 'english_based_small', 'english_based_large'];
UPDATE rpi_autopilot_settings
SET presets = ARRAY['english_based', 'english_based_small', 'english_based_large']
    || ARRAY(SELECT p FROM unnest(presets) WITH ORDINALITY AS t(p, i)
             WHERE p NOT IN ('quality', 'quality_broad', 'outside_range_small', 'outside_range_large',
                             'english_based', 'english_based_small', 'english_based_large')
             ORDER BY i)
-- once only: an admin who later changes the order keeps that choice when migrations re-run
WHERE NOT EXISTS (SELECT 1 FROM rpi_schema_migrations WHERE version = '011_rpi_english_based_presets');
INSERT INTO rpi_schema_migrations (version) VALUES ('011_rpi_english_based_presets') ON CONFLICT DO NOTHING;
