UPDATE rpi_autopilot_settings
SET presets = ARRAY(SELECT p FROM unnest(presets) WITH ORDINALITY AS t(p, i)
                    WHERE p NOT IN ('english_based_broad', 'english_based_topics') ORDER BY i)
WHERE presets && ARRAY['english_based_broad', 'english_based_topics'];
UPDATE rpi_autopilot_settings SET presets = ARRAY['english_based', 'english_based_small', 'english_based_large']
WHERE cardinality(presets) = 0;
ALTER TABLE rpi_autopilot_settings ALTER COLUMN presets
    SET DEFAULT ARRAY['english_based', 'english_based_small', 'english_based_large'];
DELETE FROM rpi_schema_migrations WHERE version = '013_rpi_wider_presets';
