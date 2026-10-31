UPDATE rpi_autopilot_settings
SET presets = ARRAY['quality', 'quality_broad', 'outside_range_small', 'outside_range_large']
    || ARRAY(SELECT p FROM unnest(presets) WITH ORDINALITY AS t(p, i)
             WHERE p NOT IN ('quality', 'quality_broad', 'outside_range_small', 'outside_range_large',
                             'english_based', 'english_based_small', 'english_based_large')
             ORDER BY i)
WHERE presets && ARRAY['english_based', 'english_based_small', 'english_based_large'];
ALTER TABLE rpi_autopilot_settings ALTER COLUMN presets
    SET DEFAULT ARRAY['quality', 'quality_broad', 'outside_range_small', 'outside_range_large'];
DELETE FROM rpi_schema_migrations WHERE version = '011_rpi_english_based_presets';
