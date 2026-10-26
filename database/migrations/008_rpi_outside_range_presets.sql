-- RPI 008: outside-range discovery presets (rpi.discovery.presets). The autopilot
-- searches them only after the main presets are exhausted, inside the same weekly
-- credit allowance. Existing settings rows get them appended to their search order.
ALTER TABLE rpi_autopilot_settings ALTER COLUMN presets
    SET DEFAULT ARRAY['quality', 'quality_broad', 'outside_range_small', 'outside_range_large'];
UPDATE rpi_autopilot_settings SET presets = presets || ARRAY['outside_range_small', 'outside_range_large']
WHERE NOT presets && ARRAY['outside_range_small', 'outside_range_large']
  -- once only: an admin who later removes them keeps that choice when migrations re-run
  AND NOT EXISTS (SELECT 1 FROM rpi_schema_migrations WHERE version = '008_rpi_outside_range_presets');
INSERT INTO rpi_schema_migrations (version) VALUES ('008_rpi_outside_range_presets') ON CONFLICT DO NOTHING;
