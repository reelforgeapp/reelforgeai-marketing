UPDATE rpi_autopilot_settings SET presets = array_remove(array_remove(presets, 'outside_range_small'), 'outside_range_large');
ALTER TABLE rpi_autopilot_settings ALTER COLUMN presets SET DEFAULT ARRAY['quality', 'quality_broad'];
DELETE FROM rpi_schema_migrations WHERE version = '008_rpi_outside_range_presets';
