ALTER TABLE rpi_creator_profile_snapshots DROP COLUMN IF EXISTS total_views;
DELETE FROM rpi_schema_migrations WHERE version = '010_rpi_total_views';
