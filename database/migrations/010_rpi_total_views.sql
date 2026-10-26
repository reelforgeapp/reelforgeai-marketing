-- RPI 010: lifetime channel views on profile snapshots (CreatorDB profile
-- `totalViews`), for the views-below-subscribers risk check
-- (rpi.audience_intelligence.quality). Code tolerates the column missing.
ALTER TABLE rpi_creator_profile_snapshots ADD COLUMN IF NOT EXISTS total_views BIGINT;
INSERT INTO rpi_schema_migrations (version) VALUES ('010_rpi_total_views') ON CONFLICT DO NOTHING;
