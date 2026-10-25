DROP INDEX IF EXISTS idx_rpi_drafts_scheduled;
ALTER TABLE rpi_outreach_drafts DROP COLUMN IF EXISTS scheduled_send_at;
DELETE FROM rpi_schema_migrations WHERE version = '006_rpi_scheduled_sends';
