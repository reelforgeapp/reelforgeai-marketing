-- RPI 006: scheduled sends. An approved draft with scheduled_send_at is sent by
-- the worker (tasks.rpi_tasks.rpi_send_scheduled) once that time has passed,
-- inside the configured send window and through every normal send rule.
ALTER TABLE rpi_outreach_drafts ADD COLUMN IF NOT EXISTS scheduled_send_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_rpi_drafts_scheduled
    ON rpi_outreach_drafts (scheduled_send_at) WHERE status = 'approved' AND scheduled_send_at IS NOT NULL;
INSERT INTO rpi_schema_migrations (version) VALUES ('006_rpi_scheduled_sends') ON CONFLICT DO NOTHING;
