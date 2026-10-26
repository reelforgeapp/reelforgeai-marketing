-- RPI 009: conversations other ReelForge systems are having with creators. The
-- partnership bot (reelforge-backend scripts/partnership-bot), which reads and
-- answers the marketing@reelforgeai.io inbox, reports each sender after every
-- run: address, name, channel identifiers and message dates (no content).
-- RPI never cold-emails a creator in such a conversation and marks a creator
-- `replied` when they write in after an RPI email (rpi.crm.external_conversations).
CREATE TABLE IF NOT EXISTS rpi_external_contacts (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    email_normalized TEXT NOT NULL,
    name TEXT,
    is_partner BOOLEAN NOT NULL DEFAULT TRUE,
    handles TEXT[] NOT NULL DEFAULT '{}',      -- lowercase, without '@'
    channel_ids TEXT[] NOT NULL DEFAULT '{}',  -- YouTube channel ids (UC...)
    first_inbound_at TIMESTAMPTZ,
    last_inbound_at TIMESTAMPTZ,
    last_outbound_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source, email_normalized)
);
CREATE INDEX IF NOT EXISTS idx_rpi_external_handles ON rpi_external_contacts USING GIN (handles);
CREATE INDEX IF NOT EXISTS idx_rpi_external_channels ON rpi_external_contacts USING GIN (channel_ids);
INSERT INTO rpi_schema_migrations (version) VALUES ('009_rpi_external_conversations') ON CONFLICT DO NOTHING;
