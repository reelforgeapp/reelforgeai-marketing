-- RPI 007: autopilot. One settings row per workspace for the unattended
-- research-to-send loop (rpi.administration.autopilot). Everything is off and
-- the weekly CreatorDB credit allowance is 0 until an admin sets them.
CREATE TABLE IF NOT EXISTS rpi_autopilot_settings (
    workspace_id UUID PRIMARY KEY DEFAULT '00000000-0000-0000-0000-000000000001',
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    auto_approve BOOLEAN NOT NULL DEFAULT FALSE,
    weekly_credit_cap INTEGER NOT NULL DEFAULT 0 CHECK (weekly_credit_cap BETWEEN 0 AND 100000),
    target_queue INTEGER NOT NULL DEFAULT 5 CHECK (target_queue BETWEEN 0 AND 100),
    send_time TEXT NOT NULL DEFAULT '11:00',
    discover_per_run INTEGER NOT NULL DEFAULT 25 CHECK (discover_per_run BETWEEN 1 AND 100),
    screen_per_run INTEGER NOT NULL DEFAULT 10 CHECK (screen_per_run BETWEEN 0 AND 100),
    analyze_per_run INTEGER NOT NULL DEFAULT 5 CHECK (analyze_per_run BETWEEN 0 AND 50),
    presets TEXT[] NOT NULL DEFAULT ARRAY['quality', 'quality_broad'],
    -- Next CreatorDB search offset per preset; 'end' once a preset is exhausted.
    discovery_cursor JSONB NOT NULL DEFAULT '{}',
    updated_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run_at TIMESTAMPTZ,
    last_run JSONB NOT NULL DEFAULT '{}'
);
INSERT INTO rpi_autopilot_settings (workspace_id) VALUES ('00000000-0000-0000-0000-000000000001')
ON CONFLICT DO NOTHING;
INSERT INTO rpi_schema_migrations (version) VALUES ('007_rpi_autopilot') ON CONFLICT DO NOTHING;
