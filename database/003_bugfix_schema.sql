-- =============================================================================
-- ReelForge Marketing Engine - Schema Updates for Bug Fixes
-- Run AFTER 001_marketing_tables.sql and 002_v2_schema.sql
-- =============================================================================

-- Add missing columns to email_sends table
ALTER TABLE email_sends ADD COLUMN IF NOT EXISTS open_count INTEGER DEFAULT 0;
ALTER TABLE email_sends ADD COLUMN IF NOT EXISTS click_count INTEGER DEFAULT 0;
ALTER TABLE email_sends ADD COLUMN IF NOT EXISTS last_opened_at TIMESTAMP;
ALTER TABLE email_sends ADD COLUMN IF NOT EXISTS last_clicked_at TIMESTAMP;
ALTER TABLE email_sends ADD COLUMN IF NOT EXISTS unsubscribed_at TIMESTAMP;
ALTER TABLE email_sends ADD COLUMN IF NOT EXISTS complained_at TIMESTAMP;
ALTER TABLE email_sends ADD COLUMN IF NOT EXISTS bounce_type VARCHAR(50);
ALTER TABLE email_sends ADD COLUMN IF NOT EXISTS bounce_reason TEXT;

-- Add missing columns to marketing_prospects table
ALTER TABLE marketing_prospects ADD COLUMN IF NOT EXISTS total_emails_sent INTEGER DEFAULT 0;
ALTER TABLE marketing_prospects ADD COLUMN IF NOT EXISTS total_emails_opened INTEGER DEFAULT 0;
ALTER TABLE marketing_prospects ADD COLUMN IF NOT EXISTS total_emails_clicked INTEGER DEFAULT 0;
ALTER TABLE marketing_prospects ADD COLUMN IF NOT EXISTS reply_sentiment VARCHAR(50);

-- Add index on email_sends.prospect_id for faster queries
CREATE INDEX IF NOT EXISTS idx_sends_prospect ON email_sends(prospect_id);

-- Add index on email_sends.sequence_id
CREATE INDEX IF NOT EXISTS idx_sends_sequence ON email_sends(sequence_id);

-- Add index on email_sends.brevo_message_id for webhook lookups
CREATE INDEX IF NOT EXISTS idx_sends_brevo_message ON email_sends(brevo_message_id);

-- Verify columns exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'email_sends' AND column_name = 'open_count') THEN
        RAISE EXCEPTION 'Migration failed: open_count column not added';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'email_sends' AND column_name = 'click_count') THEN
        RAISE EXCEPTION 'Migration failed: click_count column not added';
    END IF;
    
    RAISE NOTICE 'Schema fix migration completed successfully';
END $$;

-- Log migration
INSERT INTO audit_log (action, details, performed_by, created_at)
VALUES (
    'schema_migration',
    '{"version": "2.1-bugfix", "changes": ["open_count", "click_count", "bounce_columns", "indexes"]}'::jsonb,
    'migration_script',
    NOW()
);
