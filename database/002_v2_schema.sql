-- =============================================================================
-- ReelForge Marketing Engine - Database Migration v2.0
-- Run after 001_marketing_tables.sql
-- =============================================================================

-- Enable UUID extension if not exists
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- NEW TABLES
-- =============================================================================

-- Idempotency keys for preventing duplicate email sends
CREATE TABLE IF NOT EXISTS idempotency_keys (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    key             VARCHAR(255) UNIQUE NOT NULL,
    status          VARCHAR(50) DEFAULT 'completed',  -- processing, completed, failed
    created_at      TIMESTAMP DEFAULT NOW(),
    expires_at      TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_idem_key ON idempotency_keys(key);
CREATE INDEX IF NOT EXISTS idx_idem_expires ON idempotency_keys(expires_at);

COMMENT ON TABLE idempotency_keys IS 'Prevents duplicate email sends across retries';


-- GDPR consent log
CREATE TABLE IF NOT EXISTS consent_log (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    prospect_id     UUID REFERENCES marketing_prospects(id) ON DELETE CASCADE,
    consent_type    VARCHAR(50) NOT NULL,  -- legitimate_interest, explicit_consent
    consent_text    TEXT NOT NULL,
    ip_address      INET,
    source          VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_consent_prospect ON consent_log(prospect_id);

COMMENT ON TABLE consent_log IS 'GDPR consent tracking for all outreach';


-- Data purge schedule
CREATE TABLE IF NOT EXISTS data_purge_schedule (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    record_id       UUID NOT NULL,
    table_name      VARCHAR(100) NOT NULL,
    purge_date      DATE NOT NULL,
    reason          VARCHAR(100) DEFAULT 'retention_policy',
    purged_at       TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_purge_date ON data_purge_schedule(purge_date) WHERE purged_at IS NULL;

COMMENT ON TABLE data_purge_schedule IS 'Schedule for GDPR data retention';


-- Audit log
CREATE TABLE IF NOT EXISTS audit_log (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    action          VARCHAR(100) NOT NULL,
    details         JSONB DEFAULT '{}',
    performed_by    VARCHAR(100) DEFAULT 'system',
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_date ON audit_log(created_at);

COMMENT ON TABLE audit_log IS 'Audit trail for compliance-related actions';


-- =============================================================================
-- SCHEMA UPDATES TO EXISTING TABLES
-- =============================================================================

-- Add email verification columns to marketing_prospects
ALTER TABLE marketing_prospects ADD COLUMN IF NOT EXISTS
    email_verified BOOLEAN DEFAULT FALSE;

ALTER TABLE marketing_prospects ADD COLUMN IF NOT EXISTS
    verified_at TIMESTAMP;

ALTER TABLE marketing_prospects ADD COLUMN IF NOT EXISTS
    verification_status VARCHAR(50);  -- valid, invalid, catch_all, unknown

ALTER TABLE marketing_prospects ADD COLUMN IF NOT EXISTS
    nlp_relevance_score DECIMAL(3,2) DEFAULT 0.00;

-- Add idempotency key to email_sends
ALTER TABLE email_sends ADD COLUMN IF NOT EXISTS
    idempotency_key VARCHAR(255);

CREATE INDEX IF NOT EXISTS idx_sends_idempotency ON email_sends(idempotency_key);


-- =============================================================================
-- UPDATE DAILY STATS TABLE
-- =============================================================================

-- Add email verification stat
ALTER TABLE marketing_daily_stats ADD COLUMN IF NOT EXISTS
    emails_verified INTEGER DEFAULT 0;


-- =============================================================================
-- NEW INDEXES FOR PERFORMANCE
-- =============================================================================

-- Index for verified email filtering in outreach
CREATE INDEX IF NOT EXISTS idx_prospects_verified 
ON marketing_prospects(email_verified) 
WHERE email_verified = TRUE;

-- Index for verification status filtering
CREATE INDEX IF NOT EXISTS idx_prospects_verification_status 
ON marketing_prospects(verification_status);

-- Composite index for outreach queries
CREATE INDEX IF NOT EXISTS idx_prospects_outreach_ready 
ON marketing_prospects(status, email_verified, relevance_score)
WHERE email IS NOT NULL AND email_verified = TRUE;

-- Index for consent lookups
CREATE INDEX IF NOT EXISTS idx_consent_created 
ON consent_log(created_at);


-- =============================================================================
-- DATA MIGRATIONS
-- =============================================================================

-- Set existing prospects with email as unverified (will be verified in batch)
UPDATE marketing_prospects 
SET email_verified = FALSE, verification_status = 'pending'
WHERE email IS NOT NULL AND email_verified IS NULL;

-- Set existing prospects without email
UPDATE marketing_prospects 
SET email_verified = FALSE, verification_status = NULL
WHERE email IS NULL;


-- =============================================================================
-- FUNCTIONS
-- =============================================================================

-- Function to auto-set purge schedule when prospect is created
CREATE OR REPLACE FUNCTION schedule_prospect_purge()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO data_purge_schedule (record_id, table_name, purge_date, reason)
    VALUES (
        NEW.id,
        'marketing_prospects',
        CURRENT_DATE + INTERVAL '180 days',
        'retention_policy'
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for auto-scheduling purge
DROP TRIGGER IF EXISTS trigger_schedule_purge ON marketing_prospects;
CREATE TRIGGER trigger_schedule_purge
    AFTER INSERT ON marketing_prospects
    FOR EACH ROW
    EXECUTE FUNCTION schedule_prospect_purge();


-- Function to clean up orphaned purge schedules when prospect is deleted
CREATE OR REPLACE FUNCTION cleanup_purge_schedule()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM data_purge_schedule 
    WHERE record_id = OLD.id AND table_name = 'marketing_prospects';
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Trigger for cleanup
DROP TRIGGER IF EXISTS trigger_cleanup_purge ON marketing_prospects;
CREATE TRIGGER trigger_cleanup_purge
    BEFORE DELETE ON marketing_prospects
    FOR EACH ROW
    EXECUTE FUNCTION cleanup_purge_schedule();


-- Function to cancel purge when prospect converts
CREATE OR REPLACE FUNCTION cancel_purge_on_conversion()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status IN ('converted', 'active_affiliate') AND OLD.status NOT IN ('converted', 'active_affiliate') THEN
        UPDATE data_purge_schedule 
        SET reason = 'converted_customer', purge_date = NULL
        WHERE record_id = NEW.id AND table_name = 'marketing_prospects';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for conversion
DROP TRIGGER IF EXISTS trigger_cancel_purge ON marketing_prospects;
CREATE TRIGGER trigger_cancel_purge
    AFTER UPDATE ON marketing_prospects
    FOR EACH ROW
    EXECUTE FUNCTION cancel_purge_on_conversion();


-- =============================================================================
-- VIEWS FOR REPORTING
-- =============================================================================

-- View for compliance dashboard
CREATE OR REPLACE VIEW v_compliance_status AS
SELECT
    (SELECT COUNT(*) FROM marketing_prospects WHERE email IS NOT NULL) as total_with_email,
    (SELECT COUNT(*) FROM marketing_prospects WHERE email_verified = TRUE) as verified_emails,
    (SELECT COUNT(*) FROM consent_log) as consent_records,
    (SELECT COUNT(*) FROM data_purge_schedule WHERE purged_at IS NULL AND purge_date <= CURRENT_DATE) as pending_purges,
    (SELECT COUNT(*) FROM data_purge_schedule WHERE purged_at IS NOT NULL) as completed_purges,
    (SELECT COUNT(*) FROM marketing_prospects WHERE status = 'purged') as anonymized_records,
    (SELECT MAX(created_at) FROM audit_log WHERE action = 'data_purge') as last_purge_date;

-- View for email verification status
CREATE OR REPLACE VIEW v_verification_status AS
SELECT
    verification_status,
    COUNT(*) as count,
    ROUND(COUNT(*)::NUMERIC / NULLIF(SUM(COUNT(*)) OVER (), 0) * 100, 2) as percentage
FROM marketing_prospects
WHERE email IS NOT NULL
GROUP BY verification_status
ORDER BY count DESC;

-- View for outreach-ready prospects
CREATE OR REPLACE VIEW v_outreach_ready AS
SELECT
    id,
    full_name,
    email,
    primary_platform,
    relevance_score,
    nlp_relevance_score,
    youtube_subscribers,
    instagram_followers,
    discovered_at
FROM marketing_prospects
WHERE email IS NOT NULL
  AND email_verified = TRUE
  AND verification_status IN ('valid', 'catch_all')
  AND status IN ('discovered', 'enriched')
  AND first_contacted_at IS NULL
ORDER BY relevance_score DESC;


-- =============================================================================
-- SEED DATA FOR NEW FEATURES
-- =============================================================================

-- Add TikTok keywords for Apify discovery
INSERT INTO competitor_keywords (competitor_name, keyword, platform, is_active)
VALUES
    ('General', 'aivideo', 'tiktok', TRUE),
    ('General', 'aitools', 'tiktok', TRUE),
    ('General', 'facelessyoutube', 'tiktok', TRUE),
    ('General', 'contentcreation', 'tiktok', TRUE),
    ('General', 'videoeditingai', 'tiktok', TRUE)
ON CONFLICT DO NOTHING;

-- Add Instagram keywords for Apify discovery
INSERT INTO competitor_keywords (competitor_name, keyword, platform, is_active)
VALUES
    ('General', 'aitools', 'instagram', TRUE),
    ('General', 'aicontent', 'instagram', TRUE),
    ('General', 'contentcreator', 'instagram', TRUE),
    ('General', 'videomarketing', 'instagram', TRUE),
    ('General', 'socialmediatools', 'instagram', TRUE)
ON CONFLICT DO NOTHING;


-- =============================================================================
-- VERIFICATION
-- =============================================================================

-- Verify migration completed
DO $$
BEGIN
    -- Check new tables exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'idempotency_keys') THEN
        RAISE EXCEPTION 'Migration failed: idempotency_keys table not created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'consent_log') THEN
        RAISE EXCEPTION 'Migration failed: consent_log table not created';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'audit_log') THEN
        RAISE EXCEPTION 'Migration failed: audit_log table not created';
    END IF;
    
    -- Check new columns exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'marketing_prospects' AND column_name = 'email_verified') THEN
        RAISE EXCEPTION 'Migration failed: email_verified column not added';
    END IF;
    
    RAISE NOTICE 'Migration v2.0 completed successfully';
END $$;


-- Log migration completion
INSERT INTO audit_log (action, details, performed_by)
VALUES (
    'schema_migration',
    '{"version": "2.0", "changes": ["idempotency_keys", "consent_log", "audit_log", "email_verification", "data_purge"]}',
    'migration_script'
);
