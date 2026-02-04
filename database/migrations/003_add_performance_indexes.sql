-- Migration: Add performance indexes for common query patterns
-- Run this to improve query performance as data grows

-- Index for relevance score ordering (used in multiple queries)
CREATE INDEX IF NOT EXISTS idx_prospects_relevance_score
ON marketing_prospects(relevance_score DESC);

-- Index for email verification filtering
CREATE INDEX IF NOT EXISTS idx_prospects_email_verified
ON marketing_prospects(email_verified)
WHERE email_verified = TRUE;

-- Index for data purge queries
CREATE INDEX IF NOT EXISTS idx_prospects_discovered_at
ON marketing_prospects(discovered_at);

-- Index for prospect status filtering
CREATE INDEX IF NOT EXISTS idx_prospects_status_email
ON marketing_prospects(status, email)
WHERE email IS NOT NULL;

-- Composite index for auto-enrollment query
CREATE INDEX IF NOT EXISTS idx_prospects_enrollment_candidates
ON marketing_prospects(relevance_score DESC, status, email_verified)
WHERE email IS NOT NULL AND email_verified = TRUE;

-- Index for sequence prospect lookups
CREATE INDEX IF NOT EXISTS idx_sequences_prospect_id
ON outreach_sequences(prospect_id);

-- Index for finding active/pending sequences
CREATE INDEX IF NOT EXISTS idx_sequences_status_next_send
ON outreach_sequences(status, next_send_at)
WHERE status IN ('pending', 'active');

-- Index for Brevo sync queries
CREATE INDEX IF NOT EXISTS idx_prospects_brevo_sync
ON marketing_prospects(brevo_synced_at, updated_at)
WHERE email_verified = TRUE;

-- Analyze tables to update statistics
ANALYZE marketing_prospects;
ANALYZE outreach_sequences;
ANALYZE email_sends;
ANALYZE competitor_keywords;
