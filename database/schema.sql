-- ReelForge Marketing Engine - Database Schema v3.2
-- This schema matches the production Render database

-- Marketing Prospects (matches actual Render columns)
CREATE TABLE IF NOT EXISTS marketing_prospects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name VARCHAR(255),
    email VARCHAR(255),
    email_verified BOOLEAN DEFAULT FALSE,
    email_source VARCHAR(100),
    youtube_channel_id VARCHAR(255) UNIQUE,
    youtube_handle VARCHAR(255),
    youtube_url TEXT,
    youtube_subscribers INTEGER DEFAULT 0,
    youtube_avg_views INTEGER DEFAULT 0,
    youtube_total_videos INTEGER DEFAULT 0,
    tiktok_handle VARCHAR(255),
    tiktok_url TEXT,
    tiktok_followers INTEGER DEFAULT 0,
    instagram_handle VARCHAR(255),
    instagram_url TEXT,
    instagram_followers INTEGER DEFAULT 0,
    twitter_handle VARCHAR(255),
    linkedin_url TEXT,
    website_url TEXT,
    bio_link_url TEXT,
    relevance_score FLOAT DEFAULT 0.5,
    engagement_rate FLOAT,
    audience_size_total INTEGER DEFAULT 0,
    primary_platform VARCHAR(50) DEFAULT 'youtube',
    content_categories TEXT[],
    competitor_mentions TEXT[],
    location VARCHAR(255),
    timezone VARCHAR(100),
    language VARCHAR(50),
    status VARCHAR(50) DEFAULT 'discovered',
    source VARCHAR(100),
    source_query VARCHAR(255),
    source_video_id VARCHAR(255),
    source_video_title TEXT,
    first_contacted_at TIMESTAMP,
    last_contacted_at TIMESTAMP,
    total_emails_sent INTEGER DEFAULT 0,
    total_emails_opened INTEGER DEFAULT 0,
    total_emails_clicked INTEGER DEFAULT 0,
    replied_at TIMESTAMP,
    reply_sentiment VARCHAR(50),
    converted_to_affiliate_at TIMESTAMP,
    affiliate_id UUID,
    notes TEXT,
    tags TEXT[],
    raw_data JSONB DEFAULT '{}',
    discovered_at TIMESTAMP DEFAULT NOW(),
    last_enriched_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    verified_at TIMESTAMP,
    verification_status VARCHAR(50),
    nlp_relevance_score FLOAT,
    brevo_synced_at TIMESTAMP
);

-- Competitor Keywords
CREATE TABLE IF NOT EXISTS competitor_keywords (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    competitor_name VARCHAR(255) NOT NULL,
    keyword VARCHAR(255) NOT NULL,
    platform VARCHAR(50) DEFAULT 'youtube',
    is_active BOOLEAN DEFAULT TRUE,
    last_searched_at TIMESTAMP,
    results_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    priority INTEGER DEFAULT 0
);

-- Sequence Templates
CREATE TABLE IF NOT EXISTS sequence_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    total_steps INTEGER DEFAULT 3,
    steps JSONB DEFAULT '[]',
    stop_on TEXT[] DEFAULT ARRAY['replied', 'unsubscribed', 'bounced'],
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Email Templates
CREATE TABLE IF NOT EXISTS email_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    subject_template TEXT NOT NULL,
    html_template TEXT NOT NULL,
    text_template TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Outreach Sequences
CREATE TABLE IF NOT EXISTS outreach_sequences (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    prospect_id UUID REFERENCES marketing_prospects(id) ON DELETE CASCADE,
    sequence_template_id UUID REFERENCES sequence_templates(id),
    sequence_name VARCHAR(255),
    total_steps INTEGER DEFAULT 3,
    current_step INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'pending',
    next_send_at TIMESTAMP,
    personalization_data JSONB DEFAULT '{}',
    stopped_reason VARCHAR(255),
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Email Sends
CREATE TABLE IF NOT EXISTS email_sends (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sequence_id UUID REFERENCES outreach_sequences(id) ON DELETE CASCADE,
    prospect_id UUID REFERENCES marketing_prospects(id) ON DELETE CASCADE,
    step_number INTEGER,
    template_name VARCHAR(255),
    subject TEXT,
    to_email VARCHAR(255),
    brevo_message_id VARCHAR(255),
    idempotency_key VARCHAR(255),
    status VARCHAR(50) DEFAULT 'sent',
    sent_at TIMESTAMP,
    delivered_at TIMESTAMP,
    first_opened_at TIMESTAMP,
    first_clicked_at TIMESTAMP,
    bounced_at TIMESTAMP,
    open_count INTEGER DEFAULT 0,
    click_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Affiliates
CREATE TABLE IF NOT EXISTS affiliates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    prospect_id UUID REFERENCES marketing_prospects(id),
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255),
    status VARCHAR(50) DEFAULT 'pending',
    commission_rate FLOAT DEFAULT 0.30,
    referral_code VARCHAR(50) UNIQUE,
    total_referrals INTEGER DEFAULT 0,
    total_earnings DECIMAL(10, 2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Idempotency Keys
CREATE TABLE IF NOT EXISTS idempotency_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key VARCHAR(255) UNIQUE NOT NULL,
    status VARCHAR(50) DEFAULT 'processing',
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_prospects_email ON marketing_prospects(email);
CREATE INDEX IF NOT EXISTS idx_prospects_status ON marketing_prospects(status);
CREATE INDEX IF NOT EXISTS idx_prospects_youtube ON marketing_prospects(youtube_channel_id);
CREATE INDEX IF NOT EXISTS idx_sequences_status ON outreach_sequences(status);
CREATE INDEX IF NOT EXISTS idx_sequences_next_send ON outreach_sequences(next_send_at);
CREATE INDEX IF NOT EXISTS idx_sends_message_id ON email_sends(brevo_message_id);
CREATE INDEX IF NOT EXISTS idx_keywords_platform ON competitor_keywords(platform, is_active);

-- Unique constraint to prevent duplicate emails for the same sequence step
CREATE UNIQUE INDEX IF NOT EXISTS idx_sends_sequence_step_unique ON email_sends(sequence_id, step_number);

-- =====================================================
-- SEED DATA - Competitor Keywords for YouTube Discovery
-- =====================================================
INSERT INTO competitor_keywords (competitor_name, keyword, platform, is_active) VALUES
    ('Pictory', 'pictory review', 'youtube', TRUE),
    ('Pictory', 'pictory tutorial', 'youtube', TRUE),
    ('InVideo', 'invideo ai review', 'youtube', TRUE),
    ('InVideo', 'invideo tutorial', 'youtube', TRUE),
    ('Synthesia', 'synthesia review', 'youtube', TRUE),
    ('Synthesia', 'synthesia tutorial', 'youtube', TRUE),
    ('HeyGen', 'heygen review', 'youtube', TRUE),
    ('HeyGen', 'heygen tutorial', 'youtube', TRUE),
    ('Descript', 'descript review', 'youtube', TRUE),
    ('Descript', 'descript tutorial', 'youtube', TRUE),
    ('Runway ML', 'runway ml review', 'youtube', TRUE),
    ('Runway ML', 'runway ml tutorial', 'youtube', TRUE),
    ('Fliki', 'fliki review', 'youtube', TRUE),
    ('Fliki', 'fliki tutorial', 'youtube', TRUE),
    ('Lumen5', 'lumen5 review', 'youtube', TRUE),
    ('Lumen5', 'lumen5 tutorial', 'youtube', TRUE),
    ('General', 'ai video generator', 'youtube', TRUE),
    ('General', 'ai video editing', 'youtube', TRUE),
    ('General', 'best ai video tools', 'youtube', TRUE),
    ('General', 'ai video maker', 'youtube', TRUE)
ON CONFLICT DO NOTHING;

-- =====================================================
-- SEED DATA - Sequence Templates
-- =====================================================
INSERT INTO sequence_templates (name, description, total_steps, steps, is_active)
VALUES (
    'youtube_creator',
    'Outreach sequence for YouTube creators reviewing AI video tools',
    3,
    '[
        {"step": 1, "delay_days": 0, "delay_hours": 0, "body_template": "youtube_initial", "subject_template": "Your {{competitor}} content and the ReelForge AI affiliate program", "skip_weekends": true, "send_time_preference": "10:00"},
        {"step": 2, "delay_days": 3, "delay_hours": 0, "body_template": "youtube_followup_1", "subject_template": "Following up: ReelForge AI affiliate program", "skip_weekends": true, "send_time_preference": "14:00", "skip_if": ["replied", "clicked"]},
        {"step": 3, "delay_days": 7, "delay_hours": 0, "body_template": "youtube_followup_2", "subject_template": "Closing the loop, {{first_name}}", "skip_weekends": true, "send_time_preference": "10:00", "skip_if": ["replied"]}
    ]'::jsonb,
    TRUE
) ON CONFLICT (name) DO UPDATE SET 
    steps = EXCLUDED.steps,
    description = EXCLUDED.description;

INSERT INTO sequence_templates (name, description, total_steps, steps, is_active)
VALUES (
    'tiktok_creator',
    'Outreach sequence for TikTok creators',
    2,
    '[
        {"step": 1, "delay_days": 0, "delay_hours": 0, "body_template": "tiktok_initial", "skip_weekends": true, "send_time_preference": "12:00"},
        {"step": 2, "delay_days": 4, "delay_hours": 0, "body_template": "tiktok_followup", "skip_weekends": true, "send_time_preference": "15:00", "skip_if": ["replied", "clicked"]}
    ]'::jsonb,
    TRUE
) ON CONFLICT (name) DO UPDATE SET steps = EXCLUDED.steps;

INSERT INTO sequence_templates (name, description, total_steps, steps, is_active)
VALUES (
    'instagram_creator',
    'Outreach sequence for Instagram creators',
    2,
    '[
        {"step": 1, "delay_days": 0, "delay_hours": 0, "body_template": "instagram_initial", "skip_weekends": true, "send_time_preference": "11:00"},
        {"step": 2, "delay_days": 4, "delay_hours": 0, "body_template": "instagram_followup", "skip_weekends": true, "send_time_preference": "14:00", "skip_if": ["replied"]}
    ]'::jsonb,
    TRUE
) ON CONFLICT (name) DO UPDATE SET steps = EXCLUDED.steps;

-- =====================================================
-- SEED DATA - Email Templates
-- =====================================================
-- Email template copy lives in database/migrations/004_compliant_email_templates.sql.
-- Run it after this file (it upserts every template).
