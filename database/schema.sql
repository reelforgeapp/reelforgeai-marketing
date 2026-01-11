-- ReelForge Marketing Engine - Database Schema v3

-- Marketing Prospects
CREATE TABLE IF NOT EXISTS marketing_prospects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    youtube_channel_id VARCHAR(255) UNIQUE,
    youtube_handle VARCHAR(255),
    full_name VARCHAR(255),
    email VARCHAR(255),
    email_verified BOOLEAN DEFAULT FALSE,
    verification_status VARCHAR(50),
    verified_at TIMESTAMP,
    youtube_subscribers INTEGER DEFAULT 0,
    youtube_total_views BIGINT DEFAULT 0,
    instagram_handle VARCHAR(255),
    instagram_followers INTEGER DEFAULT 0,
    tiktok_handle VARCHAR(255),
    tiktok_followers INTEGER DEFAULT 0,
    website_url TEXT,
    bio_link_url TEXT,
    primary_platform VARCHAR(50) DEFAULT 'youtube',
    relevance_score FLOAT DEFAULT 0.5,
    competitor_mentions TEXT[],
    raw_data JSONB DEFAULT '{}',
    status VARCHAR(50) DEFAULT 'discovered',
    first_contacted_at TIMESTAMP,
    last_contacted_at TIMESTAMP,
    replied_at TIMESTAMP,
    total_emails_sent INTEGER DEFAULT 0,
    discovered_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
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
    commission_rate FLOAT DEFAULT 0.40,
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

-- Default Sequence Template
INSERT INTO sequence_templates (name, description, total_steps, steps, is_active)
VALUES (
    'youtube_creator',
    'Default outreach sequence for YouTube creators',
    3,
    '[
        {"step": 1, "delay_days": 0, "body_template": "intro_email"},
        {"step": 2, "delay_days": 3, "body_template": "followup_email"},
        {"step": 3, "delay_days": 7, "body_template": "final_email"}
    ]'::jsonb,
    TRUE
) ON CONFLICT (name) DO NOTHING;

-- Default Email Templates
INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES (
    'intro_email',
    'Partnership opportunity for {{ first_name }}',
    '<p>Hi {{ first_name }},</p><p>I noticed your content about AI video tools and thought you might be interested in ReelForge.</p><p>We offer a 40% commission on referrals. Would you like to learn more?</p><p>Best,<br>Jr</p>',
    'Hi {{ first_name }}, I noticed your content about AI video tools and thought you might be interested in ReelForge. We offer a 40% commission on referrals. Would you like to learn more? Best, Jr',
    TRUE
) ON CONFLICT (name) DO NOTHING;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES (
    'followup_email',
    'Quick follow-up, {{ first_name }}',
    '<p>Hi {{ first_name }},</p><p>Just wanted to follow up on my previous email about the ReelForge affiliate program.</p><p>Here''s your unique signup link: {{ affiliate_link }}</p><p>Best,<br>Jr</p>',
    'Hi {{ first_name }}, Just wanted to follow up on my previous email about the ReelForge affiliate program. Here''s your unique signup link: {{ affiliate_link }}. Best, Jr',
    TRUE
) ON CONFLICT (name) DO NOTHING;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES (
    'final_email',
    'Last chance - 40% commission, {{ first_name }}',
    '<p>Hi {{ first_name }},</p><p>This is my final reach out about the ReelForge affiliate program.</p><p>If you''re interested, here''s your link: {{ affiliate_link }}</p><p>Best,<br>Jr</p>',
    'Hi {{ first_name }}, This is my final reach out about the ReelForge affiliate program. If you''re interested, here''s your link: {{ affiliate_link }}. Best, Jr',
    TRUE
) ON CONFLICT (name) DO NOTHING;
