-- ============================================
-- ReelForge Marketing Engine Database Schema
-- For Render PostgreSQL
-- ============================================

-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- PROSPECTS TABLE
-- Stores discovered affiliate/influencer leads
-- ============================================
CREATE TABLE IF NOT EXISTS marketing_prospects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Identity
    full_name VARCHAR(255),
    email VARCHAR(255),
    email_verified BOOLEAN DEFAULT FALSE,
    email_source VARCHAR(100), -- 'youtube_about', 'website_contact', 'bio_link', 'instagram_bio', 'tiktok_bio'
    
    -- Social Profiles
    youtube_channel_id VARCHAR(100),
    youtube_handle VARCHAR(100),
    youtube_url VARCHAR(500),
    youtube_subscribers INTEGER DEFAULT 0,
    youtube_avg_views INTEGER DEFAULT 0,
    youtube_total_videos INTEGER DEFAULT 0,
    
    tiktok_handle VARCHAR(100),
    tiktok_url VARCHAR(500),
    tiktok_followers INTEGER DEFAULT 0,
    
    instagram_handle VARCHAR(100),
    instagram_url VARCHAR(500),
    instagram_followers INTEGER DEFAULT 0,
    
    twitter_handle VARCHAR(100),
    linkedin_url VARCHAR(500),
    website_url VARCHAR(500),
    bio_link_url VARCHAR(500), -- Linktree, Beacons, etc.
    
    -- Qualification Metrics
    relevance_score DECIMAL(3,2) DEFAULT 0.00, -- 0.00 to 1.00
    engagement_rate DECIMAL(5,2) DEFAULT 0.00,
    audience_size_total INTEGER DEFAULT 0,
    primary_platform VARCHAR(50), -- 'youtube', 'instagram', 'tiktok', 'blog'
    content_categories TEXT[], -- ['AI tools', 'marketing', 'video editing']
    competitor_mentions TEXT[], -- ['pictory', 'synthesia', 'invideo']
    
    -- Enrichment Data
    location VARCHAR(100),
    timezone VARCHAR(50),
    language VARCHAR(10) DEFAULT 'en',
    
    -- Pipeline Status
    -- discovered -> enriched -> qualified -> contacted -> negotiating -> converted -> churned
    status VARCHAR(50) DEFAULT 'discovered',
    
    -- Source Tracking
    source VARCHAR(100), -- 'youtube_api', 'apify_instagram', 'apify_tiktok', 'website_scrape', 'manual'
    source_query VARCHAR(255), -- The search query that found them
    source_video_id VARCHAR(100), -- YouTube video ID that led to discovery
    source_video_title VARCHAR(500),
    
    -- Outreach Tracking
    first_contacted_at TIMESTAMP,
    last_contacted_at TIMESTAMP,
    total_emails_sent INTEGER DEFAULT 0,
    total_emails_opened INTEGER DEFAULT 0,
    total_emails_clicked INTEGER DEFAULT 0,
    replied_at TIMESTAMP,
    reply_sentiment VARCHAR(50), -- 'positive', 'negative', 'neutral', 'unsubscribe'
    
    -- Conversion
    converted_to_affiliate_at TIMESTAMP,
    affiliate_id UUID,
    
    -- Metadata
    notes TEXT,
    tags TEXT[],
    raw_data JSONB DEFAULT '{}', -- Store full API response for debugging
    
    -- Timestamps
    discovered_at TIMESTAMP DEFAULT NOW(),
    last_enriched_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- OUTREACH SEQUENCES TABLE
-- Tracks multi-step email sequences per prospect
-- ============================================
CREATE TABLE IF NOT EXISTS outreach_sequences (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    prospect_id UUID NOT NULL REFERENCES marketing_prospects(id) ON DELETE CASCADE,
    
    -- Sequence Configuration
    sequence_template_id UUID,
    sequence_name VARCHAR(100) NOT NULL, -- 'youtube_creator', 'tiktok_creator', etc.
    total_steps INTEGER DEFAULT 3,
    
    -- Current State
    current_step INTEGER DEFAULT 0, -- 0 = not started, 1 = first email sent, etc.
    status VARCHAR(50) DEFAULT 'pending', 
    -- 'pending', 'active', 'paused', 'completed', 'stopped', 'converted'
    stopped_reason VARCHAR(100), -- 'replied', 'bounced', 'unsubscribed', 'manual', 'converted'
    
    -- Scheduling
    started_at TIMESTAMP,
    next_send_at TIMESTAMP,
    last_action_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Personalization Data (cached for email templates)
    personalization_data JSONB DEFAULT '{}',
    /*
    Example:
    {
        "first_name": "Sarah",
        "competitor": "Pictory",
        "youtube_handle": "@sarahreviews",
        "subscriber_count": "45K",
        "video_title": "Pictory AI Review 2025"
    }
    */
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- EMAIL SENDS TABLE
-- Log of every email sent
-- ============================================
CREATE TABLE IF NOT EXISTS email_sends (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sequence_id UUID REFERENCES outreach_sequences(id) ON DELETE SET NULL,
    prospect_id UUID NOT NULL REFERENCES marketing_prospects(id) ON DELETE CASCADE,
    
    -- Email Details
    step_number INTEGER NOT NULL,
    template_name VARCHAR(100),
    subject VARCHAR(500),
    to_email VARCHAR(255) NOT NULL,
    
    -- Brevo Tracking
    brevo_message_id VARCHAR(100),
    
    -- Status
    status VARCHAR(50) DEFAULT 'queued', 
    -- 'queued', 'sent', 'delivered', 'opened', 'clicked', 'bounced', 'complained', 'unsubscribed'
    
    -- Events (timestamps)
    queued_at TIMESTAMP DEFAULT NOW(),
    sent_at TIMESTAMP,
    delivered_at TIMESTAMP,
    first_opened_at TIMESTAMP,
    last_opened_at TIMESTAMP,
    open_count INTEGER DEFAULT 0,
    first_clicked_at TIMESTAMP,
    last_clicked_at TIMESTAMP,
    click_count INTEGER DEFAULT 0,
    bounced_at TIMESTAMP,
    bounce_type VARCHAR(50), -- 'hard', 'soft'
    bounce_reason VARCHAR(500),
    complained_at TIMESTAMP,
    unsubscribed_at TIMESTAMP,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- AFFILIATES TABLE
-- Active affiliates (synced with Reditus)
-- ============================================
CREATE TABLE IF NOT EXISTS affiliates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    prospect_id UUID REFERENCES marketing_prospects(id) ON DELETE SET NULL,
    
    -- Reditus Integration
    reditus_affiliate_id VARCHAR(100) UNIQUE,
    reditus_data JSONB DEFAULT '{}',
    
    -- Identity
    email VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    
    -- Affiliate Details
    affiliate_code VARCHAR(50) UNIQUE,
    affiliate_link VARCHAR(500),
    
    -- Commission Settings
    commission_rate DECIMAL(4,2) DEFAULT 0.40, -- 40%
    commission_type VARCHAR(50) DEFAULT 'recurring', -- 'one_time', 'recurring'
    tier VARCHAR(50) DEFAULT 'standard', -- 'standard', 'premium', 'vip'
    
    -- Performance Metrics
    total_clicks INTEGER DEFAULT 0,
    total_signups INTEGER DEFAULT 0,
    total_trials INTEGER DEFAULT 0,
    total_conversions INTEGER DEFAULT 0,
    total_revenue DECIMAL(12,2) DEFAULT 0.00,
    total_commission_earned DECIMAL(12,2) DEFAULT 0.00,
    total_commission_paid DECIMAL(12,2) DEFAULT 0.00,
    total_commission_pending DECIMAL(12,2) DEFAULT 0.00,
    
    -- Status
    status VARCHAR(50) DEFAULT 'pending', -- 'pending', 'active', 'inactive', 'suspended'
    
    -- Social (for attribution)
    primary_platform VARCHAR(50), -- 'youtube', 'tiktok', 'instagram', 'blog'
    youtube_handle VARCHAR(100),
    tiktok_handle VARCHAR(100),
    instagram_handle VARCHAR(100),
    website_url VARCHAR(500),
    
    -- Timestamps
    applied_at TIMESTAMP DEFAULT NOW(),
    approved_at TIMESTAMP,
    last_active_at TIMESTAMP,
    last_synced_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- DISCOVERY JOBS TABLE
-- Track scraping/discovery job runs
-- ============================================
CREATE TABLE IF NOT EXISTS discovery_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Job Configuration
    job_type VARCHAR(50) NOT NULL, 
    -- 'youtube_search', 'youtube_email_extract', 'apify_instagram', 'apify_tiktok', 'website_scrape'
    
    search_query VARCHAR(255),
    parameters JSONB DEFAULT '{}',
    
    -- Status
    status VARCHAR(50) DEFAULT 'pending', -- 'pending', 'running', 'completed', 'failed'
    error_message TEXT,
    
    -- Results
    prospects_found INTEGER DEFAULT 0,
    prospects_new INTEGER DEFAULT 0,
    prospects_updated INTEGER DEFAULT 0,
    emails_extracted INTEGER DEFAULT 0,
    
    -- Apify-specific
    apify_run_id VARCHAR(100),
    apify_dataset_id VARCHAR(100),
    
    -- Timing
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- SEQUENCE TEMPLATES TABLE
-- Email sequence definitions
-- ============================================
CREATE TABLE IF NOT EXISTS sequence_templates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Template Identity
    name VARCHAR(100) NOT NULL UNIQUE, -- 'youtube_creator', 'tiktok_creator'
    description TEXT,
    
    -- Configuration
    total_steps INTEGER DEFAULT 3,
    steps JSONB NOT NULL,
    /*
    Example steps structure:
    [
        {
            "step": 1,
            "delay_days": 0,
            "delay_hours": 0,
            "subject_template": "Loved your {{competitor}} review",
            "body_template": "youtube_initial",
            "send_time_preference": "10:00",
            "skip_weekends": true,
            "skip_if": []
        },
        {
            "step": 2,
            "delay_days": 3,
            "subject_template": "Quick follow-up: 40% commission",
            "body_template": "youtube_followup_1",
            "send_time_preference": "14:00",
            "skip_weekends": true,
            "skip_if": ["replied", "clicked"]
        }
    ]
    */
    
    -- Stop Conditions
    stop_on TEXT[] DEFAULT ARRAY['replied', 'unsubscribed', 'bounced'],
    
    -- Targeting
    target_platforms TEXT[], -- Which platforms this applies to
    target_sources TEXT[], -- Which prospect sources this applies to
    min_relevance_score DECIMAL(3,2) DEFAULT 0.50,
    min_followers INTEGER DEFAULT 1000,
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- EMAIL TEMPLATES TABLE
-- Individual email templates
-- ============================================
CREATE TABLE IF NOT EXISTS email_templates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Template Identity
    name VARCHAR(100) NOT NULL UNIQUE, -- 'youtube_initial', 'youtube_followup_1'
    description TEXT,
    category VARCHAR(50), -- 'youtube', 'tiktok', 'instagram', 'blog'
    
    -- Content
    subject_template VARCHAR(500) NOT NULL,
    html_template TEXT NOT NULL,
    text_template TEXT NOT NULL,
    
    -- Personalization Variables Available
    -- {{first_name}}, {{competitor}}, {{affiliate_link}}, {{youtube_handle}}, 
    -- {{subscriber_count}}, {{video_title}}, {{platform}}
    available_variables TEXT[],
    required_variables TEXT[],
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- COMPETITOR KEYWORDS TABLE
-- Keywords to search for competitor reviews
-- ============================================
CREATE TABLE IF NOT EXISTS competitor_keywords (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    competitor_name VARCHAR(100) NOT NULL,
    keyword VARCHAR(255) NOT NULL,
    platform VARCHAR(50) DEFAULT 'youtube', -- 'youtube', 'tiktok', 'instagram', 'google'
    
    is_active BOOLEAN DEFAULT TRUE,
    last_searched_at TIMESTAMP,
    total_prospects_found INTEGER DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT unique_keyword_platform UNIQUE (keyword, platform)
);

-- ============================================
-- DAILY STATS TABLE
-- Track daily metrics
-- ============================================
CREATE TABLE IF NOT EXISTS marketing_daily_stats (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    stat_date DATE NOT NULL UNIQUE,
    
    -- Discovery
    prospects_discovered INTEGER DEFAULT 0,
    prospects_youtube INTEGER DEFAULT 0,
    prospects_instagram INTEGER DEFAULT 0,
    prospects_tiktok INTEGER DEFAULT 0,
    emails_extracted INTEGER DEFAULT 0,
    
    -- Outreach
    emails_sent INTEGER DEFAULT 0,
    emails_delivered INTEGER DEFAULT 0,
    emails_opened INTEGER DEFAULT 0,
    emails_clicked INTEGER DEFAULT 0,
    emails_bounced INTEGER DEFAULT 0,
    emails_replied INTEGER DEFAULT 0,
    
    -- Conversions
    affiliates_signed_up INTEGER DEFAULT 0,
    
    -- Costs (for tracking)
    apify_credits_used DECIMAL(10,4) DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================

-- Prospects indexes
CREATE INDEX IF NOT EXISTS idx_prospects_status ON marketing_prospects(status);
CREATE INDEX IF NOT EXISTS idx_prospects_email ON marketing_prospects(email);
CREATE INDEX IF NOT EXISTS idx_prospects_relevance ON marketing_prospects(relevance_score DESC);
CREATE INDEX IF NOT EXISTS idx_prospects_source ON marketing_prospects(source);
CREATE INDEX IF NOT EXISTS idx_prospects_discovered ON marketing_prospects(discovered_at DESC);
CREATE INDEX IF NOT EXISTS idx_prospects_youtube_channel ON marketing_prospects(youtube_channel_id);
CREATE INDEX IF NOT EXISTS idx_prospects_youtube_handle ON marketing_prospects(youtube_handle);
CREATE INDEX IF NOT EXISTS idx_prospects_instagram ON marketing_prospects(instagram_handle);
CREATE INDEX IF NOT EXISTS idx_prospects_tiktok ON marketing_prospects(tiktok_handle);
CREATE INDEX IF NOT EXISTS idx_prospects_primary_platform ON marketing_prospects(primary_platform);
CREATE INDEX IF NOT EXISTS idx_prospects_not_contacted ON marketing_prospects(status, first_contacted_at) 
    WHERE status = 'qualified' AND first_contacted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_prospects_needs_enrichment ON marketing_prospects(status, email)
    WHERE status = 'discovered' AND email IS NULL;

-- Sequences indexes
CREATE INDEX IF NOT EXISTS idx_sequences_prospect ON outreach_sequences(prospect_id);
CREATE INDEX IF NOT EXISTS idx_sequences_status ON outreach_sequences(status);
CREATE INDEX IF NOT EXISTS idx_sequences_next_send ON outreach_sequences(next_send_at) 
    WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_sequences_pending ON outreach_sequences(status, next_send_at)
    WHERE status = 'active' AND next_send_at IS NOT NULL;

-- Email sends indexes
CREATE INDEX IF NOT EXISTS idx_sends_prospect ON email_sends(prospect_id);
CREATE INDEX IF NOT EXISTS idx_sends_sequence ON email_sends(sequence_id);
CREATE INDEX IF NOT EXISTS idx_sends_status ON email_sends(status);
CREATE INDEX IF NOT EXISTS idx_sends_brevo ON email_sends(brevo_message_id);
CREATE INDEX IF NOT EXISTS idx_sends_sent_at ON email_sends(sent_at DESC);

-- Affiliates indexes
CREATE INDEX IF NOT EXISTS idx_affiliates_email ON affiliates(email);
CREATE INDEX IF NOT EXISTS idx_affiliates_status ON affiliates(status);
CREATE INDEX IF NOT EXISTS idx_affiliates_reditus ON affiliates(reditus_affiliate_id);
CREATE INDEX IF NOT EXISTS idx_affiliates_code ON affiliates(affiliate_code);

-- Discovery jobs indexes
CREATE INDEX IF NOT EXISTS idx_jobs_status ON discovery_jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_type ON discovery_jobs(job_type);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON discovery_jobs(created_at DESC);

-- Daily stats index
CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON marketing_daily_stats(stat_date DESC);

-- ============================================
-- FUNCTIONS
-- ============================================

-- Auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply triggers (drop first if exists to avoid errors on re-run)
DROP TRIGGER IF EXISTS update_prospects_updated_at ON marketing_prospects;
CREATE TRIGGER update_prospects_updated_at BEFORE UPDATE ON marketing_prospects
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_sequences_updated_at ON outreach_sequences;
CREATE TRIGGER update_sequences_updated_at BEFORE UPDATE ON outreach_sequences
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_affiliates_updated_at ON affiliates;
CREATE TRIGGER update_affiliates_updated_at BEFORE UPDATE ON affiliates
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_sequence_templates_updated_at ON sequence_templates;
CREATE TRIGGER update_sequence_templates_updated_at BEFORE UPDATE ON sequence_templates
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_email_templates_updated_at ON email_templates;
CREATE TRIGGER update_email_templates_updated_at BEFORE UPDATE ON email_templates
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_daily_stats_updated_at ON marketing_daily_stats;
CREATE TRIGGER update_daily_stats_updated_at BEFORE UPDATE ON marketing_daily_stats
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- SEED DATA: Competitor Keywords
-- ============================================

INSERT INTO competitor_keywords (competitor_name, keyword, platform) VALUES
-- Pictory
('Pictory', 'pictory review', 'youtube'),
('Pictory', 'pictory ai review', 'youtube'),
('Pictory', 'pictory tutorial', 'youtube'),
('Pictory', 'pictory ai tutorial', 'youtube'),
('Pictory', 'is pictory worth it', 'youtube'),
-- Synthesia
('Synthesia', 'synthesia review', 'youtube'),
('Synthesia', 'synthesia ai review', 'youtube'),
('Synthesia', 'synthesia tutorial', 'youtube'),
('Synthesia', 'synthesia demo', 'youtube'),
-- InVideo
('InVideo', 'invideo review', 'youtube'),
('InVideo', 'invideo ai review', 'youtube'),
('InVideo', 'invideo tutorial', 'youtube'),
-- HeyGen
('HeyGen', 'heygen review', 'youtube'),
('HeyGen', 'heygen ai review', 'youtube'),
('HeyGen', 'heygen tutorial', 'youtube'),
-- Lumen5
('Lumen5', 'lumen5 review', 'youtube'),
('Lumen5', 'lumen5 tutorial', 'youtube'),
-- D-ID
('D-ID', 'd-id review', 'youtube'),
('D-ID', 'd-id ai review', 'youtube'),
-- General AI Video
('General', 'best ai video generator', 'youtube'),
('General', 'best ai video generator 2025', 'youtube'),
('General', 'ai video tools comparison', 'youtube'),
('General', 'ai video maker review', 'youtube'),
('General', 'faceless youtube channel ai', 'youtube'),
('General', 'ai shorts generator', 'youtube'),
-- TikTok hashtags (for Apify)
('General', 'aitools', 'tiktok'),
('General', 'aivideo', 'tiktok'),
('General', 'contentcreator', 'tiktok'),
('General', 'aimarketing', 'tiktok'),
('General', 'facelessyoutube', 'tiktok'),
-- Instagram hashtags (for Apify)
('General', 'aitools', 'instagram'),
('General', 'aicontent', 'instagram'),
('General', 'contentcreation', 'instagram'),
('General', 'videomarketing', 'instagram')
ON CONFLICT (keyword, platform) DO NOTHING;

-- ============================================
-- SEED DATA: Default Sequence Templates
-- ============================================

INSERT INTO sequence_templates (name, description, total_steps, steps, target_platforms, target_sources, min_relevance_score, min_followers)
VALUES 
(
    'youtube_creator',
    '3-step sequence for YouTube creators who review AI video tools',
    3,
    '[
        {
            "step": 1,
            "delay_days": 0,
            "delay_hours": 0,
            "subject_template": "Loved your {{competitor}} review - partnership opportunity",
            "body_template": "youtube_initial",
            "send_time_preference": "10:00",
            "skip_weekends": true,
            "skip_if": []
        },
        {
            "step": 2,
            "delay_days": 3,
            "delay_hours": 0,
            "subject_template": "Quick follow-up: 40% commission opportunity",
            "body_template": "youtube_followup_1",
            "send_time_preference": "14:00",
            "skip_weekends": true,
            "skip_if": ["replied", "clicked"]
        },
        {
            "step": 3,
            "delay_days": 7,
            "delay_hours": 0,
            "subject_template": "Last one from me (with a bonus)",
            "body_template": "youtube_followup_2",
            "send_time_preference": "10:00",
            "skip_weekends": true,
            "skip_if": ["replied"]
        }
    ]'::jsonb,
    ARRAY['youtube'],
    ARRAY['youtube_api'],
    0.50,
    5000
),
(
    'tiktok_creator',
    '2-step sequence for TikTok creators',
    2,
    '[
        {
            "step": 1,
            "delay_days": 0,
            "delay_hours": 0,
            "subject_template": "Your AI content is amazing - partnership?",
            "body_template": "tiktok_initial",
            "send_time_preference": "12:00",
            "skip_weekends": true,
            "skip_if": []
        },
        {
            "step": 2,
            "delay_days": 4,
            "delay_hours": 0,
            "subject_template": "Following up - 40% recurring commission",
            "body_template": "tiktok_followup",
            "send_time_preference": "15:00",
            "skip_weekends": true,
            "skip_if": ["replied", "clicked"]
        }
    ]'::jsonb,
    ARRAY['tiktok'],
    ARRAY['apify_tiktok'],
    0.50,
    10000
),
(
    'instagram_creator',
    '2-step sequence for Instagram creators',
    2,
    '[
        {
            "step": 1,
            "delay_days": 0,
            "delay_hours": 0,
            "subject_template": "Love your content - affiliate opportunity",
            "body_template": "instagram_initial",
            "send_time_preference": "11:00",
            "skip_weekends": true,
            "skip_if": []
        },
        {
            "step": 2,
            "delay_days": 4,
            "delay_hours": 0,
            "subject_template": "Quick follow-up on ReelForge partnership",
            "body_template": "instagram_followup",
            "send_time_preference": "14:00",
            "skip_weekends": true,
            "skip_if": ["replied"]
        }
    ]'::jsonb,
    ARRAY['instagram'],
    ARRAY['apify_instagram'],
    0.50,
    5000
)
ON CONFLICT (name) DO NOTHING;

-- ============================================
-- SEED DATA: Default Email Templates
-- ============================================

INSERT INTO email_templates (name, description, category, subject_template, html_template, text_template, available_variables, required_variables)
VALUES
(
    'youtube_initial',
    'First outreach email to YouTube creators',
    'youtube',
    'Loved your {{competitor}} review - partnership opportunity',
    E'<p>Hey {{first_name}},</p>\n\n<p>I just watched your {{competitor}} review and you clearly know your stuff when it comes to AI video tools.</p>\n\n<p>I''m Jr, founder of <a href="https://reelforgeai.io">ReelForge.ai</a> - we''re an AI video platform that creates full 90-second scripted videos from a single prompt. Not just talking heads - actual multi-scene storytelling.</p>\n\n<p><strong>Quick highlights:</strong></p>\n<ul>\n    <li>90-second scripted videos from a single prompt</li>\n    <li>Multi-scene storytelling with consistent characters</li>\n    <li>Built-in viral hooks optimized for Shorts/Reels/TikTok</li>\n</ul>\n\n<p>I''d love to get you set up as an affiliate partner:</p>\n<ul>\n    <li>40% recurring commission (not one-time)</li>\n    <li>Free Pro account for your content</li>\n    <li>Custom landing page for your audience</li>\n    <li>Early access to new features</li>\n</ul>\n\n<p><a href="{{affiliate_link}}">Sign up here</a> - takes 2 minutes.</p>\n\n<p>Happy to jump on a quick call if you want a demo first.</p>\n\n<p>Best,<br>\nJr<br>\nFounder, ReelForge.ai</p>',
    E'Hey {{first_name}},\n\nI just watched your {{competitor}} review and you clearly know your stuff when it comes to AI video tools.\n\nI''m Jr, founder of ReelForge.ai - we''re an AI video platform that creates full 90-second scripted videos from a single prompt. Not just talking heads - actual multi-scene storytelling.\n\nQuick highlights:\n- 90-second scripted videos from a single prompt\n- Multi-scene storytelling with consistent characters\n- Built-in viral hooks optimized for Shorts/Reels/TikTok\n\nI''d love to get you set up as an affiliate partner:\n- 40% recurring commission (not one-time)\n- Free Pro account for your content\n- Custom landing page for your audience\n- Early access to new features\n\nSign up here: {{affiliate_link}}\n\nHappy to jump on a quick call if you want a demo first.\n\nBest,\nJr\nFounder, ReelForge.ai',
    ARRAY['first_name', 'competitor', 'affiliate_link', 'youtube_handle', 'subscriber_count', 'video_title'],
    ARRAY['first_name', 'competitor', 'affiliate_link']
),
(
    'youtube_followup_1',
    'First follow-up to YouTube creators (day 3)',
    'youtube',
    'Quick follow-up: 40% commission opportunity',
    E'<p>Hey {{first_name}},</p>\n\n<p>Quick follow-up on my email about ReelForge.ai partnership.</p>\n\n<p>Since you cover AI video tools, thought you''d want to know - we just shipped some big updates:</p>\n<ul>\n    <li>Google Veo 3 integration for cinema-quality output</li>\n    <li>Multi-API routing (HeyGen + AI avatars)</li>\n    <li>One-click series generation for episodic content</li>\n</ul>\n\n<p>Our affiliates are seeing solid recurring income with audiences your size.</p>\n\n<p>40% recurring commission: <a href="{{affiliate_link}}">{{affiliate_link}}</a></p>\n\n<p>Let me know if you have any questions.</p>\n\n<p>Jr</p>',
    E'Hey {{first_name}},\n\nQuick follow-up on my email about ReelForge.ai partnership.\n\nSince you cover AI video tools, thought you''d want to know - we just shipped some big updates:\n- Google Veo 3 integration for cinema-quality output\n- Multi-API routing (HeyGen + AI avatars)\n- One-click series generation for episodic content\n\nOur affiliates are seeing solid recurring income with audiences your size.\n\n40% recurring commission: {{affiliate_link}}\n\nLet me know if you have any questions.\n\nJr',
    ARRAY['first_name', 'affiliate_link'],
    ARRAY['first_name', 'affiliate_link']
),
(
    'youtube_followup_2',
    'Final follow-up to YouTube creators (day 7)',
    'youtube',
    'Last one from me (with a bonus)',
    E'<p>{{first_name}},</p>\n\n<p>Last email from me - I respect your inbox.</p>\n\n<p>If timing just isn''t right, no worries at all. But wanted to sweeten the deal:</p>\n\n<p><strong>Sign up this week</strong> and I''ll bump you to <strong>50% commission for your first 3 months</strong>, plus I''ll personally create a custom demo video featuring your channel''s style.</p>\n\n<p><a href="{{affiliate_link}}">{{affiliate_link}}</a></p>\n\n<p>Either way, keep making great content.</p>\n\n<p>Jr</p>',
    E'{{first_name}},\n\nLast email from me - I respect your inbox.\n\nIf timing just isn''t right, no worries at all. But wanted to sweeten the deal:\n\nSign up this week and I''ll bump you to 50% commission for your first 3 months, plus I''ll personally create a custom demo video featuring your channel''s style.\n\n{{affiliate_link}}\n\nEither way, keep making great content.\n\nJr',
    ARRAY['first_name', 'affiliate_link'],
    ARRAY['first_name', 'affiliate_link']
),
(
    'tiktok_initial',
    'First outreach email to TikTok creators',
    'tiktok',
    'Your AI content is amazing - partnership?',
    E'<p>Hey {{first_name}},</p>\n\n<p>Been following your AI tool content on TikTok - your audience clearly trusts your recommendations.</p>\n\n<p>I built <a href="https://reelforgeai.io">ReelForge.ai</a> - it generates full 90-second scripted videos (not just talking heads) from a single prompt. Perfect for the short-form content your audience creates.</p>\n\n<p>Want to partner up?</p>\n<ul>\n    <li>40% recurring commission on every subscriber you refer</li>\n    <li>Free Pro account for your content</li>\n    <li>Early access to new features</li>\n</ul>\n\n<p>Takes 2 min to sign up: <a href="{{affiliate_link}}">{{affiliate_link}}</a></p>\n\n<p>Jr<br>\nFounder, ReelForge.ai</p>',
    E'Hey {{first_name}},\n\nBeen following your AI tool content on TikTok - your audience clearly trusts your recommendations.\n\nI built ReelForge.ai - it generates full 90-second scripted videos (not just talking heads) from a single prompt. Perfect for the short-form content your audience creates.\n\nWant to partner up?\n- 40% recurring commission on every subscriber you refer\n- Free Pro account for your content\n- Early access to new features\n\nTakes 2 min to sign up: {{affiliate_link}}\n\nJr\nFounder, ReelForge.ai',
    ARRAY['first_name', 'affiliate_link', 'tiktok_handle'],
    ARRAY['first_name', 'affiliate_link']
),
(
    'tiktok_followup',
    'Follow-up to TikTok creators (day 4)',
    'tiktok',
    'Following up - 40% recurring commission',
    E'<p>Hey {{first_name}},</p>\n\n<p>Just bumping this up - the ReelForge affiliate program is still open.</p>\n\n<p>40% recurring means you earn every month your referrals stay subscribed, not just once.</p>\n\n<p><a href="{{affiliate_link}}">{{affiliate_link}}</a></p>\n\n<p>Happy to answer any questions.</p>\n\n<p>Jr</p>',
    E'Hey {{first_name}},\n\nJust bumping this up - the ReelForge affiliate program is still open.\n\n40% recurring means you earn every month your referrals stay subscribed, not just once.\n\n{{affiliate_link}}\n\nHappy to answer any questions.\n\nJr',
    ARRAY['first_name', 'affiliate_link'],
    ARRAY['first_name', 'affiliate_link']
),
(
    'instagram_initial',
    'First outreach email to Instagram creators',
    'instagram',
    'Love your content - affiliate opportunity',
    E'<p>Hey {{first_name}},</p>\n\n<p>I came across your content creation posts on Instagram - love what you''re doing with AI tools.</p>\n\n<p>I''m the founder of <a href="https://reelforgeai.io">ReelForge.ai</a>, an AI video platform that creates full 90-second scripted videos from a single prompt. Think multi-scene storytelling, not just talking heads.</p>\n\n<p>Would love to have you as an affiliate partner:</p>\n<ul>\n    <li>40% recurring commission</li>\n    <li>Free Pro account</li>\n    <li>Custom affiliate link</li>\n</ul>\n\n<p>Sign up here: <a href="{{affiliate_link}}">{{affiliate_link}}</a></p>\n\n<p>Happy to chat more if you have questions.</p>\n\n<p>Jr<br>\nFounder, ReelForge.ai</p>',
    E'Hey {{first_name}},\n\nI came across your content creation posts on Instagram - love what you''re doing with AI tools.\n\nI''m the founder of ReelForge.ai, an AI video platform that creates full 90-second scripted videos from a single prompt. Think multi-scene storytelling, not just talking heads.\n\nWould love to have you as an affiliate partner:\n- 40% recurring commission\n- Free Pro account\n- Custom affiliate link\n\nSign up here: {{affiliate_link}}\n\nHappy to chat more if you have questions.\n\nJr\nFounder, ReelForge.ai',
    ARRAY['first_name', 'affiliate_link', 'instagram_handle'],
    ARRAY['first_name', 'affiliate_link']
),
(
    'instagram_followup',
    'Follow-up to Instagram creators (day 4)',
    'instagram',
    'Quick follow-up on ReelForge partnership',
    E'<p>Hey {{first_name}},</p>\n\n<p>Just following up on my email about the ReelForge affiliate program.</p>\n\n<p>Quick recap: 40% recurring commission on everyone you refer. Your audience would love this for creating Reels content.</p>\n\n<p><a href="{{affiliate_link}}">{{affiliate_link}}</a></p>\n\n<p>Let me know if you have any questions!</p>\n\n<p>Jr</p>',
    E'Hey {{first_name}},\n\nJust following up on my email about the ReelForge affiliate program.\n\nQuick recap: 40% recurring commission on everyone you refer. Your audience would love this for creating Reels content.\n\n{{affiliate_link}}\n\nLet me know if you have any questions!\n\nJr',
    ARRAY['first_name', 'affiliate_link'],
    ARRAY['first_name', 'affiliate_link']
)
ON CONFLICT (name) DO NOTHING;

-- ============================================
-- VERIFICATION
-- ============================================
DO $$
BEGIN
    RAISE NOTICE 'Marketing Engine database setup complete!';
    RAISE NOTICE 'Tables created: marketing_prospects, outreach_sequences, email_sends, affiliates, discovery_jobs, sequence_templates, email_templates, competitor_keywords, marketing_daily_stats';
END $$;
