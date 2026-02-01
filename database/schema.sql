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
        {"step": 1, "delay_days": 0, "delay_hours": 0, "body_template": "youtube_initial", "subject_template": "Impressed by your {{competitor}} review - Unlock 30% lifetime earnings?", "skip_weekends": true, "send_time_preference": "10:00"},
        {"step": 2, "delay_days": 3, "delay_hours": 0, "body_template": "youtube_followup_1", "subject_template": "{{first_name}}, creators are earning $1k+/mo with this—your link inside", "skip_weekends": true, "send_time_preference": "14:00", "skip_if": ["replied", "clicked"]},
        {"step": 3, "delay_days": 7, "delay_hours": 0, "body_template": "youtube_followup_2", "subject_template": "Last chance, {{first_name}}: Free Pro access + 30% commissions", "skip_weekends": true, "send_time_preference": "10:00", "skip_if": ["replied"]}
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
-- SEED DATA - Email Templates (Larry Barksdale - 30% commission)
-- =====================================================
INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES (
    'youtube_initial',
    'Impressed by your {{competitor}} review - Unlock 30% lifetime earnings?',
    '<p>Hi {{first_name}},</p>
<p>I just watched your video on {{competitor}} and was genuinely impressed by your insightful breakdown of AI video tools. It''s clear you know what creators really need.</p>
<p>I''m Larry Barksdale from ReelForge AI, where we empower creators like you to produce stunning videos in minutes—saving hours of editing time so you can focus on what you love.</p>
<p>We''ve handpicked a select group of top creators for our affiliate program, offering <strong>30% lifetime recurring commissions</strong> on every referral. Imagine earning passive income month after month from your authentic recommendations—many partners are already seeing $1,000+ monthly without extra work.</p>
<p>What if this could add a reliable revenue stream to your channel? Reply to chat more—I''d love to share how others are succeeding.</p>
<p><strong>Learn more about our affiliate program:</strong> <a href="https://www.reelforgeai.io/become-affiliate">https://www.reelforgeai.io/become-affiliate</a></p>
<p><strong>Try ReelForge AI for free:</strong> <a href="https://reelforgeai.io">https://reelforgeai.io</a></p>
<p>Best,<br>Larry Barksdale<br>ReelForge AI</p>',
    'Hi {{first_name}}, I just watched your video on {{competitor}} and was genuinely impressed. I am Larry Barksdale from ReelForge AI. We offer 30% lifetime recurring commissions. Learn more: https://www.reelforgeai.io/become-affiliate | Try free: https://reelforgeai.io -Larry',
    TRUE
) ON CONFLICT (name) DO UPDATE SET
    html_template = EXCLUDED.html_template,
    subject_template = EXCLUDED.subject_template,
    text_template = EXCLUDED.text_template;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES (
    'youtube_followup_1',
    '{{first_name}}, creators are earning $1k+/mo with this—your link inside',
    '<p>Hi {{first_name}},</p>
<p>Quick follow-up on my note about partnering with ReelForge AI. I know your time is valuable, so I''ll keep this brief.</p>
<p>Since launching, creators in our program have generated $500–$2,000/month simply by sharing honest reviews. One partner (a tech reviewer like you) hit $1,500 in their first month alone—purely from audience trust.</p>
<p>Here''s your exclusive signup link to get started with 30% lifetime commissions: <a href="{{affiliate_link}}">{{affiliate_link}}</a></p>
<p>Spots are filling up fast—any thoughts or questions? Just hit reply.</p>
<p>Best,<br>Larry Barksdale</p>',
    'Hi {{first_name}}, Quick follow-up on ReelForge. Creators earn $500-2000/month. Your link: {{affiliate_link}} -Larry',
    TRUE
) ON CONFLICT (name) DO UPDATE SET 
    html_template = EXCLUDED.html_template, 
    subject_template = EXCLUDED.subject_template,
    text_template = EXCLUDED.text_template;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES (
    'youtube_followup_2',
    'Last chance, {{first_name}}: Free Pro access + 30% commissions',
    '<p>Hi {{first_name}},</p>
<p>This is my final reach-out—I respect your inbox!</p>
<p>To make it a no-brainer, if you join this week, you''ll get:</p>
<ul>
<li>3 months FREE ReelForge Pro ($150 value) to test and create content effortlessly</li>
<li>Dedicated priority support for any questions</li>
<li>Exclusive early access to beta features before anyone else</li>
</ul>
<p>Plus, that <strong>30% lifetime recurring commission</strong> on referrals. Don''t miss out—others are already building passive income streams.</p>
<p>Sign up here: <a href="{{affiliate_link}}">{{affiliate_link}}</a></p>
<p>Keep crushing it with your content either way!</p>
<p>Best,<br>Larry Barksdale</p>',
    'Hi {{first_name}}, Last chance! Join this week for 3 months FREE Pro + 30% lifetime commissions. Link: {{affiliate_link}} -Larry',
    TRUE
) ON CONFLICT (name) DO UPDATE SET 
    html_template = EXCLUDED.html_template, 
    subject_template = EXCLUDED.subject_template,
    text_template = EXCLUDED.text_template;

-- Placeholder templates for other platforms (can be customized later)
INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES (
    'tiktok_initial',
    'Your AI content is amazing - partnership opportunity?',
    '<p>Hi {{first_name}},</p>
<p>I''ve been following your TikTok content and love your take on AI tools!</p>
<p>I''m Larry Barksdale from ReelForge AI. We''re offering select creators <strong>30% lifetime commissions</strong> on referrals.</p>
<p>Interested in learning more?</p>
<p><strong>Learn more about our affiliate program:</strong> <a href="https://www.reelforgeai.io/become-affiliate">https://www.reelforgeai.io/become-affiliate</a></p>
<p><strong>Try ReelForge AI for free:</strong> <a href="https://reelforgeai.io">https://reelforgeai.io</a></p>
<p>Best,<br>Larry Barksdale<br>ReelForge AI</p>',
    'Hi {{first_name}}, Love your TikTok content! I am Larry from ReelForge AI offering 30% lifetime commissions. Learn more: https://www.reelforgeai.io/become-affiliate | Try free: https://reelforgeai.io -Larry',
    TRUE
) ON CONFLICT (name) DO UPDATE SET
    html_template = EXCLUDED.html_template,
    subject_template = EXCLUDED.subject_template,
    text_template = EXCLUDED.text_template;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES (
    'tiktok_followup',
    'Following up - 30% recurring commission',
    '<p>Hi {{first_name}},</p>
<p>Quick follow-up on partnering with ReelForge AI.</p>
<p>Your signup link: <a href="{{affiliate_link}}">{{affiliate_link}}</a></p>
<p>Best,<br>Larry Barksdale</p>',
    'Hi {{first_name}}, Following up on ReelForge partnership. Your link: {{affiliate_link}} -Larry',
    TRUE
) ON CONFLICT (name) DO UPDATE SET 
    html_template = EXCLUDED.html_template, 
    subject_template = EXCLUDED.subject_template;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES (
    'instagram_initial',
    'Love your content - affiliate opportunity',
    '<p>Hi {{first_name}},</p>
<p>I''ve been enjoying your Instagram content about AI and creative tools!</p>
<p>I''m Larry Barksdale from ReelForge AI. We''re offering <strong>30% lifetime commissions</strong> to creators like you.</p>
<p>Want to learn more?</p>
<p><strong>Learn more about our affiliate program:</strong> <a href="https://www.reelforgeai.io/become-affiliate">https://www.reelforgeai.io/become-affiliate</a></p>
<p><strong>Try ReelForge AI for free:</strong> <a href="https://reelforgeai.io">https://reelforgeai.io</a></p>
<p>Best,<br>Larry Barksdale<br>ReelForge AI</p>',
    'Hi {{first_name}}, Love your Instagram content! I am Larry from ReelForge AI offering 30% lifetime commissions. Learn more: https://www.reelforgeai.io/become-affiliate | Try free: https://reelforgeai.io -Larry',
    TRUE
) ON CONFLICT (name) DO UPDATE SET
    html_template = EXCLUDED.html_template,
    subject_template = EXCLUDED.subject_template,
    text_template = EXCLUDED.text_template;

INSERT INTO email_templates (name, subject_template, html_template, text_template, is_active)
VALUES (
    'instagram_followup',
    'Quick follow-up on ReelForge partnership',
    '<p>Hi {{first_name}},</p>
<p>Just following up on my previous message about the ReelForge affiliate program.</p>
<p>Your signup link: <a href="{{affiliate_link}}">{{affiliate_link}}</a></p>
<p>Best,<br>Larry Barksdale</p>',
    'Hi {{first_name}}, Following up on ReelForge. Your link: {{affiliate_link}} -Larry',
    TRUE
) ON CONFLICT (name) DO UPDATE SET 
    html_template = EXCLUDED.html_template, 
    subject_template = EXCLUDED.subject_template;
