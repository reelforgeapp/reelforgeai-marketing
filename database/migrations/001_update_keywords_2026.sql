-- Migration: Update Discovery Keywords for 2026 Industry Trends
-- Run this migration to add new trending AI video tool keywords

-- First, set priority on existing high-value keywords
UPDATE competitor_keywords SET priority = 5 WHERE keyword IN (
    'pictory review', 'invideo ai review', 'heygen review', 'synthesia review'
);

-- Disable outdated/low-performing keywords (can be re-enabled via API)
UPDATE competitor_keywords SET is_active = FALSE WHERE keyword IN (
    'lumen5 review', 'lumen5 tutorial'  -- Lower search volume
);

-- =====================================================
-- NEW 2025-2026 TRENDING AI VIDEO TOOLS
-- =====================================================

-- Sora (OpenAI's video generation - massive search volume)
INSERT INTO competitor_keywords (competitor_name, keyword, platform, is_active, priority) VALUES
    ('Sora', 'sora ai review', 'youtube', TRUE, 10),
    ('Sora', 'sora openai tutorial', 'youtube', TRUE, 10),
    ('Sora', 'sora vs runway', 'youtube', TRUE, 8),
    ('Sora', 'sora video generator', 'youtube', TRUE, 9)
ON CONFLICT DO NOTHING;

-- Kling AI (Chinese competitor, trending globally)
INSERT INTO competitor_keywords (competitor_name, keyword, platform, is_active, priority) VALUES
    ('Kling AI', 'kling ai review', 'youtube', TRUE, 8),
    ('Kling AI', 'kling ai tutorial', 'youtube', TRUE, 7),
    ('Kling AI', 'kling vs sora', 'youtube', TRUE, 6)
ON CONFLICT DO NOTHING;

-- Pika Labs (fast-growing AI video startup)
INSERT INTO competitor_keywords (competitor_name, keyword, platform, is_active, priority) VALUES
    ('Pika Labs', 'pika ai review', 'youtube', TRUE, 8),
    ('Pika Labs', 'pika labs tutorial', 'youtube', TRUE, 7),
    ('Pika Labs', 'pika 1.0 review', 'youtube', TRUE, 7)
ON CONFLICT DO NOTHING;

-- Veo (Google's video AI)
INSERT INTO competitor_keywords (competitor_name, keyword, platform, is_active, priority) VALUES
    ('Veo', 'google veo review', 'youtube', TRUE, 8),
    ('Veo', 'veo ai tutorial', 'youtube', TRUE, 7)
ON CONFLICT DO NOTHING;

-- Luma Dream Machine (popular for 3D video generation)
INSERT INTO competitor_keywords (competitor_name, keyword, platform, is_active, priority) VALUES
    ('Luma', 'luma dream machine review', 'youtube', TRUE, 7),
    ('Luma', 'luma ai tutorial', 'youtube', TRUE, 6)
ON CONFLICT DO NOTHING;

-- CapCut AI features (huge user base, trending)
INSERT INTO competitor_keywords (competitor_name, keyword, platform, is_active, priority) VALUES
    ('CapCut', 'capcut ai features review', 'youtube', TRUE, 8),
    ('CapCut', 'capcut ai editing tutorial', 'youtube', TRUE, 7),
    ('CapCut', 'capcut vs invideo', 'youtube', TRUE, 6)
ON CONFLICT DO NOTHING;

-- OpusClip (AI clip generation - hot for content repurposing)
INSERT INTO competitor_keywords (competitor_name, keyword, platform, is_active, priority) VALUES
    ('OpusClip', 'opus clip review', 'youtube', TRUE, 8),
    ('OpusClip', 'opus clip tutorial', 'youtube', TRUE, 7),
    ('OpusClip', 'best ai clip generator', 'youtube', TRUE, 7)
ON CONFLICT DO NOTHING;

-- Topaz Video AI (upscaling/enhancement - premium market)
INSERT INTO competitor_keywords (competitor_name, keyword, platform, is_active, priority) VALUES
    ('Topaz', 'topaz video ai review', 'youtube', TRUE, 6),
    ('Topaz', 'topaz video enhance tutorial', 'youtube', TRUE, 5)
ON CONFLICT DO NOTHING;

-- Eleven Labs (voice AI, often paired with video tools)
INSERT INTO competitor_keywords (competitor_name, keyword, platform, is_active, priority) VALUES
    ('ElevenLabs', 'eleven labs review', 'youtube', TRUE, 7),
    ('ElevenLabs', 'eleven labs tutorial', 'youtube', TRUE, 6),
    ('ElevenLabs', 'best ai voice generator', 'youtube', TRUE, 7)
ON CONFLICT DO NOTHING;

-- =====================================================
-- HIGH-VALUE COMPARISON & CATEGORY KEYWORDS
-- =====================================================

INSERT INTO competitor_keywords (competitor_name, keyword, platform, is_active, priority) VALUES
    ('General', 'best ai video generator 2026', 'youtube', TRUE, 10),
    ('General', 'ai video editing tools comparison', 'youtube', TRUE, 9),
    ('General', 'text to video ai review', 'youtube', TRUE, 8),
    ('General', 'ai avatar video creator', 'youtube', TRUE, 8),
    ('General', 'ai youtube shorts generator', 'youtube', TRUE, 9),
    ('General', 'ai faceless video creator', 'youtube', TRUE, 8),
    ('General', 'best ai tools for youtubers', 'youtube', TRUE, 9),
    ('General', 'sora vs pictory vs invideo', 'youtube', TRUE, 7),
    ('General', 'ai video monetization', 'youtube', TRUE, 6),
    ('General', 'passive income ai videos', 'youtube', TRUE, 7)
ON CONFLICT DO NOTHING;

-- Update priority on existing high-value general keywords
UPDATE competitor_keywords SET priority = 8 WHERE keyword = 'ai video generator';
UPDATE competitor_keywords SET priority = 8 WHERE keyword = 'ai video editing';
UPDATE competitor_keywords SET priority = 9 WHERE keyword = 'best ai video tools';
UPDATE competitor_keywords SET priority = 7 WHERE keyword = 'ai video maker';

-- =====================================================
-- SUMMARY OF CHANGES
-- =====================================================
-- Added: ~35 new trending keywords
-- Priority scale: 1-10 (10 = highest search volume/relevance)
-- Focus: Sora, Kling, Pika, Veo, CapCut, OpusClip (2025-2026 breakout tools)
-- Strategy: Comparison keywords tend to attract viewers actively evaluating tools
