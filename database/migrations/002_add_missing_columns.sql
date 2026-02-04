-- Migration: Add missing columns to competitor_keywords table
-- Run this if you get "column does not exist" errors

-- Add results_count column if missing
ALTER TABLE competitor_keywords
ADD COLUMN IF NOT EXISTS results_count INTEGER DEFAULT 0;

-- Add priority column if missing
ALTER TABLE competitor_keywords
ADD COLUMN IF NOT EXISTS priority INTEGER DEFAULT 0;

-- Add last_searched_at column if missing
ALTER TABLE competitor_keywords
ADD COLUMN IF NOT EXISTS last_searched_at TIMESTAMP;

-- Verify columns exist
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'competitor_keywords';
