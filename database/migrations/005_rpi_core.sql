-- Migration 005: ReelForge Partner Intelligence (RPI) core schema
--
-- Additive only. Creates rpi_* tables next to the existing Partner Scout
-- tables (marketing_prospects, outreach_sequences, ...) without altering them.
-- Rollback: database/migrations/005_rpi_core_down.sql
--
-- Conventions
--   * Money is stored as NUMERIC (USD unless a currency column says otherwise).
--   * Every provider-derived row references rpi_data_provenance.
--   * workspace_id exists on root entities so the schema can become
--     multi-tenant later; today there is one default workspace.

BEGIN;

CREATE TABLE IF NOT EXISTS rpi_schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------------------
-- Workspace & access
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rpi_workspaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO rpi_workspaces (id, name)
VALUES ('00000000-0000-0000-0000-000000000001', 'ReelForge AI')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS rpi_principals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES rpi_workspaces(id) DEFAULT '00000000-0000-0000-0000-000000000001',
    name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('viewer', 'analyst', 'approver', 'admin', 'ingest')),
    key_hash TEXT NOT NULL UNIQUE,
    key_prefix TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS rpi_audit_log (
    id BIGSERIAL PRIMARY KEY,
    workspace_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT,
    entity_id TEXT,
    detail JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_audit_entity ON rpi_audit_log(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_rpi_audit_created ON rpi_audit_log(created_at DESC);

CREATE TABLE IF NOT EXISTS rpi_alerts (
    id BIGSERIAL PRIMARY KEY,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    message TEXT NOT NULL,
    dedupe_key TEXT UNIQUE,
    acknowledged_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------------------
-- Provenance, provider requests, caching, spending
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rpi_data_provenance (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider TEXT NOT NULL,
    operation TEXT NOT NULL,
    source_record TEXT,
    retrieved_at TIMESTAMPTZ NOT NULL,
    observed_at TIMESTAMPTZ,
    data_type TEXT NOT NULL,
    measurement TEXT NOT NULL,
    confidence REAL,
    completeness REAL,
    retention_policy TEXT NOT NULL,
    commercial_use TEXT NOT NULL,
    raw_ref TEXT,
    response_version TEXT,
    derived_metrics_prohibited BOOLEAN NOT NULL DEFAULT FALSE,
    field_measurements JSONB NOT NULL DEFAULT '{}',
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_prov_expires ON rpi_data_provenance(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_rpi_prov_provider ON rpi_data_provenance(provider);

CREATE TABLE IF NOT EXISTS rpi_data_retention_rules (
    provider TEXT NOT NULL,
    data_class TEXT NOT NULL,
    max_age_days INTEGER,
    action TEXT NOT NULL CHECK (action IN ('delete', 'refresh_or_delete', 'anonymize', 'keep')),
    notes TEXT,
    PRIMARY KEY (provider, data_class)
);
INSERT INTO rpi_data_retention_rules (provider, data_class, max_age_days, action, notes) VALUES
    ('youtube_data_api', '*', 30, 'refresh_or_delete', 'YouTube Developer Policies III.E.4(c)/(d)'),
    ('youtube_analytics_api', '*', 30, 'refresh_or_delete', 'YouTube Developer Policies III.E.4(b)'),
    ('creatordb', '*', 90, 'refresh_or_delete', 'Placeholder pending CreatorDB terms review'),
    ('sparktoro', '*', 30, 'delete', 'Placeholder pending SparkToro API terms review'),
    ('supadata', 'transcript', 30, 'delete', 'Transcripts kept only as short evidence excerpts'),
    ('fixture', '*', 7, 'delete', 'Synthetic development data')
ON CONFLICT (provider, data_class) DO NOTHING;

CREATE TABLE IF NOT EXISTS rpi_provider_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider TEXT NOT NULL,
    operation TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    job_id UUID,
    creator_id UUID,
    status TEXT NOT NULL,
    http_status INTEGER,
    attempts INTEGER NOT NULL DEFAULT 1,
    cost_units NUMERIC(14, 4) NOT NULL DEFAULT 0,
    cost_usd NUMERIC(14, 6) NOT NULL DEFAULT 0,
    cost_reported_by_provider BOOLEAN NOT NULL DEFAULT FALSE,
    duration_ms INTEGER,
    error_code TEXT,
    error TEXT,
    provider_request_id TEXT,
    response_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_req_provider_created ON rpi_provider_requests(provider, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rpi_req_hash ON rpi_provider_requests(request_hash);

CREATE TABLE IF NOT EXISTS rpi_provider_cache (
    cache_key TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    operation TEXT NOT NULL,
    payload JSONB NOT NULL,
    provenance_id UUID REFERENCES rpi_data_provenance(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rpi_cache_expires ON rpi_provider_cache(expires_at);

CREATE TABLE IF NOT EXISTS rpi_budgets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    scope TEXT NOT NULL CHECK (scope IN ('global', 'provider')),
    period TEXT NOT NULL CHECK (period IN ('daily', 'monthly')),
    provider TEXT,
    limit_usd NUMERIC(12, 2) NOT NULL CHECK (limit_usd >= 0),
    alert_ratio NUMERIC(4, 3) NOT NULL DEFAULT 0.800,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((scope = 'provider') = (provider IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_rpi_budget_scope
    ON rpi_budgets(workspace_id, scope, period, COALESCE(provider, '')) WHERE active;

-- Initial planning budget: $200/month software+data (directive: $100-$250).
INSERT INTO rpi_budgets (scope, period, provider, limit_usd, alert_ratio) VALUES
    ('global', 'monthly', NULL, 200.00, 0.800),
    ('global', 'daily', NULL, 25.00, 0.800),
    ('provider', 'monthly', 'creatordb', 120.00, 0.800),
    ('provider', 'monthly', 'sparktoro', 40.00, 0.800),
    ('provider', 'monthly', 'supadata', 15.00, 0.800),
    ('provider', 'monthly', 'anthropic', 30.00, 0.800)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS rpi_budget_change_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    budget_id UUID NOT NULL REFERENCES rpi_budgets(id),
    requested_limit_usd NUMERIC(12, 2) NOT NULL CHECK (requested_limit_usd >= 0),
    reason TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
    decided_by TEXT,
    decided_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rpi_spend_ledger (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider TEXT NOT NULL,
    operation TEXT NOT NULL,
    job_id UUID,
    creator_id UUID,
    reserved_usd NUMERIC(14, 6) NOT NULL DEFAULT 0,
    actual_usd NUMERIC(14, 6) NOT NULL DEFAULT 0,
    actual_units NUMERIC(14, 4) NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (status IN ('reserved', 'committed', 'released', 'denied')),
    priority TEXT NOT NULL DEFAULT 'normal',
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settled_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_rpi_spend_created ON rpi_spend_ledger(created_at);
CREATE INDEX IF NOT EXISTS idx_rpi_spend_job ON rpi_spend_ledger(job_id) WHERE job_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_rpi_spend_creator ON rpi_spend_ledger(creator_id) WHERE creator_id IS NOT NULL;

-- -------------------------------------------------------------------------
-- Creator identity graph
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rpi_creators (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES rpi_workspaces(id) DEFAULT '00000000-0000-0000-0000-000000000001',
    display_name TEXT,
    primary_platform TEXT NOT NULL DEFAULT 'youtube',
    primary_language TEXT,
    country TEXT,
    pipeline_status TEXT NOT NULL DEFAULT 'discovered',
    funnel_stage INTEGER NOT NULL DEFAULT 1 CHECK (funnel_stage BETWEEN 1 AND 5),
    disqualified_reason TEXT,
    merged_into_id UUID REFERENCES rpi_creators(id),
    legacy_prospect_id UUID,
    next_action TEXT,
    next_action_due DATE,
    owner TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_creators_status ON rpi_creators(workspace_id, pipeline_status);
CREATE INDEX IF NOT EXISTS idx_rpi_creators_stage ON rpi_creators(workspace_id, funnel_stage);
CREATE INDEX IF NOT EXISTS idx_rpi_creators_followup ON rpi_creators(next_action_due) WHERE next_action_due IS NOT NULL;

CREATE TABLE IF NOT EXISTS rpi_creator_social_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    platform TEXT NOT NULL,
    platform_account_id TEXT NOT NULL,
    handle TEXT,
    handle_normalized TEXT,
    url TEXT,
    provider_creator_ids JSONB NOT NULL DEFAULT '{}',
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (platform, platform_account_id)
);
CREATE INDEX IF NOT EXISTS idx_rpi_accounts_creator ON rpi_creator_social_accounts(creator_id);
CREATE INDEX IF NOT EXISTS idx_rpi_accounts_handle ON rpi_creator_social_accounts(platform, handle_normalized);

CREATE TABLE IF NOT EXISTS rpi_creator_identity_matches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_a UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    creator_b UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    decision TEXT NOT NULL CHECK (decision IN ('auto_merged', 'pending_review', 'confirmed', 'rejected')),
    confidence REAL NOT NULL,
    signals JSONB NOT NULL DEFAULT '[]',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (creator_a <> creator_b),
    UNIQUE (creator_a, creator_b)
);

CREATE TABLE IF NOT EXISTS rpi_creator_profile_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    social_account_id UUID REFERENCES rpi_creator_social_accounts(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    subscribers BIGINT,
    avg_views DOUBLE PRECISION,
    median_views DOUBLE PRECISION,
    engagement_rate DOUBLE PRECISION,
    total_videos INTEGER,
    uploads_last_90d INTEGER,
    last_upload_at TIMESTAMPTZ,
    country TEXT,
    language TEXT,
    categories TEXT[] NOT NULL DEFAULT '{}',
    description TEXT,
    provenance_id UUID NOT NULL REFERENCES rpi_data_provenance(id) ON DELETE CASCADE,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_snapshots_creator ON rpi_creator_profile_snapshots(creator_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS rpi_audience_estimates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    dimension TEXT NOT NULL,
    bucket TEXT NOT NULL,
    share DOUBLE PRECISION NOT NULL CHECK (share >= 0 AND share <= 1),
    measurement TEXT NOT NULL,
    provenance_id UUID NOT NULL REFERENCES rpi_data_provenance(id) ON DELETE CASCADE,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_audience_creator ON rpi_audience_estimates(creator_id, dimension);

CREATE TABLE IF NOT EXISTS rpi_creator_content (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    platform TEXT NOT NULL,
    platform_content_id TEXT NOT NULL,
    title TEXT,
    description TEXT,
    url TEXT,
    published_at TIMESTAMPTZ,
    duration_seconds INTEGER,
    is_short BOOLEAN,
    views BIGINT,
    likes BIGINT,
    comments BIGINT,
    sponsored_flag BOOLEAN,
    transcript_excerpt TEXT,
    transcript_provenance_id UUID REFERENCES rpi_data_provenance(id) ON DELETE SET NULL,
    provenance_id UUID NOT NULL REFERENCES rpi_data_provenance(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (platform, platform_content_id)
);
CREATE INDEX IF NOT EXISTS idx_rpi_content_creator ON rpi_creator_content(creator_id, published_at DESC);

CREATE TABLE IF NOT EXISTS rpi_content_classifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    content_id UUID REFERENCES rpi_creator_content(id) ON DELETE CASCADE,
    taxonomy_version TEXT NOT NULL,
    method TEXT NOT NULL,          -- keyword_prefilter | claude_fast | claude_deep | manual
    model TEXT,
    prompt_version TEXT,
    labels JSONB NOT NULL DEFAULT '[]',
    relevance REAL,
    audience_problem TEXT,
    product_capabilities JSONB NOT NULL DEFAULT '[]',
    buyer_intent JSONB NOT NULL DEFAULT '[]',
    evidence JSONB NOT NULL DEFAULT '[]',
    confidence REAL,
    uncertainty_reasons JSONB NOT NULL DEFAULT '[]',
    source_providers TEXT[] NOT NULL DEFAULT '{}',
    cost_usd NUMERIC(12, 6) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_class_creator ON rpi_content_classifications(creator_id, created_at DESC);

CREATE TABLE IF NOT EXISTS rpi_competitors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    domains TEXT[] NOT NULL DEFAULT '{}',
    aliases TEXT[] NOT NULL DEFAULT '{}',
    category TEXT NOT NULL,
    overlap TEXT NOT NULL DEFAULT 'adjacent' CHECK (overlap IN ('direct', 'adjacent', 'complementary')),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rpi_sponsorship_relationships (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    brand_name TEXT NOT NULL,
    brand_domain TEXT,
    competitor_id UUID REFERENCES rpi_competitors(id) ON DELETE SET NULL,
    relationship_type TEXT NOT NULL CHECK (relationship_type IN
        ('confirmed_paid', 'disclosed_affiliate', 'possible_promotional', 'ordinary_mention')),
    content_id UUID REFERENCES rpi_creator_content(id) ON DELETE CASCADE,
    evidence_url TEXT,
    evidence JSONB NOT NULL DEFAULT '[]',
    published_at TIMESTAMPTZ,
    source TEXT NOT NULL,
    provenance_id UUID REFERENCES rpi_data_provenance(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (creator_id, brand_name, content_id, relationship_type)
);

CREATE TABLE IF NOT EXISTS rpi_audience_quality_signals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    signal_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('info', 'review', 'elevated')),
    value DOUBLE PRECISION,
    detail TEXT NOT NULL,
    data_basis JSONB NOT NULL DEFAULT '{}',
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_quality_creator ON rpi_audience_quality_signals(creator_id);

CREATE TABLE IF NOT EXISTS rpi_creator_contacts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    contact_type TEXT NOT NULL CHECK (contact_type IN ('email', 'form', 'agency', 'social_dm')),
    value TEXT NOT NULL,
    value_normalized TEXT NOT NULL,
    source TEXT NOT NULL,          -- provider name, 'creator_supplied', 'operator'
    is_business_contact BOOLEAN,
    verification_status TEXT NOT NULL DEFAULT 'unverified',
    verified_at TIMESTAMPTZ,
    provenance_id UUID REFERENCES rpi_data_provenance(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (creator_id, contact_type, value_normalized)
);
CREATE INDEX IF NOT EXISTS idx_rpi_contacts_value ON rpi_creator_contacts(value_normalized);

-- -------------------------------------------------------------------------
-- Discovery, jobs and qualification
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rpi_icp_profiles (
    key TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    config JSONB NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rpi_saved_searches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    name TEXT NOT NULL,
    criteria JSONB NOT NULL,
    strategies TEXT[] NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rpi_research_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN
        ('queued', 'running', 'paused_budget', 'awaiting_approval', 'succeeded', 'failed', 'canceled')),
    priority TEXT NOT NULL DEFAULT 'normal',
    params JSONB NOT NULL DEFAULT '{}',
    progress JSONB NOT NULL DEFAULT '{}',
    budget_usd NUMERIC(12, 4),
    spent_usd NUMERIC(14, 6) NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_rpi_jobs_status ON rpi_research_jobs(status, created_at);

CREATE TABLE IF NOT EXISTS rpi_discovery_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    job_id UUID REFERENCES rpi_research_jobs(id) ON DELETE SET NULL,
    criteria JSONB NOT NULL,
    strategies TEXT[] NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    total_found INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    new_creators INTEGER NOT NULL DEFAULT 0,
    preliminary_qualified INTEGER NOT NULL DEFAULT 0,
    niche_distribution JSONB NOT NULL DEFAULT '{}',
    strategy_breakdown JSONB NOT NULL DEFAULT '{}',
    estimated_enrichment_cost_usd NUMERIC(12, 4) NOT NULL DEFAULT 0,
    actual_cost_usd NUMERIC(12, 6) NOT NULL DEFAULT 0,
    warnings JSONB NOT NULL DEFAULT '[]',
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS rpi_discovery_candidates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES rpi_discovery_runs(id) ON DELETE CASCADE,
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    strategy TEXT NOT NULL,
    source_provider TEXT NOT NULL,
    inclusion_reasons JSONB NOT NULL DEFAULT '[]',
    missing_fields JSONB NOT NULL DEFAULT '[]',
    was_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
    preliminary_score REAL,
    preliminary_qualified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, creator_id, strategy)
);
CREATE INDEX IF NOT EXISTS idx_rpi_candidates_creator ON rpi_discovery_candidates(creator_id);

CREATE TABLE IF NOT EXISTS rpi_qualification_scores (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    scoring_version TEXT NOT NULL,
    stage INTEGER NOT NULL,
    niche_relevance REAL,
    activity REAL,
    audience_fit JSONB NOT NULL DEFAULT '{}',
    risk_level TEXT,
    commercial_fit REAL,
    overall REAL,
    confidence REAL,
    recommendation TEXT,
    components JSONB NOT NULL DEFAULT '{}',
    evidence JSONB NOT NULL DEFAULT '[]',
    missing_data JSONB NOT NULL DEFAULT '[]',
    excluded_sources JSONB NOT NULL DEFAULT '[]',
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_scores_creator ON rpi_qualification_scores(creator_id, computed_at DESC);

-- -------------------------------------------------------------------------
-- CRM, suppression, outreach
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rpi_pipeline_events (
    id BIGSERIAL PRIMARY KEY,
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    from_status TEXT,
    to_status TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_pipeline_creator ON rpi_pipeline_events(creator_id, created_at);

CREATE TABLE IF NOT EXISTS rpi_notes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    kind TEXT NOT NULL DEFAULT 'note' CHECK (kind IN ('note', 'sponsorship_request', 'negotiation', 'call', 'manual_analytics')),
    body TEXT NOT NULL,
    data JSONB NOT NULL DEFAULT '{}',
    author TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rpi_suppression_list (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind TEXT NOT NULL CHECK (kind IN ('email', 'domain', 'creator')),
    value_hash TEXT NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (kind, value_hash)
);

CREATE TABLE IF NOT EXISTS rpi_outreach_settings (
    workspace_id UUID PRIMARY KEY DEFAULT '00000000-0000-0000-0000-000000000001',
    sending_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    daily_send_limit INTEGER NOT NULL DEFAULT 10 CHECK (daily_send_limit >= 0),
    min_days_between_contacts INTEGER NOT NULL DEFAULT 30,
    sender_name TEXT,
    sender_email TEXT,
    postal_address TEXT,
    enabled_by TEXT,
    enabled_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO rpi_outreach_settings (workspace_id) VALUES ('00000000-0000-0000-0000-000000000001')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS rpi_outreach_drafts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    contact_id UUID REFERENCES rpi_creator_contacts(id) ON DELETE SET NULL,
    subject TEXT NOT NULL,
    body_text TEXT NOT NULL,
    grounding JSONB NOT NULL DEFAULT '[]',
    validation JSONB NOT NULL DEFAULT '{}',
    generator TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN
        ('draft', 'approved', 'rejected', 'sending', 'sent', 'failed', 'blocked')),
    created_by TEXT NOT NULL,
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    sent_at TIMESTAMPTZ,
    provider_message_id TEXT,
    idempotency_key TEXT NOT NULL UNIQUE,
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_drafts_creator ON rpi_outreach_drafts(creator_id, status);
-- At most one in-flight approved/sending message per creator.
CREATE UNIQUE INDEX IF NOT EXISTS uq_rpi_drafts_inflight
    ON rpi_outreach_drafts(creator_id) WHERE status IN ('approved', 'sending');

CREATE TABLE IF NOT EXISTS rpi_outreach_events (
    id BIGSERIAL PRIMARY KEY,
    draft_id UUID REFERENCES rpi_outreach_drafts(id) ON DELETE CASCADE,
    creator_id UUID REFERENCES rpi_creators(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,  -- sent | delivered | bounced | replied | unsubscribed | complaint
    provider_event_id TEXT UNIQUE,
    detail JSONB NOT NULL DEFAULT '{}',
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------------------
-- Deals, campaigns, attribution
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rpi_deals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    structure TEXT NOT NULL CHECK (structure IN
        ('fixed', 'affiliate_only', 'fixed_plus_recurring', 'reduced_fixed_limited_commission', 'fixed_plus_bonus')),
    upfront_fee_usd NUMERIC(12, 2) NOT NULL DEFAULT 0,
    commission_rate NUMERIC(5, 4),
    commission_months INTEGER,
    bonus_terms JSONB NOT NULL DEFAULT '{}',
    deliverables TEXT,
    status TEXT NOT NULL DEFAULT 'proposed' CHECK (status IN
        ('proposed', 'negotiating', 'agreed', 'rejected', 'completed', 'canceled')),
    requires_documented_agreement BOOLEAN NOT NULL DEFAULT FALSE,
    contract_reference TEXT,
    forecast_snapshot JSONB NOT NULL DEFAULT '{}',
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rpi_campaigns (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    creator_id UUID NOT NULL REFERENCES rpi_creators(id),
    deal_id UUID REFERENCES rpi_deals(id),
    name TEXT NOT NULL,
    referral_code TEXT NOT NULL UNIQUE,
    utm_source TEXT,
    utm_medium TEXT,
    utm_campaign TEXT,
    landing_page TEXT,
    start_date DATE,
    end_date DATE,
    attribution_window_days INTEGER NOT NULL DEFAULT 30 CHECK (attribution_window_days > 0),
    status TEXT NOT NULL DEFAULT 'planned' CHECK (status IN ('planned', 'live', 'completed', 'canceled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_rpi_campaign_utm ON rpi_campaigns(utm_campaign) WHERE utm_campaign IS NOT NULL;

CREATE TABLE IF NOT EXISTS rpi_attribution_touches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id UUID REFERENCES rpi_campaigns(id),
    anonymous_id_hash TEXT,
    customer_ref TEXT,
    referral_code TEXT,
    utm_source TEXT,
    utm_medium TEXT,
    utm_campaign TEXT,
    landing_page_variant TEXT,
    consent_state TEXT NOT NULL CHECK (consent_state IN ('granted', 'denied', 'unknown')),
    touched_at TIMESTAMPTZ NOT NULL,
    idempotency_key TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_touches_anon ON rpi_attribution_touches(anonymous_id_hash, touched_at);
CREATE INDEX IF NOT EXISTS idx_rpi_touches_customer ON rpi_attribution_touches(customer_ref, touched_at);

CREATE TABLE IF NOT EXISTS rpi_customers (
    customer_ref TEXT PRIMARY KEY,         -- ReelForge product user id
    billing_customer_id TEXT UNIQUE,       -- e.g. Stripe customer id
    signed_up_at TIMESTAMPTZ,
    first_render_at TIMESTAMPTZ,
    first_export_at TIMESTAMPTZ,
    first_paid_at TIMESTAMPTZ,
    canceled_at TIMESTAMPTZ,
    current_plan TEXT,
    billing_period TEXT,
    first_touch_campaign_id UUID REFERENCES rpi_campaigns(id),
    last_touch_campaign_id UUID REFERENCES rpi_campaigns(id),
    first_touch_at TIMESTAMPTZ,
    last_touch_at TIMESTAMPTZ,
    signup_referral_code TEXT,
    attribution_locked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_customers_first_touch ON rpi_customers(first_touch_campaign_id);
CREATE INDEX IF NOT EXISTS idx_rpi_customers_last_touch ON rpi_customers(last_touch_campaign_id);

-- -------------------------------------------------------------------------
-- Billing events and commercial ledger
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rpi_billing_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider TEXT NOT NULL,
    provider_event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    billing_customer_id TEXT,
    occurred_at TIMESTAMPTZ NOT NULL,
    livemode BOOLEAN,
    payload_sha256 TEXT NOT NULL,
    payload JSONB NOT NULL,
    signature_verified BOOLEAN NOT NULL,
    processing_status TEXT NOT NULL DEFAULT 'received' CHECK (processing_status IN
        ('received', 'processed', 'ignored', 'failed')),
    processing_error TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    UNIQUE (provider, provider_event_id)
);

CREATE TABLE IF NOT EXISTS rpi_fx_rates (
    rate_date DATE NOT NULL,
    currency TEXT NOT NULL,
    usd_per_unit NUMERIC(18, 8) NOT NULL CHECK (usd_per_unit > 0),
    source TEXT NOT NULL,
    PRIMARY KEY (rate_date, currency)
);

CREATE TABLE IF NOT EXISTS rpi_ledger_entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_ref TEXT,
    campaign_id UUID REFERENCES rpi_campaigns(id),
    entry_type TEXT NOT NULL CHECK (entry_type IN (
        'subscription_charge', 'renewal', 'refund', 'discount', 'chargeback', 'tax',
        'affiliate_commission', 'influencer_fee', 'render_cost', 'storage_cost', 'delivery_cost',
        'ai_cost', 'payment_fee', 'other_variable_cost', 'other_campaign_cost')),
    amount_minor BIGINT NOT NULL,          -- signed, in currency minor units; revenue positive, costs negative
    currency TEXT NOT NULL,
    amount_usd NUMERIC(14, 4) NOT NULL,    -- signed
    fx_source TEXT NOT NULL,
    plan TEXT,
    billing_period TEXT,
    occurred_at TIMESTAMPTZ NOT NULL,
    is_estimate BOOLEAN NOT NULL DEFAULT FALSE,
    estimate_basis TEXT,
    source TEXT NOT NULL,                  -- billing_webhook | product_cost_feed | operator | allocation
    source_event_id UUID REFERENCES rpi_billing_events(id),
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_ledger_customer ON rpi_ledger_entries(customer_ref, occurred_at);
CREATE INDEX IF NOT EXISTS idx_rpi_ledger_campaign ON rpi_ledger_entries(campaign_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_rpi_ledger_type ON rpi_ledger_entries(entry_type, occurred_at);

-- -------------------------------------------------------------------------
-- Creator-authorized analytics (YouTube Analytics OAuth)
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rpi_oauth_grants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    scopes TEXT[] NOT NULL,
    encrypted_refresh_token TEXT NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reconfirm_by TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    revocation_source TEXT,
    data_delete_by TIMESTAMPTZ,
    UNIQUE (creator_id, provider)
);

CREATE TABLE IF NOT EXISTS rpi_oauth_states (
    state_hash TEXT PRIMARY KEY,
    creator_id UUID NOT NULL REFERENCES rpi_creators(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ
);

-- -------------------------------------------------------------------------
-- Evaluation benchmark (manually labeled creators)
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rpi_benchmark_labels (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    platform TEXT NOT NULL,
    platform_account_id TEXT NOT NULL,
    segment TEXT NOT NULL,
    relevant BOOLEAN NOT NULL,
    labels TEXT[] NOT NULL DEFAULT '{}',
    labeled_by TEXT NOT NULL,
    notes TEXT,
    labeled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (platform, platform_account_id)
);

INSERT INTO rpi_schema_migrations (version) VALUES ('005_rpi_core') ON CONFLICT DO NOTHING;

COMMIT;
