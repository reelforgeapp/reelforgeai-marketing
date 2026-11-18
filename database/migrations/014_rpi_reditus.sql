-- RPI 014: a read-only copy of the Reditus affiliate program (rpi.attribution.reditus).
-- The rpi-reditus-sync task refreshes these tables from the Reditus API every day and
-- links each affiliate to the RPI creator it belongs to. RPI never writes to Reditus.
-- No customer email is stored: a referral keeps Reditus' uid, which is the product
-- user id (= rpi_customers.customer_ref). A partner's login email is kept only to
-- match the affiliate to a creator's contacts.
-- (012 and 013 are claimed by work not yet merged.)
CREATE TABLE IF NOT EXISTS rpi_reditus_partnerships (
    partnership_id TEXT PRIMARY KEY,
    partner_id TEXT NOT NULL,
    slug TEXT,                              -- ?red= value of the affiliate link = campaign referral_code
    referral_link TEXT,
    name TEXT,
    email_normalized TEXT,
    website_url TEXT,
    country TEXT,
    state TEXT NOT NULL,                    -- pending, accepted, declined, terminated
    tier_name TEXT,
    referrals_count INTEGER NOT NULL DEFAULT 0,       -- as Reditus reports it (clicks included)
    paid_referrals_count INTEGER NOT NULL DEFAULT 0,
    total_sold_reported NUMERIC,                      -- Reditus' figure, unit undocumented
    commission_generated_reported NUMERIC,            -- Reditus' figure, unit undocumented
    currency TEXT,
    has_payout_method BOOLEAN NOT NULL DEFAULT FALSE, -- a PayPal address is on file
    applied_at TIMESTAMPTZ,
    state_set_at TIMESTAMPTZ,
    creator_id UUID REFERENCES rpi_creators(id) ON DELETE SET NULL,
    campaign_id UUID REFERENCES rpi_campaigns(id) ON DELETE SET NULL,
    match_basis TEXT,                       -- campaign_code, email, conversation_email, website
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_reditus_partnerships_slug ON rpi_reditus_partnerships (lower(slug));
CREATE INDEX IF NOT EXISTS idx_rpi_reditus_partnerships_creator ON rpi_reditus_partnerships (creator_id);

CREATE TABLE IF NOT EXISTS rpi_reditus_referrals (
    referral_id TEXT PRIMARY KEY,
    partner_id TEXT,
    customer_ref TEXT,                      -- Reditus uid (product user id); NULL for a bare click
    state TEXT NOT NULL,                    -- new, clicked, converted, paid, churned, rejected
    reject_reason TEXT,
    created_at TIMESTAMPTZ,
    state_set_at TIMESTAMPTZ,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_reditus_referrals_customer ON rpi_reditus_referrals (customer_ref)
    WHERE customer_ref IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_rpi_reditus_referrals_partner ON rpi_reditus_referrals (partner_id);

CREATE TABLE IF NOT EXISTS rpi_reditus_commissions (
    commission_id TEXT PRIMARY KEY,
    partner_id TEXT,
    referral_id TEXT,
    state TEXT NOT NULL,                    -- pending, rejected, paid
    amount_minor BIGINT NOT NULL,
    currency TEXT NOT NULL,
    one_off_reason TEXT,
    reject_reason TEXT,
    created_at TIMESTAMPTZ,
    paid_at TIMESTAMPTZ,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_rpi_reditus_commissions_partner ON rpi_reditus_commissions (partner_id);

INSERT INTO rpi_schema_migrations (version) VALUES ('014_rpi_reditus') ON CONFLICT DO NOTHING;
