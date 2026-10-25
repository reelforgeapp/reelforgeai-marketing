-- Rollback for 005_rpi_core.sql
-- Drops every rpi_* table. Existing Partner Scout tables are untouched.
-- DESTRUCTIVE: export rpi_ledger_entries / rpi_billing_events first if they
-- hold real financial history (see docs/rpi/DEPLOYMENT_RUNBOOK.md).

BEGIN;

DROP TABLE IF EXISTS
    rpi_benchmark_labels,
    rpi_oauth_states,
    rpi_oauth_grants,
    rpi_ledger_entries,
    rpi_fx_rates,
    rpi_billing_events,
    rpi_customers,
    rpi_attribution_touches,
    rpi_campaigns,
    rpi_deals,
    rpi_outreach_events,
    rpi_outreach_drafts,
    rpi_outreach_settings,
    rpi_suppression_list,
    rpi_notes,
    rpi_pipeline_events,
    rpi_qualification_scores,
    rpi_discovery_candidates,
    rpi_discovery_runs,
    rpi_research_jobs,
    rpi_saved_searches,
    rpi_icp_profiles,
    rpi_creator_contacts,
    rpi_audience_quality_signals,
    rpi_sponsorship_relationships,
    rpi_competitors,
    rpi_content_classifications,
    rpi_creator_content,
    rpi_audience_estimates,
    rpi_creator_profile_snapshots,
    rpi_creator_identity_matches,
    rpi_creator_social_accounts,
    rpi_creators,
    rpi_spend_ledger,
    rpi_budget_change_requests,
    rpi_budgets,
    rpi_provider_cache,
    rpi_provider_requests,
    rpi_data_retention_rules,
    rpi_data_provenance,
    rpi_alerts,
    rpi_audit_log,
    rpi_principals,
    rpi_workspaces
CASCADE;

DELETE FROM rpi_schema_migrations WHERE version = '005_rpi_core';

COMMIT;
