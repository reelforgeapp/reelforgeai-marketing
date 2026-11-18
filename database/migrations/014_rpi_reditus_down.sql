DROP TABLE IF EXISTS rpi_reditus_commissions;
DROP TABLE IF EXISTS rpi_reditus_referrals;
DROP TABLE IF EXISTS rpi_reditus_partnerships;
DELETE FROM rpi_schema_migrations WHERE version = '014_rpi_reditus';
