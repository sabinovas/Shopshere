-- Delta Sharing: give a partner read access to gold.product_performance (Week 8)
-- Run on a SQL warehouse as a metastore admin or a user with CREATE SHARE / CREATE RECIPIENT.
-- Statements separated by ';;' lines (for run_sql.ps1).

-- 1. The share (internal: nobody can read it until a recipient is granted)
CREATE SHARE IF NOT EXISTS shopsphere_partner
COMMENT 'Product performance for a partner: sales, price, reviews and late-delivery rate per product'
;;
ALTER SHARE shopsphere_partner ADD MATERIALIZED VIEW shopsphere_dev.gold.product_performance
  COMMENT 'One row per product; refreshed by the medallion pipeline'
;;
SHOW ALL IN SHARE shopsphere_partner

-- 2. NOT RUN YET: the external step. It creates credentials for someone outside the
--    workspace and gives them read access, so it needs an explicit go-ahead.
--
-- Open sharing (the partner doesn't use Databricks): DESCRIBE RECIPIENT returns a one-time
-- activation link that downloads the credential file for the partner.
--   CREATE RECIPIENT IF NOT EXISTS partner_demo COMMENT 'Demo partner (open sharing)';
--   GRANT SELECT ON SHARE shopsphere_partner TO RECIPIENT partner_demo;
--   DESCRIBE RECIPIENT partner_demo;   -- activation_link
--
-- Databricks-to-Databricks sharing instead: CREATE RECIPIENT partner_demo USING ID '<metastore sharing identifier>';
--
-- Revoke or remove:
--   REVOKE SELECT ON SHARE shopsphere_partner FROM RECIPIENT partner_demo;
--   DROP RECIPIENT partner_demo;
