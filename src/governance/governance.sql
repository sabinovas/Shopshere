-- ShopSphere governance: grants, PII masks and a regional row filter (PLAN.md section 5, Week 5)
--
-- NOT RUN YET. Review and run on a SQL warehouse as the catalog owner or a metastore admin.
-- Statements are separated by lines with only ';;' (for run_sql.ps1).
--
-- Groups: data_engineers, analysts, data_scientists, partners. Unity Catalog grants need ACCOUNT
-- groups: the first run (2026-11-08) failed with "Could not find principal" because the Week 5
-- groups were workspace-local, so the four were created at account level and the script rerun.
--
-- Before enabling the masks, add the pipeline owner to data_engineers. The pipeline reads
-- gold.dim_customer to build customer_rfm; if the owner were masked, RFM would group by
-- masked IDs.

-- 1. Grants --------------------------------------------------------------------------------

-- Data engineers: everything in the dev catalog
GRANT ALL PRIVILEGES ON CATALOG shopsphere_dev TO `data_engineers`
;;
-- Analysts: read gold only
GRANT USE CATALOG ON CATALOG shopsphere_dev TO `analysts`
;;
GRANT USE SCHEMA, SELECT ON SCHEMA shopsphere_dev.gold TO `analysts`
;;
-- Data scientists: read silver and gold, build in ml
GRANT USE CATALOG ON CATALOG shopsphere_dev TO `data_scientists`
;;
GRANT USE SCHEMA, SELECT ON SCHEMA shopsphere_dev.silver TO `data_scientists`
;;
GRANT USE SCHEMA, SELECT ON SCHEMA shopsphere_dev.gold TO `data_scientists`
;;
GRANT USE SCHEMA, SELECT, CREATE TABLE, CREATE FUNCTION, CREATE MODEL ON SCHEMA shopsphere_dev.ml TO `data_scientists`
;;
-- Partners: nothing here; they get gold.product_performance through Delta Sharing (Week 8)

-- 2. Mask and filter functions ---------------------------------------------------------------
-- Shows the value to data engineers and data scientists, a masked value to everyone else
CREATE OR REPLACE FUNCTION shopsphere_dev.ops.mask_pii(value STRING)
RETURNS STRING
COMMENT 'Column mask for customer identifiers and zip codes'
RETURN CASE
  WHEN is_account_group_member('data_engineers') OR is_account_group_member('data_scientists') THEN value
  WHEN value IS NULL THEN NULL
  ELSE concat('***', right(value, 2))
END
;;
-- Which states each regional analyst group may see
CREATE TABLE IF NOT EXISTS shopsphere_dev.ops.region_access (
  group_name STRING COMMENT 'Account group',
  state      STRING COMMENT 'Two-letter Brazilian state code'
) COMMENT 'Row filter mapping for gold.dim_customer'
;;
-- Example: the analysts group covers the Southeast. Change or extend as needed.
INSERT INTO shopsphere_dev.ops.region_access
SELECT * FROM VALUES ('analysts', 'SP'), ('analysts', 'RJ'), ('analysts', 'MG'), ('analysts', 'ES') AS v(group_name, state)
WHERE NOT EXISTS (SELECT 1 FROM shopsphere_dev.ops.region_access)
;;
-- Row filter: engineers and scientists see all states, others only the states mapped to their group
CREATE OR REPLACE FUNCTION shopsphere_dev.ops.state_filter(customer_state STRING)
RETURNS BOOLEAN
COMMENT 'Row filter for gold.dim_customer'
RETURN is_account_group_member('data_engineers')
    OR is_account_group_member('data_scientists')
    OR EXISTS (
         SELECT 1 FROM shopsphere_dev.ops.region_access r
         WHERE r.state = customer_state AND is_account_group_member(r.group_name)
       )
;;
GRANT EXECUTE ON FUNCTION shopsphere_dev.ops.mask_pii TO `analysts`
;;
GRANT EXECUTE ON FUNCTION shopsphere_dev.ops.state_filter TO `analysts`
;;
GRANT SELECT ON TABLE shopsphere_dev.ops.region_access TO `analysts`

-- 3. Apply the mask and filter ------------------------------------------------------------
-- gold.dim_customer is a pipeline materialized view, so ALTER TABLE ... SET MASK doesn't work.
-- The mask and the filter are declared in src/pipelines/gold.py and switched on with the
-- pipeline configuration "shopsphere.apply_governance" = "true". Then run a pipeline update.

-- 4. Check (as a member of analysts only): customer_unique_id masked, only SP/RJ/MG/ES rows
-- SELECT customer_state, count(*), min(customer_unique_id) FROM shopsphere_dev.gold.dim_customer GROUP BY 1
