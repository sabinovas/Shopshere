-- Metric views: one definition of each KPI, used by the dashboard, Genie and SQL users.
-- Query with MEASURE(), for example:
--   SELECT `Order Month`, MEASURE(`Gross Revenue`) FROM shopsphere_dev.gold.sales_metrics GROUP BY ALL
-- Run on a SQL warehouse (metric views need DBR 16.4+ or Databricks SQL).
-- Statements are separated by lines with only ';;' (for run_sql.ps1).

CREATE OR REPLACE VIEW shopsphere_dev.gold.sales_metrics
WITH METRICS
LANGUAGE YAML
COMMENT 'Sales KPIs per order item. Canceled and unavailable orders are excluded.'
AS $$
version: 0.1
source: shopsphere_dev.gold.fact_sales
filter: order_status NOT IN ('canceled', 'unavailable')
joins:
  - name: d
    source: shopsphere_dev.gold.dim_date
    on: source.date_key = d.date_key
  - name: p
    source: shopsphere_dev.gold.dim_product
    on: source.product_sk = p.product_sk
  - name: s
    source: shopsphere_dev.gold.dim_seller
    on: source.seller_sk = s.seller_sk
dimensions:
  - name: Order Date
    expr: d.date
  - name: Order Month
    expr: DATE_TRUNC('MONTH', d.date)
  - name: Order Year
    expr: d.year
  - name: Day Name
    expr: d.day_name
  - name: Category
    expr: p.category
  - name: Seller State
    expr: s.seller_state
  - name: Payment Type
    expr: payment_type
  - name: Order Status
    expr: order_status
measures:
  - name: Orders
    expr: COUNT(DISTINCT order_id)
  - name: Items Sold
    expr: COUNT(1)
  - name: Product Revenue
    expr: SUM(price)
  - name: Freight Revenue
    expr: SUM(freight)
  - name: Gross Revenue
    expr: SUM(item_total)
  - name: Avg Order Value
    expr: SUM(item_total) / COUNT(DISTINCT order_id)
  - name: Avg Delivery Days
    expr: AVG(delivery_days)
  - name: Late Delivery Rate
    expr: AVG(CASE WHEN delivered_late THEN 1.0 ELSE 0.0 END)
$$
;;
CREATE OR REPLACE VIEW shopsphere_dev.gold.funnel_metrics
WITH METRICS
LANGUAGE YAML
COMMENT 'Clickstream funnel, counted per session_id'
AS $$
version: 0.1
source: shopsphere_dev.gold.funnel_conversion
dimensions:
  - name: Session Date
    expr: session_date
measures:
  - name: Sessions
    expr: SUM(sessions)
  - name: Sessions With Cart
    expr: SUM(sessions_with_cart)
  - name: Sessions With Purchase
    expr: SUM(sessions_with_purchase)
  - name: View To Cart Rate
    expr: SUM(sessions_with_cart) / SUM(sessions_with_view)
  - name: Cart To Purchase Rate
    expr: SUM(sessions_with_purchase) / SUM(sessions_with_cart)
  - name: Conversion Rate
    expr: SUM(sessions_with_purchase) / SUM(sessions)
$$
;;
-- Check: the metric view must agree with the materialized KPI table
SELECT
  (SELECT MEASURE(`Gross Revenue`) FROM shopsphere_dev.gold.sales_metrics)  AS metric_view_revenue,
  (SELECT SUM(gross_revenue) FROM shopsphere_dev.gold.kpi_daily_sales)       AS kpi_table_revenue,
  (SELECT MEASURE(`Orders`) FROM shopsphere_dev.gold.sales_metrics)          AS metric_view_orders
;;
SELECT `Order Year`, MEASURE(`Orders`) AS orders, ROUND(MEASURE(`Gross Revenue`), 2) AS revenue,
       ROUND(MEASURE(`Late Delivery Rate`), 4) AS late_rate
FROM shopsphere_dev.gold.sales_metrics GROUP BY ALL ORDER BY 1
