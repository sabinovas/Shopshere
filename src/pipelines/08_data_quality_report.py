# Databricks notebook source
# MAGIC %md
# MAGIC # Data-quality report
# MAGIC
# MAGIC Reads the pipeline event log (published as `ops.pipeline_event_log`), takes the
# MAGIC expectation results of the latest update, and appends them to `ops.dq_results`.
# MAGIC Also checks a few things expectations can't: SCD2 has one current version per
# MAGIC customer, `fact_sales` matches `order_items`, and the quarantine reasons.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "shopsphere_dev")  # set per target by the bundle job
CATALOG = dbutils.widgets.get("catalog")
EVENT_LOG = f"{CATALOG}.ops.pipeline_event_log"

latest_update = spark.sql(f"""
    SELECT origin.update_id FROM {EVENT_LOG}
    WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1
""").first()[0]

expectations = spark.sql(f"""
    SELECT origin.update_id                          AS update_id,
           timestamp                                 AS event_ts,
           e.dataset                                 AS dataset,
           e.name                                    AS expectation,
           e.passed_records                          AS passed,
           e.failed_records                          AS failed
    FROM {EVENT_LOG}
    LATERAL VIEW explode(from_json(details:flow_progress.data_quality.expectations,
        'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
    WHERE event_type = 'flow_progress' AND origin.update_id = '{latest_update}'
""")

report = (
    expectations.groupBy("update_id", "dataset", "expectation")
    .agg(F.sum("passed").alias("passed"), F.sum("failed").alias("failed"), F.max("event_ts").alias("event_ts"))
    .withColumn("failed_pct", F.round(F.col("failed") / (F.col("passed") + F.col("failed")) * 100, 3))
    .withColumn("_reported_ts", F.current_timestamp())
    .orderBy(F.desc("failed"), "dataset", "expectation")
)
report.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{CATALOG}.ops.dq_results")
display(report)

# COMMAND ----------

checks = spark.sql(f"""
    SELECT
      (SELECT count(*) FROM (SELECT customer_id FROM {CATALOG}.silver.customers_scd2
                             WHERE __END_AT IS NULL GROUP BY customer_id HAVING count(*) > 1))   AS customers_with_two_current_rows,
      (SELECT count(*) FROM {CATALOG}.silver.customers_scd2 WHERE __END_AT IS NOT NULL)          AS closed_customer_versions,
      (SELECT count(*) FROM {CATALOG}.gold.fact_sales)                                           AS fact_rows,
      (SELECT count(*) FROM {CATALOG}.silver.order_items)                                        AS order_item_rows,
      (SELECT count(*) FROM {CATALOG}.ops.orders_quarantine)                                     AS quarantined_orders
""").first().asDict()
quarantine = [
    f"{r.order_id}: {', '.join(r.failed_rules)}"
    for r in spark.table(f"{CATALOG}.ops.orders_quarantine").select("order_id", "failed_rules").collect()
]

result = {
    "update_id": latest_update,
    "expectations": [
        f"{r.dataset} / {r.expectation}: passed {r.passed}, failed {r.failed}"
        for r in report.collect()
    ],
    "checks": checks,
    "quarantine": quarantine,
}
print(result)

# Fail the task (and so the job, which then emails) if a structural check breaks.
# Warn-level expectation failures are reported above but don't fail the run.
assert checks["customers_with_two_current_rows"] == 0, "SCD2: a customer has two current versions"
assert checks["fact_rows"] == checks["order_item_rows"], "fact_sales and silver.order_items row counts differ"
assert checks["fact_rows"] > 0, "fact_sales is empty"
dbutils.notebook.exit(str(result))
