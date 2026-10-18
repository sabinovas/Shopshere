# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Delta Lake features
# MAGIC
# MAGIC Works on a copy of `silver.orders` (`ops.delta_demo_orders`), because pipeline
# MAGIC tables are managed by the pipeline and shouldn't be changed by hand.
# MAGIC
# MAGIC 1. **MERGE**: upsert a batch of changes (updates and one insert)
# MAGIC 2. **Deletion vectors**: `DELETE` marks rows as deleted instead of rewriting files
# MAGIC 3. **Time travel**: read an older version with `VERSION AS OF`
# MAGIC 4. **Change Data Feed**: read the row-level changes between versions with `table_changes`
# MAGIC 5. **OPTIMIZE**: compact small files (and apply the deletion vectors)
# MAGIC 6. **VACUUM**: list the old files that would be removed (`DRY RUN`), then vacuum with the
# MAGIC    default 7-day retention, so time travel keeps working

# COMMAND ----------

CATALOG = "shopsphere_dev"
T = f"{CATALOG}.ops.delta_demo_orders"
results = {}

# Drop first, so a re-run starts again at version 0 (CREATE OR REPLACE keeps the history)
spark.sql(f"DROP TABLE IF EXISTS {T}")
spark.sql(f"""
    CREATE TABLE {T}
    TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.enableDeletionVectors = true)
    AS SELECT order_id, customer_id, order_status, order_purchase_ts, order_delivered_customer_ts
       FROM {CATALOG}.silver.orders
""")
results["v0_rows"] = spark.table(T).count()

# COMMAND ----------

# 1. MERGE: 100 shipped orders become delivered, plus one new order
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW order_changes AS
    (SELECT order_id, customer_id, 'delivered' AS order_status, order_purchase_ts,
            order_purchase_ts + INTERVAL 7 DAYS AS order_delivered_customer_ts
     FROM {T} WHERE order_status = 'shipped' ORDER BY order_id LIMIT 100)
    UNION ALL
    SELECT 'demo-new-order-0001', 'demo-customer', 'created', current_timestamp(), NULL
""")
merge = spark.sql(f"""
    MERGE INTO {T} AS t
    USING order_changes AS s ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET t.order_status = s.order_status,
                                 t.order_delivered_customer_ts = s.order_delivered_customer_ts
    WHEN NOT MATCHED THEN INSERT *
""").first().asDict()
results["merge"] = {k: merge[k] for k in ("num_affected_rows", "num_updated_rows", "num_inserted_rows")}

# COMMAND ----------

# 2. DELETE with deletion vectors: canceled orders
deleted = spark.sql(f"DELETE FROM {T} WHERE order_status = 'canceled'").first()[0]
delete_metrics = spark.sql(f"DESCRIBE HISTORY {T} LIMIT 1").first()["operationMetrics"]
results["delete"] = {
    "rows_deleted": deleted,
    "deletion_vectors_added": delete_metrics.get("numDeletionVectorsAdded"),
    "files_rewritten": delete_metrics.get("numAddedFiles"),
}

# COMMAND ----------

# 3. Time travel
results["time_travel"] = {
    "rows_v0": spark.sql(f"SELECT count(*) FROM {T} VERSION AS OF 0").first()[0],
    "rows_now": spark.table(T).count(),
    "canceled_v0": spark.sql(f"SELECT count(*) FROM {T} VERSION AS OF 0 WHERE order_status = 'canceled'").first()[0],
    "canceled_now": spark.sql(f"SELECT count(*) FROM {T} WHERE order_status = 'canceled'").first()[0],
}

# COMMAND ----------

# 4. Change Data Feed from version 1 (the MERGE) onwards
cdf = spark.sql(f"""
    SELECT _commit_version, _change_type, count(*) AS rows
    FROM table_changes('{T}', 1)
    GROUP BY 1, 2 ORDER BY 1, 2
""")
display(cdf)
results["change_data_feed"] = [f"v{r._commit_version} {r._change_type}: {r.rows}" for r in cdf.collect()]

# COMMAND ----------

# 5. OPTIMIZE
opt = spark.sql(f"OPTIMIZE {T}").first()["metrics"]
results["optimize"] = {"files_removed": opt["numFilesRemoved"], "files_added": opt["numFilesAdded"]}

# 6. VACUUM: dry run first, then with the default retention (7 days)
results["vacuum_dry_run_files"] = spark.sql(f"VACUUM {T} DRY RUN").count()
spark.sql(f"VACUUM {T}")

history = spark.sql(f"DESCRIBE HISTORY {T}").select("version", "operation").orderBy("version").collect()
results["history"] = [f"v{r.version} {r.operation}" for r in history]

print(results)
dbutils.notebook.exit(str(results))
