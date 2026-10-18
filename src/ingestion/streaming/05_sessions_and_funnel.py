# Databricks notebook source
# MAGIC %md
# MAGIC # Silver sessions and the funnel metric
# MAGIC
# MAGIC Streams `bronze.raw_clicks` into `silver.sessions`:
# MAGIC - **Watermark** of 10 minutes on `event_ts`: events more than 10 minutes behind the
# MAGIC   latest event seen are too late and are dropped, so state doesn't grow forever.
# MAGIC - **Deduplication** on `event_id` within the watermark (Event Hubs is at-least-once).
# MAGIC - **Session window** with a 10-minute gap: a session closes after 10 quiet minutes.
# MAGIC   In append mode a session is written only once the watermark passes its end, so the
# MAGIC   most recent sessions appear on a later run, when newer events have arrived.
# MAGIC
# MAGIC Then rebuilds `gold.funnel_conversion` (view → add_to_cart → purchase, per day) from
# MAGIC `silver.sessions`. The full gold layer moves to Declarative Pipelines in Week 4.
# MAGIC Status: run successfully on 2026-10-18 (DBR 17.3 LTS, single node).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "shopsphere_dev"
CHECKPOINTS = "abfss://checkpoints@stshopspheredev27.dfs.core.windows.net"

clicks = (
    spark.readStream.table(f"{CATALOG}.bronze.raw_clicks")
    .where("event_id IS NOT NULL AND event_ts IS NOT NULL")
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicatesWithinWatermark(["event_id"])
)

sessions = (
    clicks.groupBy(F.session_window("event_ts", "10 minutes").alias("w"), "session_id")
    .agg(
        F.first("customer_id", ignorenulls=True).alias("customer_id"),
        F.first("device").alias("device"),
        F.first("referrer").alias("referrer"),
        F.min("event_ts").alias("session_start"),
        F.max("event_ts").alias("session_end"),
        F.count("*").alias("events"),
        F.count_if(F.col("event_type") == "view").alias("views"),
        F.count_if(F.col("event_type") == "add_to_cart").alias("add_to_carts"),
        F.count_if(F.col("event_type") == "purchase").alias("purchases"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("price"))).alias("revenue"),
    )
    .select(
        "session_id", "customer_id", "device", "referrer", "session_start", "session_end",
        (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")).alias("duration_s"),
        "events", "views", "add_to_carts", "purchases",
        F.coalesce("revenue", F.lit(0.0)).alias("revenue"),
        F.current_timestamp().alias("_processed_ts"),
    )
)

(
    sessions.writeStream.outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINTS}/silver/sessions")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.silver.sessions")
    .awaitTermination()
)

# COMMAND ----------

# A late event can sit more than 10 minutes before the rest of its session, so it opens a
# separate window: one session_id can have several rows in silver.sessions. Combine them
# by session_id first so the funnel counts real sessions, not window fragments.
funnel = spark.sql(f"""
    WITH by_session AS (
        SELECT session_id, min(session_start) AS session_start, sum(views) AS views,
               sum(add_to_carts) AS add_to_carts, sum(purchases) AS purchases, sum(revenue) AS revenue
        FROM {CATALOG}.silver.sessions
        GROUP BY session_id
    )
    SELECT date(session_start)                                             AS session_date,
           count(*)                                                        AS sessions,
           count_if(views > 0)                                             AS sessions_with_view,
           count_if(add_to_carts > 0)                                      AS sessions_with_cart,
           count_if(purchases > 0)                                         AS sessions_with_purchase,
           round(count_if(add_to_carts > 0) / count_if(views > 0), 4)      AS view_to_cart_rate,
           round(count_if(purchases > 0) / count_if(add_to_carts > 0), 4)  AS cart_to_purchase_rate,
           round(count_if(purchases > 0) / count(*), 4)                    AS conversion_rate,
           round(sum(revenue), 2)                                          AS revenue
    FROM by_session
    GROUP BY date(session_start)
""")
funnel.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.funnel_conversion")
display(funnel)

# COMMAND ----------

# Compare bronze with silver: sessions newer than the watermark are still open, and events
# that arrived too late were dropped from their session.
check = spark.sql(f"""
    WITH b AS (SELECT count(DISTINCT session_id) AS bronze_sessions, count(DISTINCT event_id) AS bronze_events
               FROM {CATALOG}.bronze.raw_clicks WHERE event_id IS NOT NULL),
         s AS (SELECT count(*) AS silver_rows, count(DISTINCT session_id) AS silver_sessions,
                      sum(events) AS events_in_sessions FROM {CATALOG}.silver.sessions)
    SELECT * FROM b CROSS JOIN s
""").first().asDict()
check["funnel"] = [
    {k: str(v) for k, v in r.asDict().items()}
    for r in spark.table(f"{CATALOG}.gold.funnel_conversion").collect()
]
print(check)
dbutils.notebook.exit(str(check))
