# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline `shopsphere-medallion`: silver layer
# MAGIC
# MAGIC Lakeflow Declarative Pipeline source. Reads `bronze`, writes `silver` and `ops`.
# MAGIC
# MAGIC | Table | Type | How |
# MAGIC |---|---|---|
# MAGIC | `silver.customers_scd2` | streaming table | AUTO CDC from snapshot, SCD Type 2 on city, state and zip |
# MAGIC | `silver.orders` | streaming table | AUTO CDC from snapshot, SCD Type 1 (upsert: the declarative MERGE) |
# MAGIC | `ops.orders_quarantine` | materialized view | orders that fail a rule, with the rules they broke |
# MAGIC | `silver.order_items`, `payments`, `products`, `sellers`, `reviews` | materialized views | typed and checked |
# MAGIC | `silver.sessions` | streaming table | clickstream sessions: watermark, dedup, session window |
# MAGIC
# MAGIC Expectations: **warn** (`expect`, row kept, metric recorded), **drop** (`expect_or_drop`)
# MAGIC and **fail** (`expect_or_fail`, the update stops). Results are in the event log
# MAGIC (`ops.pipeline_event_log`), which `08_data_quality_report` summarises.
# MAGIC
# MAGIC Bronze columns are strings (the Olist loader used `NVARCHAR(MAX)`), so silver casts them.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("shopsphere.catalog", "shopsphere_dev")
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
OPS = f"{CATALOG}.ops"

# AUTO CDC from snapshot: newer runtimes call it create_auto_cdc_from_snapshot_flow,
# older ones apply_changes_from_snapshot. Same arguments.
auto_cdc_from_snapshot = getattr(dlt, "create_auto_cdc_from_snapshot_flow", None) or dlt.apply_changes_from_snapshot


def ts(col):
    return F.to_timestamp(F.nullif(F.trim(F.col(col)), F.lit("")))


def num(col, type_="double"):
    return F.nullif(F.trim(F.col(col)), F.lit("")).cast(type_)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers: SCD Type 2

# COMMAND ----------

# A temporary (pipeline-private) table, not a view: expectations on a view that feeds
# AUTO CDC from snapshot are not applied, so bad rows would reach the target.
@dlt.table(temporary=True, comment="Typed, checked snapshot of bronze.raw_customers; source for the SCD2 flow")
@dlt.expect_or_fail("customer_id_present", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_state", "length(customer_state) = 2")
def customers_snapshot():
    return spark.read.table(f"{BRONZE}.raw_customers").select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_unique_id").alias("customer_unique_id"),
        F.lpad(F.trim("customer_zip_code_prefix"), 5, "0").alias("customer_zip_code_prefix"),
        F.initcap(F.trim("customer_city")).alias("customer_city"),
        F.upper(F.trim("customer_state")).alias("customer_state"),
    )


dlt.create_streaming_table(
    name=f"{SILVER}.customers_scd2",
    comment="Customers with history (SCD Type 2). __START_AT / __END_AT give each version's validity; __END_AT is null for the current version.",
)
auto_cdc_from_snapshot(
    target=f"{SILVER}.customers_scd2",
    source="customers_snapshot",
    keys=["customer_id"],
    stored_as_scd_type=2,
    track_history_column_list=["customer_zip_code_prefix", "customer_city", "customer_state"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders: upsert (SCD Type 1) and quarantine

# COMMAND ----------

ORDER_STATUSES = "('created','approved','invoiced','processing','shipped','delivered','canceled','unavailable')"

# Rows that break one of these go to ops.orders_quarantine instead of silver.orders
ORDER_RULES = {
    "order_id_present": "order_id IS NOT NULL",
    "customer_id_present": "customer_id IS NOT NULL",
    "known_status": f"order_status IN {ORDER_STATUSES}",
    "purchase_ts_present": "order_purchase_ts IS NOT NULL",
}
# Suspicious but kept (warn only)
ORDER_WARNINGS = {
    "delivered_after_purchase": "order_delivered_customer_ts IS NULL OR order_delivered_customer_ts >= order_purchase_ts",
    "delivered_has_delivery_date": "order_status <> 'delivered' OR order_delivered_customer_ts IS NOT NULL",
}


@dlt.view(comment="Typed orders with a flag for each quarantine rule")
def orders_typed():
    df = spark.read.table(f"{BRONZE}.raw_orders").select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.lower(F.trim("order_status")).alias("order_status"),
        ts("order_purchase_timestamp").alias("order_purchase_ts"),
        ts("order_approved_at").alias("order_approved_ts"),
        ts("order_delivered_carrier_date").alias("order_delivered_carrier_ts"),
        ts("order_delivered_customer_date").alias("order_delivered_customer_ts"),
        ts("order_estimated_delivery_date").alias("order_estimated_delivery_ts"),
    )
    failed = [F.when(~F.expr(rule), F.lit(name)) for name, rule in ORDER_RULES.items()]
    return df.withColumn("failed_rules", F.array_compact(F.array(*failed)))


# Temporary table for the same reason as customers_snapshot. An order that disappears from
# here (for example, it starts failing a rule) is removed from silver.orders by the snapshot flow.
@dlt.table(temporary=True, comment="Orders that pass every quarantine rule; source for silver.orders")
@dlt.expect_all(ORDER_WARNINGS)
@dlt.expect_all_or_drop(ORDER_RULES)
def orders_clean():
    return (
        dlt.read("orders_typed")
        .drop("failed_rules")
        .withColumn(
            "delivery_days",
            F.datediff("order_delivered_customer_ts", "order_purchase_ts"),
        )
    )


dlt.create_streaming_table(
    name=f"{SILVER}.orders",
    comment="Current state of each order. Upserted from the bronze snapshot (SCD Type 1).",
)
auto_cdc_from_snapshot(
    target=f"{SILVER}.orders",
    source="orders_clean",
    keys=["order_id"],
    stored_as_scd_type=1,
)


@dlt.table(
    name=f"{OPS}.orders_quarantine",
    comment="Orders rejected by a quarantine rule, with the rules they failed",
)
def orders_quarantine():
    return (
        dlt.read("orders_typed")
        .where(F.size("failed_rules") > 0)
        .withColumn("_quarantined_ts", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order items, payments, products, sellers, reviews

# COMMAND ----------

@dlt.table(name=f"{SILVER}.order_items", comment="One row per item in an order")
@dlt.expect_or_drop("keys_present", "order_id IS NOT NULL AND product_id IS NOT NULL AND seller_id IS NOT NULL")
@dlt.expect_or_drop("non_negative_price", "price >= 0 AND freight_value >= 0")
def order_items():
    return spark.read.table(f"{BRONZE}.raw_order_items").select(
        F.trim("order_id").alias("order_id"),
        num("order_item_id", "int").alias("order_item_id"),
        F.trim("product_id").alias("product_id"),
        F.trim("seller_id").alias("seller_id"),
        ts("shipping_limit_date").alias("shipping_limit_ts"),
        num("price", "decimal(10,2)").alias("price"),
        num("freight_value", "decimal(10,2)").alias("freight_value"),
    )


@dlt.table(name=f"{SILVER}.payments", comment="Payments; an order can have several")
@dlt.expect_or_drop("non_negative_value", "payment_value >= 0")
@dlt.expect("installments_at_least_one", "payment_installments >= 1")
def payments():
    return spark.read.table(f"{BRONZE}.raw_payments").select(
        F.trim("order_id").alias("order_id"),
        num("payment_sequential", "int").alias("payment_sequential"),
        F.lower(F.trim("payment_type")).alias("payment_type"),
        num("payment_installments", "int").alias("payment_installments"),
        num("payment_value", "decimal(10,2)").alias("payment_value"),
    )


@dlt.table(name=f"{SILVER}.products", comment="Products with the English category name")
@dlt.expect_or_drop("product_id_present", "product_id IS NOT NULL")
@dlt.expect("has_category", "product_category_name IS NOT NULL")
@dlt.expect("has_weight", "product_weight_g IS NOT NULL")
def products():
    translation = spark.read.table(f"{BRONZE}.raw_category_translation")
    # The CSV header had a byte-order mark, so take the columns by position
    translation = translation.select(
        F.trim(F.col(translation.columns[0])).alias("product_category_name"),
        F.trim(F.col(translation.columns[1])).alias("product_category_name_english"),
    )
    products = spark.read.table(f"{BRONZE}.raw_products").select(
        F.trim(F.col("product_id").cast("string")).alias("product_id"),
        F.nullif(F.trim(F.col("product_category_name").cast("string")), F.lit("")).alias("product_category_name"),
        F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
        F.col("product_weight_g").cast("double").alias("product_weight_g"),
        F.col("product_length_cm").cast("double").alias("product_length_cm"),
        F.col("product_height_cm").cast("double").alias("product_height_cm"),
        F.col("product_width_cm").cast("double").alias("product_width_cm"),
    )
    return products.join(translation, "product_category_name", "left").withColumn(
        "product_category_name_english", F.coalesce("product_category_name_english", "product_category_name", F.lit("unknown"))
    )


@dlt.table(name=f"{SILVER}.sellers", comment="Sellers")
@dlt.expect_or_drop("seller_id_present", "seller_id IS NOT NULL")
def sellers():
    return spark.read.table(f"{BRONZE}.raw_sellers").select(
        F.trim(F.col("seller_id").cast("string")).alias("seller_id"),
        F.lpad(F.trim(F.col("seller_zip_code_prefix").cast("string")), 5, "0").alias("seller_zip_code_prefix"),
        F.initcap(F.trim("seller_city")).alias("seller_city"),
        F.upper(F.trim("seller_state")).alias("seller_state"),
    )


@dlt.table(name=f"{SILVER}.reviews", comment="Order reviews (score 1-5, optional text)")
@dlt.expect_or_drop("valid_score", "review_score BETWEEN 1 AND 5")
@dlt.expect_or_drop("order_id_present", "order_id IS NOT NULL")
def reviews():
    return spark.read.table(f"{BRONZE}.raw_reviews").select(
        F.trim("review_id").alias("review_id"),
        F.trim("order_id").alias("order_id"),
        num("review_score", "int").alias("review_score"),
        F.nullif(F.trim("review_comment_title"), F.lit("")).alias("review_comment_title"),
        F.nullif(F.trim("review_comment_message"), F.lit("")).alias("review_comment_message"),
        ts("review_creation_date").alias("review_creation_ts"),
        ts("review_answer_timestamp").alias("review_answer_ts"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clickstream sessions (streaming)
# MAGIC
# MAGIC Same logic as `src/ingestion/streaming/05_sessions_and_funnel`, now inside the pipeline.

# COMMAND ----------

@dlt.table(
    name=f"{SILVER}.sessions",
    comment="Click sessions: 10-minute watermark, dedup on event_id, 10-minute session gap. One session_id can have several rows when a late event opened its own window.",
)
@dlt.expect("session_has_view", "views > 0")
def sessions():
    clicks = (
        spark.readStream.table(f"{BRONZE}.raw_clicks")
        .where("event_id IS NOT NULL AND event_ts IS NOT NULL")
        .withWatermark("event_ts", "10 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    return (
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
        )
    )
