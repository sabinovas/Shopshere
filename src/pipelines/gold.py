# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline `shopsphere-medallion`: gold layer
# MAGIC
# MAGIC Star schema and aggregates as **materialized views** (PLAN.md section 6).
# MAGIC
# MAGIC - Dimensions: `dim_customer` (SCD Type 2, from `silver.customers_scd2`), `dim_product`,
# MAGIC   `dim_seller`, `dim_date`, `dim_geography`
# MAGIC - Fact: `fact_sales`, one row per order item, with **liquid clustering** on `date_key`
# MAGIC - Aggregates: `kpi_daily_sales`, `customer_rfm`, `product_performance`, `funnel_conversion`
# MAGIC
# MAGIC Surrogate keys are `xxhash64` of the natural key (plus the version start for
# MAGIC `dim_customer`), so they stay the same across refreshes.

# COMMAND ----------

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = spark.conf.get("shopsphere.catalog", "shopsphere_dev")
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions

# COMMAND ----------

@dlt.table(name=f"{GOLD}.dim_customer", comment="Customer dimension, one row per customer version (SCD Type 2)")
@dlt.expect_or_fail("customer_sk_present", "customer_sk IS NOT NULL")
def dim_customer():
    return spark.read.table(f"{SILVER}.customers_scd2").select(
        F.xxhash64("customer_id", "__START_AT").alias("customer_sk"),
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        F.col("__START_AT").alias("valid_from"),
        F.col("__END_AT").alias("valid_to"),
        F.col("__END_AT").isNull().alias("is_current"),
    )


@dlt.table(name=f"{GOLD}.dim_product", comment="Product dimension")
def dim_product():
    return spark.read.table(f"{SILVER}.products").select(
        F.xxhash64("product_id").alias("product_sk"),
        "product_id",
        F.col("product_category_name").alias("category_pt"),
        F.col("product_category_name_english").alias("category"),
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    )


@dlt.table(name=f"{GOLD}.dim_seller", comment="Seller dimension")
def dim_seller():
    return spark.read.table(f"{SILVER}.sellers").select(
        F.xxhash64("seller_id").alias("seller_sk"),
        "seller_id",
        F.xxhash64("seller_zip_code_prefix", "seller_city", "seller_state").alias("geography_sk"),
        "seller_city",
        "seller_state",
    )


@dlt.table(name=f"{GOLD}.dim_geography", comment="Zip prefix, city and state, from customers and sellers")
def dim_geography():
    customers = spark.read.table(f"{SILVER}.customers_scd2").select(
        F.col("customer_zip_code_prefix").alias("zip_code_prefix"),
        F.col("customer_city").alias("city"),
        F.col("customer_state").alias("state"),
    )
    sellers = spark.read.table(f"{SILVER}.sellers").select(
        F.col("seller_zip_code_prefix").alias("zip_code_prefix"),
        F.col("seller_city").alias("city"),
        F.col("seller_state").alias("state"),
    )
    return (
        customers.unionByName(sellers)
        .distinct()
        .withColumn("geography_sk", F.xxhash64("zip_code_prefix", "city", "state"))
        .withColumn(
            "region",
            F.when(F.col("state").isin("SP", "RJ", "MG", "ES"), "Southeast")
            .when(F.col("state").isin("PR", "SC", "RS"), "South")
            .when(F.col("state").isin("MS", "MT", "GO", "DF"), "Central-West")
            .when(F.col("state").isin("BA", "SE", "AL", "PE", "PB", "RN", "CE", "PI", "MA"), "Northeast")
            .otherwise("North"),
        )
    )


@dlt.table(name=f"{GOLD}.dim_date", comment="One row per day across the order history")
def dim_date():
    bounds = spark.read.table(f"{SILVER}.orders").agg(
        F.to_date(F.min("order_purchase_ts")).alias("lo"),
        F.to_date(F.max("order_purchase_ts")).alias("hi"),
    )
    days = bounds.select(F.explode(F.sequence("lo", "hi")).alias("date"))
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact

# COMMAND ----------

@dlt.table(
    name=f"{GOLD}.fact_sales",
    comment="One row per order item. customer_sk is the customer version valid at purchase time (or the first version, for orders older than the history).",
    cluster_by=["date_key"],
)
@dlt.expect_or_drop("has_customer", "customer_sk IS NOT NULL")
@dlt.expect("has_date", "date_key IS NOT NULL")
def fact_sales():
    orders = spark.read.table(f"{SILVER}.orders")
    items = spark.read.table(f"{SILVER}.order_items")

    # Main payment type per order = the type with the largest amount
    pay = spark.read.table(f"{SILVER}.payments")
    w_pay = Window.partitionBy("order_id").orderBy(F.desc("payment_value"))
    main_payment = (
        pay.withColumn("rn", F.row_number().over(w_pay)).where("rn = 1")
        .select("order_id", "payment_type", "payment_installments")
    )

    # Point-in-time join to the SCD2 customer dimension
    cust = spark.read.table(f"{GOLD}.dim_customer").select("customer_sk", "customer_id", "valid_from", "valid_to")
    order_cust = (
        orders.select("order_id", "customer_id", "order_purchase_ts")
        .join(cust, "customer_id")
        .where(F.col("valid_to").isNull() | (F.col("order_purchase_ts") < F.col("valid_to")))
        .withColumn("rn", F.row_number().over(Window.partitionBy("order_id").orderBy("valid_from")))
        .where("rn = 1")
        .select("order_id", "customer_sk")
    )

    return (
        items.join(orders, "order_id")
        .join(order_cust, "order_id", "left")
        .join(main_payment, "order_id", "left")
        .select(
            "order_id",
            F.col("order_item_id").alias("item_id"),
            "customer_sk",
            F.xxhash64("product_id").alias("product_sk"),
            F.xxhash64("seller_id").alias("seller_sk"),
            F.date_format("order_purchase_ts", "yyyyMMdd").cast("int").alias("date_key"),
            "order_status",
            "price",
            F.col("freight_value").alias("freight"),
            (F.col("price") + F.col("freight_value")).alias("item_total"),
            "payment_type",
            "payment_installments",
            "delivery_days",
            (F.col("order_delivered_customer_ts") > F.col("order_estimated_delivery_ts")).alias("delivered_late"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregates

# COMMAND ----------

@dlt.table(name=f"{GOLD}.kpi_daily_sales", comment="Daily sales KPIs (delivered and in-progress orders; canceled and unavailable excluded)", cluster_by=["date_key"])
def kpi_daily_sales():
    fact = spark.read.table(f"{GOLD}.fact_sales").where(~F.col("order_status").isin("canceled", "unavailable"))
    return fact.groupBy("date_key").agg(
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("items"),
        F.countDistinct("customer_sk").alias("customers"),
        F.round(F.sum("price"), 2).alias("product_revenue"),
        F.round(F.sum("freight"), 2).alias("freight_revenue"),
        F.round(F.sum("item_total"), 2).alias("gross_revenue"),
        F.round(F.sum("item_total") / F.countDistinct("order_id"), 2).alias("avg_order_value"),
        F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
        F.round(F.avg(F.col("delivered_late").cast("int")), 4).alias("late_delivery_rate"),
    )


@dlt.table(name=f"{GOLD}.customer_rfm", comment="Recency, frequency, monetary per person (customer_unique_id), scored 1-5")
def customer_rfm():
    fact = spark.read.table(f"{GOLD}.fact_sales").where("order_status = 'delivered'")
    cust = spark.read.table(f"{GOLD}.dim_customer").select("customer_sk", "customer_unique_id")
    dates = spark.read.table(f"{GOLD}.dim_date").select("date_key", "date")
    base = fact.join(cust, "customer_sk").join(dates, "date_key")
    as_of = base.agg(F.max("date").alias("as_of"))
    rfm = (
        base.groupBy("customer_unique_id")
        .agg(
            F.max("date").alias("last_order_date"),
            F.countDistinct("order_id").alias("frequency"),
            F.round(F.sum("item_total"), 2).alias("monetary"),
        )
        .crossJoin(as_of)
        .withColumn("recency_days", F.datediff("as_of", "last_order_date"))
    )
    return (
        rfm.withColumn("r_score", 6 - F.ntile(5).over(Window.orderBy("recency_days")))
        .withColumn("f_score", F.ntile(5).over(Window.orderBy("frequency")))
        .withColumn("m_score", F.ntile(5).over(Window.orderBy("monetary")))
        .withColumn("rfm_segment", F.concat("r_score", "f_score", "m_score"))
        .drop("as_of")
    )


@dlt.table(name=f"{GOLD}.product_performance", comment="Sales and review score per product. Shared with a partner in Week 8.")
def product_performance():
    fact = spark.read.table(f"{GOLD}.fact_sales").where(~F.col("order_status").isin("canceled", "unavailable"))
    reviews = (
        spark.read.table(f"{SILVER}.reviews")
        .groupBy("order_id").agg(F.avg("review_score").alias("review_score"))
    )
    prod = spark.read.table(f"{GOLD}.dim_product").select("product_sk", "product_id", "category")
    return (
        fact.join(reviews, "order_id", "left")
        .groupBy("product_sk")
        .agg(
            F.count("*").alias("units_sold"),
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("price"), 2).alias("revenue"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.round(F.avg("review_score"), 2).alias("avg_review_score"),
            F.round(F.avg(F.col("delivered_late").cast("int")), 4).alias("late_delivery_rate"),
        )
        .join(prod, "product_sk", "left")
    )


@dlt.table(name=f"{GOLD}.funnel_conversion", comment="Clickstream funnel per day: view -> add_to_cart -> purchase, counted per session_id")
def funnel_conversion():
    by_session = (
        spark.read.table(f"{SILVER}.sessions")
        .groupBy("session_id")
        .agg(
            F.min("session_start").alias("session_start"),
            F.sum("views").alias("views"),
            F.sum("add_to_carts").alias("add_to_carts"),
            F.sum("purchases").alias("purchases"),
            F.sum("revenue").alias("revenue"),
        )
    )
    return by_session.groupBy(F.to_date("session_start").alias("session_date")).agg(
        F.count("*").alias("sessions"),
        F.count_if(F.col("views") > 0).alias("sessions_with_view"),
        F.count_if(F.col("add_to_carts") > 0).alias("sessions_with_cart"),
        F.count_if(F.col("purchases") > 0).alias("sessions_with_purchase"),
        F.round(F.count_if(F.col("add_to_carts") > 0) / F.count_if(F.col("views") > 0), 4).alias("view_to_cart_rate"),
        F.round(F.count_if(F.col("purchases") > 0) / F.count_if(F.col("add_to_carts") > 0), 4).alias("cart_to_purchase_rate"),
        F.round(F.count_if(F.col("purchases") > 0) / F.count("*"), 4).alias("conversion_rate"),
        F.round(F.sum("revenue"), 2).alias("revenue"),
    )
