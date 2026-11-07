# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: orders, customers, payments, order items, reviews from Azure SQL (JDBC)
# MAGIC
# MAGIC Reads the Olist tables from `sql-shopsphere-dev27 / shopsphere_olist` and writes
# MAGIC `shopsphere_dev.bronze.raw_orders`, `raw_customers`, `raw_payments`, `raw_order_items`,
# MAGIC `raw_reviews` and `raw_category_translation`.
# MAGIC
# MAGIC The SQL password comes from the `shopsphere-kv` secret scope (Key Vault secret
# MAGIC `sql-admin-password`), never from this notebook. Lakeflow Connect could replace this
# MAGIC later; JDBC is simpler to start with.
# MAGIC Status: run successfully on 2026-10-18 (DBR 17.3 LTS, single node).

# COMMAND ----------

dbutils.widgets.text("catalog", "shopsphere_dev")  # set per target by the bundle job
CATALOG = dbutils.widgets.get("catalog")
JDBC_URL = (
    "jdbc:sqlserver://sql-shopsphere-dev27.database.windows.net:1433;"
    "database=shopsphere_olist;encrypt=true;trustServerCertificate=false;loginTimeout=30"
)
SQL_USER = "sqladmin"
SQL_PASSWORD = dbutils.secrets.get(scope="shopsphere-kv", key="sql-admin-password")

from pyspark.sql import functions as F

# Azure SQL table -> bronze table
SOURCES = {
    "olist_orders": "raw_orders",
    "olist_customers": "raw_customers",
    "olist_order_payments": "raw_payments",
    # Added in Week 4: needed for the gold star schema (fact_sales, dim_product) and Week 6 features
    "olist_order_items": "raw_order_items",
    "olist_order_reviews": "raw_reviews",
    "product_category_name_translation": "raw_category_translation",
}

for source_table, bronze_table in SOURCES.items():
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"dbo.{source_table}")
        .option("user", SQL_USER)
        .option("password", SQL_PASSWORD)
        .load()
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.lit(f"azure_sql:shopsphere_olist.dbo.{source_table}"))
    )

    # Full reload each run for now. Incremental (CDC or watermark) comes in Week 4 with AUTO CDC.
    (
        df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.bronze.{bronze_table}")
    )
    print(f"{bronze_table}: {spark.table(f'{CATALOG}.bronze.{bronze_table}').count()} rows")
