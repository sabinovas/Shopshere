# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: products and sellers from the landing zone (Auto Loader)
# MAGIC
# MAGIC Reads `landing/products` (JSON Lines) and `landing/sellers` (CSV) into
# MAGIC `shopsphere_dev.bronze.raw_products` and `raw_sellers`.
# MAGIC Adds `_ingest_ts` and `_source_file`, as PLAN.md Week 2 requires.
# MAGIC
# MAGIC Schema inference, schema evolution (`addNewColumns`) and rescued data are on.
# MAGIC Status: run successfully on 2026-10-18 (DBR 17.3 LTS, single node).

# COMMAND ----------

dbutils.widgets.text("catalog", "shopsphere_dev")  # set per target by the bundle job
CATALOG = dbutils.widgets.get("catalog")
LANDING = "abfss://landing@stshopspheredev27.dfs.core.windows.net"
CHECKPOINTS = "abfss://checkpoints@stshopspheredev27.dfs.core.windows.net/bronze"

from pyspark.sql import functions as F

def autoloader_to_bronze(source_path, fmt, table, extra_options=None):
    """Incrementally load one landing folder into a bronze table."""
    options = {
        "cloudFiles.format": fmt,
        "cloudFiles.schemaLocation": f"{CHECKPOINTS}/_schemas/{table}",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
    }
    options.update(extra_options or {})

    df = (
        spark.readStream.format("cloudFiles")
        .options(**options)
        .load(source_path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    return (
        df.writeStream.option("checkpointLocation", f"{CHECKPOINTS}/{table}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)  # process what has landed, then stop
        .toTable(f"{CATALOG}.bronze.{table}")
    )

# COMMAND ----------

products_query = autoloader_to_bronze(f"{LANDING}/products", "json", "raw_products")
products_query.awaitTermination()

sellers_query = autoloader_to_bronze(
    f"{LANDING}/sellers", "csv", "raw_sellers", {"header": "true"}
)
sellers_query.awaitTermination()

# COMMAND ----------

display(spark.table(f"{CATALOG}.bronze.raw_products").limit(5))
display(spark.table(f"{CATALOG}.bronze.raw_sellers").limit(5))
