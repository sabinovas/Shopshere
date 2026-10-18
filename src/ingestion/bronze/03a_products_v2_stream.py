# Databricks notebook source
# MAGIC %md
# MAGIC # Child of 03: one Auto Loader pass over `landing/products_v2`
# MAGIC
# MAGIC Processes everything that has landed, then stops. When it meets a new column it stops
# MAGIC with an error and records the new schema; `03_schema_evolution_demo` catches that and
# MAGIC runs this notebook again. Kept separate so the expected stop doesn't fail the parent.

# COMMAND ----------

CATALOG = "shopsphere_dev"
LANDING = "abfss://landing@stshopspheredev27.dfs.core.windows.net/products_v2"
CHECKPOINTS = "abfss://checkpoints@stshopspheredev27.dfs.core.windows.net/bronze"
TABLE = "raw_products_evolving"

from pyspark.sql import functions as F

(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/_schemas/{TABLE}")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(LANDING)
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .writeStream.option("checkpointLocation", f"{CHECKPOINTS}/{TABLE}")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.bronze.{TABLE}")
    .awaitTermination()
)
