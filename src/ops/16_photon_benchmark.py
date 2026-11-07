# Databricks notebook source
# MAGIC %md
# MAGIC # Photon on/off benchmark
# MAGIC
# MAGIC Times the same four queries; run it once on a cluster with Photon and once without
# MAGIC (`runtime_engine` PHOTON vs STANDARD, otherwise identical single-node clusters). Results are
# MAGIC appended to `ops.photon_benchmark`, one row per query and engine.
# MAGIC
# MAGIC The Olist tables are small (112k sales rows), so the data is scaled up synthetically:
# MAGIC 30 million rows keyed to the real product catalogue, plus `fact_sales` repeated 100 times.
# MAGIC Every query ends in a `noop` write, so Spark computes the full result without the cost of
# MAGIC writing it. Each query runs 3 times and the median is kept; the disk cache is off.

# COMMAND ----------

import statistics
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "shopsphere_dev")
dbutils.widgets.dropdown("engine", "STANDARD", ["STANDARD", "PHOTON"])  # label only: set it to the cluster's runtime_engine
CATALOG = dbutils.widgets.get("catalog")
engine = dbutils.widgets.get("engine")
ROWS = 30_000_000
RUNS = 3

spark.conf.set("spark.databricks.io.cache.enabled", "false")

products = spark.table(f"{CATALOG}.gold.dim_product").select("product_sk", "category")
n_products = products.count()
keyed = products.withColumn("k", F.row_number().over(Window.orderBy("product_sk")) - 1)

big = (
    spark.range(ROWS)
    .withColumn("k", (F.col("id") * 7919) % n_products)
    .withColumn("amount", F.round(F.rand(seed=1) * 500, 2))
    .withColumn("day", F.date_add(F.lit("2017-01-01"), (F.col("id") % 600).cast("int")))
    .withColumn("customer", F.col("id") % 95000)
)
fact_x100 = spark.table(f"{CATALOG}.gold.fact_sales").crossJoin(spark.range(100).withColumnRenamed("id", "copy"))

QUERIES = {
    "aggregate": lambda: big.groupBy("k").agg(F.sum("amount"), F.avg("amount"), F.count("*"), F.max("day")),
    "join_and_aggregate": lambda: big.join(F.broadcast(keyed), "k").groupBy("category", F.year("day").alias("y"))
                                     .agg(F.sum("amount").alias("revenue"), F.countDistinct("customer").alias("customers")),
    "window_top3": lambda: big.withColumn("r", F.row_number().over(Window.partitionBy("k").orderBy(F.desc("amount"))))
                              .where("r <= 3"),
    "fact_sales_x100": lambda: fact_x100.groupBy("date_key", "payment_type")
                                        .agg(F.sum("item_total"), F.avg("delivery_days"), F.count("*")),
}

# COMMAND ----------

results = []
for name, build in QUERIES.items():
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        build().write.format("noop").mode("overwrite").save()
        times.append(time.perf_counter() - t0)
    results.append((engine, name, round(statistics.median(times), 2), [round(t, 2) for t in times]))
    print(engine, name, results[-1][2], "s")

(
    spark.createDataFrame(results, "engine string, query string, median_s double, runs_s array<double>")
    .withColumn("rows", F.lit(ROWS))
    .withColumn("spark_version", F.lit(spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "")))
    .withColumn("_run_ts", F.current_timestamp())
    .write.mode("append").saveAsTable(f"{CATALOG}.ops.photon_benchmark")
)
dbutils.notebook.exit(str({"engine": engine, "median_s": {r[1]: r[2] for r in results}}))
