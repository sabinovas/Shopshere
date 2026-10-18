# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: clickstream from Event Hubs (Kafka API)
# MAGIC
# MAGIC Reads the `clickstream` Event Hub in `evhns-shopsphere-dev27` through its Kafka
# MAGIC endpoint and appends to `shopsphere_dev.bronze.raw_clicks`.
# MAGIC Events come from `data_generator/clickstream_generator.py`.
# MAGIC
# MAGIC Widget `trigger`: `availableNow` (default) reads what is in the hub and stops;
# MAGIC `continuous` keeps running with a 10-second micro-batch, for a live demo.
# MAGIC Status: run successfully on 2026-10-18 (DBR 17.3 LTS, single node).

# COMMAND ----------

# MAGIC %run ./_clickstream_common

# COMMAND ----------

dbutils.widgets.text("trigger", "availableNow")
TRIGGER = dbutils.widgets.get("trigger")

writer = read_clicks().writeStream.option("checkpointLocation", f"{CHECKPOINTS}/bronze/raw_clicks")
if TRIGGER == "availableNow":
    writer = writer.trigger(availableNow=True)
else:
    writer = writer.trigger(processingTime="10 seconds")
writer.toTable(BRONZE_CLICKS).awaitTermination()

# COMMAND ----------

summary = spark.sql(f"""
    SELECT count(*) AS rows, count(DISTINCT event_id) AS distinct_events,
           count_if(event_id IS NULL) AS unparsed,
           string(min(event_ts)) AS first_event, string(max(event_ts)) AS last_event
    FROM {BRONZE_CLICKS}
""").first().asDict()
summary["events_in_hub"] = events_in_hub()
print(summary)
dbutils.notebook.exit(str(summary))
