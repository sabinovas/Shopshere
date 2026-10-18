# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: checkpointing and failure recovery
# MAGIC
# MAGIC Shows that a stream stopped part-way through resumes from its checkpoint without
# MAGIC losing or duplicating events.
# MAGIC
# MAGIC 1. Start the bronze clickstream stream with small micro-batches (2,000 events each).
# MAGIC 2. After two batches, stop it abruptly, as if the cluster had failed.
# MAGIC 3. Restart it with the **same checkpoint**. It continues from the last committed offsets.
# MAGIC 4. Check that `raw_clicks` has every event in the hub exactly once.
# MAGIC
# MAGIC Run it after new events have been sent that `04_clickstream_to_bronze` has not read yet.
# MAGIC Why it works: the checkpoint stores the Kafka offsets of each micro-batch before it
# MAGIC runs, and the Delta sink records the batch ID it committed. A batch that was cut off is
# MAGIC replayed with the same offsets, and Delta skips a batch ID it has already written.
# MAGIC Status: run successfully on 2026-10-18 (DBR 17.3 LTS, single node).

# COMMAND ----------

# MAGIC %run ./_clickstream_common

# COMMAND ----------

import time

CHECKPOINT = f"{CHECKPOINTS}/bronze/raw_clicks"


def bronze_counts():
    return spark.sql(
        f"SELECT count(*) AS rows, count(DISTINCT event_id) AS distinct_events FROM {BRONZE_CLICKS}"
    ).first().asDict()


before = bronze_counts()
hub_total = events_in_hub()
print("before:", before, "| events in hub:", hub_total)
assert hub_total > before["rows"], "No unread events in the hub. Send some with the generator first."

# COMMAND ----------

# Steps 1-2: run with small batches, then "crash" after two batches
query = (
    read_clicks(max_offsets_per_trigger=2000)
    .writeStream.option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="5 seconds")
    .toTable(BRONZE_CLICKS)
)
deadline = time.time() + 300
while len(query.recentProgress) < 2 and time.time() < deadline:
    time.sleep(2)
query.stop()
batches_before_crash = [p["batchId"] for p in query.recentProgress]
after_crash = bronze_counts()
print("stopped after batches", batches_before_crash, "->", after_crash)

# COMMAND ----------

# Step 3: restart from the same checkpoint and read everything that is left
(
    read_clicks()
    .writeStream.option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(BRONZE_CLICKS)
    .awaitTermination()
)
after_restart = bronze_counts()

# COMMAND ----------

# Step 4: every event exactly once
result = {
    "before": before,
    "after_crash": after_crash,
    "after_restart": after_restart,
    "events_in_hub": hub_total,
    "batches_before_crash": batches_before_crash,
}
print(result)
assert after_crash["rows"] < hub_total, "The first run read everything; nothing was left to recover."
assert after_restart["rows"] == after_restart["distinct_events"], "Duplicates found"
assert after_restart["rows"] == hub_total, "Rows missing after restart"
dbutils.notebook.exit(str(result))
