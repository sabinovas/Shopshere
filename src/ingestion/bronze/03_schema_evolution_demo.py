# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: schema evolution demo (PLAN.md Week 2)
# MAGIC
# MAGIC 1. **Batch 1** lands with columns `product_id`, `product_category_name`, `product_weight_g` (a number).
# MAGIC    Auto Loader infers the schema and creates `raw_products_evolving`.
# MAGIC 2. **Batch 2** lands with a new column `eco_label`, and one row whose weight is text.
# MAGIC 3. With `schemaEvolutionMode = addNewColumns`, the stream **stops** when it meets the new column
# MAGIC    and records the new schema. That stop is expected: restarting the stream picks the column up.
# MAGIC 4. The text weight doesn't fit the inferred numeric type, so it is kept in `_rescued_data`
# MAGIC    instead of failing the job.
# MAGIC
# MAGIC The stream itself runs in the child notebook `03a_products_v2_stream`, so the expected stop
# MAGIC is caught here as an ordinary exception. This notebook resets its own table, checkpoint and
# MAGIC landing folder first, so it can be re-run.

# COMMAND ----------

CATALOG = "shopsphere_dev"
LANDING = "abfss://landing@stshopspheredev27.dfs.core.windows.net/products_v2"
CHECKPOINTS = "abfss://checkpoints@stshopspheredev27.dfs.core.windows.net/bronze"
TABLE = "raw_products_evolving"
FQN = f"{CATALOG}.bronze.{TABLE}"
STREAM_NOTEBOOK = "./03a_products_v2_stream"

# Reset so every run starts from the same state.
spark.sql(f"DROP TABLE IF EXISTS {FQN}")
for path in [LANDING, f"{CHECKPOINTS}/{TABLE}", f"{CHECKPOINTS}/_schemas/{TABLE}"]:
    dbutils.fs.rm(path, recurse=True)

def run_stream():
    """One Auto Loader pass; returns False if it stopped on a schema change."""
    try:
        dbutils.notebook.run(STREAM_NOTEBOOK, 600)
        return True
    except Exception as e:
        print("Stream stopped (expected when a new column arrives):", str(e).splitlines()[0][:200])
        return False

# COMMAND ----------

# Batch 1: original schema, numeric weight.
batch1 = [
    '{"product_id": "demo-1", "product_category_name": "perfumaria", "product_weight_g": 225}',
    '{"product_id": "demo-2", "product_category_name": "beleza", "product_weight_g": 100}',
]
dbutils.fs.put(f"{LANDING}/batch-1.json", "\n".join(batch1) + "\n", overwrite=True)
assert run_stream(), "Batch 1 should load without a schema change"

print("After batch 1:", spark.table(FQN).columns)
display(spark.table(FQN))

# COMMAND ----------

# Batch 2: a new column, and a weight that is text instead of a number.
batch2 = [
    '{"product_id": "demo-3", "product_category_name": "moveis", "product_weight_g": 900, "eco_label": "yes"}',
    '{"product_id": "demo-4", "product_category_name": "moveis", "product_weight_g": "heavy", "eco_label": "no"}',
]
dbutils.fs.put(f"{LANDING}/batch-2.json", "\n".join(batch2) + "\n", overwrite=True)

# First pass stops on eco_label and records the new schema; the restart loads the batch.
if not run_stream():
    assert run_stream(), "The restart should load batch 2 with the evolved schema"

# COMMAND ----------

# Result: eco_label is a new column (null for batch 1), and the text weight is in _rescued_data.
result = spark.table(FQN)
assert "eco_label" in result.columns
assert result.count() == 4
print("After batch 2:", result.columns)
display(
    result.select("product_id", "product_weight_g", "eco_label", "_rescued_data", "_source_file")
    .orderBy("product_id")
)
