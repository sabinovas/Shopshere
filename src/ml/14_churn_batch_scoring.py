# Databricks notebook source
# MAGIC %md
# MAGIC # Churn: nightly batch scoring
# MAGIC
# MAGIC Scores the latest feature snapshot with `ml.churn_model@champion` and appends to
# MAGIC `ml.churn_predictions` (the inference table watched by Lakehouse Monitoring).
# MAGIC Re-running for the same snapshot and model version replaces those rows, so the job can
# MAGIC retry safely. Runs in the daily job after the pipeline.

# COMMAND ----------

import mlflow
import pandas as pd
from mlflow import MlflowClient
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "shopsphere_dev")  # set per target by the bundle job
CATALOG = dbutils.widgets.get("catalog")
ML = f"{CATALOG}.ml"
MODEL = f"{ML}.churn_model"
PREDICTIONS = f"{ML}.churn_predictions"
FEATURES = [
    "customer_state", "recency_days", "tenure_days", "frequency", "monetary", "avg_order_value", "avg_items",
    "categories_bought", "avg_freight_ratio", "avg_review_score", "low_review_share", "avg_delivery_days",
    "late_delivery_share", "avg_installments", "credit_card_share",
]
mlflow.set_registry_uri("databricks-uc")
version = MlflowClient().get_model_version_by_alias(MODEL, "champion").version
model = mlflow.sklearn.load_model(f"models:/{MODEL}@champion")

latest = spark.table(f"{ML}.features_customer").agg(F.max("as_of_date")).first()[0]
pdf = spark.table(f"{ML}.features_customer").where(F.col("as_of_date") == latest).toPandas()
for c in FEATURES[1:]:
    pdf[c] = pdf[c].astype("float64")

pdf["churn_probability"] = model.predict_proba(pdf[FEATURES])[:, 1]
pdf["churn_prediction"] = (pdf["churn_probability"] >= 0.5).astype(int)
pdf["churned"] = pd.array([None] * len(pdf), dtype="Int64")  # unknown until the next 180 days pass
pdf["model_version"] = str(version)
# When the scoring ran (the monitor's time windows use this); as_of_date says which snapshot
pdf["scored_ts"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
pdf["risk_band"] = pd.cut(pdf["churn_probability"], [-0.01, 0.9, 0.97, 1.0], labels=["return likely", "watch", "churn likely"]).astype(str)

scored = spark.createDataFrame(pdf[["customer_unique_id", "as_of_date", *FEATURES, "churned", "churn_probability",
                                    "churn_prediction", "model_version", "scored_ts", "risk_band"]])
if spark.catalog.tableExists(PREDICTIONS):
    spark.sql(f"DELETE FROM {PREDICTIONS} WHERE as_of_date = DATE'{latest}' AND model_version = '{version}'")
scored.write.mode("append").option("mergeSchema", "true").saveAsTable(PREDICTIONS)

bands = spark.table(PREDICTIONS).where(F.col("as_of_date") == latest).groupBy("risk_band").count().collect()
result = {"as_of_date": str(latest), "model_version": version, "rows": len(pdf), "risk_bands": {r.risk_band: r["count"] for r in bands}}
print(result)
dbutils.notebook.exit(str(result))
