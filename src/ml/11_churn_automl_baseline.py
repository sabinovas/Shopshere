# Databricks notebook source
# MAGIC %md
# MAGIC # Churn: AutoML baseline
# MAGIC
# MAGIC Runs Databricks AutoML (classification) on the training split, registers the best model
# MAGIC as a version of `ml.churn_model` and gives it the alias **@champion** (the first model is
# MAGIC the champion until a challenger beats it in `12_churn_xgboost_optuna`).
# MAGIC Primary metric: ROC AUC. Time budget: 10 minutes.

# COMMAND ----------

import mlflow
from databricks import automl
from mlflow import MlflowClient

CATALOG = "shopsphere_dev"
ML = f"{CATALOG}.ml"
MODEL = f"{ML}.churn_model"
EXPERIMENT_DIR = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/shopsphere/ml"
FEATURE_COLS = [
    "customer_state", "recency_days", "tenure_days", "frequency", "monetary", "avg_order_value", "avg_items",
    "categories_bought", "avg_freight_ratio", "avg_review_score", "low_review_share", "avg_delivery_days",
    "late_delivery_share", "avg_installments", "credit_card_share",
]

train = (
    spark.table(f"{ML}.churn_labels").where("split = 'train'")
    .join(spark.table(f"{ML}.features_customer"), ["customer_unique_id", "as_of_date"])
    .select(*FEATURE_COLS, "churned")
)

summary = automl.classify(
    dataset=train,
    target_col="churned",
    primary_metric="roc_auc",
    timeout_minutes=10,
    experiment_dir=EXPERIMENT_DIR,
    experiment_name="churn_automl_baseline",
)
best = summary.best_trial
print("best trial:", best.model_description)

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
mv = mlflow.register_model(best.model_path, MODEL, tags={"source": "automl", "week": "6"})
client = MlflowClient()
client.set_registered_model_alias(MODEL, "champion", mv.version)
client.update_model_version(MODEL, mv.version, description=f"AutoML baseline: {best.model_description}")

result = {
    "model_version": mv.version,
    "alias": "champion",
    "best_model": best.model_description,
    # Metric key names differ between AutoML versions, so match on the name
    "val_metrics": {k: round(v, 4) for k, v in best.metrics.items() if k.startswith("val_") and ("roc_auc" in k or "precision_recall_auc" in k)},
    "trials": len(summary.trials),
    "experiment": summary.experiment.name,
}
print(result)
dbutils.notebook.exit(str(result))
