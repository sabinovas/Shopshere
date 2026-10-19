# Databricks notebook source
# MAGIC %md
# MAGIC # Churn: XGBoost tuned with Optuna (challenger)
# MAGIC
# MAGIC 1. Tune an XGBoost pipeline with Optuna (3-fold stratified CV ROC AUC on the training
# MAGIC    split). Each trial is a nested MLflow run.
# MAGIC 2. Refit the best parameters on the whole training split and register it in
# MAGIC    `ml.churn_model` with the alias **@challenger**.
# MAGIC 3. Score **@champion** and **@challenger** on the same holdout. The one with the higher
# MAGIC    ROC AUC becomes @champion and the other @challenger.
# MAGIC 4. Write holdout predictions of the champion to `ml.churn_baseline`, the baseline for
# MAGIC    drift monitoring of `ml.churn_predictions`.
# MAGIC
# MAGIC The registered pipeline's pyfunc returns the churn probability (`predict_proba`), so
# MAGIC Model Serving returns probabilities too.

# COMMAND ----------

import subprocess
import sys

try:
    import optuna
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "optuna"])
    import optuna

import mlflow
import numpy as np
import pandas as pd
from mlflow import MlflowClient
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBClassifier

CATALOG = "shopsphere_dev"
ML = f"{CATALOG}.ml"
MODEL = f"{ML}.churn_model"
USER = spark.sql("SELECT current_user()").first()[0]
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{USER}/shopsphere/ml/churn_xgboost_optuna")
client = MlflowClient()

CATEGORICAL = ["customer_state"]
NUMERIC = [
    "recency_days", "tenure_days", "frequency", "monetary", "avg_order_value", "avg_items", "categories_bought",
    "avg_freight_ratio", "avg_review_score", "low_review_share", "avg_delivery_days", "late_delivery_share",
    "avg_installments", "credit_card_share",
]
FEATURES = CATEGORICAL + NUMERIC

data = (
    spark.table(f"{ML}.churn_labels")
    .join(spark.table(f"{ML}.features_customer"), ["customer_unique_id", "as_of_date"])
    .select("customer_unique_id", "as_of_date", "split", *FEATURES, "churned")
    .toPandas()
)
for c in NUMERIC:
    data[c] = data[c].astype("float64")
train, holdout = data[data.split == "train"], data[data.split == "holdout"]
X, y = train[FEATURES], train["churned"]
print(f"train {len(train)} rows, churn rate {y.mean():.4f}; holdout {len(holdout)} rows")


def make_pipeline(params):
    pre = ColumnTransformer(
        [("state", OneHotEncoder(handle_unknown="ignore", min_frequency=50), CATEGORICAL)],
        remainder="passthrough",
    )
    model = XGBClassifier(
        **params, objective="binary:logistic", eval_metric="auc", tree_method="hist", n_jobs=4, random_state=42
    )
    return Pipeline([("pre", pre), ("model", model)])

# COMMAND ----------

def objective(trial):
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 100, 600, step=50),
        "max_depth": trial.suggest_int("max_depth", 2, 8),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "min_child_weight": trial.suggest_float("min_child_weight", 1, 20, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 0.01, 10, log=True),
        "scale_pos_weight": trial.suggest_float("scale_pos_weight", 0.02, 1.0, log=True),
    }
    cv = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
    auc = cross_val_score(make_pipeline(params), X, y, cv=cv, scoring="roc_auc").mean()
    with mlflow.start_run(nested=True, run_name=f"trial-{trial.number}"):
        mlflow.log_params(params)
        mlflow.log_metric("cv_roc_auc", auc)
    return auc


with mlflow.start_run(run_name="xgboost_optuna") as parent:
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=42))
    study.optimize(objective, n_trials=30)
    best_params = study.best_params
    mlflow.log_params({f"best_{k}": v for k, v in best_params.items()})
    mlflow.log_metric("best_cv_roc_auc", study.best_value)

    final = make_pipeline(best_params).fit(X, y)
    proba = final.predict_proba(holdout[FEATURES])[:, 1]
    mlflow.log_metric("holdout_roc_auc", roc_auc_score(holdout.churned, proba))
    mlflow.log_metric("holdout_pr_auc_returning", average_precision_score(1 - holdout.churned, 1 - proba))

    signature = infer_signature(X.head(100), final.predict_proba(X.head(100)))
    info = mlflow.sklearn.log_model(
        final,
        artifact_path="model",
        signature=signature,
        input_example=X.head(5),
        pyfunc_predict_fn="predict_proba",
        registered_model_name=MODEL,
    )

new_version = info.registered_model_version
client.set_registered_model_alias(MODEL, "challenger", new_version)
client.update_model_version(MODEL, new_version, description=f"XGBoost + Optuna (30 trials), best CV ROC AUC {study.best_value:.4f}")
client.set_model_version_tag(MODEL, new_version, "source", "xgboost_optuna")

# COMMAND ----------

# Champion vs challenger on the same holdout
def holdout_scores(alias):
    mv = client.get_model_version_by_alias(MODEL, alias)
    model = mlflow.sklearn.load_model(f"models:/{MODEL}@{alias}")
    p = model.predict_proba(holdout[FEATURES])[:, 1]
    return mv.version, p, {
        "roc_auc": round(roc_auc_score(holdout.churned, p), 4),
        "pr_auc_returning": round(average_precision_score(1 - holdout.churned, 1 - p), 4),
    }


champ_v, champ_p, champ_m = holdout_scores("champion")
chall_v, chall_p, chall_m = holdout_scores("challenger")
promoted = chall_m["roc_auc"] > champ_m["roc_auc"]
if promoted:
    client.set_registered_model_alias(MODEL, "champion", chall_v)
    client.set_registered_model_alias(MODEL, "challenger", champ_v)
champion_version, champion_p = (chall_v, chall_p) if promoted else (champ_v, champ_p)

# COMMAND ----------

# Baseline for drift monitoring: champion predictions on the holdout, with labels
baseline = holdout[["customer_unique_id", "as_of_date", *FEATURES, "churned"]].copy()
baseline["churn_probability"] = champion_p
baseline["churn_prediction"] = (champion_p >= 0.5).astype("int64")
baseline["churned"] = baseline["churned"].astype("int64")  # monitoring needs label and prediction of the same type
baseline["model_version"] = str(champion_version)
baseline["scored_ts"] = pd.Timestamp(baseline["as_of_date"].iloc[0])
spark.createDataFrame(baseline).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ML}.churn_baseline")

result = {
    "best_cv_roc_auc": round(study.best_value, 4),
    "best_params": {k: (round(v, 4) if isinstance(v, float) else v) for k, v in best_params.items()},
    "before": {"champion": {"version": champ_v, **champ_m}, "challenger": {"version": chall_v, **chall_m}},
    "promoted_challenger": promoted,
    "now": {
        "champion": client.get_model_version_by_alias(MODEL, "champion").version,
        "challenger": client.get_model_version_by_alias(MODEL, "challenger").version,
    },
    "holdout_rows": len(holdout),
    "holdout_churn_rate": round(float(holdout.churned.mean()), 4),
}
print(result)
dbutils.notebook.exit(str(result))
