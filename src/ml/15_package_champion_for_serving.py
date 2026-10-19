# Databricks notebook source
# MAGIC %md
# MAGIC # Package the champion for Model Serving
# MAGIC
# MAGIC Models logged on DBR 16.4 ML pin `pandas==1.5.3`, which has no wheel for the Python 3.12
# MAGIC serving image, so the endpoint's container build fails (`No module named 'pkg_resources'`
# MAGIC while building pandas from source). This notebook re-logs the **same fitted model** with:
# MAGIC - the same requirements, except pandas and psutil pinned to versions with Python 3.12 wheels
# MAGIC - `pyfunc_predict_fn="predict_proba"`, so the endpoint returns churn probabilities
# MAGIC - pandas 1.x index objects inside the fitted pipeline replaced (pandas 2 can't unpickle them),
# MAGIC   checked by comparing predictions before and after
# MAGIC
# MAGIC It registers the result as a new version of `ml.churn_model` and moves **@champion** to
# MAGIC it (same weights, so batch scores don't change). The old version keeps a `superseded_by` tag.

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
from mlflow.models import infer_signature

CATALOG = "shopsphere_dev"
ML = f"{CATALOG}.ml"
MODEL = f"{ML}.churn_model"
# Pins with Python 3.12 wheels for the serving image (DBR 16.4 ML pins older versions)
SERVING_PINS = {"pandas": "pandas==2.2.3", "psutil": "psutil==5.9.8"}
FEATURES = [
    "customer_state", "recency_days", "tenure_days", "frequency", "monetary", "avg_order_value", "avg_items",
    "categories_bought", "avg_freight_ratio", "avg_review_score", "low_review_share", "avg_delivery_days",
    "late_delivery_share", "avg_installments", "credit_card_share",
]
USER = spark.sql("SELECT current_user()").first()[0]
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{USER}/shopsphere/ml/churn_serving_package")
client = MlflowClient()

source = client.get_model_version_by_alias(MODEL, "champion")
if source.tags.get("serving_package") == "true":
    # Re-package from the original version, so pins can be changed and rerun
    source = client.get_model_version(MODEL, source.tags["packaged_from"])

uri = f"models:/{MODEL}/{source.version}"
model = mlflow.sklearn.load_model(uri)

# The fitted pipeline holds pandas 1.x index objects (Int64Index and friends, module
# pandas.core.indexes.numeric), which pandas 2 removed, so the serving image can't unpickle it.
# Replace them with object-dtype base Index objects, then check nothing pandas-1-only is left.
import re

import cloudpickle
import numpy as np
import pandas as pd

LEGACY_INDEX = tuple(
    t for t in (getattr(pd, n, None) for n in ("Int64Index", "UInt64Index", "Float64Index", "NumericIndex")) if t is not None
)


def sanitize(obj, memo=None):
    """Return obj with legacy pandas indexes replaced. memo maps id -> (original, result), so an
    object reached twice gets the same cleaned result (keeping the original alive keeps ids unique)."""
    memo = {} if memo is None else memo
    if id(obj) in memo:
        return memo[id(obj)][1]
    if LEGACY_INDEX and isinstance(obj, LEGACY_INDEX):
        result = pd.Index(list(obj), dtype=object, name=obj.name)
        memo[id(obj)] = (obj, result)
        return result
    if isinstance(obj, (pd.DataFrame, pd.Series)):
        result = obj.copy()
        memo[id(obj)] = (obj, result)
        result.index = sanitize(obj.index, memo)
        if isinstance(obj, pd.DataFrame):
            result.columns = sanitize(obj.columns, memo)
        return result
    if isinstance(obj, tuple):
        result = obj if hasattr(obj, "_fields") else type(obj)(sanitize(v, memo) for v in obj)
        memo[id(obj)] = (obj, result)
        return result
    memo[id(obj)] = (obj, obj)  # containers and estimators are changed in place
    if isinstance(obj, dict):
        for k in list(obj):
            obj[k] = sanitize(obj[k], memo)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            obj[i] = sanitize(v, memo)
    elif hasattr(obj, "__dict__") and not isinstance(obj, (type, np.ndarray)) and not callable(obj):
        for k, v in list(vars(obj).items()):
            try:
                setattr(obj, k, sanitize(v, memo))
            except (AttributeError, TypeError):
                pass
    return obj


def pandas_modules(m):
    blob = cloudpickle.dumps(m)
    return sorted(set(x.decode() for x in re.findall(rb"pandas(?:\.[A-Za-z_]+)+", blob)))


before_modules = pandas_modules(model)
probe = spark.table(f"{ML}.features_customer").limit(2000).toPandas()
for c in FEATURES[1:]:
    probe[c] = probe[c].astype("float64")
p_before = model.predict_proba(probe[FEATURES])[:, 1]
model = sanitize(model)
p_after = model.predict_proba(probe[FEATURES])[:, 1]
after_modules = pandas_modules(model)
assert np.allclose(p_before, p_after), "sanitizing changed the predictions"
assert not any("indexes.numeric" in m for m in after_modules), f"pandas 1.x index types still referenced: {after_modules}"
print("pandas modules before:", before_modules, "| after:", after_modules)
reqs = [l.strip() for l in open(mlflow.pyfunc.get_model_dependencies(uri)).read().splitlines() if l.strip()]
reqs = [SERVING_PINS.get(l.split("==")[0].strip().lower(), l) for l in reqs]
for name, pin in SERVING_PINS.items():
    if pin not in reqs:
        reqs.append(pin)

example = spark.table(f"{ML}.features_customer").select(*FEATURES).limit(200).toPandas()
for c in FEATURES[1:]:
    example[c] = example[c].astype("float64")
signature = infer_signature(example, model.predict_proba(example))

with mlflow.start_run(run_name=f"package_v{source.version}_for_serving"):
    info = mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        signature=signature,
        input_example=example.head(3),
        pip_requirements=reqs,
        pyfunc_predict_fn="predict_proba",
        registered_model_name=MODEL,
    )

new = info.registered_model_version
client.update_model_version(MODEL, new, description=f"Version {source.version} repackaged for Model Serving ({', '.join(SERVING_PINS.values())}, returns probabilities). Same fitted model.")
client.set_model_version_tag(MODEL, new, "serving_package", "true")
client.set_model_version_tag(MODEL, new, "packaged_from", source.version)
client.set_model_version_tag(MODEL, source.version, "superseded_by", new)
client.set_registered_model_alias(MODEL, "champion", new)

result = {
    "packaged_from": source.version,
    "champion_now": new,
    "requirements": reqs,
    "pandas_modules_before": before_modules,
    "pandas_modules_after": after_modules,
    "max_prediction_change": float(np.abs(p_before - p_after).max()),
}
print(result)
dbutils.notebook.exit(str(result))
