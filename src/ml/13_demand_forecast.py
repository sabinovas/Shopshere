# Databricks notebook source
# MAGIC %md
# MAGIC # Demand forecast per product category (Prophet)
# MAGIC
# MAGIC Weekly units sold for the 8 largest categories, from `gold.fact_sales`.
# MAGIC - Weeks from 2017-01-02 (earlier weeks are too sparse) to the last complete week.
# MAGIC - **Backtest:** fit on all but the last 8 weeks, forecast those 8 weeks, compute MAPE.
# MAGIC - **Forecast:** refit on every week and forecast 8 weeks ahead.
# MAGIC
# MAGIC One Prophet model per category, trained in parallel with `applyInPandas`.
# MAGIC Results go to `ml.forecast_predictions`; the backtest MAPE per category is logged to MLflow.

# COMMAND ----------

import subprocess
import sys

try:
    from prophet import Prophet
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "prophet"])
    from prophet import Prophet

import logging

import mlflow
import pandas as pd
from pyspark.sql import functions as F

logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

CATALOG = "shopsphere_dev"
GOLD, ML = f"{CATALOG}.gold", f"{CATALOG}.ml"
HORIZON, TOP_N = 8, 8
USER = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{USER}/shopsphere/ml/demand_forecast")

sales = (
    spark.table(f"{GOLD}.fact_sales").where(~F.col("order_status").isin("canceled", "unavailable"))
    .join(spark.table(f"{GOLD}.dim_date").select("date_key", "date"), "date_key")
    .join(spark.table(f"{GOLD}.dim_product").select("product_sk", "category"), "product_sk")
    .withColumn("week", F.date_trunc("week", "date").cast("date"))
)
# The Olist export stops around 2018-08-29, so the last weeks are only partly recorded
# (2018-08-27 has 132 items against about 2,000 normally). End the series at the last week
# whose volume is at least 75% of the median of the 8 weeks before it (2018-08-20 has 58%).
totals = sales.groupBy("week").count().orderBy("week").toPandas()
totals["ref"] = totals["count"].rolling(8).median().shift(1)
complete = totals[(totals["ref"].isna()) | (totals["count"] >= 0.75 * totals["ref"])]
last_full_week = complete["week"].max()
top = [r.category for r in sales.groupBy("category").count().orderBy(F.desc("count")).limit(TOP_N).collect()]

weekly = (
    sales.where(F.col("category").isin(top) & (F.col("week") >= "2017-01-02") & (F.col("week") <= F.lit(last_full_week)))
    .groupBy("category", "week").agg(F.count("*").alias("units"))
)
print("categories:", top, "| last full week:", last_full_week)

# COMMAND ----------

OUT_SCHEMA = "category string, week date, actual double, yhat double, yhat_lower double, yhat_upper double, kind string"


def fit_predict(df, train, periods, kind):
    m = Prophet(weekly_seasonality=False, daily_seasonality=False, yearly_seasonality=False, interval_width=0.8)
    m.add_country_holidays(country_name="BR")
    m.fit(train.rename(columns={"week": "ds", "units": "y"}))
    future = m.make_future_dataframe(periods=periods, freq="W-MON", include_history=False)
    fc = m.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]].rename(columns={"ds": "week"})
    fc["week"] = pd.to_datetime(fc["week"]).dt.date
    out = fc.merge(df[["week", "units"]].rename(columns={"units": "actual"}), on="week", how="left")
    out["kind"] = kind
    return out


def forecast_category(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("week")
    pdf["week"] = pd.to_datetime(pdf["week"]).dt.date
    backtest = fit_predict(pdf, pdf.iloc[:-HORIZON], HORIZON, "backtest")
    forecast = fit_predict(pdf, pdf, HORIZON, "forecast")
    out = pd.concat([backtest, forecast])
    out["category"] = pdf["category"].iloc[0]
    for c in ["yhat", "yhat_lower", "yhat_upper"]:
        out[c] = out[c].clip(lower=0)
    out["actual"] = out["actual"].astype("float64")
    return out[["category", "week", "actual", "yhat", "yhat_lower", "yhat_upper", "kind"]]


result = weekly.groupBy("category").applyInPandas(forecast_category, OUT_SCHEMA)
result = result.withColumn("_created_ts", F.current_timestamp())
result.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ML}.forecast_predictions")

# COMMAND ----------

# MAPE (average % error per week) and WAPE (total absolute error / total units, robust to small weeks)
mape = (
    spark.table(f"{ML}.forecast_predictions").where("kind = 'backtest' AND actual > 0")
    .groupBy("category").agg(
        F.round(F.avg(F.abs(F.col("yhat") - F.col("actual")) / F.col("actual")) * 100, 1).alias("mape_pct"),
        F.round(F.sum(F.abs(F.col("yhat") - F.col("actual"))) / F.sum("actual") * 100, 1).alias("wape_pct"),
    )
    .orderBy("wape_pct").collect()
)
next_weeks = (
    spark.table(f"{ML}.forecast_predictions").where("kind = 'forecast'")
    .groupBy("category").agg(F.round(F.sum("yhat")).alias("units_next_8_weeks"))
    .collect()
)
with mlflow.start_run(run_name="prophet_top8_categories"):
    mlflow.log_params({"horizon_weeks": HORIZON, "categories": ",".join(top), "last_full_week": str(last_full_week)})
    for r in mape:
        mlflow.log_metric(f"mape_{r.category}", r.mape_pct)
        mlflow.log_metric(f"wape_{r.category}", r.wape_pct)
    mlflow.log_metric("mape_mean", sum(r.mape_pct for r in mape) / len(mape))
    mlflow.log_metric("wape_mean", sum(r.wape_pct for r in mape) / len(mape))

out = {
    "last_full_week": str(last_full_week),
    "backtest_pct": {r.category: {"mape": r.mape_pct, "wape": r.wape_pct} for r in mape},
    "forecast_units_next_8_weeks": {r.category: r.units_next_8_weeks for r in next_weeks},
}
print(out)
dbutils.notebook.exit(str(out))
