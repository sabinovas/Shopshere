# Databricks notebook source
# MAGIC %md
# MAGIC # Churn features and labels
# MAGIC
# MAGIC Builds the feature table `ml.features_customer` (Unity Catalog feature table, primary key
# MAGIC `customer_unique_id` + `as_of_date`) and the labels `ml.churn_labels`.
# MAGIC
# MAGIC - **Training snapshot** `as_of_date = 2018-03-01`: features use orders before that date.
# MAGIC   Label `churned = 1` if the customer places no order in the next 180 days.
# MAGIC - **Scoring snapshot** `as_of_date = 2018-09-01` (end of the data): features only, no label.
# MAGIC
# MAGIC Olist customers rarely come back (about 97% never order again), so the classes are very
# MAGIC unbalanced: the interesting customers are the few who return. Models are judged on ROC AUC
# MAGIC and on PR AUC for the returning class.
# MAGIC
# MAGIC `split` (train 80% / holdout 20%, hashed on the customer) is fixed so AutoML and the custom
# MAGIC model are compared on the same holdout. Runs on the ML runtime (feature engineering client).

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "shopsphere_dev")  # set per target by the bundle job
CATALOG = dbutils.widgets.get("catalog")
GOLD, SILVER, ML = f"{CATALOG}.gold", f"{CATALOG}.silver", f"{CATALOG}.ml"
FEATURES = f"{ML}.features_customer"
LABELS = f"{ML}.churn_labels"
TRAIN_AS_OF, SCORE_AS_OF, LABEL_DAYS = "2018-03-01", "2018-09-01", 180

# One row per order: totals, delivery, payment, review, customer person and state
fact = spark.table(f"{GOLD}.fact_sales").where(~F.col("order_status").isin("canceled", "unavailable"))
cust = spark.table(f"{GOLD}.dim_customer").select("customer_sk", "customer_unique_id", "customer_state")
dates = spark.table(f"{GOLD}.dim_date").select("date_key", "date")
prod = spark.table(f"{GOLD}.dim_product").select("product_sk", "category")
reviews = spark.table(f"{SILVER}.reviews").groupBy("order_id").agg(F.avg("review_score").alias("review_score"))

orders = (
    fact.join(cust, "customer_sk").join(dates, "date_key").join(prod, "product_sk", "left")
    .groupBy("order_id", "customer_unique_id", "customer_state", "date")
    .agg(
        F.sum("item_total").alias("order_total"),
        F.sum("freight").alias("order_freight"),
        F.count("*").alias("items"),
        F.countDistinct("category").alias("categories"),
        F.first("delivery_days", ignorenulls=True).alias("delivery_days"),
        F.max(F.col("delivered_late").cast("int")).alias("late"),
        F.first("payment_type", ignorenulls=True).alias("payment_type"),
        F.first("payment_installments", ignorenulls=True).alias("installments"),
    )
    .join(reviews, "order_id", "left")
)
orders.cache()

# COMMAND ----------

def snapshot(as_of):
    hist = orders.where(F.col("date") < F.lit(as_of))
    return (
        hist.groupBy("customer_unique_id")
        .agg(
            F.max("customer_state").alias("customer_state"),
            F.datediff(F.lit(as_of), F.max("date")).alias("recency_days"),
            F.datediff(F.lit(as_of), F.min("date")).alias("tenure_days"),
            F.countDistinct("order_id").alias("frequency"),
            F.round(F.sum("order_total"), 2).cast("double").alias("monetary"),
            F.round(F.avg("order_total"), 2).cast("double").alias("avg_order_value"),
            F.avg("items").alias("avg_items"),
            F.sum("categories").alias("categories_bought"),
            F.avg(F.col("order_freight") / F.col("order_total")).cast("double").alias("avg_freight_ratio"),
            F.avg("review_score").alias("avg_review_score"),
            F.avg(F.when(F.col("review_score") <= 2, 1.0).otherwise(0.0)).alias("low_review_share"),
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.avg("late").cast("double").alias("late_delivery_share"),
            F.avg("installments").alias("avg_installments"),
            F.avg(F.when(F.col("payment_type") == "credit_card", 1.0).otherwise(0.0)).alias("credit_card_share"),
        )
        .withColumn("as_of_date", F.lit(as_of).cast("date"))
    )


features = snapshot(TRAIN_AS_OF).unionByName(snapshot(SCORE_AS_OF))

fe = FeatureEngineeringClient()
if not spark.catalog.tableExists(FEATURES):
    fe.create_table(
        name=FEATURES,
        primary_keys=["customer_unique_id", "as_of_date"],
        timeseries_columns="as_of_date",
        df=features,
        description="Customer RFM, review, delivery and payment features per snapshot date (Week 6)",
    )
else:
    fe.write_table(name=FEATURES, df=features, mode="merge")

# COMMAND ----------

# Labels for the training snapshot: churned = no order in the 180 days after as_of_date
future = (
    orders.where((F.col("date") >= F.lit(TRAIN_AS_OF)) & (F.col("date") < F.date_add(F.lit(TRAIN_AS_OF), LABEL_DAYS)))
    .select("customer_unique_id").distinct().withColumn("returned", F.lit(1))
)
labels = (
    snapshot(TRAIN_AS_OF).select("customer_unique_id", "as_of_date")
    .join(future, "customer_unique_id", "left")
    .select(
        "customer_unique_id",
        "as_of_date",
        F.when(F.col("returned").isNull(), 1).otherwise(0).alias("churned"),
        # Deterministic 80/20 split on the customer, shared by every model
        F.when(F.abs(F.xxhash64("customer_unique_id")) % 10 < 8, "train").otherwise("holdout").alias("split"),
    )
)
labels.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(LABELS)

summary = {
    "feature_rows": spark.table(FEATURES).groupBy("as_of_date").count().orderBy("as_of_date").collect(),
    "labels": spark.table(LABELS).groupBy("split", "churned").count().orderBy("split", "churned").collect(),
}
summary = {k: [r.asDict() for r in v] for k, v in summary.items()}
print(summary)
dbutils.notebook.exit(str(summary))
