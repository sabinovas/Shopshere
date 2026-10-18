# Databricks notebook source
# MAGIC %md
# MAGIC # Shared settings for the clickstream notebooks
# MAGIC
# MAGIC Loaded with `%run ./_clickstream_common`. Defines the Event Hubs Kafka options, the
# MAGIC event schema and `read_clicks()`, which returns the parsed click stream.
# MAGIC The connection string comes from the `shopsphere-kv` secret scope (Key Vault secret
# MAGIC `eventhub-listen-conn`, a Listen-only rule on the hub).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

CATALOG = "shopsphere_dev"
NAMESPACE = "evhns-shopsphere-dev27"
TOPIC = "clickstream"
CHECKPOINTS = "abfss://checkpoints@stshopspheredev27.dfs.core.windows.net"
BRONZE_CLICKS = f"{CATALOG}.bronze.raw_clicks"

_conn_str = dbutils.secrets.get(scope="shopsphere-kv", key="eventhub-listen-conn")

# Event Hubs Kafka endpoint: SASL PLAIN, user "$ConnectionString", password = connection string.
# Databricks shades the Kafka client, hence the "kafkashaded." prefix on the login module.
KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{NAMESPACE}.servicebus.windows.net:9093",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{_conn_str}";'
    ),
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    "subscribe": TOPIC,
}

CLICK_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("event_type", StringType()),
    StructField("session_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("device", StringType()),
    StructField("referrer", StringType()),
    StructField("ip", StringType()),
    StructField("user_agent", StringType()),
])


def read_clicks(max_offsets_per_trigger=20000):
    """Parsed click stream from Event Hubs, with the bronze metadata columns.

    Bronze keeps the raw JSON in `value` as well, so nothing is lost if parsing fails.
    `_source_file` records the hub, partition and offset of each event.
    """
    raw = (
        spark.readStream.format("kafka")
        .options(**KAFKA_OPTIONS)
        .option("startingOffsets", "earliest")  # first run only; later runs resume from the checkpoint
        .option("failOnDataLoss", "false")  # the hub keeps 24 hours; don't fail if old offsets expired
        .option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
        .load()
    )
    return (
        raw.select(
            F.col("value").cast("string").alias("value"),
            "partition",
            "offset",
            F.col("timestamp").alias("enqueued_ts"),
        )
        .withColumn("e", F.from_json("value", CLICK_SCHEMA))
        .select(
            "e.*",
            "value",
            "enqueued_ts",
            F.current_timestamp().alias("_ingest_ts"),
            F.concat_ws("/", F.lit(f"eventhub:{NAMESPACE}/{TOPIC}"), "partition", "offset").alias("_source_file"),
        )
        .withColumn("event_ts", F.to_timestamp("event_ts"))
    )


def events_in_hub():
    """Number of events currently retained in the hub (batch read of all partitions)."""
    return (
        spark.read.format("kafka").options(**KAFKA_OPTIONS)
        .option("startingOffsets", "earliest").option("endingOffsets", "latest")
        .load().count()
    )
