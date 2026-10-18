# Databricks notebook source
# MAGIC %md
# MAGIC # Landing files from Azure SQL (replaces the local upload)
# MAGIC
# MAGIC Writes the Week 2 landing files straight from `shopsphere_olist` into the `landing`
# MAGIC container, so nothing has to be uploaded from a laptop:
# MAGIC - `landing/products/` as JSON Lines (the PLAN.md "product catalog JSON")
# MAGIC - `landing/sellers/` as CSV with a header
# MAGIC
# MAGIC Needs the external location `el-landing-dev` (see the setup notes) and the
# MAGIC `shopsphere-kv` secret scope.
# MAGIC Status: run successfully on 2026-10-18 (DBR 17.3 LTS, single node).

# COMMAND ----------

LANDING = "abfss://landing@stshopspheredev27.dfs.core.windows.net"
JDBC_URL = (
    "jdbc:sqlserver://sql-shopsphere-dev27.database.windows.net:1433;"
    "database=shopsphere_olist;encrypt=true;trustServerCertificate=false;loginTimeout=30"
)
SQL_USER = "sqladmin"
SQL_PASSWORD = dbutils.secrets.get(scope="shopsphere-kv", key="sql-admin-password")

def read_sql(table):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"dbo.{table}")
        .option("user", SQL_USER)
        .option("password", SQL_PASSWORD)
        .load()
    )

# COMMAND ----------

# Products as JSON Lines. Overwrite so re-runs give the same files.
(
    read_sql("olist_products")
    .write.mode("overwrite")
    .json(f"{LANDING}/products")
)

# Sellers as CSV with a header, one file.
(
    read_sql("olist_sellers")
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{LANDING}/sellers")
)

# COMMAND ----------

display(dbutils.fs.ls(f"{LANDING}/products"))
display(dbutils.fs.ls(f"{LANDING}/sellers"))
