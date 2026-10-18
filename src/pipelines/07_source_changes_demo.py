# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: change the source, then watch the pipeline react
# MAGIC
# MAGIC Makes small, marked changes in Azure SQL (`shopsphere_olist`) and reloads bronze, so
# MAGIC the next pipeline update shows:
# MAGIC
# MAGIC | Change in Azure SQL | Expected in the pipeline |
# MAGIC |---|---|
# MAGIC | 3 customers move to Campinas, SP | `silver.customers_scd2`: old version closed (`__END_AT` set), new current version |
# MAGIC | 1 shipped order becomes delivered | `silver.orders`: the row is updated in place (SCD Type 1 upsert) |
# MAGIC | 2 bad orders inserted (unknown status, missing customer) | `ops.orders_quarantine`: 2 rows with the rules they failed; not in `silver.orders` |
# MAGIC
# MAGIC Widget `mode`: `apply` (default) or `revert`. `apply` saves the original values in
# MAGIC `ops.source_demo_changes` first; `revert` puts them back and deletes the demo orders.
# MAGIC Either way it then reruns the bronze JDBC notebook (widget `bronze_notebook`).
# MAGIC Uses the JDBC driver through the JVM, so it needs a dedicated (single-user) cluster.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "apply", ["apply", "revert"])
# Path of the bronze JDBC notebook, relative to this one (repo layout by default)
dbutils.widgets.text("bronze_notebook", "../ingestion/bronze/02_jdbc_orders_customers_payments")
MODE = dbutils.widgets.get("mode")

CATALOG = "shopsphere_dev"
LOG_TABLE = f"{CATALOG}.ops.source_demo_changes"
JDBC_URL = (
    "jdbc:sqlserver://sql-shopsphere-dev27.database.windows.net:1433;"
    "database=shopsphere_olist;encrypt=true;trustServerCertificate=false;loginTimeout=30"
)
SQL_PASSWORD = dbutils.secrets.get(scope="shopsphere-kv", key="sql-admin-password")
DEMO_ORDERS = ["demo-bad-status-0001", "demo-no-customer-0002"]


def run_sql(statements):
    """Run write statements in one transaction against Azure SQL."""
    jvm = spark._sc._jvm
    conn = jvm.java.sql.DriverManager.getConnection(JDBC_URL, "sqladmin", SQL_PASSWORD)
    try:
        conn.setAutoCommit(False)
        counts = []
        for sql, params in statements:
            ps = conn.prepareStatement(sql)
            for i, p in enumerate(params, start=1):
                ps.setString(i, p)
            counts.append(ps.executeUpdate())
            ps.close()
        conn.commit()
        return counts
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def read_sql(query):
    return (
        spark.read.format("jdbc").option("url", JDBC_URL).option("query", query)
        .option("user", "sqladmin").option("password", SQL_PASSWORD).load()
    )

# COMMAND ----------

if MODE == "apply":
    if spark.catalog.tableExists(LOG_TABLE) and spark.table(LOG_TABLE).count() > 0:
        raise Exception(f"Demo already applied; run with mode=revert first ({LOG_TABLE} has rows).")

    customers = read_sql("""
        SELECT TOP 3 customer_id, customer_zip_code_prefix, customer_city, customer_state
        FROM dbo.olist_customers WHERE customer_state <> 'SP' ORDER BY customer_id
    """).collect()
    order = read_sql("""
        SELECT TOP 1 order_id, order_status, order_delivered_customer_date
        FROM dbo.olist_orders WHERE order_status = 'shipped' ORDER BY order_id
    """).collect()[0]
    some_customer = customers[0]["customer_id"]

    # Save the originals before changing anything
    originals = [("customer", c["customer_id"], c["customer_zip_code_prefix"], c["customer_city"], c["customer_state"]) for c in customers]
    originals.append(("order", order["order_id"], order["order_status"], order["order_delivered_customer_date"], None))
    spark.createDataFrame(originals, "kind string, id string, v1 string, v2 string, v3 string") \
        .write.mode("overwrite").saveAsTable(LOG_TABLE)

    statements = [
        ("UPDATE dbo.olist_customers SET customer_zip_code_prefix = '13010', customer_city = 'campinas', customer_state = 'SP' WHERE customer_id = ?", [c["customer_id"]])
        for c in customers
    ]
    statements.append((
        "UPDATE dbo.olist_orders SET order_status = 'delivered', order_delivered_customer_date = CONVERT(nvarchar(30), DATEADD(day, 7, CAST(order_purchase_timestamp AS datetime2)), 120) WHERE order_id = ?",
        [order["order_id"]],
    ))
    statements.append((
        "INSERT INTO dbo.olist_orders (order_id, customer_id, order_status, order_purchase_timestamp) VALUES (?, ?, 'lost_in_space', '2018-08-01 10:00:00')",
        [DEMO_ORDERS[0], some_customer],
    ))
    statements.append((
        "INSERT INTO dbo.olist_orders (order_id, customer_id, order_status, order_purchase_timestamp) VALUES (?, NULL, 'created', '2018-08-01 11:00:00')",
        [DEMO_ORDERS[1]],
    ))
    counts = run_sql(statements)
    changed = {"customers": [c["customer_id"] for c in customers], "order_delivered": order["order_id"], "bad_orders": DEMO_ORDERS, "rows_changed": counts}

else:
    log = spark.table(LOG_TABLE).collect()
    statements = []
    for r in log:
        if r["kind"] == "customer":
            statements.append((
                "UPDATE dbo.olist_customers SET customer_zip_code_prefix = ?, customer_city = ?, customer_state = ? WHERE customer_id = ?",
                [r["v1"], r["v2"], r["v3"], r["id"]],
            ))
        else:
            statements.append((
                "UPDATE dbo.olist_orders SET order_status = ?, order_delivered_customer_date = ? WHERE order_id = ?",
                [r["v1"], r["v2"], r["id"]],
            ))
    statements.append(("DELETE FROM dbo.olist_orders WHERE order_id IN (?, ?)", DEMO_ORDERS))
    counts = run_sql(statements)
    spark.sql(f"DROP TABLE {LOG_TABLE}")
    changed = {"reverted": [r["id"] for r in log], "rows_changed": counts}

print(changed)

# COMMAND ----------

# Reload bronze from Azure SQL so the next pipeline update sees the change
dbutils.notebook.run(dbutils.widgets.get("bronze_notebook"), 1200)
dbutils.notebook.exit(str(changed))
