"""Load the Olist CSV files into Azure SQL (shopsphere_olist) as the source system.

Raw load: every column is NVARCHAR(MAX) so nothing fails on parsing.
Typing and cleanup happen later, in Silver.

Usage (password comes from the environment, never from the command line or a file):
    set SQL_PASSWORD=<from kv-shopsphere-dev27 / sql-admin-password>
    python load_olist_to_sql.py "C:\\Users\\Sabino\\Downloads\\archive"
"""

import csv
import os
import sys
from pathlib import Path

import pyodbc

SERVER = "sql-shopsphere-dev27.database.windows.net"
DATABASE = "shopsphere_olist"
USERNAME = "sqladmin"
BATCH_SIZE = 5000

# CSV file name -> SQL table name
TABLES = {
    "olist_customers_dataset.csv": "olist_customers",
    "olist_geolocation_dataset.csv": "olist_geolocation",
    "olist_order_items_dataset.csv": "olist_order_items",
    "olist_order_payments_dataset.csv": "olist_order_payments",
    "olist_order_reviews_dataset.csv": "olist_order_reviews",
    "olist_orders_dataset.csv": "olist_orders",
    "olist_products_dataset.csv": "olist_products",
    "olist_sellers_dataset.csv": "olist_sellers",
    "product_category_name_translation.csv": "product_category_name_translation",
}


def connect():
    password = os.environ["SQL_PASSWORD"]
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    return pyodbc.connect(conn_str, autocommit=False)


def quote(name):
    return "[" + name.replace("]", "]]") + "]"


def load_file(conn, csv_path, table):
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

        cur = conn.cursor()
        cur.fast_executemany = True
        cur.execute(f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL DROP TABLE dbo.{table};")
        cols_sql = ", ".join(f"{quote(c)} NVARCHAR(MAX) NULL" for c in header)
        cur.execute(f"CREATE TABLE dbo.{table} ({cols_sql});")
        conn.commit()

        placeholders = ", ".join("?" for _ in header)
        insert_sql = f"INSERT INTO dbo.{table} VALUES ({placeholders});"

        total = 0
        batch = []
        for row in reader:
            # Empty CSV fields become NULL so they are not stored as empty strings.
            batch.append([value if value != "" else None for value in row])
            if len(batch) >= BATCH_SIZE:
                cur.executemany(insert_sql, batch)
                conn.commit()
                total += len(batch)
                batch = []
        if batch:
            cur.executemany(insert_sql, batch)
            conn.commit()
            total += len(batch)
        cur.close()
    return total


def main(folder):
    folder = Path(folder)
    conn = connect()
    try:
        for file_name, table in TABLES.items():
            csv_path = folder / file_name
            if not csv_path.exists():
                sys.exit(f"Missing file: {csv_path}")
            rows = load_file(conn, csv_path, table)
            print(f"{table}: {rows} rows loaded")
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python load_olist_to_sql.py <folder with the Olist CSVs>")
    main(sys.argv[1])
