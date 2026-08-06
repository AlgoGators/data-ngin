"""Throwaway diagnostic: print actual column names for the tables
check_data_freshness.py queries. Not wired into any workflow long-term --
deleted again once the real fix is confirmed."""

import os

import psycopg2

conn = psycopg2.connect(
    host=os.environ["DB_HOST"],
    port=os.environ.get("DB_PORT", "5432"),
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    dbname=os.environ["DB_NAME"],
    connect_timeout=10,
)
with conn.cursor() as cur:
    for schema, table in [("futures_data", "ohlcv_1d"), ("equities_data", "ohlcv_1d")]:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, table),
        )
        rows = cur.fetchall()
        print(f"{schema}.{table}:")
        if not rows:
            print("  (table not found)")
        for col, dtype in rows:
            print(f"  {col} ({dtype})")
conn.close()
