import os
import logging
from datetime import date
from typing import Dict, List, Optional, Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from fredapi import Fred

logger = logging.getLogger("FREDMacroPipeline")

# ── Series configuration ────────────────────────────────────────────────────
# Single source of truth: maps each macro_data table to its columns and FRED IDs.
# A None FRED ID means the column is skipped during fetch (stored as NULL).
SERIES_CONFIG: Dict[str, Dict[str, Optional[str]]] = {
    "inflation": {
        "cpi": "CPIAUCSL",
        "core_cpi": "CPILFESL",
        "core_pce": "PCEPILFE",
        "breakeven_5y": "T5YIE",
    },
    "growth": {
        "nonfarm_payrolls": "PAYEMS",
        "unemployment_rate": "UNRATE",
        "manufacturing_capacity_util": "MCUMFN",
        "industrial_production": "INDPRO",
        "retail_sales": "RSAFS",
        "gdp": "GDP",
    },
    "yield_curve": {
        "treasury_2y": "DGS2",
        "treasury_10y": "DGS10",
        "yield_spread_10y_2y": "T10Y2Y",
        "fed_funds_rate": "FEDFUNDS",
    },
    "credit_spreads": {
        "ig_credit_spread": "BAMLC0A4CBBB",
        "high_yield_spread": "BAMLH0A0HYM2",
    },
    "liquidity": {
        "m2_money_supply": "M2SL",
        "ted_spread": "TEDRATE",
        "fed_balance_sheet": "WALCL",
    },
    "market": {
        "vix": "VIXCLS",
        "dxy": "DTWEXBGS",
        "tips_10y": "DFII10",
        "wti_crude": "DCOILWTICO",
        "gdp_nowcast": "GDPNOW",  # Atlanta Fed GDPNow (quarterly)
    },
}

SCHEMA = "macro_data"
START_DATE = "2011-03-23"

# ── DDL ─────────────────────────────────────────────────────────────────────
DDL = """
CREATE SCHEMA IF NOT EXISTS macro_data;

CREATE TABLE IF NOT EXISTS macro_data.inflation (
    date DATE PRIMARY KEY,
    cpi DOUBLE PRECISION,
    core_cpi DOUBLE PRECISION,
    core_pce DOUBLE PRECISION,
    breakeven_5y DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS macro_data.growth (
    date DATE PRIMARY KEY,
    nonfarm_payrolls DOUBLE PRECISION,
    unemployment_rate DOUBLE PRECISION,
    manufacturing_capacity_util DOUBLE PRECISION,
    industrial_production DOUBLE PRECISION,
    retail_sales DOUBLE PRECISION,
    gdp DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS macro_data.yield_curve (
    date DATE PRIMARY KEY,
    treasury_2y DOUBLE PRECISION,
    treasury_10y DOUBLE PRECISION,
    yield_spread_10y_2y DOUBLE PRECISION,
    fed_funds_rate DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS macro_data.credit_spreads (
    date DATE PRIMARY KEY,
    ig_credit_spread DOUBLE PRECISION,
    high_yield_spread DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS macro_data.liquidity (
    date DATE PRIMARY KEY,
    m2_money_supply DOUBLE PRECISION,
    ted_spread DOUBLE PRECISION,
    fed_balance_sheet DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS macro_data.market (
    date DATE PRIMARY KEY,
    vix DOUBLE PRECISION,
    dxy DOUBLE PRECISION,
    tips_10y DOUBLE PRECISION,
    wti_crude DOUBLE PRECISION,
    gdp_nowcast DOUBLE PRECISION
);
"""


def get_connection() -> psycopg2.extensions.connection:
    load_dotenv()
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )


def create_schema_and_tables(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()
    logger.info("Schema and tables created (or already exist).")


def fetch_series(
    fred: Fred, series_id: str, start_date: str
) -> Optional[pd.Series]:
    try:
        data = fred.get_series(series_id, observation_start=start_date)
        logger.info("Fetched %s: %d observations", series_id, len(data))
        return data
    except Exception as e:
        logger.error("Failed to fetch %s: %s", series_id, e)
        return None


def build_table_dataframe(
    fred: Fred, columns: Dict[str, Optional[str]], start_date: str
) -> pd.DataFrame:
    series_frames: List[pd.Series] = []
    col_names: List[str] = []

    for col_name, fred_id in columns.items():
        if fred_id is None:
            continue
        series = fetch_series(fred, fred_id, start_date)
        if series is not None:
            series.name = col_name
            series_frames.append(series)
            col_names.append(col_name)

    if not series_frames:
        return pd.DataFrame()

    # Outer-join on date — each series keeps its own observation dates,
    # no resampling or forward-fill. NULLs where data doesn't exist.
    df = pd.concat(series_frames, axis=1, join="outer", sort=True)
    df.index.name = "date"
    df = df.reset_index()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Add columns that were skipped (e.g. gdp_nowcast) as NULL
    for col_name, fred_id in columns.items():
        if fred_id is None and col_name not in df.columns:
            df[col_name] = None

    return df


def upsert_table(
    conn: psycopg2.extensions.connection,
    table: str,
    columns: Dict[str, Optional[str]],
    df: pd.DataFrame,
) -> int:
    if df.empty:
        logger.warning("No data for %s.%s — skipping.", SCHEMA, table)
        return 0

    col_names = list(columns.keys())
    all_cols = ["date"] + col_names

    # Only update columns that have a FRED source — avoids overwriting
    # externally-populated columns (e.g. gdp_nowcast) with NULL.
    update_cols = [col for col in col_names if columns.get(col) is not None]
    update_set = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_cols)

    sql = f"""
        INSERT INTO {SCHEMA}.{table} ({', '.join(all_cols)})
        VALUES %s
        ON CONFLICT (date) DO UPDATE SET {update_set}
    """

    # Build rows — replace pandas NaN with None for proper SQL NULL
    rows = []
    for _, row in df.iterrows():
        values = [row["date"]]
        for col in col_names:
            val = row.get(col)
            if val is None or (not isinstance(val, date) and pd.isna(val)):
                values.append(None)
            else:
                values.append(float(val))
        rows.append(tuple(values))

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()

    logger.info("Upserted %d rows into %s.%s", len(rows), SCHEMA, table)
    return len(rows)


def run_pipeline(start_date: str = START_DATE) -> Dict[str, int]:
    load_dotenv()

    fred_api_key = os.getenv("FRED_API_KEY")
    if not fred_api_key:
        raise ValueError("FRED_API_KEY environment variable is not set.")

    fred = Fred(api_key=fred_api_key)
    conn = get_connection()

    try:
        create_schema_and_tables(conn)

        stats: Dict[str, int] = {}
        for table_name, columns in SERIES_CONFIG.items():
            logger.info("Processing table: %s.%s", SCHEMA, table_name)
            df = build_table_dataframe(fred, columns, start_date)
            count = upsert_table(conn, table_name, columns, df)
            stats[table_name] = count

        total = sum(stats.values())
        if total == 0:
            raise RuntimeError(
                "FRED pipeline fetched 0 rows across all tables. "
                "Check API key and network connectivity."
            )

        logger.info("Pipeline complete. Summary: %s", stats)
        return stats
    finally:
        conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    run_pipeline()
