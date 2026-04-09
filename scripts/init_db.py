"""
Database initialization script.
Creates all schemas and tables needed by the data pipelines.

Usage:
    python -m scripts.init_db

Requires DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME in .env
"""

import os
import logging
import psycopg2
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("InitDB")

# ── EODHD tables ────────────────────────────────────────────────────────────
EODHD_DDL = """
CREATE SCHEMA IF NOT EXISTS jonah_nissan;

CREATE TABLE IF NOT EXISTS jonah_nissan.eodhd_raw (
    time TIMESTAMP NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    adjusted_close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (time, symbol)
);

CREATE TABLE IF NOT EXISTS jonah_nissan.eodhd_data (
    time TIMESTAMP NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    adjusted_close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (time, symbol)
);
"""

# ── Macro data tables (FRED pipeline) ──────────────────────────────────────
MACRO_DDL = """
CREATE SCHEMA IF NOT EXISTS macro_data;

CREATE TABLE IF NOT EXISTS macro_data.inflation (
    date DATE PRIMARY KEY,
    cpi DOUBLE PRECISION,
    core_cpi DOUBLE PRECISION,
    core_pce DOUBLE PRECISION,
    breakeven_5y DOUBLE PRECISION,
    breakeven_10y DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS macro_data.growth (
    date DATE PRIMARY KEY,
    nonfarm_payrolls DOUBLE PRECISION,
    unemployment_rate DOUBLE PRECISION,
    manufacturing_capacity_util DOUBLE PRECISION,
    industrial_production DOUBLE PRECISION,
    retail_sales DOUBLE PRECISION,
    gdp DOUBLE PRECISION,
    consumer_sentiment DOUBLE PRECISION,
    manufacturing_employment DOUBLE PRECISION,
    cfnai DOUBLE PRECISION,
    init_claims DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS macro_data.yield_curve (
    date DATE PRIMARY KEY,
    treasury_2y DOUBLE PRECISION,
    treasury_10y DOUBLE PRECISION,
    treasury_30y DOUBLE PRECISION,
    yield_spread_10y_2y DOUBLE PRECISION,
    fed_funds_rate DOUBLE PRECISION,
    sofr DOUBLE PRECISION,
    butterfly_spread DOUBLE PRECISION
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

CREATE TABLE IF NOT EXISTS macro_data.bsts_etf_prices (
    date DATE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    adjusted_close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (date, symbol)
);
"""

# ── Migration DDL for existing databases ──────────────────────────────────
MACRO_MIGRATION_DDL = """
ALTER TABLE macro_data.inflation ADD COLUMN IF NOT EXISTS breakeven_10y DOUBLE PRECISION;
ALTER TABLE macro_data.growth ADD COLUMN IF NOT EXISTS cfnai DOUBLE PRECISION;
ALTER TABLE macro_data.growth ADD COLUMN IF NOT EXISTS init_claims DOUBLE PRECISION;
"""


def run():
    load_dotenv()

    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )

    try:
        with conn.cursor() as cur:
            logger.info("Creating EODHD schema and tables...")
            cur.execute(EODHD_DDL)

            logger.info("Creating macro_data schema and tables...")
            cur.execute(MACRO_DDL)

            logger.info("Running macro_data migrations (ADD COLUMN IF NOT EXISTS)...")
            cur.execute(MACRO_MIGRATION_DDL)

        conn.commit()
        logger.info("All schemas and tables created successfully.")

        # Verify by listing what was created
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema IN ('jonah_nissan', 'macro_data')
                ORDER BY table_schema, table_name
            """)
            tables = cur.fetchall()
            for schema, table in tables:
                logger.info("  %s.%s", schema, table)

    finally:
        conn.close()


if __name__ == "__main__":
    run()
