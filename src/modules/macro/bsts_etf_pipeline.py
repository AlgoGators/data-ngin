import os
import logging
from datetime import date
from typing import Dict, List, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import requests

logger = logging.getLogger("BSTSEtfPipeline")

SCHEMA = "macro_data"
TABLE = "bsts_etf_prices"
START_DATE = "2011-03-23"

EODHD_BASE_URL = "https://eodhd.com/api"

# 8 ETFs used by the BSTS regime detection model.
# These provide market-price-based signals complementary to FRED macro data.
BSTS_ETF_TICKERS: List[str] = [
    "SPY.US",   # S&P 500 — US equities
    "EEM.US",   # Emerging markets equities
    "TLT.US",   # 20+ Year Treasury — long duration
    "HYG.US",   # High yield corporate bonds — credit risk
    "GLD.US",   # Gold — safe haven / inflation hedge
    "UUP.US",   # US Dollar Index — dollar strength
    "USO.US",   # Crude oil — energy / commodity cycle
    "CPER.US",  # Copper — industrial demand / growth proxy
]


def get_connection() -> psycopg2.extensions.connection:
    load_dotenv()
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )


def fetch_etf(
    api_key: str, ticker: str, start_date: str, end_date: str
) -> Optional[pd.DataFrame]:
    """Fetch EOD OHLCV data for a single ETF from EODHD."""
    url = f"{EODHD_BASE_URL}/eod/{ticker}"
    params = {
        "api_token": api_key,
        "from": start_date,
        "to": end_date,
        "fmt": "json",
        "order": "a",
    }

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("Failed to fetch %s: %s", ticker, e)
        return None

    if not data or (isinstance(data, list) and len(data) == 1 and "warning" in data[0]):
        logger.warning("No data returned for %s", ticker)
        return None

    df = pd.DataFrame(data)
    # Strip the exchange suffix for storage (SPY.US -> SPY)
    df["symbol"] = ticker.split(".")[0]
    return df


def clean_etf_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and clean raw ETF OHLCV data."""
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["date"] = df["date"].dt.date

    price_cols = ["open", "high", "low", "close", "adjusted_close"]
    for col in price_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    # Drop rows with non-positive prices
    valid_prices = (df[price_cols] > 0).all(axis=1)
    df = df[valid_prices]

    df = df.drop_duplicates(subset=["symbol", "date"])
    df = df.sort_values(by=["symbol", "date"]).reset_index(drop=True)

    return df


def upsert_etf_prices(
    conn: psycopg2.extensions.connection, df: pd.DataFrame
) -> int:
    """Upsert cleaned ETF data into macro_data.bsts_etf_prices."""
    if df.empty:
        logger.warning("No ETF data to upsert — skipping.")
        return 0

    cols = ["date", "symbol", "open", "high", "low", "close", "adjusted_close", "volume"]

    sql = f"""
        INSERT INTO {SCHEMA}.{TABLE} ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (date, symbol) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            adjusted_close = EXCLUDED.adjusted_close,
            volume = EXCLUDED.volume
    """

    rows = []
    for _, row in df.iterrows():
        values = []
        for col in cols:
            val = row.get(col)
            if val is None or (not isinstance(val, (date, str)) and pd.isna(val)):
                values.append(None)
            elif col == "volume":
                values.append(int(val) if pd.notna(val) else None)
            elif col in ("date", "symbol"):
                values.append(val)
            else:
                values.append(float(val))
        rows.append(tuple(values))

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()

    logger.info("Upserted %d rows into %s.%s", len(rows), SCHEMA, TABLE)
    return len(rows)


def run_pipeline(start_date: str = START_DATE) -> Dict[str, int]:
    """Fetch, clean, and upsert all BSTS ETF price series."""
    load_dotenv()

    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        raise ValueError("EODHD_API_KEY environment variable is not set.")

    end_date = date.today().isoformat()
    conn = get_connection()

    try:
        stats: Dict[str, int] = {}
        all_frames: List[pd.DataFrame] = []

        for ticker in BSTS_ETF_TICKERS:
            logger.info("Fetching %s", ticker)
            raw_df = fetch_etf(api_key, ticker, start_date, end_date)
            if raw_df is not None and not raw_df.empty:
                cleaned = clean_etf_data(raw_df)
                symbol = ticker.split(".")[0]
                stats[symbol] = len(cleaned)
                all_frames.append(cleaned)
            else:
                symbol = ticker.split(".")[0]
                stats[symbol] = 0

        if all_frames:
            combined = pd.concat(all_frames, ignore_index=True)
            upsert_etf_prices(conn, combined)

        total = sum(stats.values())
        if total == 0:
            raise RuntimeError(
                "BSTS ETF pipeline fetched 0 rows across all tickers. "
                "Check EODHD API key and network connectivity."
            )

        logger.info("BSTS ETF pipeline complete. Summary: %s", stats)
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
