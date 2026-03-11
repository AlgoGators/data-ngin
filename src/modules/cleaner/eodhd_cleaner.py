import logging
import pandas as pd
from typing import List
from src.modules.cleaner.cleaner import Cleaner

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["time", "open", "high", "low", "close", "adjusted_close", "volume", "symbol"]

NUMERIC_COLUMNS = ["open", "high", "low", "close", "adjusted_close", "volume"]


class EODHDCleaner(Cleaner):
    """
    Cleaner for EODHD End-of-Day OHLCV equity/ETF data.

    Handles:
      - Column validation (required fields from fetcher output)
      - Missing/null data via forward-fill then drop
      - Type coercion (timestamps, numerics)
      - Outlier flagging (negative prices/volume)
      - Duplicate row removal
      - Time gap detection (trading-day aware, freq='B' for business days)
    """

    def validate_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Ensures all required OHLCV columns are present.
        Drops any columns not in the expected schema to keep downstream clean.

        Raises:
            ValueError: If any required column is completely absent from the DataFrame.
        """
        if data.empty:
            logger.warning("[EODHDCleaner] Received empty DataFrame — skipping validation.")
            return data

        missing = [col for col in REQUIRED_COLUMNS if col not in data.columns]
        if missing:
            raise ValueError(
                f"[EODHDCleaner] Missing required columns: {missing}. "
                f"Got columns: {list(data.columns)}"
            )

        # Keep only the schema columns — drop any extras from raw API response
        data = data[REQUIRED_COLUMNS].copy()
        logger.info(f"[EODHDCleaner] validate_fields passed. Shape: {data.shape}")
        return data

    def handle_missing_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Handles missing values across all columns.

        Strategy (per column type):
          - Numeric OHLCV: forward-fill up to 1 period (carry last known price),
            then drop any row still null (no prior data to fill from).
          - 'time': rows with null timestamps are always dropped — unusable.
          - 'symbol': rows with null symbol are dropped — can't attribute to asset.

        Also removes rows where any price column is <= 0 (bad data / delisted artifact).
        """
        if data.empty:
            return data

        initial_len = len(data)

        # Drop rows with null timestamps or symbols — unrecoverable
        data = data.dropna(subset=["time", "symbol"])
        dropped_ts = initial_len - len(data)
        if dropped_ts > 0:
            logger.warning(f"[EODHDCleaner] Dropped {dropped_ts} rows with null time/symbol.")

        # Forward-fill numeric columns (limit=1 to avoid propagating stale data too far)
        data[NUMERIC_COLUMNS] = data[NUMERIC_COLUMNS].ffill(limit=1)

        # Drop any rows still null after fill
        pre_drop = len(data)
        data = data.dropna(subset=NUMERIC_COLUMNS)
        dropped_null = pre_drop - len(data)
        if dropped_null > 0:
            logger.warning(f"[EODHDCleaner] Dropped {dropped_null} rows with unfillable null OHLCV values.")

        # Drop rows with non-positive prices or volume (corrupted / bad tick)
        price_cols = ["open", "high", "low", "close", "adjusted_close"]
        invalid_price_mask = (data[price_cols] <= 0).any(axis=1)
        invalid_vol_mask = data["volume"] < 0
        invalid_mask = invalid_price_mask | invalid_vol_mask

        dropped_invalid = invalid_mask.sum()
        if dropped_invalid > 0:
            logger.warning(
                f"[EODHDCleaner] Dropped {dropped_invalid} rows with non-positive price or negative volume."
            )
        data = data[~invalid_mask]

        logger.info(
            f"[EODHDCleaner] handle_missing_data complete. "
            f"Retained {len(data)}/{initial_len} rows."
        )
        return data

    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Standardizes data types and formats for DB insertion.

        Transformations:
          - 'time': parse to pd.Timestamp (UTC normalized, date-only for EOD)
          - Numeric columns: cast to float64 (open/high/low/close/adjusted_close)
          - 'volume': cast to int64 (whole shares)
          - 'symbol': strip whitespace, uppercase
          - Sort by (symbol, time) ascending
          - Remove exact duplicate rows
        """
        if data.empty:
            return data

        # Parse timestamps — EODHD returns "YYYY-MM-DD" strings
        data["time"] = pd.to_datetime(data["time"], errors="coerce")

        # Drop rows where timestamp coercion failed
        bad_ts = data["time"].isna().sum()
        if bad_ts > 0:
            logger.warning(f"[EODHDCleaner] Dropping {bad_ts} rows with unparseable timestamps.")
            data = data.dropna(subset=["time"])

        # Cast price columns to float64
        for col in ["open", "high", "low", "close", "adjusted_close"]:
            data[col] = pd.to_numeric(data[col], errors="coerce").astype("float64")

        # Volume as integer — coerce bad values to NaN then drop
        data["volume"] = pd.to_numeric(data["volume"], errors="coerce")
        bad_vol = data["volume"].isna().sum()
        if bad_vol > 0:
            logger.warning(f"[EODHDCleaner] Dropping {bad_vol} rows with non-numeric volume.")
            data = data.dropna(subset=["volume"])
        data["volume"] = data["volume"].astype("int64")

        # Normalize symbol
        data["symbol"] = data["symbol"].str.strip().str.upper()

        # Deduplicate exact rows
        pre_dedup = len(data)
        data = data.drop_duplicates(subset=["symbol", "time"])
        deduped = pre_dedup - len(data)
        if deduped > 0:
            logger.info(f"[EODHDCleaner] Removed {deduped} duplicate (symbol, time) rows.")

        # Sort for DB insertion order
        data = data.sort_values(by=["symbol", "time"]).reset_index(drop=True)

        logger.info(f"[EODHDCleaner] transform_data complete. Final shape: {data.shape}")
        return data

    def detect_time_gaps(self, data: pd.DataFrame, time_column: str = "time", freq: str = "B") -> List[pd.Timestamp]:
        """
        Detects gaps in trading days using business-day frequency ('B').
        Overrides base to default freq='B' since EOD data skips weekends/holidays.

        Args:
            data (pd.DataFrame): Cleaned data with a 'time' column.
            time_column (str): Timestamp column name. Defaults to 'time'.
            freq (str): Pandas offset alias. Defaults to 'B' (business days).

        Returns:
            List[pd.Timestamp]: Missing business-day timestamps.
        """
        return super().detect_time_gaps(data, time_column=time_column, freq=freq)