import logging
import pandas as pd
from enum import Enum
from typing import Dict, Any, List
from src.modules.cleaner.cleaner import Cleaner


class RequiredFields(Enum):
    """
    Required fields for cleaned Tiingo equity data.

    These map 1:1 to the columns of the `equities_data.equities` table:
    raw OHLCV + adjusted OHLCV + dividend/split event fields.
    """
    TIME = "time"
    SYMBOL = "symbol"
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOLUME = "volume"
    ADJ_OPEN = "adj_open"
    ADJ_HIGH = "adj_high"
    ADJ_LOW = "adj_low"
    ADJUSTED_CLOSE = "adjusted_close"
    ADJ_VOLUME = "adj_volume"
    DIV_CASH = "div_cash"
    SPLIT_FACTOR = "split_factor"


# Price columns that must be strictly positive to be considered valid.
PRICE_COLUMNS = ["open", "high", "low", "close", "adj_open", "adj_high", "adj_low", "adjusted_close"]

# Float columns coerced to float64. adj_volume is split-adjusted (volume * cumulative
# split factor) so it can be fractional — kept as float, unlike raw integer volume.
FLOAT_COLUMNS = PRICE_COLUMNS + ["adj_volume", "div_cash", "split_factor"]


class TiingoCleaner(Cleaner):
    """
    Cleaner for Tiingo End-of-Day equity/ETF OHLCV data.

    Mirrors the DatabentoCleaner contract: clean() returns a List[Dict] ready for
    TimescaleDBInserter.insert_data (which reads each row's dict keys), rather than
    a DataFrame. Equity-specific behavior:
      - parses Tiingo's ISO-8601 'Z' timestamps to UTC
      - keeps an adjusted_close column
      - drops rows with non-positive prices / negative volume
      - de-duplicates on (symbol, time)
      - no futures back-adjustment
    """

    def __init__(self, config: Dict[str, Any] = None) -> None:
        self.config: Dict[str, Any] = config or {}
        self.logger: logging.Logger = logging.getLogger("TiingoCleaner")

    def clean(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Validate, handle missing data, and transform raw Tiingo data into a list of
        row dicts ready for database insertion.

        Args:
            data (pd.DataFrame): Raw data from TiingoFetcher.

        Returns:
            List[Dict[str, Any]]: Cleaned rows. Empty list if input is empty.
        """
        if data is None or data.empty:
            self.logger.warning("[TiingoCleaner] Received empty DataFrame — nothing to clean.")
            return []

        data = self.validate_fields(data)
        data = self.handle_missing_data(data)
        data = self.transform_data(data)
        return data.to_dict(orient="records")

    def validate_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure all required columns are present, then slice to exactly the schema
        columns (dropping any extras the fetcher may have left in).

        Raises:
            ValueError: If any required column is absent.
        """
        required: List[str] = [f.value for f in RequiredFields]
        missing: List[str] = [c for c in required if c not in data.columns]
        if missing:
            self.logger.error(f"[TiingoCleaner] Missing required fields: {missing}")
            raise ValueError(f"Missing required fields: {missing}")

        data = data[required].copy()
        self.logger.info(f"[TiingoCleaner] validate_fields passed. Shape: {data.shape}")
        return data

    def handle_missing_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply config-driven missing-data handling (same switch contract as the
        DatabentoCleaner), then drop unrecoverable / corrupt rows.

        Always drops rows with null time/symbol and rows with non-positive prices
        or negative volume, regardless of config.
        """
        numeric_columns = data.select_dtypes(include=["int64", "float64"]).columns

        method_switch = {
            "drop_nan": lambda d: d.dropna(),
            "forward_fill": lambda d: d.ffill(),
            "backward_fill": lambda d: d.bfill(),
            "interpolate": lambda d: d.infer_objects().interpolate(),
            "zero_fill": lambda d: d.fillna(0),
            "mean_fill": lambda d: d.fillna({col: d[col].mean() for col in numeric_columns}),
            "median_fill": lambda d: d.fillna({col: d[col].median() for col in numeric_columns}),
            "custom_fill": lambda d: d.fillna(self.config.get("missing_data", {}).get("custom_value", 0)),
        }
        for method, action in method_switch.items():
            if self.config.get("missing_data", {}).get(method, "False") == "True":
                self.logger.info(f"[TiingoCleaner] Applying {method.replace('_', ' ')}.")
                data = action(data)

        initial_len = len(data)

        # Always drop rows we can't attribute or place in time.
        data = data.dropna(subset=["time", "symbol"])

        # Drop corrupt rows: non-positive prices or negative volume.
        invalid = (data[PRICE_COLUMNS] <= 0).any(axis=1) | (data["volume"] < 0)
        dropped = int(invalid.sum())
        if dropped:
            self.logger.warning(
                f"[TiingoCleaner] Dropped {dropped} rows with non-positive price or negative volume."
            )
        data = data[~invalid].copy()  # .copy() keeps writes in transform_data safe on pandas 2.x (no CoW)

        self.logger.info(
            f"[TiingoCleaner] handle_missing_data complete. Retained {len(data)}/{initial_len} rows."
        )
        return data

    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize types and ordering for DB insertion:
          - 'time' parsed to UTC datetime (Tiingo returns ISO-8601 with 'Z')
          - prices -> float64, volume -> int64
          - symbol stripped/uppercased
          - de-duplicated on (symbol, time), sorted ascending
        """
        # Tiingo timestamps look like "2024-01-02T00:00:00.000Z" -> parse as UTC.
        data["time"] = pd.to_datetime(data["time"], utc=True, errors="coerce")
        bad_ts = int(data["time"].isna().sum())
        if bad_ts:
            self.logger.warning(f"[TiingoCleaner] Dropping {bad_ts} rows with unparseable timestamps.")
            data = data.dropna(subset=["time"])

        for col in FLOAT_COLUMNS:
            data[col] = pd.to_numeric(data[col], errors="coerce").astype("float64")

        data["volume"] = pd.to_numeric(data["volume"], errors="coerce")
        bad_vol = int(data["volume"].isna().sum())
        if bad_vol:
            self.logger.warning(f"[TiingoCleaner] Dropping {bad_vol} rows with non-numeric volume.")
            data = data.dropna(subset=["volume"])
        data["volume"] = data["volume"].astype("int64")

        data["symbol"] = data["symbol"].str.strip().str.upper()

        pre_dedup = len(data)
        data = data.drop_duplicates(subset=["symbol", "time"])
        if pre_dedup - len(data):
            self.logger.info(f"[TiingoCleaner] Removed {pre_dedup - len(data)} duplicate (symbol, time) rows.")

        data = data.sort_values(by=["symbol", "time"]).reset_index(drop=True)
        self.logger.info(f"[TiingoCleaner] transform_data complete. Final shape: {data.shape}")
        return data

    def detect_time_gaps(self, data: pd.DataFrame, time_column: str = "time", freq: str = "B") -> List[pd.Timestamp]:
        """
        Detect missing trading days using business-day frequency ('B'), since EOD
        equity data skips weekends/holidays.
        """
        return super().detect_time_gaps(data, time_column=time_column, freq=freq)
