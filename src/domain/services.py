import pandas as pd
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from src.domain.models import RollEvent, BackAdjustment, StalenessReport


class SymbolRemapper:
    """
    Domain service for remapping raw provider symbols to their storage symbol
    (e.g. E-mini futures to their Micro equivalents for DB storage).

    Extracted verbatim from the `symbol_remap` dict that used to be duplicated
    inline in both DatabentoFetcher and BatchDownloadDatabentoFetcher.
    """

    DEFAULT_REMAP: Dict[str, str] = {
        "ES": "MES",
        "RTY": "M2K",
        "NQ": "MNQ",
        "YM": "MYM",
    }

    def __init__(self, remap_table: Optional[Dict[str, str]] = None) -> None:
        self.remap_table: Dict[str, str] = remap_table if remap_table is not None else dict(self.DEFAULT_REMAP)

    def remap(self, symbol: str) -> str:
        return self.remap_table.get(symbol, symbol)


class BackAdjuster:
    """
    Domain service wrapping the volume-based futures roll/back-adjustment
    algorithm. The roll-detection and cumulative-adjustment math is extracted
    verbatim from DatabentoCleaner.apply_back_adjustment -- no change there.

    `applies_to` is a deliberate, documented fix vs. the original: the old
    DatabentoCleaner called this unconditionally, even for EQUITY rows, where
    a futures-roll adjustment is meaningless. `apply()` now skips the
    adjustment when given an `asset_type` that doesn't match `applies_to`,
    configurable via config.yaml's `back_adjustment.applies_to` (defaults to
    "FUTURE", matching the only asset type this ever meaningfully ran for).
    """

    def __init__(self, applies_to: str = "FUTURE") -> None:
        self.applies_to = applies_to

    def detect_rolls(self, data: pd.DataFrame) -> List[RollEvent]:
        roll_events: List[RollEvent] = []
        for i in range(1, len(data)):
            if data.loc[i, "symbol"] != data.loc[i - 1, "symbol"]:
                if data.loc[i, "volume"] > data.loc[i - 1, "volume"]:
                    adjustment = data.loc[i - 1, "close"] - data.loc[i, "open"]
                    roll_events.append(RollEvent(
                        index=i,
                        prior_symbol=data.loc[i - 1, "symbol"],
                        new_symbol=data.loc[i, "symbol"],
                        adjustment=adjustment,
                    ))
        return roll_events

    def apply(self, data: pd.DataFrame, asset_type: Optional[str] = None) -> pd.DataFrame:
        if asset_type is not None and asset_type != self.applies_to:
            return data

        data = data.reset_index(drop=True)

        roll_events = self.detect_rolls(data)
        back_adjustment = BackAdjustment(roll_events=roll_events)

        cumulative_adjustments = [
            back_adjustment.cumulative_adjustment_at(i) for i in range(len(data))
        ]

        data["open"] = data["open"] + cumulative_adjustments
        data["high"] = data["high"] + cumulative_adjustments
        data["low"] = data["low"] + cumulative_adjustments
        data["close"] = data["close"] + cumulative_adjustments

        return data


class MissingDataFiller:
    """
    Domain service wrapping the missing-data fill-strategy dispatch. Extracted
    from DatabentoCleaner.handle_missing_data.

    A deliberate, documented fix vs. the original: strategy flags are now
    checked for truthiness rather than `== "True"`. The original string
    compare meant a real YAML bool `true` was silently a no-op (pinned by a
    Phase 0 characterization test) -- now that config.yaml is validated
    through Pydantic's `MissingDataConfig` (real bool fields), that compare
    would have started silently ignoring every flag. `is_active` still
    accepts the legacy "True"/"False" strings so callers that build this
    service directly with an unvalidated dict keep working too.
    """

    def __init__(self, missing_data_config: Optional[Dict] = None) -> None:
        self.config: Dict = missing_data_config or {}

    @staticmethod
    def _is_active(value: Any) -> bool:
        if isinstance(value, str):
            return value.strip().lower() == "true"
        return bool(value)

    def fill(self, data: pd.DataFrame) -> pd.DataFrame:
        numeric_columns = data.select_dtypes(include=["int64", "float64"]).columns

        method_switch = {
            "drop_nan": lambda d: d.dropna(),
            "forward_fill": lambda d: d.ffill(),
            "backward_fill": lambda d: d.bfill(),
            "interpolate": lambda d: d.infer_objects().interpolate(),
            "zero_fill": lambda d: d.fillna(0),
            "mean_fill": lambda d: d.fillna({col: d[col].mean() for col in numeric_columns}),
            "median_fill": lambda d: d.fillna({col: d[col].median() for col in numeric_columns}),
            "custom_fill": lambda d: d.fillna(self.config.get("custom_value", 0)),
        }

        for method, action in method_switch.items():
            if self._is_active(self.config.get(method, False)):
                data = action(data)

        return data


class StalenessChecker:
    """
    Domain service for detecting stale or gapped OHLCV data. Rebuilt
    replacement for src/modules/data_staleness.py (deleted in the repository
    consolidation phase): that file was broken -- it queried a
    `timestamp_column` that doesn't exist in the real schema (`time`) and
    loaded config from the wrong relative path -- and wasn't wired into any
    DAG, so nothing depended on it, but the *capability* (warn when the
    pipeline has silently stopped receiving fresh data) had no replacement
    until now. Pure domain logic -- takes dates/timestamps already read by a
    RepositoryPort, does no I/O of its own.
    """

    def __init__(self, max_staleness: timedelta = timedelta(days=1)) -> None:
        self.max_staleness = max_staleness

    def check_staleness(self, latest_date: Optional[str], now: Optional[datetime] = None) -> StalenessReport:
        """
        Args:
            latest_date (Optional[str]): The most recent date with data, as
                returned by RepositoryPort.get_latest_date() ("YYYY-MM-DD"),
                or None if the table is empty.
            now (Optional[datetime]): Defaults to datetime.now(); overridable for testing.
        """
        now = now or datetime.now()

        if latest_date is None:
            return StalenessReport(is_stale=True, message="No data found in the database.")

        latest = datetime.strptime(latest_date, "%Y-%m-%d")
        age = now - latest
        is_stale = age > self.max_staleness

        message = (
            f"Data is stale: latest date {latest_date} is {age} old (threshold {self.max_staleness})."
            if is_stale
            else f"Data is current: latest date {latest_date} is {age} old."
        )
        return StalenessReport(is_stale=is_stale, message=message, latest_date=latest_date, age=age)

    def detect_date_gaps(self, dates: List[str], freq: str = "D") -> List[str]:
        """
        Given a list of date strings (any order, duplicates OK), returns the
        missing dates within [min(dates), max(dates)] at the given frequency.
        Equivalent to the old data_staleness.py's row-to-row gap scan, but
        operating on dates already fetched by a caller (e.g.
        RepositoryPort.get_ohlcv_data()) rather than doing its own query --
        deliberately not auto-run on every pipeline run (a full-table date
        scan is expensive at scale); callers that want it call it explicitly.
        """
        if not dates:
            return []
        parsed = pd.to_datetime(sorted(set(dates)))
        full_range = pd.date_range(start=parsed.min(), end=parsed.max(), freq=freq)
        missing = full_range.difference(parsed)
        return missing.strftime("%Y-%m-%d").tolist()
