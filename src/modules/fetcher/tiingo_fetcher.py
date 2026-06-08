import logging
import os
import hashlib
import aiohttp
import pandas as pd
from typing import Dict, Any, List
from dotenv import load_dotenv

# Reuse the existing Fetcher base class
from src.modules.fetcher.fetcher import Fetcher

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = "https://api.tiingo.com/tiingo/daily"

# Tiingo field name -> pipeline/DB column name. We keep the full raw + adjusted
# set plus the dividend/split event fields (all returned in the same response).
RENAME_MAP = {
    "date": "time",
    "adjOpen": "adj_open",
    "adjHigh": "adj_high",
    "adjLow": "adj_low",
    "adjClose": "adjusted_close",
    "adjVolume": "adj_volume",
    "divCash": "div_cash",
    "splitFactor": "split_factor",
}

# Columns persisted to the equities / equities_raw tables. The fetcher trims its
# output to exactly these so the raw insert (which inserts every column verbatim)
# aligns with the table schema.
OUTPUT_COLUMNS = [
    "time",
    "open", "high", "low", "close", "volume",            # raw / as-traded
    "adj_open", "adj_high", "adj_low", "adjusted_close", "adj_volume",  # split/div adjusted
    "div_cash", "split_factor",                           # adjustment events
    "symbol",
]

# HTTP statuses that mean "this key is unusable right now" -> rotate to another key.
# 429 = hourly/daily allocation exhausted; 401/403 = invalid/placeholder/over-quota key.
KEY_LEVEL_FAILURES = (401, 403, 429)


class TiingoFetcher(Fetcher):
    """
    Fetcher for the Tiingo End-of-Day daily prices API, with multi-key rotation.

    Endpoint: https://api.tiingo.com/tiingo/daily/{ticker}/prices
    Auth:     ?token=<key>; keys are collected from every TIINGO_API_KEY* env var.
    Dates:    startDate / endDate are INCLUSIVE (no +1-day translation).

    Rotation:
      - Each symbol gets a STABLE primary key (hashlib-based) so it lands on the
        same key every run -> keeps each key under its 500-unique-symbols/month cap.
      - On a key-level failure (429/401/403) the key is disabled FOR THIS RUN and
        the symbol is retried with the next key. If all keys are disabled, raise
        (the orchestrator's per-symbol except logs it; the symbol refills next run).

    Docs: https://www.tiingo.com/documentation/end-of-day
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self.api_keys: List[str] = self._collect_api_keys()
        if not self.api_keys:
            raise EnvironmentError(
                "No TIINGO_API_KEY* keys set in environment / .env file"
            )
        # Keys disabled for THIS run (rate-limited / invalid). A fresh fetcher is
        # built per pipeline run, so this resets every run.
        self._disabled_keys: set = set()

    @staticmethod
    def _collect_api_keys() -> List[str]:
        """Collect every non-empty env var named TIINGO_API_KEY* in deterministic
        (name-sorted) order. Adding a key is purely a .env edit -- no code change."""
        keys: List[str] = []
        for name in sorted(os.environ):
            if name.startswith("TIINGO_API_KEY"):
                value = os.environ[name].strip()
                if value:
                    keys.append(value)
        return keys

    def _primary_index(self, symbol: str) -> int:
        """Stable per-symbol key assignment. Uses hashlib (NOT built-in hash(),
        which is salted per-process via PYTHONHASHSEED) so a symbol maps to the
        same key across runs/containers."""
        digest = hashlib.sha1(symbol.encode("utf-8")).hexdigest()
        return int(digest, 16) % len(self.api_keys)

    async def fetch_data(
        self,
        symbol: str,
        loaded_asset_type: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Fetch EOD OHLCV data for a single ticker, rotating keys on key-level failures.

        Returns:
            pd.DataFrame with OUTPUT_COLUMNS (empty DataFrame with those columns if
            Tiingo returns no data).

        Raises:
            RuntimeError: on a non-key error (e.g. 5xx) or when all keys are exhausted.
        """
        url = f"{BASE_URL}/{symbol}/prices"
        base_params = {"startDate": start_date, "endDate": end_date, "format": "json"}

        n = len(self.api_keys)
        start = self._primary_index(symbol)
        last_error = None

        logger.info(f"[Tiingo] Fetching EOD data for {symbol} from {start_date} to {end_date}")

        for offset in range(n):
            idx = (start + offset) % n
            key = self.api_keys[idx]
            if key in self._disabled_keys:
                continue
            params = {**base_params, "token": key}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._to_dataframe(data, symbol)
                    body = await response.text()
                    if response.status in KEY_LEVEL_FAILURES:
                        logger.warning(
                            f"[Tiingo] key #{idx} unusable for {symbol} "
                            f"(HTTP {response.status}); rotating to next key"
                        )
                        self._disabled_keys.add(key)
                        last_error = f"HTTP {response.status}: {body}"
                        continue
                    # Non-key error (5xx, etc.): fail this symbol, don't burn the key.
                    raise RuntimeError(f"[Tiingo] HTTP {response.status} for {symbol}: {body}")

        raise RuntimeError(
            f"[Tiingo] all {n} keys exhausted for {symbol}; last error: {last_error}"
        )

    def _to_dataframe(self, data: Any, symbol: str) -> pd.DataFrame:
        """Map Tiingo JSON to the persisted column set (rename + trim + tag symbol)."""
        if not data:
            logger.warning(f"[Tiingo] No data returned for {symbol}")
            return pd.DataFrame(columns=OUTPUT_COLUMNS)

        df = pd.DataFrame(data)
        df.rename(columns=RENAME_MAP, inplace=True)
        df["symbol"] = symbol

        missing = [c for c in OUTPUT_COLUMNS if c not in df.columns]
        if missing:
            raise RuntimeError(
                f"[Tiingo] Response for {symbol} missing expected fields {missing}. "
                f"Got columns: {list(df.columns)}"
            )
        df = df[OUTPUT_COLUMNS].copy()
        logger.info(f"[Tiingo] Fetched {len(df)} rows for {symbol}")
        return df


# Optional live smoke test: run directly with real keys in .env.
#   python -m src.modules.fetcher.tiingo_fetcher
if __name__ == "__main__":
    import asyncio

    async def _smoke():
        fetcher = TiingoFetcher(config={"provider": {"asset": "EQUITY"}})
        print(f"Collected {len(fetcher.api_keys)} key(s)")
        df = await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-05")
        print(df)
        print("columns:", list(df.columns))

    asyncio.run(_smoke())
