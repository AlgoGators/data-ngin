import logging
import os
import aiohttp
import pandas as pd
from typing import Dict, Any
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


class TiingoFetcher(Fetcher):
    """
    Fetcher for the Tiingo End-of-Day daily prices API.
    Fetches EOD OHLCV (+ adjusted close) data for equities/ETFs.

    Endpoint: https://api.tiingo.com/tiingo/daily/{ticker}/prices
    Auth:     ?token=<TIINGO_API_KEY>
    Dates:    startDate / endDate are INCLUSIVE (unlike Databento's exclusive end),
              so no +1-day translation is required.

    Docs: https://www.tiingo.com/documentation/end-of-day
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        # Tiingo tokens are 40-char lowercase hex; .strip() guards against stray
        # whitespace in the .env file.
        self.api_key: str = os.getenv("TIINGO_API_KEY", "").strip()
        if not self.api_key:
            raise EnvironmentError("TIINGO_API_KEY not set in environment / .env file")

    async def fetch_data(
        self,
        symbol: str,
        loaded_asset_type: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Fetch EOD OHLCV data for a single ticker from Tiingo.

        Args:
            symbol (str):            Plain ticker, e.g. "AAPL" (no exchange suffix).
            loaded_asset_type (str): Asset type, e.g. "EQUITY" (kept to match the
                                     Orchestrator's call signature; not used for routing).
            start_date (str):        "YYYY-MM-DD" (inclusive).
            end_date (str):          "YYYY-MM-DD" (inclusive).

        Returns:
            pd.DataFrame: OHLCV dataframe with the full raw + adjusted + event column
                          set (see OUTPUT_COLUMNS). Empty DataFrame (with those columns)
                          if Tiingo returns no data.
        """
        url = f"{BASE_URL}/{symbol}/prices"
        params = {
            "token": self.api_key,
            "startDate": start_date,
            "endDate": end_date,      # INCLUSIVE — no +1 needed
            "format": "json",
        }

        logger.info(f"[Tiingo] Fetching EOD data for {symbol} from {start_date} to {end_date}")

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    text = await response.text()
                    raise RuntimeError(f"[Tiingo] HTTP {response.status} for {symbol}: {text}")
                data = await response.json()

        if not data:
            logger.warning(f"[Tiingo] No data returned for {symbol}")
            return pd.DataFrame(columns=OUTPUT_COLUMNS)

        df = pd.DataFrame(data)

        # Map Tiingo field names to the pipeline's column convention.
        df.rename(columns=RENAME_MAP, inplace=True)

        # Tag rows with the symbol so the cleaner/inserter can attribute them.
        df["symbol"] = symbol

        # Trim to the persisted schema so the raw insert matches the table columns.
        missing = [c for c in OUTPUT_COLUMNS if c not in df.columns]
        if missing:
            raise RuntimeError(
                f"[Tiingo] Response for {symbol} missing expected fields {missing}. "
                f"Got columns: {list(df.columns)}"
            )
        df = df[OUTPUT_COLUMNS].copy()

        logger.info(f"[Tiingo] Fetched {len(df)} rows for {symbol}")
        return df
