import logging
import os
import asyncio
import aiohttp
import pandas as pd
from typing import Dict, Any
from dotenv import load_dotenv

# Reuse your existing Fetcher base class
from src.modules.fetcher.fetcher import Fetcher

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = "https://eodhd.com/api"


class EODHDFetcher(Fetcher):
    """
    Fetcher for EODHD Historical Data API.
    Fetches EOD (End-of-Day) OHLCV data for equities.

    Free tier supports demo symbols (AAPL.US, MSFT.US, etc.)
    and returns JSON/CSV data from the /eod endpoint.

    Docs: https://eodhd.com/financial-apis/api-for-historical-data-and-volumes/
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self.api_key: str = os.getenv("EODHD_API_KEY", "")
        if not self.api_key:
            raise EnvironmentError("EODHD_API_KEY not set in environment / .env file")

    async def fetch_data(
        self,
        symbol: str,
        loaded_asset_type: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Fetch EOD OHLCV data for a single symbol from EODHD.

        Args:
            symbol (str):            Ticker in EODHD format, e.g. "AAPL.US"
            loaded_asset_type (str): Asset type string, e.g. "EQUITY" (unused for routing here,
                                     kept to match Orchestrator's call signature)
            start_date (str):        "YYYY-MM-DD"
            end_date (str):          "YYYY-MM-DD"

        Returns:
            pd.DataFrame: Raw OHLCV dataframe with columns:
                          date, open, high, low, close, adjusted_close, volume, symbol
        """
        url = f"{BASE_URL}/eod/{symbol}"
        params = {
            "api_token": self.api_key,
            "from": start_date,
            "to": end_date,
            "fmt": "json",
            "order": "a",   # ascending date order
        }

        logger.info(f"[EODHD] Fetching EOD data for {symbol} from {start_date} to {end_date}")

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    text = await response.text()
                    raise RuntimeError(
                        f"[EODHD] HTTP {response.status} for {symbol}: {text}"
                    )
                data = await response.json()

        if not data:
            logger.warning(f"[EODHD] No data returned for {symbol}")
            return pd.DataFrame()

        # Handle API warning responses (e.g. free-tier limitations)
        if isinstance(data, list) and len(data) == 1 and "warning" in data[0]:
            logger.warning(f"[EODHD] API warning for {symbol}: {data[0]['warning']}")
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # Rename to match your pipeline's expected column naming convention
        df.rename(columns={"date": "time"}, inplace=True)

        # Tag with symbol so downstream cleaner/inserter knows the source
        df["symbol"] = symbol

        logger.info(f"[EODHD] Fetched {len(df)} rows for {symbol}")
        return df