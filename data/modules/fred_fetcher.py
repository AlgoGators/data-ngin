import os
import pandas as pd
from typing import Dict, Any
from fredapi import Fred
from data.modules.fetcher import Fetcher
import logging


class FREDFetcher(Fetcher):
    """
    Fetcher for retrieving data from the FRED API, including index name and metadata.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the FREDFetcher with API settings.

        Args:
            config (Dict[str, Any]): Configuration dictionary.
        """
        super().__init__(config=config)
        self.api_key = os.getenv("FRED_API_KEY")
        logging.info(f"API Key found: {'Yes' if self.api_key else 'No'}")
        logging.info(f"API Key length: {len(self.api_key) if self.api_key else 0}")

        if not self.api_key:
            raise ValueError("FRED API key not found in environment variables.")
        
        # Check if the API key has any whitespace or newlines
        if self.api_key.strip() != self.api_key:
            self.api_key = self.api_key.strip()
            logging.warning("API key contained whitespace - stripped")
            
        if len(self.api_key) != 32:
            raise ValueError(f"FRED API key should be 32 characters, got {len(self.api_key)}")
        
        self.client = Fred(api_key=self.api_key)
        self.series_metadata = config.get("loader", {}).get("series_metadata", {})

    async def fetch_data(self, symbol: str, series_info: Dict[str, Any], start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetches data for a specific series from FRED.
        """
        try:
            fred_id = series_info["fred_id"]
            logging.info(f"Fetching FRED series {fred_id} for {symbol}")
            
            # Fetch time-series data
            data = self.client.get_series(
                fred_id,
                observation_start=start_date,
                observation_end=end_date
            )
            
            if data.empty:
                logging.warning(f"No data found for series {fred_id}")
                return pd.DataFrame(columns=["time", "region", "value", "metadata"])

            # Prepare DataFrame
            df = data.reset_index()
            df.columns = ["time", "value"]

            # Add metadata
            df["index_name"] = symbol
            df["metadata"] = [series_info["metadata"]] * len(df)

            return df[["time", "index_name", "value", "metadata"]]

        except Exception as e:
            logging.error(f"Error fetching data for series {symbol}: {e}")
            raise