import os
import pandas as pd
from typing import Dict, Any
from fredapi import Fred
from data.modules.fetcher import Fetcher


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
        if not self.api_key:
            raise ValueError("FRED API key not found in environment variables.")
        self.client = Fred(api_key=self.api_key)
        self.series_metadata = config.get("loader", {}).get("series_metadata", {})

    def fetch_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetches data for a specific series from FRED.

        Args:
            symbol (str): The FRED series ID.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.

        Returns:
            pd.DataFrame: Data with columns ['time', 'index_name', 'value', 'metadata'].
        """
        try:
            # Fetch time-series data
            data = self.client.get_series(symbol, observation_start=start_date, observation_end=end_date)
            if data.empty:
                self.logger.warning(f"No data found for series {symbol}.")
                return pd.DataFrame(columns=["time", "index_name", "value", "metadata"])

            # Prepare DataFrame
            df = data.reset_index()
            df.columns = ["time", "value"]

            # Inject index_name and metadata
            index_name = self.series_metadata.get(symbol, {}).get("index_name", symbol)
            metadata = self.series_metadata.get(symbol, {}).get("metadata", {})

            df["index_name"] = index_name
            df["metadata"] = [metadata.copy() for _ in range(len(df))]  # Create a copy of metadata dict for each row

            return df[["time", "index_name", "value", "metadata"]]

        except Exception as e:
            self.logger.error(f"Error fetching data for series {symbol}: {e}")
            raise