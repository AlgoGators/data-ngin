import asyncio
from typing import List, Dict, Any
import databento as db
import pandas as pd
from data.modules.fetcher import Fetcher
import logging

logging.basicConfig(level=logging.INFO)

class DatabentoFetcher(Fetcher):
    """
    A Fetcher subclass for retrieving data from Databento's API, processing it
    according to the specifications for insertion into TimescaleDB.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the DatabentoFetcher with API connection settings and configurations.

        Args:
            config (Dict[str, Any]): Configuration settings, including API details.
        """
        super().__init__(config)
        self.api_key: str = config["providers"]["databento"]["api_key"]
        self.client: db.Historical = db.Historical(self.api_key)
        self.logger: logging.Logger = logging.getLogger("DatabentoFetcher")

    def fetch_data(self, symbol: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Synchronously fetches data by calling the async fetch_and_process_data and blocking until it completes.

        Args:
            symbol (str): The symbol for which to fetch data.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.

        Returns:
            List[Dict[str, Any]]: Cleaned data as a list of dictionaries.
        """
        return asyncio.run(self.fetch_and_process_data(symbol, start_date, end_date))

    async def fetch_and_process_data(
        self, symbol: str, start_date: str, end_date: str, schema: str = "ohlcv-1d", roll_type: str = "c", contract_type: str = "front"
    ) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches historical OHLCV data from Databento, cleans it, and prepares it for database insertion.

        Args:
            symbol (str): The symbol to fetch data for.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.
            schema (str): Data aggregation schema, e.g., 'ohlcv-1d'.
            roll_type (str): Type of roll for futures contracts.
            contract_type (str): Contract type to retrieve (e.g., 'front').

        Returns:
            List[Dict[str, Any]]: List of dictionaries containing cleaned data ready for TimescaleDB insertion.
        """
        symbols: str = f"{symbol}.{roll_type}.{contract_type}"
        self.logger.info(f"Fetching data for {symbol} from {start_date} to {end_date}")

        try:
            data: db.Timeseries = await asyncio.to_thread(
                self.client.timeseries.get_range,
                dataset=self.config["dataset"],
                symbols=[symbols],
                schema=db.Schema.from_str(schema),
                start=start_date,
                end=end_date,
                stype_in=db.SType.CONTINUOUS,
                stype_out=db.SType.INSTRUMENT_ID,
            )

            cleaned_data: List[Dict[str, Any]] = self.clean_data(data.to_df())
            return cleaned_data

        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            raise

    def clean_data(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Cleans and formats raw OHLCV data fetched from Databento, preparing it for database insertion.

        Args:
            data (pd.DataFrame): Raw data from Databento API.

        Returns:
            List[Dict[str, Any]]: Cleaned data as a list of dictionaries.
        """
        required_columns = ["date", "open", "high", "low", "close", "volume"]

        cleaned_data: List[Dict[str, Any]] = [
            {
                "time": row["date"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"]),
                "symbol": row.get("symbol", None)  # Include symbol if needed for db
            }
            for _, row in data.iterrows() if all(col in row for col in required_columns)
        ]
        
        return cleaned_data
