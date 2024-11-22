import asyncio
import os
import logging
from typing import List, Dict, Any
import databento as db
import pandas as pd
from data.modules.fetcher import Fetcher


class DatabentoFetcher(Fetcher):
    """
    A Fetcher subclass for retrieving data from Databento's API, processing it,
    and preparing it for insertion into TimescaleDB.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the DatabentoFetcher with API connection settings and configurations.

        Args:
            config (Dict[str, Any]): Configuration settings, including API details.
        """
        super().__init__(config)
        self.api_key: str = os.getenv("DATABENTO_API_KEY")
        self.client: db.Historical = db.Historical(self.api_key)
        self.logger: logging.Logger = logging.getLogger("DatabentoFetcher")
        self.logger.setLevel(logging.INFO)

    async def fetch_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        schema: str = None,
        roll_type: str = None,
        contract_type: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches historical OHLCV data from Databento, cleans it,
        and prepares it for database insertion.

        Args:
            symbol (str): The symbol to fetch data for.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.
            schema (str): Data aggregation schema (e.g., 'ohlcv-1d').
            roll_type (str): Roll type for futures contracts (e.g., 'c').
            contract_type (str): Contract type to retrieve (e.g., 'front').

        Returns:
            List[Dict[str, Any]]: List of dictionaries containing cleaned data ready for TimescaleDB insertion.
        """
        schema = schema or self.config["providers"]["databento"]["datasets"]["GLOBEX"]["aggregation_levels"][0]
        roll_type = roll_type or self.config["providers"]["databento"]["roll_type"][0]
        contract_type = contract_type or self.config["providers"]["databento"]["contract_type"][0]

        self.logger.info(f"Fetching data for {symbol} from {start_date} to {end_date}")
        symbols: str = f"{symbol}.{roll_type}.{contract_type}"

        try:
            # Fetch data
            data: db.Timeseries = await asyncio.to_thread(
                self.client.timeseries.get_range,
                dataset=self.config["providers"]["databento"]["datasets"]["GLOBEX"]["schema_name"],
                symbols=[symbols],
                schema=db.Schema.from_str(schema),
                start=start_date,
                end=end_date,
                stype_in=db.SType.CONTINUOUS,
                stype_out=db.SType.INSTRUMENT_ID,
            )
            self.logger.info(f"Data fetched successfully for {symbol}.")
            
            # Clean data
            cleaned_data: List[Dict[str, Any]] = self.clean_data(data.to_df())
            self.logger.info(f"Data cleaned successfully for {symbol}.")
            return cleaned_data

        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            raise

    def clean_data(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Cleans and formats raw OHLCV data fetched from Databento.

        Args:
            data (pd.DataFrame): Raw data from Databento API.

        Returns:
            List[Dict[str, Any]]: Cleaned data as a list of dictionaries.

        Raises:
            ValueError: If required columns are missing in the data.
        """
        if data.empty:
            self.logger.error("Received empty DataFrame for cleaning.")
            raise ValueError("DataFrame is empty. Cannot clean data.")

        required_columns = {"date", "open", "high", "low", "close", "volume"}
        missing_columns = required_columns - set(data.columns)
        if missing_columns:
            self.logger.error(f"Missing columns in data: {missing_columns}")
            raise ValueError(f"Missing columns in data: {missing_columns}")

        cleaned_data: List[Dict[str, Any]] = [
            {
                "time": row["date"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"]),
                "symbol": row.get("symbol", None)  # Include symbol if needed for DB
            }
            for _, row in data.iterrows()
        ]
        return cleaned_data
