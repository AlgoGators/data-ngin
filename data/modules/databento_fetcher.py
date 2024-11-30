import asyncio
import os
import logging
from typing import List, Dict, Any
import databento as db
import pandas as pd
from data.modules.fetcher import Fetcher


class DatabentoFetcher(Fetcher):
    """
    A Fetcher subclass for retrieving raw data from Databento's API.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the DatabentoFetcher with API connection settings and configurations.

        Args:
            config (Dict[str, Any]): Configuration settings.
        """
        super().__init__(config)
        self.api_key: str = os.getenv("DATABENTO_API_KEY")
        self.client: db.Historical = db.Historical(self.api_key)
        self.logger: logging.Logger = logging.getLogger("DatabentoFetcher")
        self.logger.setLevel(logging.INFO)

    async def fetch_data(
        self,
        symbol: str,
        dataset: str,
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
        schema = schema or self.config["providers"]["databento"]["schema"]
        roll_type = roll_type or self.config["providers"]["databento"]["roll_type"]
        contract_type = contract_type or self.config["providers"]["databento"]["contract_type"]

        self.logger.info(f"Fetching data for {symbol} from {start_date} to {end_date}")
        symbols: str = f"{symbol}.{roll_type}.{contract_type}"

        try:
            # Fetch data
            data: db.Timeseries = await asyncio.to_thread(
                self.client.timeseries.get_range,
                dataset=dataset,
                symbols=[symbols],
                schema=db.Schema.from_str(schema),
                start=start_date,
                end=end_date,
                stype_in=db.SType.CONTINUOUS,
                stype_out=db.SType.INSTRUMENT_ID,
            )
            self.logger.info(f"Data fetched successfully for {symbol}.")
            return data.to_df()

        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            raise

    
