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
        api_key: str = os.getenv("DATABENTO_API_KEY")
        self.client: db.Historical = db.Historical(api_key)
        self.logger: logging.Logger = logging.getLogger("DatabentoFetcher")
        self.logger.setLevel(logging.INFO)

    async def fetch_data(
        self,
        symbol: str,
        loaded_asset_type: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """
        Asynchronously fetches historical data based on asset type and dataset settings.

        Args:
            symbol (str): The symbol to fetch data for.
            start_date (str): Start date for fetching.
            end_date (str): End date for fetching.

        Returns:
            List[Dict[str, Any]]: Retrieved data as a list of dictionaries.
        """
        # Get schema and dataset settings from config
        schema: str = self.config["provider"]["schema"]
        dataset: str = self.config["provider"]["dataset"]
        
        # Check if loaded asset type matches config
        asset_type_config: str = self.config["provider"]["asset"]
        if loaded_asset_type != asset_type_config:
            raise ValueError(f"Asset type mismatch: {loaded_asset_type} != {asset_type_config}")
        else:
            asset_type = loaded_asset_type

        if asset_type == "FUTURE":
            # If pulling futures data, grab roll type and contract type from config then format symbol
            roll_type: str = self.config["provider"]["roll_type"]
            contract_type: str = self.config["provider"]["contract_type"]
            formatted_symbol: str = f"{symbol}.{roll_type}.{contract_type}"
            stype_in = db.SType.CONTINUOUS
            stype_out = db.SType.INSTRUMENT_ID
        elif asset_type == "EQUITY":
            # TODO: Implement equity data fetching
            pass
        elif asset_type == "OPTION":
            # TODO: Implement option data fetching
            pass

        try:
            # Fetch data
            data: db.Timeseries = await asyncio.to_thread(
                self.client.timeseries.get_range_async,
                dataset=dataset,
                symbols=formatted_symbol,
                schema=db.Schema.from_str(schema),
                start=start_date,
                end=end_date,
                stype_in=stype_in,
                stype_out=stype_out,
            )
            self.logger.info(f"Data fetched successfully for {symbol}.")
            return data.to_df()

        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            raise

    
