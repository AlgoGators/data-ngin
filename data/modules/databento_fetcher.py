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
) -> pd.DataFrame: 
        """
        Asynchronously fetches historical data based on asset type and dataset settings.

        Args:
            symbol (str): The symbol to fetch data for.
            loaded_asset_type (str): Type of asset to load (e.g., "FUTURE")
            start_date (str): Start date for fetching.
            end_date (str): End date for fetching.

        Returns:
            pd.DataFrame: Retrieved data as a pandas DataFrame.

        Raises:
            ValueError: If asset type doesn't match configuration
            Exception: If API call fails
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
            # Fetch data - note we await the result directly
            data = await self.client.timeseries.get_range_async(
                dataset=dataset,
                symbols=formatted_symbol,
                schema=db.Schema.from_str(schema),
                start=start_date,
                end=end_date,
                stype_in=stype_in,
                stype_out=stype_out,
            )
            
            # Convert to DataFrame
            df = data.to_df()
            
            # Check if we got any data
            if df.empty:
                self.logger.warning(f"No data found for {symbol} between {start_date} and {end_date}")
                # Create DataFrame with expected columns but no rows
                return pd.DataFrame(columns=[
                    "time", "open", "high", "low", "close", 
                    "volume", "symbol"
                ])
                
            self.logger.info(f"Data fetched successfully for {symbol}.")
            return df

        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            raise

    
