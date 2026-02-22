import asyncio
import os
import logging
from typing import List, Dict, Any, Optional
import databento as db
import pandas as pd
from src.modules.fetcher.fetcher import Fetcher
from datetime import timedelta   # New Import
import datetime # NEW IMPORT

class BatchDownloadDatabentoFetcher(Fetcher):
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
        self.logger: logging.Logger = logging.getLogger("BatchDownloadDatabentoFetcher")
        self.logger.setLevel(logging.INFO)

    async def generate_and_fetch_data(self, symbol: str, loaded_asset_type: str,start_date: str, end_date: str,unit: str, max_units_allowed: int):
        master_df = pd.DataFrame()

        batches = self.generate_batches(start_date,end_date, unit,max_units_allowed)

        for batch in batches:
             data = await self.fetch_data(symbol, loaded_asset_type, start_date = batch[0], end_date = batch[1])
             self.logger.info(f"Succesfully Fetched Data for Batch {batch[0]} to {batch[1]}.")
             master_df = pd.concat([data, master_df], ignore_index = True)

        return master_df

    
    def generate_batches(self, start_date: str, end_date: str, unit: str, max_units_allowed: int):
  
        """
        Generate time batches based on a specified time unit and maximum units allowed.
        
        Args:
            start_date (str): The starting date/time (in a format recognized by pd.Timestamp)
            end_date (str): The ending date/time.
            max_units_allowed (int): Maximum number of time units for each batch.
            unit (str): The time unit to use ("daily", "hourly", or "min").
        
        Returns:
            List of lists containing [batch_start, batch_end] formatted as strings.
        
        Raises:
            ValueError: If unit is not daily, hourly or min.
            TypeError: If max_units_allowed is not an integer.
            
        """
        

    
        # Convert the start and end strings into pandas Timestamps.
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)

        unit_lower = unit.lower()
        if unit_lower == "daily":
            time_unit = 'D'
            date_format = '%Y-%m-%d'
        elif unit_lower == "hourly":
            time_unit = 'H'
            date_format = '%Y-%m-%d %H:%M:%S'
        elif unit_lower == "min":
            time_unit = 'min'
            date_format = '%Y-%m-%d %H:%M:%S'
        else:
            raise ValueError(f"Unsupported time unit: {unit}")
        
        if type(max_units_allowed) != int:
            raise TypeError("The maximum units allowed must be an integer.")

        # Create the time delta for each batch.
        delta = pd.Timedelta(max_units_allowed, unit=time_unit)

        batches = []
        current_ts = start_ts

        while current_ts < end_ts:
            batch_end = current_ts + delta
            if batch_end > end_ts:
                batch_end = end_ts  
            batches.append([current_ts.strftime(date_format), batch_end.strftime(date_format)])
            current_ts = batch_end 
       
        return batches
    
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
            ValueError: If asset type doesn't match the configuration or an unsupported asset type is provided.
            Exception: If an error occurs during data retrieval.
        """
        schema: str = self.config["provider"]["schema"]
        dataset: str = self.config["provider"]["dataset"]
        asset_type_config: str = self.config["provider"]["asset"]

        # Check asset type
        if loaded_asset_type != asset_type_config:
            raise ValueError(f"Asset type mismatch: {loaded_asset_type} != {asset_type_config}")
        
        if loaded_asset_type == "FUTURE":
            roll_type: str = self.config["provider"]["roll_type"]
            contract_type: str = self.config["provider"]["contract_type"]
            formatted_symbol: str = f"{symbol}.{roll_type}.{contract_type}"
            stype_in = db.SType.CONTINUOUS
            stype_out = db.SType.INSTRUMENT_ID
        elif loaded_asset_type == "EQUITY":
            formatted_symbol = symbol
            stype_in = db.SType.RAW_SYMBOL
            stype_out = db.SType.INSTRUMENT_ID
        else:
            raise ValueError(f"Unsupported asset type: {loaded_asset_type}")
        

        try:
            
            # Fetch data
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

                        # --- Remap E-mini symbols to Micro equivalents for DB storage ---
            symbol_remap = {
                "ES": "MES",
                "RTY": "M2K",
                "NQ": "MNQ",
                "YM": "MYM"
            }

            mapped_symbol = symbol_remap.get(symbol, symbol)
            if mapped_symbol != symbol:
                self.logger.info(f"Fetched {symbol} data, remapping to {mapped_symbol} for storage")

            df["symbol"] = mapped_symbol

            # Check if data is empty
            if df.empty:
                self.logger.warning(f"No data found for {symbol} between {start_date} and {end_date}")
                return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "symbol"])

            if "ts_event" in df.index.names:
                df.reset_index(inplace=True)

            self.logger.info(f"Data fetched successfully for {symbol}.")
            return df


        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            raise    



## THIS METHOD MIGHT BE SLOW
    async def fetch_data_with_limit(
        self,
        symbol: str,
        loaded_asset_type: str,
        start_date: str,
        end_date: str,
        max_entries: int,
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
            ValueError: If asset type doesn't match the configuration or an unsupported asset type is provided.
            Exception: If an error occurs during data retrieval.
        """
        schema: str = self.config["provider"]["schema"]
        dataset: str = self.config["provider"]["dataset"]
        asset_type_config: str = self.config["provider"]["asset"]

        # Check asset type
        if loaded_asset_type != asset_type_config:
            raise ValueError(f"Asset type mismatch: {loaded_asset_type} != {asset_type_config}")
        
        if loaded_asset_type == "FUTURE":
            roll_type: str = self.config["provider"]["roll_type"]
            contract_type: str = self.config["provider"]["contract_type"]
            formatted_symbol: str = f"{symbol}.{roll_type}.{contract_type}"
            stype_in = db.SType.CONTINUOUS
            stype_out = db.SType.INSTRUMENT_ID
        elif loaded_asset_type == "EQUITY":
            formatted_symbol = symbol
            stype_in = db.SType.RAW_SYMBOL
            stype_out = db.SType.INSTRUMENT_ID
        else:
            raise ValueError(f"Unsupported asset type: {loaded_asset_type}")
        
        try:
            start = pd.Timestamp(start_date)
            end = pd.Timestamp(end_date)
            while start <= end:
                count = 0
                data = []
                # Fetch data
                async for record in await self.client.timeseries.get_range_async(
                    dataset=dataset,
                    symbols=formatted_symbol,
                    schema=db.Schema.from_str(schema),
                    start=start_date,
                    end=end_date,
                    stype_in=stype_in,
                    stype_out=stype_out,
                ):
                    data.append(record)
                    count += 1
                # Convert to DataFrame

                if count == max_entries:
                    count = 0
                    df = data.to_df()
                    start += max_entries
        

                    # Make sure db can run w/timestamps isntead of str -- would be easier

                # Check if data is empty
                if df.empty:
                    self.logger.warning(f"No data found for {symbol} between {start_date} and {end_date}")
                    return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "symbol"])

                if "ts_event" in df.index.names:
                    df.reset_index(inplace=True)

                self.logger.info(f"Data fetched successfully for {symbol}.")
                return df

        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            raise
        