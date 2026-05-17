import asyncio
import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import databento as db
import pandas as pd
from src.modules.fetcher.fetcher import Fetcher


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
            ValueError: If asset type doesn't match the configuration or an unsupported asset type is provided.
            Exception: If an error occurs during data retrieval.
        """
        schema: str = self.config["provider"]["schema"]
        dataset: str = self.config["provider"]["dataset"]
        asset_type_config: str = self.config["provider"]["asset"]

        # Check asset type
        if loaded_asset_type != asset_type_config:
            raise ValueError(f"Asset type mismatch: {loaded_asset_type} != {asset_type_config}")

        # --- Remap base symbols for futures BEFORE building formatted_symbol ---
        symbol_remap = {
            "ES": "MES",
            "RTY": "M2K",
            "NQ": "MNQ",
            "YM": "MYM",
        }

        if loaded_asset_type == "FUTURE":
            roll_type: str = self.config["provider"]["roll_type"]
            contract_type: str = self.config["provider"]["contract_type"]

            base_symbol_for_api = symbol_remap.get(symbol, symbol)
            if base_symbol_for_api != symbol:
                self.logger.info(
                    "Remapping futures base symbol from %s to %s for Databento + storage",
                    symbol,
                    base_symbol_for_api,
                )

            # This is what Databento sees AND what we will store, e.g. MES.v.0
            formatted_symbol: str = f"{base_symbol_for_api}.{roll_type}.{contract_type}"
            stype_in = db.SType.CONTINUOUS
            stype_out = db.SType.INSTRUMENT_ID
        elif loaded_asset_type == "EQUITY":
            formatted_symbol = symbol
            stype_in = db.SType.RAW_SYMBOL
            stype_out = db.SType.INSTRUMENT_ID
        else:
            raise ValueError(f"Unsupported asset type: {loaded_asset_type}")

        self.logger.info(
            "Preparing fetch for symbol=%s loaded_asset_type=%s formatted_symbol=%s "
            "roll_type=%s contract_type=%s stype_in=%s stype_out=%s",
            symbol,
            loaded_asset_type,
            formatted_symbol,
            self.config["provider"].get("roll_type"),
            self.config["provider"].get("contract_type"),
            stype_in,
            stype_out,
        )

        # Databento's `end` parameter is exclusive; shift by +1 day so callers'
        # inclusive end_date semantics are preserved at the API boundary.
        end_date_exclusive = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            # Fetch data
            data = await self.client.timeseries.get_range_async(
                dataset=dataset,
                symbols=formatted_symbol,
                schema=db.Schema.from_str(schema),
                start=start_date,
                end=end_date_exclusive,
                stype_in=stype_in,
                stype_out=stype_out,
            )
            # Convert to DataFrame
            df = data.to_df()

            # IMPORTANT: This controls what ultimately gets inserted into the DB.
            # For futures, df["symbol"] will be something like MES.v.0; for non-remapped
            # futures (e.g. 6A) it will be 6A.v.0; for equities it is the raw symbol.
            df["symbol"] = formatted_symbol
            self.logger.info(
                "Setting DataFrame symbol column. original_symbol=%s formatted_symbol=%s stored_symbol=%s "
                "unique_symbols_in_df_sample=%s",
                symbol,
                formatted_symbol,
                formatted_symbol,
                df["symbol"].head().unique().tolist() if "symbol" in df.columns else "N/A",
            )

            # Check if data is empty
            if df.empty:
                self.logger.warning(f"No data found for {symbol} between {start_date} and {end_date}")
                return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "symbol"])

            if "ts_event" in df.index.names:
                df.reset_index(inplace=True)

            self.logger.info(
                "Data fetched successfully for %s. rows=%d columns=%s",
                symbol,
                len(df),
                list(df.columns),
            )
            return df

        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            raise
        