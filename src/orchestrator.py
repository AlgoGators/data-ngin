import logging
import asyncio
import pandas as pd
from typing import Dict, Any, List, Optional, Union
from dotenv import load_dotenv
from utils.dynamic_loader import get_instance, determine_date_range
from src.modules.data_access import DataAccess

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class Orchestrator:
    """
    Orchestrator class to execute the end-to-end data pipeline.
    Manages dynamic loading of loaders, fetchers, cleaners, and inserters.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config: Dict[str, Any] = config
        self.data_access: DataAccess = DataAccess()

        # Dynamically load system components based on configuration
        self.loader: Any = get_instance(self.config, "loader", "class")
        self.fetcher: Any = get_instance(self.config, "fetcher", "class")
        self.cleaner: Any = get_instance(self.config, "cleaner", "class")
        self.inserter: Any = get_instance(self.config, "inserter", "class")

    async def retrieve_and_process_data(self, symbol_metadata: Dict[str, Any], start_date: str, end_date: str) -> None:
        """
        Fetch, clean, and insert data for a single symbol.
        Note: Database connection is handled by the caller (run method).
        """
        try:
            batch_config = self.config.get("batch_downloading", {})
            is_batch_enabled = batch_config.get("batch", False)
            
            display_name = symbol_metadata.get('dataSymbol', 'Unknown')
            instrument = symbol_metadata.get('instrumentType', 'Unknown')

            if is_batch_enabled:
                unit = batch_config.get("unit")
                max_units = batch_config.get("max_units")

                logging.info(f"Fetching via Batch Download: {display_name} ({instrument})")
                raw_data: pd.DataFrame = await self.fetcher.generate_and_fetch_data(
                    symbol_metadata=symbol_metadata,
                    start_date=start_date,
                    end_date=end_date,
                    max_units_allowed=max_units,
                    unit=unit
                )
            else:
                logging.info(f"Fetching raw data for symbol: {display_name}")
                raw_data: pd.DataFrame = await self.fetcher.fetch_data(
                    symbol_metadata=symbol_metadata,
                    start_date=start_date,
                    end_date=end_date
                )
               
            if raw_data is None or raw_data.empty:
                logging.warning(f"No data returned for {display_name}")
                return

            # DYNAMIC TABLE NAMING
            safe_table_name = instrument.replace('/', '_').replace('-', '_').lower()

            # Insert raw data
            logging.info(f"Inserting raw data for: {display_name}")
            self.inserter.insert_data(
                data=raw_data.to_dict(orient="records"), 
                schema=self.config["database"]["target_schema"], 
                table=f"{safe_table_name}_raw"
            )
            
            # Clean data
            logging.info(f"Cleaning data for: {display_name}")
            cleaned_result = self.cleaner.clean(raw_data)

            # Check if result is a DataFrame and use .empty, otherwise check length if it's a list
            is_empty = cleaned_result.empty if isinstance(cleaned_result, pd.DataFrame) else not cleaned_result

            if is_empty:
                logging.warning(f"Cleaned data is empty for {display_name}")
                return

            # Insert cleaned data
            logging.info(f"Inserting cleaned data for: {display_name}")

            # Ensure we pass a list of dicts to the inserter
            data_to_insert = (
                cleaned_result.to_dict(orient="records") 
                if isinstance(cleaned_result, pd.DataFrame) 
                else cleaned_result
            )

            self.inserter.insert_data(
                data=data_to_insert, 
                schema=self.config["database"]["target_schema"], 
                table=safe_table_name
            )

        except Exception as e:
            logging.error(f"Failed to process symbol {display_name}: {e}")

    async def run(self) -> None:
        """
        Executes the data pipeline sequentially to ensure API stability and efficient DB usage.
        """
        try:
            logging.info("Loading metadata...")
            symbols_metadata = self.loader.load_symbols()

            # Determine date range from config
            start_date, end_date = determine_date_range(self.config)
            logging.info(f"Global Pipeline Range: {start_date} to {end_date}")
    
            # 1. Open Database connection ONCE for the whole batch
            self.inserter.connect()

            # 2. Sequential Execution (Loop)
            # We avoid asyncio.gather because the EIA API is unstable under concurrent load.
            if isinstance(symbols_metadata, list):
                # New path: List of dictionaries from CSV
                for metadata in symbols_metadata:
                    await self.retrieve_and_process_data(metadata, start_date, end_date)
                    # Optional: small delay to be gentle on the API
                    await asyncio.sleep(0.5) 
            else:
                # Legacy path: Dictionary mapping ticker to type
                for s, t in symbols_metadata.items():
                    await self.retrieve_and_process_data({"dataSymbol": s, "instrumentType": t}, start_date, end_date)
                    await asyncio.sleep(0.5)

            logging.info("Pipeline execution completed successfully.")

        except Exception as e:
            logging.error(f"Pipeline execution failed: {e}")
            raise
        finally:
            # 3. Close Database connection ONCE at the very end
            if hasattr(self.inserter, 'close'):
                self.inserter.close()
                logging.info("Database connection closed.")