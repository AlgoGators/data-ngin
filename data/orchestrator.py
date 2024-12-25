import logging
import asyncio
import pandas as pd
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from utils.dynamic_loader import get_instance, determine_date_range
from data.modules.data_access import DataAccess


# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class Orchestrator:
    """
    Orchestrator class to execute the end-to-end data pipeline using asyncio.
    Manages dynamic loading of loaders, fetchers, cleaners, and inserters.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the Orchestrator with a given configuration.

        Attributes:
            config (Dict[str, Any]): Configuration settings     
            data_access (DataAccess): Data access object for interacting with the database.
            loader (Any): Loader instance for loading metadata.
            fetcher (Any): Fetcher instance for fetching raw data.
            cleaner (Any): Cleaner instance for cleaning raw data.
            inserter (Any): Inserter instance for inserting cleaned data.

        Args:
            config (Dict[str, Any]): Configuration settings     
        """
        self.config: Dict[str, Any] = config
        self.data_access: DataAccess = DataAccess()

        # Dynamically load system components based on configuration
        self.loader: Any = get_instance(self.config, "loader", "class")
        self.fetcher: Any = get_instance(self.config, "fetcher", "class")
        self.cleaner: Any = get_instance(self.config, "cleaner", "class")
        self.inserter: Any = get_instance(self.config, "inserter", "class")

    async def retrieve_and_process_data(self, symbol: Dict[str, str], start_date: str, end_date: str) -> None:
        """
        Fetch, clean, and insert data for a single symbol.

        Args:
            symbol (Dict[str, str]): Metadata for the symbol.
            start_date (str): Start date for fetching data.
            end_date (str): End date for fetching data. 
        """
        try:
            logging.info(f"Fetching raw data for symbol: {symbol['dataSymbol']}")
            
            # Fetch raw data
            raw_data: pd.DataFrame = await self.fetcher.fetch_data(
                symbol=symbol['dataSymbol'],
                loaded_asset_type=symbol['instrumentType'],
                start_date=start_date,
                end_date=end_date,
            )

            # Connect to the database
            self.inserter.connect()
        
            # Insert raw data
            logging.info(f"Inserting raw data for symbol: {symbol['dataSymbol']}")
            self.inserter.insert_data(
                data=raw_data.to_dict(orient="records"), 
                schema=self.config["database"]["target_schema"], 
                table=self.config["database"]["raw_table"]
            )
            
            # Clean data
            logging.info(f"Cleaning data for symbol: {symbol['dataSymbol']}")
            cleaned_data: List[Dict[str, Any]] = self.cleaner.clean(raw_data)

            # Insert cleaned data
            logging.info(f"Inserting data for symbol: {symbol['dataSymbol']}")
            self.inserter.insert_data(
                data=cleaned_data, 
                schema=self.config["database"]["target_schema"], 
                table=self.config["database"]["table"]
            )

        except Exception as e:
            logging.error(f"Failed to process symbol {symbol['dataSymbol']}: {e}")

        finally:
            self.inserter.close()

    async def run(self) -> None:
        """
        Executes the data pipeline for all symbols asynchronously.
        """
        try:
            # Load metadata
            logging.info("Loading metadata...")
            symbols: Dict[str, str] = self.loader.load_symbols()

            # Determine date range
            start_date, end_date = determine_date_range(self.config)

            logging.info(f"Fetching data from {start_date} to {end_date}")
    
            # Fetch, clean, and insert data for all symbols
            await asyncio.gather(*[
                self.retrieve_and_process_data({"dataSymbol": symbol, "instrumentType": asset_type}, start_date, end_date)
                for symbol, asset_type in symbols.items()
            ])
            logging.info("Pipeline execution completed successfully.")

        except Exception as e:
            logging.error(f"Pipeline execution failed: {e}")
            raise
