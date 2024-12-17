import logging
import os
import yaml
import asyncio
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from utils.dynamic_loader import get_instance
from data.modules.data_access import DataAccess
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class Orchestrator:
    """
    Orchestrator class to execute the end-to-end data pipeline using asyncio.

    Dynamically loads:
        - Loader
        - Fetcher
        - Cleaner
        - Inserter
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the Orchestrator with a given configuration.

        Args:
            config (Dict[str, Any]): Configuration settings        
        """
        # Load configuration file
        self.config: Dict[str, Any] = config

        # Dynamically load modules
        self.loader: Any = get_instance(self.config, "loader", "class")
        self.fetcher: Any = get_instance(self.config, "fetcher", "class")
        self.cleaner: Any = get_instance(self.config, "cleaner", "class")
        self.inserter: Any = get_instance(self.config, "inserter", "class")

        # Initialize data access layer
        self.data_access: DataAccess = DataAccess()

    async def fetch_data(self, symbol: Dict[str, str], start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
        """
        Fetch, clean, and insert data for a single symbol.

        Args:
            symbol (Dict[str, str]): Metadata for the symbol (e.g., from contract.csv).
            start_date (Optional[str]): Start date for fetching data. Defaults to config value.
            end_date (Optional[str]): End date for fetching data. Defaults to config value.
        """
        try:
            # Use configured time range if not provided
            start_date: str = start_date or self.config["time_range"]["start_date"]
            end_date: str = end_date or self.config["time_range"]["end_date"]

            raw_data: List[Dict[str, Any]] = await self.fetcher.fetch_data(
                symbol=symbol['dataSymbol'],
                dataset=self.config['providers']['databento']['dataset'],
                start_date=start_date,
                end_date=end_date,
                schema=self.config["providers"]["databento"]["schema"],
                roll_type=self.config["providers"]["databento"]["roll_type"],
                contract_type=self.config["providers"]["databento"]["contract_type"]
            )

            logging.info(f"Cleaning data for symbol: {symbol['dataSymbol']}")
            cleaned_data: List[Dict[str, Any]] = self.cleaner.clean(raw_data)

            logging.info(f"Inserting data for symbol: {symbol['dataSymbol']}")
            self.inserter.connect()
            self.inserter.insert_data(
                data=cleaned_data,
                schema=self.config["database"]["target_schema"],
                table=self.config["database"]["table"]
            )

        except Exception as e:
            logging.error(f"Failed to process symbol {symbol['dataSymbol']}: {e}")

    async def run(self) -> None:
        """
        Executes the data pipeline for all symbols asynchronously.
        """
        try:
            # Load metadata
            logging.info("Loading metadata...")
            symbols: Dict[str, str] = self.loader.load_symbols()

            # Determine the start and end dates
            latest_date: Optional[str] = self.data_access.get_latest_date()
            start_date: str = (
                (datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                if latest_date else self.config["time_range"]["start_date"]
            )
            end_date: str = datetime.now().strftime("%Y-%m-%d")

            logging.info(f"Fetching data from {start_date} to {end_date}")
    
            await asyncio.gather(*[
                self.fetch_data({"dataSymbol": symbol, "instrumentType": asset_type}, start_date, end_date)
                for symbol, asset_type in symbols.items()
            ])
            logging.info("Pipeline execution completed successfully.")

        except Exception as e:
            logging.error(f"Pipeline execution failed: {e}")
            raise
