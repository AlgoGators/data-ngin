import logging
import os
import yaml
import asyncio
from dotenv import load_dotenv
from utils.dynamic_loader import get_instance  # Import the dynamic loader functions

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

    def __init__(self, config_path: str) -> None:
        """
        Initializes the Orchestrator with a given configuration.

        Args:
            config_path (str): Path to the configuration file.
        """
        # Load configuration file
        with open(config_path, "r") as file:
            self.config = yaml.safe_load(file)

        # Dynamically load the loader, fetcher, cleaner, and inserter
        self.loader = get_instance(self.config, "loader", "class", file_path=self.config["loader"]["file_path"])
        self.fetcher = get_instance(self.config, "providers", "fetcher_class", provider="databento")
        self.cleaner = get_instance(self.config, "providers", "cleaner_class", provider="databento")
        self.inserter = get_instance(self.config, "inserter", "class")

    async def fetch_and_process(self, symbol: dict) -> None:
        """
        Fetch, clean, and insert data for a single symbol.

        Args:
            symbol (dict): Metadata for the symbol (e.g., from contract.csv).
        """
        try:
            logging.info(f"Fetching data for symbol: {symbol['dataSymbol']}")
            raw_data = await self.fetcher.fetch_and_process_data(
                symbol=symbol["dataSymbol"],
                start_date=self.config["time_range"]["start_date"],
                end_date=self.config["time_range"]["end_date"],
                schema=self.config["providers"]["databento"]["datasets"]["GLOBEX"]["aggregation_levels"][0],
                roll_type=self.config["providers"]["databento"]["roll_type"],
                contract_type=self.config["providers"]["databento"]["contract_type"]
            )

            logging.info(f"Cleaning data for symbol: {symbol['dataSymbol']}")
            cleaned_data = self.cleaner.clean(raw_data)

            logging.info(f"Inserting data for symbol: {symbol['dataSymbol']}")
            self.inserter.insert_data(
                data=cleaned_data,
                schema=self.config["database"]["target_schema"],
                table=self.config["providers"]["databento"]["datasets"]["GLOBEX"]["table_prefix"] + "1d"
            )

        except Exception as e:
            logging.error(f"Failed to process symbol {symbol['dataSymbol']}: {e}")

    async def run(self) -> None:
        """
        Executes the data pipeline for all symbols asynchronously.
        """
        try:
            # Step 1: Load metadata
            logging.info("Loading metadata...")
            symbols = self.loader.load_symbols(self.config["loader"]["file_path"])

            # Step 2: Fetch, clean, and insert data for all symbols concurrently
            await asyncio.gather(*[self.fetch_and_process(symbol) for symbol in symbols])

            logging.info("Pipeline execution completed successfully.")

        except Exception as e:
            logging.error(f"Pipeline execution failed: {e}")
            raise
