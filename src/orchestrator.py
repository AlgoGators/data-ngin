import logging
import asyncio
import pandas as pd
import os
import time
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from utils.dynamic_loader import get_instance, determine_date_range
from src.modules.data_access import DataAccess
from dashboard.metrics.prometheus_registry import start_metrics_server
from dashboard.metrics.metrics_definitions import PIPELINE_RUNS, EXECUTION_TIME, STAGE_ERRORS, RECORDS_PROCESSED, DATA_COMPLETENESS, LAST_SUCCESSFUL_RUN


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

        # Metrics server
        port = int(os.getenv("METRICS_PORT", "8003"))
        start_metrics_server(port)
        self.pipeline_name: str = self.config.get("pipeline_name", "default")

    async def retrieve_and_process_data(self, symbol: Dict[str, str], start_date: str, end_date: str) -> None:
        """
        Fetch, clean, and insert data for a single symbol.

        Args:
            symbol (Dict[str, str]): Metadata for the symbol.
            start_date (str): Start date for fetching data.
            end_date (str): End date for fetching data. 
        """
        try:
            batch_config = self.config.get("batch_downloading")
            is_batch_enabled = batch_config.get("batch")

            if is_batch_enabled:
                unit = batch_config.get("unit")
                max_units = batch_config.get("max_units")

                logging.info(f"Fetching raw data via Batch Download with parameters: {symbol['dataSymbol']}, loaded_asset_type: {symbol['instrumentType']}, start: {start_date}, end: {end_date}, max_units: {max_units}")
                start = time.perf_counter()
                try:
                    raw_data: pd.DataFrame = await self.fetcher.generate_and_fetch_data(
                        symbol=symbol['dataSymbol'],
                        loaded_asset_type=symbol['instrumentType'],
                        start_date=start_date,
                        end_date=end_date,
                        max_units_allowed = max_units,
                        unit = unit
                    )
                except Exception as e:
                    STAGE_ERRORS.labels(stage="fetcher", pipeline=self.pipeline_name, exc_type=type(e).__name__).inc()
                    raise
                duration = time.perf_counter() - start
                EXECUTION_TIME.labels(stage="fetcher").observe(duration)
                logging.info(f"Fetcher completed in {duration:.3f} seconds")

            
            elif not is_batch_enabled:

                logging.info(f"Fetching raw data for symbol: {symbol['dataSymbol']}")
                    
                    # Fetch raw data
                raw_data: pd.DataFrame = await self.fetcher.fetch_data(
                    symbol=symbol['dataSymbol'],
                    loaded_asset_type=symbol['instrumentType'],
                    start_date=start_date,
                    end_date=end_date,)
               
            # Connect to the database
            self.inserter.connect()
        
            # Insert raw data
            logging.info(f"Inserting raw data for symbol: {symbol['dataSymbol']}")
            start = time.perf_counter()
            try:
                start = time.perf_counter()
                self.inserter.insert_data(
                    data=raw_data.to_dict(orient="records"), 
                    schema=self.config["database"]["target_schema"], 
                    table=self.config["database"]["raw_table"]
                )
                n_raw = len(raw_data)
                RECORDS_PROCESSED.labels(dataset="raw", pipeline=self.pipeline_name).inc(n_raw)
            except Exception as e:
                STAGE_ERRORS.labels(stage="inserter", pipeline=self.pipeline_name, exc_type=type(e).__name__).inc()
                raise
            duration = time.perf_counter() - start
            EXECUTION_TIME.labels(stage="inserter").observe(duration)
            logging.info(f"Raw inserter completed in {duration:.3f} seconds")
            
            # Clean data
            logging.info(f"Cleaning data for symbol: {symbol['dataSymbol']}")
            start = time.perf_counter()
            try:
                cleaned_data: List[Dict[str, Any]] = self.cleaner.clean(raw_data)
            except Exception as e:
                STAGE_ERRORS.labels(stage="cleaner", pipeline=self.pipeline_name, exc_type=type(e).__name__).inc()
                raise
            duration = time.perf_counter() - start
            EXECUTION_TIME.labels(stage="cleaner").observe(duration)

            # Insert cleaned data
            logging.info(f"Inserting data for symbol: {symbol['dataSymbol']}")
            start = time.perf_counter()
            try:
                start = time.perf_counter()
                self.inserter.insert_data(
                    data=cleaned_data, 
                    schema=self.config["database"]["target_schema"], 
                    table=self.config["database"]["table"]
                )
                n_cleaned = len(cleaned_data)
                RECORDS_PROCESSED.labels(dataset="cleaned", pipeline=self.pipeline_name).inc(n_cleaned)
                expected_rows = self.config.get("expected_rows_per_asset", 0)
                if expected_rows > 0:
                    completeness_ratio = n_cleaned / expected_rows
                    DATA_COMPLETENESS.labels(symbol=symbol["dataSymbol"], pipeline=self.pipeline_name).set(completeness_ratio)
                else:
                    logging.warning("Expected rows per asset not defined; skipping completeness metric.")
            except Exception as e:
                STAGE_ERRORS.labels(stage="inserter", pipeline=self.pipeline_name, exc_type=type(e).__name__).inc()
                raise
            duration = time.perf_counter() - start
            EXECUTION_TIME.labels(stage="inserter").observe(duration)
            logging.info(f"Clean inserter completed in {duration:.3f} seconds")


        except Exception as e:
            logging.error(f"Failed to process symbol {symbol['dataSymbol']}: {e}")

        finally:
            self.inserter.close()

    async def run(self) -> None:
        """
        Executes the data pipeline for all symbols asynchronously.
        """
        try:
            pipeline_start = time.perf_counter()

            # Load metadata
            logging.info("Loading metadata...")
            start = time.perf_counter()
            try:
                symbols: Dict[str, str] = self.loader.load_symbols()
            except Exception as e:
                STAGE_ERRORS.labels(stage="loader", pipeline=self.pipeline_name, exc_type=type(e).__name__).inc()
                raise
            duration = time.perf_counter() - start
            EXECUTION_TIME.labels(stage="loader").observe(duration)
            logging.info(f"Loader completed in {duration:.3f} seconds")


            # Determine date range
            start_date, end_date = determine_date_range(self.config)

            logging.info(f"Fetching data from {start_date} to {end_date}")
    
            # Fetch, clean, and insert data for all symbols
            await asyncio.gather(*[
                self.retrieve_and_process_data({"dataSymbol": symbol, "instrumentType": asset_type}, start_date, end_date)
                for symbol, asset_type in symbols.items()
            ])
            pipeline_duration = time.perf_counter() - pipeline_start
            EXECUTION_TIME.labels(stage="total").observe(pipeline_duration)
            logging.info("Pipeline execution completed successfully.")
            current_timestamp = time.time()
            LAST_SUCCESSFUL_RUN.labels(pipeline=self.pipeline_name).set(current_timestamp)
            logging.info(f"Last successful pipeline run recorded at {current_timestamp}.")

        except Exception as e:
            logging.error(f"Pipeline execution failed: {e}")
            raise
