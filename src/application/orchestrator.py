import logging
import asyncio
import pandas as pd
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
from src.utils.dynamic_loader import get_instance, determine_date_range
from src.utils.logging_config import setup_logging
from src.domain.ports import CleanerPort, FetcherPort, RepositoryPort, SymbolSourcePort


# Load environment variables
load_dotenv()

# Set up logging
setup_logging()


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
            loader (SymbolSourcePort): Loader instance for loading metadata.
            fetcher (FetcherPort): Fetcher instance for fetching raw data.
            cleaner (CleanerPort): Cleaner instance for cleaning raw data.
            inserter (RepositoryPort): Inserter instance for inserting cleaned data (built fresh per symbol -- see retrieve_and_process_data).

        Args:
            config (Dict[str, Any]): Configuration settings

        Raises:
            TypeError: If a configured loader/fetcher/cleaner class doesn't
                implement the port its role requires (see get_instance's
                expected_port) -- e.g. a config.yaml typo pointing "fetcher"
                at a class with no retrieve() method fails here, not mid-pipeline.
        """
        self.config: Dict[str, Any] = config

        # Dynamically load system components based on configuration. Each is
        # checked against the port its role requires (src/domain/ports.py) --
        # a misconfigured class fails at construction, not mid-pipeline.
        self.loader: SymbolSourcePort = get_instance(self.config, "loader", "class", expected_port=SymbolSourcePort)
        self.fetcher: FetcherPort = get_instance(self.config, "fetcher", "class", expected_port=FetcherPort)
        self.cleaner: CleanerPort = get_instance(self.config, "cleaner", "class", expected_port=CleanerPort)

    async def retrieve_and_process_data(self, symbol: Dict[str, str], start_date: str, end_date: str) -> bool:
        """
        Fetch, clean, and insert data for a single symbol.

        Args:
            symbol (Dict[str, str]): Metadata for the symbol.
            start_date (str): Start date for fetching data.
            end_date (str): End date for fetching data.

        Returns:
            bool: True if the symbol was processed successfully.

        Raises:
            Exception: Re-raises any error encountered while processing the symbol,
                so that `run()`'s `asyncio.gather` can see and report the failure
                instead of it being silently swallowed.
        """
        # A fresh inserter instance (and connection) per symbol, since symbols are
        # processed concurrently via asyncio.gather in run() -- sharing one inserter
        # across tasks meant one symbol's `finally: close()` could tear down the
        # connection another symbol was still inserting on.
        inserter: RepositoryPort = get_instance(self.config, "inserter", "class", expected_port=RepositoryPort)
        batch_config = self.config.get("batch_downloading", {})

        try:
            logging.info(f"Fetching raw data for symbol: {symbol['dataSymbol']}, loaded_asset_type: {symbol['instrumentType']}, start: {start_date}, end: {end_date}")
            raw_data: pd.DataFrame = await self.fetcher.retrieve(
                symbol=symbol['dataSymbol'],
                loaded_asset_type=symbol['instrumentType'],
                start_date=start_date,
                end_date=end_date,
                batch_config=batch_config,
            )

            # Connect to the database
            inserter.connect()

            # Insert raw data
            logging.info(f"Inserting raw data for symbol: {symbol['dataSymbol']}")
            inserter.insert_data(
                data=raw_data.to_dict(orient="records"),
                schema=self.config["database"]["target_schema"],
                table=self.config["database"]["raw_table"]
            )

            # Clean data
            logging.info(f"Cleaning data for symbol: {symbol['dataSymbol']}")
            cleaned_data: List[Dict[str, Any]] = self.cleaner.clean(raw_data)

            # Insert cleaned data
            logging.info(f"Inserting data for symbol: {symbol['dataSymbol']}")
            inserter.insert_data(
                data=cleaned_data,
                schema=self.config["database"]["target_schema"],
                table=self.config["database"]["table"]
            )

            return True

        except Exception as e:
            logging.error(f"Failed to process symbol {symbol['dataSymbol']}: {e}")
            raise

        finally:
            inserter.close()

    def get_symbols(self) -> List[Dict[str, str]]:
        """
        Stage-level entry point: resolves which symbols to process, without
        touching the date range or fetching/cleaning/inserting anything. Kept
        separate from get_date_range() so a caller that only needs one half
        (e.g. an Airflow task) doesn't pay for the other -- get_date_range()
        can open a DB connection via determine_date_range(); a symbols-only
        caller shouldn't need the DB to be reachable at all.

        Returns:
            List[Dict[str, str]]: [{"dataSymbol": ..., "instrumentType": ...}, ...]
        """
        logging.info("Loading metadata...")
        symbols: Dict[str, str] = self.loader.load_symbols()
        return [
            {"dataSymbol": symbol, "instrumentType": asset_type}
            for symbol, asset_type in symbols.items()
        ]

    def get_date_range(self) -> Dict[str, str]:
        """
        Stage-level entry point: resolves the date range to process, without
        touching symbols. See get_symbols() for why these are separate methods.

        Returns:
            Dict[str, str]: {"start_date": str, "end_date": str}
        """
        start_date, end_date = determine_date_range(self.config)
        logging.info(f"Fetching data from {start_date} to {end_date}")
        return {"start_date": start_date, "end_date": end_date}

    def get_symbols_and_date_range(self) -> Dict[str, Any]:
        """
        Convenience wrapper combining get_symbols() + get_date_range() for
        callers (e.g. run()) that need both. Airflow tasks that only need one
        half should call that method directly instead -- see get_symbols().

        Returns:
            Dict[str, Any]: {"symbols": [{"dataSymbol": ..., "instrumentType": ...}, ...],
                "start_date": str, "end_date": str}
        """
        return {
            "symbols": self.get_symbols(),
            **self.get_date_range(),
        }

    async def run(self) -> None:
        """
        Executes the data pipeline for all symbols asynchronously.

        Raises:
            RuntimeError: If any symbol failed to process. Lists every failed
                symbol and its error so a partial failure can never look like
                a full success (see retrieve_and_process_data, which used to
                swallow per-symbol exceptions and made asyncio.gather report
                success even when every symbol failed).
        """
        meta = self.get_symbols_and_date_range()
        symbols = meta["symbols"]
        start_date, end_date = meta["start_date"], meta["end_date"]

        # Fetch, clean, and insert data for all symbols
        results = await asyncio.gather(*[
            self.retrieve_and_process_data(symbol, start_date, end_date)
            for symbol in symbols
        ], return_exceptions=True)

        failures = [
            (symbol["dataSymbol"], result)
            for symbol, result in zip(symbols, results)
            if isinstance(result, Exception)
        ]

        if failures:
            summary = "; ".join(f"{symbol}: {error}" for symbol, error in failures)
            logging.error(f"Pipeline execution failed for {len(failures)}/{len(symbols)} symbol(s): {summary}")
            raise RuntimeError(f"Pipeline execution failed for {len(failures)}/{len(symbols)} symbol(s): {summary}")

        logging.info("Pipeline execution completed successfully.")
