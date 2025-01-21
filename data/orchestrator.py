import logging
import asyncio
import pandas as pd
from typing import Dict, Any, List, Optional
import json
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

    async def retrieve_and_process_data(self, 
        series: str, 
        series_info: Dict[str, Any],
        start_date: str, 
        end_date: str
        ) -> None:
        """
        Fetch, clean, and insert data for a single symbol.

        Args:
            symbol (Dict[str, str]): Metadata for the symbol.
            start_date (str): Start date for fetching data.
            end_date (str): End date for fetching data. 

        Raises:
            Exception: If an error occurs during pipeline execution
        """
        try:
            logging.info(f"Processing data for series: {series}")
            
            # Extract schema and table from the table path
            schema, table = series_info['table'].split('.')
            logging.info(f"Target schema: {schema}, table: {table}")
            
            # Fetch raw data
            raw_data = await self.fetcher.fetch_data(
                symbol=series,  # Pass the series identifier
                series_info=series_info,
                start_date=start_date,
                end_date=end_date
            )

            if raw_data.empty:
                logging.warning(f"No data retrieved for series {series}")
                return

            # Clean data
            cleaned_data = self.cleaner.clean(raw_data)

            # Debug: Print DataFrame info and first few rows
            logging.info(f"Cleaned data info:")
            logging.info(f"Columns: {cleaned_data.columns.tolist()}")
            logging.info(f"Data types: {cleaned_data.dtypes}")
            logging.info("First row:")
            logging.info(cleaned_data.iloc[0].to_dict())

            # Connect and insert data
            self.inserter.connect()

            # Try direct DataFrame to_dict conversion first
            try:
                logging.info("Attempting direct DataFrame to records conversion...")
                records = cleaned_data.to_dict(orient='records')
                logging.info(f"Sample record: {records[0]}")
                
                self.inserter.insert_data(
                    data=records,
                    schema=schema,
                    table=table
                )
            except Exception as e:
                logging.error(f"Direct conversion failed: {e}")
                logging.info("Trying manual record conversion...")
                
                # Fallback to manual record creation
                records = []
                for idx, row in cleaned_data.iterrows():
                    record = {
                        'time': row['time'].isoformat() if hasattr(row['time'], 'isoformat') else str(row['time']),
                        'region': row['region'],
                        'value': float(row['value']) if pd.notnull(row['value']) else None,
                        'metadata': row['metadata']
                    }
                records.append(record)
            
                logging.info(f"Sample record for insertion: {records[0]}")
                
                self.inserter.insert_data(
                    data=records,
                    schema=schema,
                    table=table
                )

            logging.info(f"Successfully processed and inserted data for {series}")

        except Exception as e:
            logging.error(f"Failed to process series {series}: {e}")
            raise
        
        finally:
            self.inserter.close()

    async def run(self) -> None:
        """
        Executes the data pipeline for all series asynchronously.

        Raises:
            Exception: If an error occurs during pipeline execution
        """
        try:
            # Load metadata
            logging.info("Loading metadata...")
            series_data = self.loader.load_symbols()

            # Get date range from config
            start_date, end_date = determine_date_range(self.config)
            logging.info(f"Fetching data from {start_date} to {end_date}")
            
            # Process each series
            await asyncio.gather(*[
                self.retrieve_and_process_data(
                    series=series,
                    series_info=info,
                    start_date=start_date,
                    end_date=end_date
                )
                for series, info in series_data.items()
            ])

            logging.info("Pipeline execution completed successfully.")

        except Exception as e:
            logging.error(f"Pipeline execution failed: {e}")
            raise
