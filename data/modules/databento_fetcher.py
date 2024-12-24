import asyncio
import os
import logging
from typing import List, Dict, Any, Optional
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
        
        # Determine fetch type
        if self.config["provider"]["batch_job"]:
            return await self._handle_batch_job(
                symbol=formatted_symbol,
                dataset=dataset,
                schema=schema,
                start_date=start_date,
                end_date=end_date,
                stype_in=stype_in,
                stype_out=stype_out,
            )
        else:
            return await self._handle_regular_fetch(
                symbol=formatted_symbol,
                dataset=dataset,
                schema=schema,
                start_date=start_date,
                end_date=end_date,
                stype_in=stype_in,
                stype_out=stype_out,
            )
        
    async def _handle_regular_fetch(
        self,
        symbol: str,
        dataset: str,
        schema: str,
        start_date: str,
        end_date: str,
        stype_in: db.SType,
        stype_out: db.SType,
    ) -> pd.DataFrame:
        """
        Handles fetching data regularly (non-batch).

        Args:
            symbol (str): The symbol to fetch data for.
            asset_type (str): Type of asset to load (e.g., "FUTURE")
            dataset (str): The dataset to fetch data from.
            schema (str): The schema to fetch data in.
            start_date (str): Start date for fetching.
            end_date (str): End date for fetching.
            stype_in (db.SType): Input symbology type.
            stype_out (db.SType): Output symbology type.

        Returns:
            pd.DataFrame: Retrieved data as a pandas DataFrame.
        """
        try:
            # Fetch data
            data = await self.client.timeseries.get_range_async(
                dataset=dataset,
                symbols=symbol,
                schema=db.Schema.from_str(schema),
                start=start_date,
                end=end_date,
                stype_in=stype_in,
                stype_out=stype_out,
            )
            df = data.to_df()
            if df.empty:
                self.logger.warning(f"No data found for {symbol} between {start_date} and {end_date}")
                return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "symbol"])

            self.logger.info(f"Data fetched successfully for {symbol}.")
            return df

        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            raise

    async def _handle_batch_job(
        self,
        symbol: str,
        dataset: str,
        schema: str,
        start_date: str,
        end_date: str,
        stype_in: db.SType,
        stype_out: db.SType,
    ) -> pd.DataFrame:
        """
        Handles fetching data via a batch job.

        Args:
            symbol (str): The symbol to fetch data for.
            asset_type (str): Type of asset to load (e.g., "FUTURE")
            dataset (str): The dataset to fetch data from.
            schema (str): The schema to fetch data in.
            start_date (str): Start date for fetching.
            end_date (str): End date for fetching.
            stype_in (db.SType): Input symbology type.
            stype_out (db.SType): Output symbology type.

        Returns:
            pd.DataFrame: Retrieved data as a pandas DataFrame.
        """
        # Submit batch job
        job_details = self.submit_batch_job(
            dataset=dataset,
            schema=schema,
            symbols=symbol,
            start_date=start_date,
            end_date=end_date,
            stype_in=stype_in,
            stype_out=stype_out
        )
        job_id = job_details["id"]
        self.logger.info(f"Submitted batch job {job_id} for {symbol}.")

        # Download batch data and parse it
        output_path = f"/tmp/{job_id}"
        await self._download_batch(job_id, output_path)

        # Parse data from downloaded files
        return self._parse_batch_files(output_path, schema)
    
    def submit_batch_job(
        self,
        dataset: str,
        schema: str,
        symbols: List[str],
        start_date: str,
        end_date: str,
        stype_in: db.SType,
        stype_out: db.SType
    ) -> Dict[str, Any]:
        """
        Submits a batch job to Databento.

        Args:
            dataset (str): Dataset identifier.
            symbols (List[str]): List of symbols to fetch.
            start_date (str): Start date for the batch.
            end_date (str): End date for the batch.

        Returns:
            Dict[str, Any]: Details of the submitted batch job.
        """
        return self.client.batch.submit_job(
            dataset=dataset,
            symbols=symbols,
            schema=db.Schema.from_str(schema),
            encoding="dbn",
            start=start_date,
            end=end_date,
            delivery="download",
            stype_in=stype_in,
            stype_out=stype_out
        )
    
    async def _download_batch(self, job_id: str, output_path: str) -> None:
        """
        Downloads batch data files for a given job ID.

        Args:
            job_id (str): Databento batch job ID.
            output_path (str): The path to save the downloaded files.
        """
        await self.client.batch.download_async(job_id=job_id, output_dir=output_path)
        self.logger.info(f"Batch files downloaded for job {job_id}.")

    def _parse_batch_files(self, output_path: str) -> pd.DataFrame:
        """
        Parses batch data files into a single DataFrame.

        Args:
            output_path (str): The path to the downloaded batch files.
            schema (str): Schema of the data

        Returns:
            pd.DataFrame: Parsed data as a pandas DataFrame.
        """
        all_data = []
        for file in os.listdir(output_path):
            if file.endswith(".dbn"):
                file_path = os.path.join(output_path, file)
                data = db.DBNStore.from_file(file_path)
                df = data.to_df()
                all_data.append(df)
        return pd.concat(all_data, ignore_index=True)
