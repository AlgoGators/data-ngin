from abc import ABC, abstractmethod
from typing import List, Dict, Any
import logging
import pandas as pd


class Fetcher(ABC):
    """
    Abstract base class for Fetcher modules responsible for retrieving financial data
    for validated symbols from various data providers.

    Attributes:
        config (Dict[str, Any]): Configuration settings loaded from config.yaml.
        logger (logging.Logger): Logger for fetcher-specific logging.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the Fetcher with a provided configuration dictionary.

        Args:
            config (Dict[str, Any]): Configuration settings.
        """
        self.config: Dict[str, Any] = config
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    @abstractmethod
    def fetch_data(self, symbol: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Abstract method to fetch historical data for a given symbol over a specified date range.

        Args:
            symbol (str): The symbol for which to fetch data.
            start_date (str): Start date of the data in 'YYYY-MM-DD' format.
            end_date (str): End date of the data in 'YYYY-MM-DD' format.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing a row of data.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        pass

    async def retrieve(
        self,
        symbol: str,
        loaded_asset_type: str,
        start_date: str,
        end_date: str,
        batch_config: Dict[str, Any] = None,
    ):
        """
        Uniform fetch entry point. Default implementation delegates straight to
        `fetch_data`; subclasses that support batching (e.g. `BatchDownloadDatabentoFetcher`)
        override this to split the range into batches first. Callers don't need to
        know which behavior a given fetcher class implements.

        Args:
            symbol (str): The symbol to fetch data for.
            loaded_asset_type (str): Type of asset to load (e.g., "FUTURE").
            start_date (str): Start date for fetching.
            end_date (str): End date for fetching.
            batch_config (Dict[str, Any]): Batch-downloading settings (unit, max_units); ignored by non-batching fetchers.
        """
        return await self.fetch_data(
            symbol=symbol,
            loaded_asset_type=loaded_asset_type,
            start_date=start_date,
            end_date=end_date,
        )

#9
    def detect_time_gaps(self, data: List[Dict[str, Any]], time_column: str, freq: str) -> List[str]:
        """
        Detects missing time gaps in the fetched financial data.

        Args:
            data (List[Dict[str, Any]]): The fetched data as a list of dictionaries.
            time_column (str): The key of the dictionary that holds the timestamp information.
            freq (str): The expected frequency of the timestamps (e.g., 'D' for daily).

        Returns:
            List[str]: A list of missing time periods (timestamps).
        """
        # Convert the fetched data into a pandas DataFrame for easier processing
        df = pd.DataFrame(data)
        
        if time_column not in df.columns:
            raise ValueError(f"Time column '{time_column}' not found in the fetched data.")

        df[time_column] = pd.to_datetime(df[time_column])
        df = df.sort_values(by=time_column)

        # Generate a complete time range based on the desired frequency
        time_range = pd.date_range(start=df[time_column].min(), end=df[time_column].max(), freq=freq)

        # Find missing timestamps
        missing_timestamps = time_range.difference(df[time_column])

        return missing_timestamps.astype(str).tolist()

    def log_missing_data(self, missing_timestamps: List[str]) -> None:
        """
        Logs missing time periods into a log file.

        Args:
            missing_timestamps (List[str]): List of missing time periods.
        """
        if missing_timestamps:
            for timestamp in missing_timestamps:
                self.logger.warning(f"Missing data at timestamp: {timestamp}")
        else:
            self.logger.info("No missing data detected.")

    def fetch_and_validate(self, symbol: str, start_date: str, end_date: str, time_column: str, freq: str) -> List[Dict[str, Any]]:
        """
        Fetches and validates the data, logging any missing time periods.

        Args:
            symbol (str): The symbol for which to fetch data.
            start_date (str): Start date of the data in 'YYYY-MM-DD' format.
            end_date (str): End date of the data in 'YYYY-MM-DD' format.
            time_column (str): The key of the dictionary that holds the timestamp information.
            freq (str): The expected frequency of the timestamps (e.g., 'D' for daily).

        Returns:
            List[Dict[str, Any]]: The fetched data with missing time gaps logged.
        """
        # Fetch the data
        data = self.fetch_data(symbol, start_date, end_date)

        # Detect missing time gaps
        missing_timestamps = self.detect_time_gaps(data, time_column, freq)

        # Log missing data if any
        self.log_missing_data(missing_timestamps)

        return data