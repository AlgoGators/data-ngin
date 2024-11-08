import yaml
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Union
from datetime import datetime, timedelta
import logging
import pandas as pd
logging.basicConfig(level=logging.INFO)

class Fetcher(ABC):
    """
    Abstract base class for Fetcher modules that handle fetching financial data
    for validated symbols from various data providers.
    
    Attributes:
        config (Dict[str, Any]): Configuration settings loaded from config.yaml.
        batch_size (int): Maximum days of data to fetch in a single batch.
    """
    
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the Fetcher with a provided configuration dictionary.
        
        Args:
            config (Dict[str, Any]): Configuration settings.
        """
        self.config: Dict[str, Any] = config
        self.batch_size: int = config.get("fetcher", {}).get("batch_size_days", 30)
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

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
        """
        pass

    def fetch_data_in_batches(self, symbol: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Fetches data in batches to handle large date ranges by splitting them into manageable chunks.
        
        Args:
            symbol (str): The symbol to fetch data for.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.
        
        Returns:
            List[Dict[str, Any]]: Consolidated list of data rows across all batches.
        """
        all_data: List[Dict[str, Any]] = []
        current_start: datetime = datetime.strptime(start_date, "%Y-%m-%d")
        final_end: datetime = datetime.strptime(end_date, "%Y-%m-%d")
        
        while current_start <= final_end:
            batch_end: datetime = min(current_start + timedelta(days=self.batch_size - 1), final_end)
            self.logger.info(f"Fetching batch: {current_start.date()} to {batch_end.date()} for symbol: {symbol}")
            
            try:
                batch_data: List[Dict[str, Any]] = self.fetch_data(
                    symbol,
                    current_start.strftime("%Y-%m-%d"),
                    batch_end.strftime("%Y-%m-%d")
                )
                all_data.extend(batch_data)
            except Exception as e:
                self.logger.error(f"Error fetching data for {symbol} from {current_start.date()} to {batch_end.date()}: {e}")
            
            current_start = batch_end + timedelta(days=1)
        
        return all_data

    def clean_data(self, data: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Cleans and prepares raw data for database insertion, ensuring required fields are present.
        
        Args:
            data (pd.DataFrame): Raw data from Databento API.
        
        Returns:
            List[Dict[str, Any]]: Cleaned data as a list of dictionaries.
        """
        required_columns: List[str] = ["time", "open", "high", "low", "close", "volume"]
        
        # Verify required columns are in the DataFrame
        if not all(col in data.columns for col in required_columns):
            self.logger.warning("Some required columns are missing from the data.")
            return []

        # Process each row and ensure all required columns are included
        cleaned_data: List[Dict[str, Any]] = [
            {
                "time": row["time"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"])
            }
            for _, row in data.iterrows()
            if pd.notnull(row["time"]) and all(pd.notnull(row[col]) for col in required_columns[1:])
        ]
        
        return cleaned_data

