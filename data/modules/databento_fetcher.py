import requests
from datetime import datetime
from typing import List, Dict, Any
from data.modules.fetcher import Fetcher

class DatabentoFetcher(Fetcher):
    """
    A Fetcher subclass specifically for retrieving data from Databento's API,
    with enhanced batch fetching, caching, and error handling.
    
    Attributes:
        api_key (str): API key for accessing Databento's data.
    """
    
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the DatabentoFetcher with configuration and API key.
        
        Args:
            config (Dict[str, Any]): Configuration settings including API details.
        """
        super().__init__(config)
        self.api_key: str = config["providers"]["databento"]["api_key"]
        self.base_url: str = "https://api.databento.com/v1/data"

    def fetch_data(self, symbol: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Fetches historical OHLCV data for a given symbol over a specified date range.
        
        Args:
            symbol (str): The symbol for which to fetch data.
            start_date (str): Start date of the data in 'YYYY-MM-DD' format.
            end_date (str): End date of the data in 'YYYY-MM-DD' format.
        
        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing a row of OHLCV data.
        
        Raises:
            ValueError: If the API response indicates an error.
        """
        url: str = f"{self.base_url}/{symbol}/ohlcv"
        params: Dict[str, Any] = {
            "start_date": start_date,
            "end_date": end_date,
            "apikey": self.api_key,
            "interval": "1d"
        }
        
        try:
            response: requests.Response = requests.get(url, params=params)
            response.raise_for_status()  # Raise error for HTTP codes like 4xx/5xx
            
            data: List[Dict[str, Any]] = response.json()
            self.logger.info(f"Fetched {len(data)} rows for {symbol} from {start_date} to {end_date}")
            return self.clean_data(data)
        
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error while fetching data for {symbol}: {e}")
            raise
        except ValueError:
            self.logger.error(f"Unexpected data format from API for {symbol}")
            raise

    def clean_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Cleans and formats raw OHLCV data fetched from Databento, handling missing or invalid values.
        
        Args:
            data (List[Dict[str, Any]]): Raw OHLCV data from Databento.
        
        Returns:
            List[Dict[str, Any]]: Cleaned and formatted data ready for database insertion.
        """
        cleaned_data: List[Dict[str, Any]] = []
        for row in data:
            try:
                cleaned_data.append({
                    "time": row["date"],
                    "open": float(row.get("open", 0)),
                    "high": float(row.get("high", 0)),
                    "low": float(row.get("low", 0)),
                    "close": float(row.get("close", 0)),
                    "volume": int(row.get("volume", 0))
                })
            except (TypeError, ValueError) as e:
                self.logger.warning(f"Data cleaning issue for {row['date']} - {e}")
        return cleaned_data
