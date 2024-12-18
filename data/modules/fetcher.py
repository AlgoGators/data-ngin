from abc import ABC, abstractmethod
from typing import List, Dict, Any
import logging


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
