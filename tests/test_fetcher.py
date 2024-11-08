import unittest
from unittest.mock import patch
from datetime import datetime
import pandas as pd
from data.modules.fetcher import Fetcher
from typing import List, Dict, Any

class MockFetcher(Fetcher):
    """
    A mock subclass of Fetcher to test base functionality without implementing
    the abstract fetch_data method.
    """
    
    def fetch_data(self, symbol: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        A dummy implementation of the abstract method, not used in tests.
        
        Args:
            symbol (str): The symbol to fetch data for.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.
        
        Returns:
            List[Dict[str, Any]]: Empty list for testing purposes.
        """
        return []

class TestFetcher(unittest.TestCase):
    """
    Unit tests for the Fetcher base class, using the MockFetcher subclass to test methods.
    """

    def setUp(self) -> None:
        """
        Set up a mock fetcher instance with a test configuration.
        """
        # Minimal configuration for testing purposes
        self.config: Dict[str, Any] = {
            "fetcher": {
                "batch_size_days": 30
            },
            "providers": {
                "databento": {
                    "supported_assets": ["EQUITY", "FUTURES"]
                }
            }
        }
        self.fetcher: MockFetcher = MockFetcher(config=self.config)

    def test_clean_data(self):
        """
        Test that clean_data processes raw data correctly into the expected format.
        """
        # Mock data simulating raw API response
        raw_data: pd.DataFrame = pd.DataFrame({
            "time": ["2023-01-01", "2023-01-02"],
            "open": [100.5, 101.0],
            "high": [101.0, 102.0],
            "low": [99.5, 100.0],
            "close": [100.0, 101.5],
            "volume": [1500, 1600],
            "symbol": ["AAPL", "AAPL"]
        })

        # Expected cleaned output format
        expected_cleaned_data: List[Dict[str, Any]] = [
            {
                "time": "2023-01-01",
                "open": 100.5,
                "high": 101.0,
                "low": 99.5,
                "close": 100.0,
                "volume": 1500
            },
            {
                "time": "2023-01-02",
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume": 1600
            }
        ]

        # Run clean_data and check if the output matches the expected result
        cleaned_data: List[Dict[str, Any]] = self.fetcher.clean_data(raw_data)
        self.assertEqual(cleaned_data, expected_cleaned_data)

    def test_config(self):
        """
        Test that the configuration is stored correctly by checking the config attribute.
        """
        # Verify that the configuration in the fetcher instance matches the provided setup
        self.assertEqual(self.fetcher.config, self.config)

    def test_fetch_data_in_batches(self):
        """
        Test that fetch_data_in_batches correctly splits long date ranges into smaller intervals.
        """
        # Assuming start and end dates span more than one batch (e.g., 60 days)
        start_date: str = "2023-01-01"
        end_date: str = "2023-03-01"
        
        # Mock the fetch_data method to prevent actual calls
        with patch.object(MockFetcher, 'fetch_data', return_value=[{"time": "2023-01-01", "open": 100.5}]) as mock_fetch:
            result: List[Dict[str, Any]] = self.fetcher.fetch_data_in_batches(symbol="AAPL", start_date=start_date, end_date=end_date)
            # Check that fetch_data is called multiple times for batching
            self.assertTrue(mock_fetch.called)
            self.assertGreaterEqual(mock_fetch.call_count, 2, "fetch_data should be called multiple times for batching")

if __name__ == "__main__":
    unittest.main()
