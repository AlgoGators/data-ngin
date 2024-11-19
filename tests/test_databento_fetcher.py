import unittest
from unittest.mock import patch, AsyncMock, MagicMock
from datetime import datetime
import pandas as pd
import asyncio
from data.modules.databento_fetcher import DatabentoFetcher  
from typing import List, Dict, Any
import databento as db

class TestDatabentoFetcher(unittest.IsolatedAsyncioTestCase):
    """
    Unit and integration tests for the DatabentoFetcher class.
    """

    def setUp(self) -> None:
        """
        Set up a DatabentoFetcher instance with a mock configuration.
        """
        self.config: Dict[str, Any] = {
            "providers": {
                "databento": {
                    "api_key": "mock_api_key"
                }
            },
            "dataset": "GLBX.MDP3"
        }
        self.fetcher: DatabentoFetcher = DatabentoFetcher(config=self.config)

    async def test_fetch_and_process_data(self):
        """
        Test fetch_and_process_data with proper mocking of databento.Historical.
        """
        with patch("data.modules.databento_fetcher.db.Historical") as mock_historical:
            # Mock Historical client
            mock_client_instance = mock_historical.return_value
            mock_client_instance.timeseries.get_range.return_value.to_df.return_value = pd.DataFrame({
                "date": ["2023-01-01", "2023-01-02"],
                "open": [100.5, 101.0],
                "high": [101.0, 102.0],
                "low": [99.5, 100.0],
                "close": [100.0, 101.5],
                "volume": [1500, 1600],
                "symbol": ["ES", "ES"]
            })

            # Instantiate DatabentoFetcher
            config = {
                "providers": {"databento": {"api_key": "mock_api_key"}},
                "dataset": "GLBX.MDP3"
            }
            fetcher = DatabentoFetcher(config)

            # Call the method under test
            result = await fetcher.fetch_and_process_data(
                symbol="ES",
                start_date="2023-01-01",
                end_date="2023-01-02",
                schema="ohlcv-1d",
                roll_type="c",
                contract_type="front"
            )

            # Expected cleaned data
            expected_result = [
                {
                    "time": "2023-01-01",
                    "open": 100.5,
                    "high": 101.0,
                    "low": 99.5,
                    "close": 100.0,
                    "volume": 1500,
                    "symbol": "ES"
                },
                {
                    "time": "2023-01-02",
                    "open": 101.0,
                    "high": 102.0,
                    "low": 100.0,
                    "close": 101.5,
                    "volume": 1600,
                    "symbol": "ES"
                }
            ]

            assert result == expected_result



    def test_clean_data(self):
        """
        Test the clean_data method to ensure raw data is cleaned and formatted correctly.
        """
        raw_data: pd.DataFrame = pd.DataFrame({
            "date": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
            "open": [100.5, 101.0],
            "high": [101.0, 102.0],
            "low": [99.5, 100.0],
            "close": [100.0, 101.5],
            "volume": [1500, 1600],
            "symbol": ["ES", "ES"]
        })

        expected_cleaned_data: List[Dict[str, Any]] = [
            {
                "time": datetime(2023, 1, 1),
                "open": 100.5,
                "high": 101.0,
                "low": 99.5,
                "close": 100.0,
                "volume": 1500,
                "symbol": "ES"
            },
            {
                "time": datetime(2023, 1, 2),
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume": 1600,
                "symbol": "ES"
            }
        ]

        # Call clean_data and verify result
        result: List[Dict[str, Any]] = self.fetcher.clean_data(raw_data)
        self.assertEqual(result, expected_cleaned_data)

    async def test_fetch_data_error_handling(self):
        """
        Test fetch_and_process_data to verify it logs errors and raises exceptions properly.
        """
        with patch("data.modules.databento_fetcher.db.Historical") as mock_historical:
            # Mock the Historical client instance
            mock_client_instance = mock_historical.return_value
            
            # Simulate an exception in `get_range`
            mock_client_instance.timeseries.get_range.side_effect = Exception("API error")
            
            # Instantiate DatabentoFetcher
            config = {
                "providers": {"databento": {"api_key": "mock_api_key"}},
                "dataset": "GLBX.MDP3"
            }
            fetcher = DatabentoFetcher(config)

            # Use assertRaises to verify exception handling
            with self.assertRaises(Exception) as context:
                await fetcher.fetch_and_process_data(
                    symbol="ES",
                    start_date="2023-01-01",
                    end_date="2023-01-02",
                    schema="ohlcv-1d",
                    roll_type="c",
                    contract_type="front"
                )
            
            # Assert that the exception message matches the mock's side effect
            self.assertEqual(str(context.exception), "API error")


if __name__ == "__main__":
    unittest.main()
