import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import pandas as pd
from data.modules.databento_fetcher import DatabentoFetcher
from typing import List, Dict, Any

class TestDatabentoFetcher(unittest.IsolatedAsyncioTestCase):
    """
    Unit and integration tests for the DatabentoFetcher class.

    This test suite ensures the functionality of the DatabentoFetcher module, including:
    - Fetching and processing data from the Databento API.
    - Cleaning raw data into a format suitable for database insertion.
    - Handling errors and edge cases in the data fetching pipeline.
    """

    def setUp(self) -> None:
        """
        Initialize a DatabentoFetcher instance with a mock configuration.

        A mock configuration is created with a fake API key and dataset.
        This configuration is passed to the DatabentoFetcher for testing.
        """
        self.mock_config: Dict[str, Any] = {
            "loader": {"class": "CSVLoader", "module": "csv_loader", "file_path": ""},
            "fetcher": {"class": "DatabentoFetcher", "module": "databento_fetcher"},
            "cleaner": {"class": "DatabentoCleaner", "module": "databento_cleaner"},
            "inserter": {"class": "TimescaleDBInserter", "module": "timescaledb_inserter"},
            "time_range": {
                "start_date": "2023-01-01",
                "end_date": "2023-01-02",
            },
            "providers": {
                "databento": {
                    "supported_assets": "FUTURE",
                    "dataset": "GLBX.MDP3",
                    "schema": "ohlcv-1d",
                    "roll_type": "c",
                    "contract_type": "0"
                }
            },
            "database": {
                "target_schema": "futures_data",
                "table": "ohlcv_1d"
            },
        }

        self.fetcher: DatabentoFetcher = DatabentoFetcher(config=self.mock_config)

    async def test_fetch_data(self) -> None:
        """
        Test the `fetch_data` method using mocked Databento API calls.

        This test validates the data fetching and processing workflow. It ensures:
        - Data is fetched from the mocked Databento API.
        - Raw data is cleaned and transformed correctly.

        Assertions:
            - The returned data matches the expected cleaned format.
            - The mocked Databento API is called with the expected arguments.
        """
        with patch("data.modules.databento_fetcher.db.Historical") as mock_historical:
            # Mock the Databento Historical client
            mock_client_instance: MagicMock = MagicMock()
            mock_historical.return_value = mock_client_instance

            # Mock the `timeseries.get_range` response
            mock_client_instance.timeseries.get_range.return_value.to_df.return_value = pd.DataFrame({
                "time": ["2023-01-01", "2023-01-02"],
                "open": [100.5, 101.0],
                "high": [101.0, 102.0],
                "low": [99.5, 100.0],
                "close": [100.0, 101.5],
                "volume": [1500, 1600],
                "symbol": ["ES", "ES"]
            })

            # Reinitialize DatabentoFetcher after applying the mock
            fetcher: DatabentoFetcher = DatabentoFetcher(config=self.mock_config)

            # Call the method under test
            result: List[Dict[str, Any]] = await fetcher.fetch_data(
                symbol="ES",
                dataset=self.mock_config["providers"]["databento"]["dataset"],
                start_date=self.mock_config["time_range"]["start_date"],
                end_date=self.mock_config["time_range"]["end_date"],
                schema=self.mock_config["providers"]["databento"]["schema"],
                roll_type=self.mock_config["providers"]["databento"]["roll_type"],
                contract_type=self.mock_config["providers"]["databento"]["contract_type"],
            )

            # Define expected cleaned data
            expected_result: pd.DataFrame = pd.DataFrame( [
                {"time": "2023-01-01", "open": 100.5, "high": 101.0, "low": 99.5, "close": 100.0, "volume": 1500, "symbol": "ES"},
                {"time": "2023-01-02", "open": 101.0, "high": 102.0, "low": 100.0, "close": 101.5, "volume": 1600, "symbol": "ES"}
            ])

            # Assert the returned data matches the expected result
            self.assertTrue(expected_result.equals(result))

            # Verify the mocked API was called with the correct arguments
            mock_client_instance.timeseries.get_range.assert_called_once_with(
                dataset="GLBX.MDP3",
                symbols=["ES.c.0"],
                schema="ohlcv-1d",
                start="2023-01-01",
                end="2023-01-02",
                stype_in="continuous",
                stype_out="instrument_id",
            )

    async def test_fetch_data_error_handling(self) -> None:
        """
        Test error handling in `fetch__data`.

        This test ensures:
        - The method raises an exception when the Databento API encounters an error.
        - Appropriate error messages are propagated.

        Assertions:
            - The raised exception matches the expected error.
        """
        with patch("data.modules.databento_fetcher.db.Historical") as mock_historical:
            # Mock the Databento Historical client
            mock_client_instance: MagicMock = mock_historical.return_value

            # Simulate an exception in the `get_range` method
            mock_client_instance.timeseries.get_range.side_effect = Exception("API error")

            # Reinitialize DatabentoFetcher after applying the mock
            fetcher: DatabentoFetcher = DatabentoFetcher(config=self.mock_config)

            # Use `assertRaises` to verify exception handling
            with self.assertRaises(Exception) as context:
                await fetcher.fetch_data(
                    symbol="ES",
                    dataset=self.mock_config["providers"]["databento"]["dataset"],
                    start_date=self.mock_config["time_range"]["start_date"],
                    end_date=self.mock_config["time_range"]["end_date"],
                    schema=self.mock_config["providers"]["databento"]["schema"],
                    roll_type=self.mock_config["providers"]["databento"]["roll_type"],
                    contract_type=self.mock_config["providers"]["databento"]["contract_type"],
                )

            # Assert the exception message matches the simulated API error
            self.assertEqual(str(context.exception), "API error")


if __name__ == "__main__":
    unittest.main()
