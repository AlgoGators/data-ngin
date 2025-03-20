import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from typing import List, Dict, Any
import pandas as pd
import databento as db
from data.modules.fetcher.databento_fetcher import DatabentoFetcher

class TestDatabentoFetcher(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """
        Initialize DatabentoFetcher with mock configuration.
        """
        self.mock_config = {
            "fetcher": {"class": "DatabentoFetcher", "module": "databento_fetcher"},
            "time_range": {"start_date": "2023-01-01", "end_date": "2023-01-02"},
            "provider": {
                "name": "databento",
                "asset": "FUTURE",
                "dataset": "GLBX.MDP3",
                "schema": "ohlcv-1d",
                "roll_type": "c",
                "contract_type": "0",
            },
        }
        
        # Mock db.Schema and db.SType
        self.mock_schema = MagicMock()
        self.mock_stype = MagicMock()
        
        # Create patches
        self.patches = [
            patch("data.modules.databento_fetcher.db.Historical"),
            patch("data.modules.databento_fetcher.db.Schema", self.mock_schema),
            patch("data.modules.databento_fetcher.db.SType", self.mock_stype)
        ]
        
        # Start all patches
        self.mocks = [p.start() for p in self.patches]
        self.mock_historical = self.mocks[0]
        self.mock_client_instance = self.mock_historical.return_value
        
        # Set up Schema.from_str
        self.mock_schema.from_str.return_value = "ohlcv-1d"
        
        # Set up SType enum values
        self.mock_stype.CONTINUOUS = "continuous"
        self.mock_stype.INSTRUMENT_ID = "instrument_id"
        
        # Initialize fetcher
        self.fetcher = DatabentoFetcher(config=self.mock_config)
        
        # Add cleanup
        for p in self.patches:
            self.addCleanup(p.stop)

    async def test_fetch_data(self) -> None:
        """
        Test that `fetch_data` correctly fetches and processes data.
        """
        # Create a mock DataFrame
        mock_df = pd.DataFrame({
            "time": ["2023-01-01", "2023-01-02"],
            "open": [100.5, 101.0],
            "high": [101.0, 102.0],
            "low": [99.5, 100.0],
            "close": [100.0, 101.5],
            "volume": [1500, 1600],
            "symbol": ["ES", "ES"]
        })

        # Set up the mock response
        mock_response = MagicMock()
        mock_response.to_df.return_value = mock_df
        self.mock_client_instance.timeseries.get_range_async = AsyncMock(return_value=mock_response)

        # Call the method
        result = await self.fetcher.fetch_data(
            symbol="ES",
            loaded_asset_type="FUTURE",
            start_date=self.mock_config["time_range"]["start_date"],
            end_date=self.mock_config["time_range"]["end_date"],
        )

        # Assert equality
        pd.testing.assert_frame_equal(result, mock_df)

    async def test_fetch_data_error_handling(self) -> None:
        """
        Test that `fetch_data` raises an exception when the API fails.
        """
        # Simulate API error
        mock_error = Exception("API error")
        self.mock_client_instance.timeseries.get_range_async.side_effect = mock_error

        with self.assertRaises(Exception) as context:
            await self.fetcher.fetch_data(
                symbol="ES",
                loaded_asset_type="FUTURE",
                start_date=self.mock_config["time_range"]["start_date"],
                end_date=self.mock_config["time_range"]["end_date"],
            )

        self.assertEqual(str(context.exception), "API error")

    async def test_fetch_data_no_data(self) -> None:
        """
        Test handling of no data returned from API.
        """
        # Create empty DataFrame response
        mock_response = MagicMock()
        mock_response.to_df.return_value = pd.DataFrame()
        self.mock_client_instance.timeseries.get_range_async = AsyncMock(return_value=mock_response)

        result = await self.fetcher.fetch_data(
            symbol="ES",
            loaded_asset_type="FUTURE",
            start_date=self.mock_config["time_range"]["start_date"],
            end_date=self.mock_config["time_range"]["end_date"],
        )

        # Should return empty DataFrame with expected columns
        expected_columns = ["time", "open", "high", "low", "close", "volume", "symbol"]
        self.assertTrue(result.empty)
        self.assertListEqual(list(result.columns), expected_columns)