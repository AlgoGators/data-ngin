import unittest
import tempfile
import os
import pandas as pd
from unittest.mock import patch, MagicMock, AsyncMock
from data.orchestrator import Orchestrator
from typing import Dict, Any


class TestIntegrationPipeline(unittest.IsolatedAsyncioTestCase):
    """
    Integration tests for the full data pipeline using the Orchestrator class.
    """

    def setUp(self) -> None:
        """
        Set up mock configuration and create temporary test files.
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
            "provider": {
                "asset": "FUTURE",
                "dataset": "GLBX.MDP3",
                "schema": "ohlcv-1d",
                "roll_type": "c",
                "contract_type": "front",
            },
            "database": {
                "target_schema": "futures_data",
                "raw_table": "ohlcv_1d_raw",
                "table": "ohlcv_1d",
            },
        }

        # Create a temporary CSV file to simulate the contracts CSV
        self.temp_csv = tempfile.NamedTemporaryFile(
            delete=False, mode="w", suffix=".csv"
        )
        self.temp_csv.write("dataSymbol,instrumentType\nES,FUTURE\nNQ,FUTURE\n")
        self.temp_csv.close()

        # Update the config file path in mock_config
        self.mock_config["loader"]["file_path"] = self.temp_csv.name

    def tearDown(self) -> None:
        """
        Clean up temporary files and resources.
        """
        if os.path.exists(self.temp_csv.name):
            os.remove(self.temp_csv.name)

    @patch("data.orchestrator.DataAccess")
    @patch("data.orchestrator.get_instance")
    async def test_pipeline_run(
        self,
        mock_get_instance: MagicMock,
        mock_data_access: MagicMock,
    ) -> None:
        """
        Test that the pipeline processes symbols end-to-end.
        """
        # Create mock data
        mock_df = pd.DataFrame({
            "time": ["2023-01-01"],
            "symbol": ["ES"],
            "open": [100.5]
        })
        mock_cleaned_data = [{"time": "2023-01-01", "cleaned": True}]

        # Set up mock components
        mock_loader = MagicMock()
        mock_fetcher = MagicMock()
        mock_cleaner = MagicMock()
        mock_inserter = MagicMock()

        # Configure mock loader
        mock_loader.load_symbols.return_value = {"ES": "FUTURE", "NQ": "FUTURE"}

        # Configure mock fetcher
        mock_fetcher.fetch_data = AsyncMock(return_value=mock_df)

        # Configure mock cleaner
        mock_cleaner.clean.return_value = mock_cleaned_data

        # Configure mock inserter
        mock_inserter.connect = MagicMock()
        mock_inserter.close = MagicMock()
        mock_inserter.insert_data = MagicMock()

        # Set up get_instance to return our mocks
        mock_get_instance.side_effect = [mock_loader, mock_fetcher, mock_cleaner, mock_inserter]

        # Initialize the Orchestrator
        orchestrator = Orchestrator(config=self.mock_config)

        # Run the pipeline
        await orchestrator.run()

        # Verify loader was called
        mock_loader.load_symbols.assert_called_once()

        # Verify fetcher was called for each symbol
        self.assertEqual(mock_fetcher.fetch_data.call_count, 2)
        mock_fetcher.fetch_data.assert_any_call(
            symbol="ES",
            loaded_asset_type="FUTURE",
            start_date="2023-01-01",
            end_date="2023-01-02",
        )
        mock_fetcher.fetch_data.assert_any_call(
            symbol="NQ",
            loaded_asset_type="FUTURE",
            start_date="2023-01-01",
            end_date="2023-01-02",
        )

        # Verify database connections
        self.assertEqual(mock_inserter.connect.call_count, 2)  # Once for each symbol
        self.assertEqual(mock_inserter.close.call_count, 2)    # Once for each symbol

        # Verify raw data insertion
        mock_inserter.insert_data.assert_any_call(
            data=mock_df.to_dict(orient="records"),
            schema="futures_data",
            table="ohlcv_1d_raw"
        )

        # Verify cleaner was called
        self.assertEqual(mock_cleaner.clean.call_count, 2)
        mock_cleaner.clean.assert_any_call(mock_df)

        # Verify cleaned data insertion
        mock_inserter.insert_data.assert_any_call(
            data=mock_cleaned_data,
            schema="futures_data",
            table="ohlcv_1d"
        )

        # Verify total number of insert_data calls (2 per symbol: raw + cleaned)
        self.assertEqual(mock_inserter.insert_data.call_count, 4)


if __name__ == "__main__":
    unittest.main()