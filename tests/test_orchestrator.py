import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import pandas as pd
from typing import Dict, Any
from data.orchestrator import Orchestrator


class TestOrchestrator(unittest.IsolatedAsyncioTestCase):
    """
    Tests for the Orchestrator class.
    """

    def setUp(self) -> None:
        """
        Set up mock configuration and patch dynamic imports.
        """
        self.mock_config: Dict[str, Any] = {
            "loader": {"class": "CSVLoader", "module": "csv_loader", "file_path": ""},
            "fetcher": {"class": "DatabentoFetcher", "module": "databento_fetcher"},
            "cleaner": {"class": "DatabentoCleaner", "module": "databento_cleaner"},
            "inserter": {"class": "TimescaleDBInserter", "module": "timescaledb_inserter"},
            "time_range": {"start_date": "2023-01-01", "end_date": "2023-01-02"},
            "database": {"target_schema": "futures_data", "raw_table": "ohlcv_1d_raw", "table": "ohlcv_1d"}
        }

    @patch("data.orchestrator.get_instance")
    @patch("data.orchestrator.DataAccess")
    def test_orchestrator_initialization(self, mock_data_access: MagicMock, mock_get_instance: MagicMock) -> None:
        """
        Test that Orchestrator initializes all modules dynamically.
        """
        mock_loader = MagicMock()
        mock_fetcher = MagicMock()
        mock_cleaner = MagicMock()
        mock_inserter = MagicMock()

        mock_get_instance.side_effect = [mock_loader, mock_fetcher, mock_cleaner, mock_inserter]

        orchestrator = Orchestrator(config=self.mock_config)

        # Verify initialization
        self.assertEqual(mock_get_instance.call_count, 4)
        self.assertEqual(orchestrator.loader, mock_loader)
        self.assertEqual(orchestrator.fetcher, mock_fetcher)
        self.assertEqual(orchestrator.cleaner, mock_cleaner)
        self.assertEqual(orchestrator.inserter, mock_inserter)
        mock_data_access.assert_called_once()

    @patch("data.orchestrator.DataAccess")
    @patch("data.orchestrator.get_instance")
    @patch("data.orchestrator.determine_date_range")
    @patch("data.modules.csv_loader.CSVLoader.load_symbols")
    async def test_orchestrator_run(
        self,
        mock_load_symbols: MagicMock,
        mock_determine_date_range: MagicMock,
        mock_get_instance: MagicMock,
        mock_data_access: MagicMock,
    ) -> None:
        """
        Test that Orchestrator run() processes all symbols asynchronously.
        """
        # Set up mocks
        mock_load_symbols.return_value = {"ES": "FUTURE", "NQ": "FUTURE"}
        mock_determine_date_range.return_value = ("2023-01-01", "2023-01-02")

        # Create mock instances
        mock_loader = MagicMock()
        mock_loader.load_symbols = mock_load_symbols
        mock_fetcher = MagicMock()
        mock_cleaner = MagicMock()
        mock_inserter = MagicMock()

        mock_get_instance.side_effect = [mock_loader, mock_fetcher, mock_cleaner, mock_inserter]

        # Create orchestrator with mocked components
        orchestrator = Orchestrator(config=self.mock_config)
        
        # Mock retrieve_and_process_data
        orchestrator.retrieve_and_process_data = AsyncMock()

        # Execute test
        await orchestrator.run()

        # Verify calls
        self.assertEqual(orchestrator.retrieve_and_process_data.call_count, 2)
        orchestrator.retrieve_and_process_data.assert_any_call(
            {"dataSymbol": "ES", "instrumentType": "FUTURE"},
            "2023-01-01",
            "2023-01-02"
        )
        orchestrator.retrieve_and_process_data.assert_any_call(
            {"dataSymbol": "NQ", "instrumentType": "FUTURE"},
            "2023-01-01",
            "2023-01-02"
        )

        # Test without dates in config
        config_without_dates = self.mock_config.copy()
        del config_without_dates["time_range"]
        
        # Reset mocks
        orchestrator.retrieve_and_process_data.reset_mock()
        mock_determine_date_range.reset_mock()
        
        # Create new orchestrator without dates
        new_mock_get_instance = MagicMock()
        new_mock_get_instance.side_effect = [mock_loader, mock_fetcher, mock_cleaner, mock_inserter]
        with patch("data.orchestrator.get_instance", new_mock_get_instance):
            orchestrator = Orchestrator(config=config_without_dates)
            orchestrator.retrieve_and_process_data = AsyncMock()
            await orchestrator.run()
        
        # Verify determine_date_range was called once
        mock_determine_date_range.assert_called_once()

    @patch("data.orchestrator.DataAccess")
    @patch("data.orchestrator.get_instance")
    async def test_retrieve_and_process_data(
        self,
        mock_get_instance: MagicMock,
        mock_data_access: MagicMock,
    ) -> None:
        """
        Test that retrieve_and_process_data calls fetcher, cleaner, and inserter in sequence.
        """
        # Create mock data
        mock_df = pd.DataFrame({
            "time": ["2023-01-01"],
            "symbol": ["ES"],
            "open": [100.5]
        })
        mock_cleaned_data = [{"time": "2023-01-01", "cleaned": True}]

        # Set up mocks
        mock_loader = MagicMock()
        mock_fetcher = MagicMock()
        mock_cleaner = MagicMock()
        mock_inserter = MagicMock()

        # Configure mock fetcher
        mock_fetcher.fetch_data = AsyncMock(return_value=mock_df)
        
        # Configure mock cleaner
        mock_cleaner.clean.return_value = mock_cleaned_data
        
        # Configure mock inserter
        mock_inserter.insert_data = MagicMock()
        mock_inserter.connect = MagicMock()
        mock_inserter.close = MagicMock()

        mock_get_instance.side_effect = [mock_loader, mock_fetcher, mock_cleaner, mock_inserter]

        # Create orchestrator and execute test
        orchestrator = Orchestrator(config=self.mock_config)
        await orchestrator.retrieve_and_process_data(
            {"dataSymbol": "ES", "instrumentType": "FUTURE"},
            "2023-01-01",
            "2023-01-02"
        )

        # Verify the entire pipeline
        mock_fetcher.fetch_data.assert_called_once_with(
            symbol="ES",
            loaded_asset_type="FUTURE",
            start_date="2023-01-01",
            end_date="2023-01-02"
        )

        mock_inserter.connect.assert_called_once()
        
        # Verify raw data insertion
        mock_inserter.insert_data.assert_any_call(
            data=mock_df.to_dict(orient="records"),
            schema="futures_data",
            table="ohlcv_1d_raw"
        )

        # Verify cleaner was called
        mock_cleaner.clean.assert_called_once_with(mock_df)
        
        # Verify cleaned data insertion
        mock_inserter.insert_data.assert_any_call(
            data=mock_cleaned_data,
            schema="futures_data",
            table="ohlcv_1d"
        )

        # Verify connection was closed
        mock_inserter.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()