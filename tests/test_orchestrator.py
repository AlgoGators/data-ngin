import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio
from typing import Dict, List, Any
from data.orchestrator import Orchestrator


class TestOrchestrator(unittest.IsolatedAsyncioTestCase):
    """
    Tests for the Orchestrator class.
    """

    def setUp(self) -> None:
        """
        Set up mock configuration and patch dynamic imports.
        """
        # Mock configuration
        self.config_path: str = "data/config/config.yaml"
        self.mock_config: Dict[str, Any] = {
            "loader": {"class": "CSVLoader", "module": "csv_loader"},
            "fetcher": {"class": "DatabentoFetcher", "module": "databento_fetcher"},
            "cleaner": {"class": "DatabentoCleaner", "module": "databento_cleaner"},
            "inserter": {"class": "TimescaleDBInserter", "module": "timescaledb_inserter"},
            "time_range": {
                "start_date": "2023-01-01",
                "end_date": "2023-01-31",
            },
        }

    @patch("utils.dynamic_loader.get_instance")
    def test_orchestrator_initialization(self, mock_get_instance: MagicMock) -> None:
        """
        Test that Orchestrator initializes all modules dynamically.

        Args:
            mock_get_instance (MagicMock): Mocked get_instance function.
        """
        # Mock get_instance to return a mock object for each module
        mock_loader: MagicMock = MagicMock()
        mock_fetcher: MagicMock = MagicMock()
        mock_cleaner: MagicMock = MagicMock()
        mock_inserter: MagicMock = MagicMock()

        mock_get_instance.side_effect = [mock_loader, mock_fetcher, mock_cleaner, mock_inserter]

        # Initialize the Orchestrator
        orchestrator: Orchestrator = Orchestrator(config_path=self.config_path)

        # Verify get_instance was called for each module
        self.assertEqual(mock_get_instance.call_count, 4)
        self.assertEqual(orchestrator.loader, mock_loader)
        self.assertEqual(orchestrator.fetcher, mock_fetcher)
        self.assertEqual(orchestrator.cleaner, mock_cleaner)
        self.assertEqual(orchestrator.inserter, mock_inserter)

    @patch("data.orchestrator.Orchestrator.fetch_and_process", new_callable=AsyncMock)
    @patch("data.modules.csv_loader.CSVLoader.load_symbols", return_value=[
        {"dataSymbol": "ES"}, {"dataSymbol": "NQ"}
    ])
    async def test_orchestrator_run(self, mock_load_symbols: MagicMock, mock_fetch_and_process: AsyncMock) -> None:
        """
        Test that the Orchestrator run() processes all symbols asynchronously.

        Args:
            mock_load_symbols (MagicMock): Mocked load_symbols method.
            mock_fetch_and_process (AsyncMock): Mocked fetch_and_process method.
        """
        # Initialize the Orchestrator
        orchestrator: Orchestrator = Orchestrator(config_path=self.config_path)

        # Call run()
        await orchestrator.run()

        # Verify that load_symbols was called once
        mock_load_symbols.assert_called_once_with("contracts/contract.csv")

        # Verify that fetch_and_process was called for each symbol
        self.assertEqual(mock_fetch_and_process.call_count, 2)
        mock_fetch_and_process.assert_any_call({"dataSymbol": "ES"})
        mock_fetch_and_process.assert_any_call({"dataSymbol": "NQ"})

    @patch("data.modules.databento_fetcher.DatabentoFetcher.fetch_and_process_data", new_callable=AsyncMock)
    @patch("data.modules.databento_cleaner.DatabentoCleaner.clean", return_value=[{"time": "2023-01-01"}])
    @patch("data.modules.timescaledb_inserter.TimescaleDBInserter.insert_data")
    async def test_fetch_and_process(self, mock_insert_data: MagicMock, mock_clean: MagicMock, mock_fetch: AsyncMock) -> None:
        """
        Test that fetch_and_process calls fetcher, cleaner, and inserter in sequence.

        Args:
            mock_insert_data (MagicMock): Mocked insert_data method.
            mock_clean (MagicMock): Mocked clean method.
            mock_fetch (AsyncMock): Mocked fetch_and_process_data method.
        """
        # Mock fetch data return
        mock_fetch.return_value = [{"time": "2023-01-01", "symbol": "ES", "open": 100.5}]

        # Initialize the Orchestrator
        orchestrator: Orchestrator = Orchestrator(config_path=self.config_path)

        # Call fetch_and_process
        await orchestrator.fetch_and_process({"dataSymbol": "ES"})

        # Verify the fetcher was called with the correct arguments
        mock_fetch.assert_called_once_with(
            symbol="ES",
            start_date="2023-01-01",
            end_date="2023-01-31",
            schema="ohlcv-1d",
            roll_type="c",
            contract_type="front"
        )

        # Verify the cleaner and inserter were called
        mock_clean.assert_called_once_with([{"time": "2023-01-01", "symbol": "ES", "open": 100.5}])
        mock_insert_data.assert_called_once_with(
            data=[{"time": "2023-01-01"}],
            schema="futures_data",
            table="ohlcv_1d"
        )


if __name__ == "__main__":
    unittest.main()
