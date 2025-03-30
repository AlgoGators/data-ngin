import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Dict, Any
from src.orchestrator import Orchestrator


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
    def test_orchestrator_initialization(self, mock_get_instance: MagicMock) -> None:
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

        # Verify correct arguments passed
        mock_get_instance.assert_any_call(self.mock_config, "loader", "class")
        mock_get_instance.assert_any_call(self.mock_config, "fetcher", "class")
        mock_get_instance.assert_any_call(self.mock_config, "cleaner", "class")
        mock_get_instance.assert_any_call(self.mock_config, "inserter", "class")

    @patch("data.orchestrator.determine_date_range", return_value=("2023-01-01", "2023-01-02"))
    @patch("data.orchestrator.Orchestrator.retrieve_and_process_data", new_callable=AsyncMock)
    @patch("data.modules.csv_loader.CSVLoader.load_symbols", return_value={"ES": "FUTURE", "NQ": "FUTURE"})
    async def test_orchestrator_run(
        self,
        mock_load_symbols: MagicMock,
        mock_process_data: AsyncMock,
        mock_determine_date_range: MagicMock,
    ) -> None:
        """
        Test that Orchestrator run() processes all symbols asynchronously.
        If dates are present in config, determine_date_range should NOT be called.
        """

        # Case 1: Dates Already Provided in Config
        self.mock_config["time_range"]["start_date"] = "2023-01-01"
        self.mock_config["time_range"]["end_date"] = "2023-01-02"

        orchestrator = Orchestrator(config=self.mock_config)
        await orchestrator.run()

        # Ensure load_symbols and retrieve_and_process_data were called
        mock_load_symbols.assert_called_once()
        self.assertEqual(mock_process_data.call_count, 2)
        mock_process_data.assert_any_call(
            {"dataSymbol": "ES", "instrumentType": "FUTURE"}, "2023-01-01", "2023-01-02"
        )
        mock_process_data.assert_any_call(
            {"dataSymbol": "NQ", "instrumentType": "FUTURE"}, "2023-01-01", "2023-01-02"
        )

        # Verify determine_date_range was called
        mock_determine_date_range.assert_called()

        # Case 2: No Dates in Config (Use determine_date_range)
        del self.mock_config["time_range"]["start_date"]
        del self.mock_config["time_range"]["end_date"]

        orchestrator = Orchestrator(config=self.mock_config)
        await orchestrator.run()



    @patch("data.modules.databento_fetcher.DatabentoFetcher.fetch_data", new_callable=AsyncMock)
    @patch("data.modules.databento_cleaner.DatabentoCleaner.clean", return_value=[{"time": "2023-01-01"}])
    @patch("data.modules.timescaledb_inserter.TimescaleDBInserter.insert_data")
    async def test_retrieve_and_process_data(
        self,
        mock_insert_data: MagicMock,
        mock_clean: MagicMock,
        mock_fetch: AsyncMock,
    ) -> None:
        """
        Test that retrieve_and_process_data calls fetcher, cleaner, and inserter in sequence.
        """
        mock_fetch.return_value = [{"time": "2023-01-01", "symbol": "ES", "open": 100.5}]

        orchestrator = Orchestrator(config=self.mock_config)
        await orchestrator.retrieve_and_process_data({"dataSymbol": "ES", "instrumentType": "FUTURE"}, "2023-01-01", "2023-01-02")

        mock_fetch.assert_called_once_with(
            symbol="ES",
            loaded_asset_type="FUTURE",
            start_date="2023-01-01",
            end_date="2023-01-02",
        )

        mock_clean.assert_called_once_with([{"time": "2023-01-01", "symbol": "ES", "open": 100.5}])

        mock_insert_data.assert_called_with(
            data=[{"time": "2023-01-01"}],
            schema="futures_data",
            table="ohlcv_1d"
        )


if __name__ == "__main__":
    unittest.main()
