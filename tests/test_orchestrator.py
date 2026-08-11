import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Dict, Any
from src.application.orchestrator import Orchestrator
from src.domain.ports import CleanerPort, FetcherPort, SymbolSourcePort


class TestOrchestrator(unittest.IsolatedAsyncioTestCase):
    """
    Tests for the Orchestrator class.
    """

    def setUp(self) -> None:
        """
        Set up mock configuration and patch dynamic imports.
        """
        self.mock_config: Dict[str, Any] = {
            "loader": {"class": "CSVLoader", "module": "loader.csv_loader", "file_path": ""},
            "fetcher": {"class": "DatabentoFetcher", "module": "fetcher.databento_fetcher"},
            "cleaner": {"class": "DatabentoCleaner", "module": "cleaner.databento_cleaner"},
            "inserter": {"class": "OhlcvRepository", "module": "repository.ohlcv_repository"},
            "time_range": {"start_date": "2023-01-01", "end_date": "2023-01-02"},
            "database": {"target_schema": "futures_data", "raw_table": "ohlcv_1d_raw", "table": "ohlcv_1d"},
            "batch_downloading": {"batch": False, "unit": "Daily", "max_units": 30},
        }

    @patch("src.application.orchestrator.get_instance")
    def test_orchestrator_initialization(self, mock_get_instance: MagicMock) -> None:
        """
        Test that Orchestrator initializes loader/fetcher/cleaner dynamically.
        The inserter is intentionally NOT built here -- it's built fresh per
        symbol inside retrieve_and_process_data so concurrent symbols never
        share a connection (see the connect/close race fix).
        """
        mock_loader = MagicMock()
        mock_fetcher = MagicMock()
        mock_cleaner = MagicMock()

        mock_get_instance.side_effect = [mock_loader, mock_fetcher, mock_cleaner]

        orchestrator = Orchestrator(config=self.mock_config)

        self.assertEqual(mock_get_instance.call_count, 3)
        self.assertEqual(orchestrator.loader, mock_loader)
        self.assertEqual(orchestrator.fetcher, mock_fetcher)
        self.assertEqual(orchestrator.cleaner, mock_cleaner)
        self.assertFalse(hasattr(orchestrator, "inserter"))

        mock_get_instance.assert_any_call(self.mock_config, "loader", "class", expected_port=SymbolSourcePort)
        mock_get_instance.assert_any_call(self.mock_config, "fetcher", "class", expected_port=FetcherPort)
        mock_get_instance.assert_any_call(self.mock_config, "cleaner", "class", expected_port=CleanerPort)

    @patch("src.application.orchestrator.determine_date_range", return_value=("2023-01-01", "2023-01-02"))
    @patch("src.application.orchestrator.Orchestrator.retrieve_and_process_data", new_callable=AsyncMock)
    @patch("src.application.orchestrator.get_instance")
    async def test_orchestrator_run(
        self,
        mock_get_instance: MagicMock,
        mock_process_data: AsyncMock,
        mock_determine_date_range: MagicMock,
    ) -> None:
        """
        Test that Orchestrator run() processes all symbols asynchronously.
        """
        mock_loader = MagicMock()
        mock_loader.load_symbols.return_value = {"ES": "FUTURE", "NQ": "FUTURE"}
        mock_get_instance.side_effect = [mock_loader, MagicMock(), MagicMock()]

        orchestrator = Orchestrator(config=self.mock_config)
        await orchestrator.run()

        mock_loader.load_symbols.assert_called_once()
        self.assertEqual(mock_process_data.call_count, 2)
        mock_process_data.assert_any_call(
            {"dataSymbol": "ES", "instrumentType": "FUTURE"}, "2023-01-01", "2023-01-02"
        )
        mock_process_data.assert_any_call(
            {"dataSymbol": "NQ", "instrumentType": "FUTURE"}, "2023-01-01", "2023-01-02"
        )
        mock_determine_date_range.assert_called()

    @patch("src.application.orchestrator.determine_date_range", return_value=("2023-01-01", "2023-01-02"))
    @patch("src.application.orchestrator.Orchestrator.retrieve_and_process_data", new_callable=AsyncMock)
    @patch("src.application.orchestrator.get_instance")
    async def test_orchestrator_run_raises_on_symbol_failure(
        self,
        mock_get_instance: MagicMock,
        mock_process_data: AsyncMock,
        mock_determine_date_range: MagicMock,
    ) -> None:
        """
        A failure processing one symbol must surface as a raised error from
        run(), not be silently swallowed -- asyncio.gather previously reported
        overall success even when every symbol failed.
        """
        mock_loader = MagicMock()
        mock_loader.load_symbols.return_value = {"ES": "FUTURE", "NQ": "FUTURE"}
        mock_get_instance.side_effect = [mock_loader, MagicMock(), MagicMock()]
        mock_process_data.side_effect = [RuntimeError("boom"), True]

        orchestrator = Orchestrator(config=self.mock_config)

        with self.assertRaises(RuntimeError):
            await orchestrator.run()

    @patch("src.application.orchestrator.determine_date_range", return_value=("2023-01-01", "2023-01-02"))
    @patch("src.application.orchestrator.get_instance")
    def test_get_symbols_and_date_range(self, mock_get_instance: MagicMock, mock_determine_date_range: MagicMock) -> None:
        """
        Stage-level entry point used by the Airflow DAG to fan out per-symbol
        work independently of the run() monolith -- returns symbols in the
        same dict shape retrieve_and_process_data expects.
        """
        mock_loader = MagicMock()
        mock_loader.load_symbols.return_value = {"ES": "FUTURE", "NQ": "FUTURE"}
        mock_get_instance.side_effect = [mock_loader, MagicMock(), MagicMock()]

        orchestrator = Orchestrator(config=self.mock_config)
        meta = orchestrator.get_symbols_and_date_range()

        self.assertEqual(meta["start_date"], "2023-01-01")
        self.assertEqual(meta["end_date"], "2023-01-02")
        self.assertIn({"dataSymbol": "ES", "instrumentType": "FUTURE"}, meta["symbols"])
        self.assertIn({"dataSymbol": "NQ", "instrumentType": "FUTURE"}, meta["symbols"])

    @patch("src.application.orchestrator.determine_date_range")
    @patch("src.application.orchestrator.get_instance")
    def test_get_symbols_does_not_touch_date_range(
        self, mock_get_instance: MagicMock, mock_determine_date_range: MagicMock
    ) -> None:
        """
        get_symbols() must not call determine_date_range() -- a caller that
        only needs the symbol list (e.g. the DAG's resolve_symbols task)
        shouldn't require the DB to be reachable at all.
        """
        mock_loader = MagicMock()
        mock_loader.load_symbols.return_value = {"ES": "FUTURE", "NQ": "FUTURE"}
        mock_get_instance.side_effect = [mock_loader, MagicMock(), MagicMock()]

        orchestrator = Orchestrator(config=self.mock_config)
        symbols = orchestrator.get_symbols()

        mock_determine_date_range.assert_not_called()
        self.assertIn({"dataSymbol": "ES", "instrumentType": "FUTURE"}, symbols)
        self.assertIn({"dataSymbol": "NQ", "instrumentType": "FUTURE"}, symbols)

    @patch("src.application.orchestrator.determine_date_range", return_value=("2023-01-01", "2023-01-02"))
    @patch("src.application.orchestrator.get_instance")
    def test_get_date_range_does_not_touch_symbols(
        self, mock_get_instance: MagicMock, mock_determine_date_range: MagicMock
    ) -> None:
        """
        get_date_range() must not call loader.load_symbols() -- a caller that
        only needs the date range (e.g. the DAG's resolve_date_range task)
        shouldn't reload the symbols CSV.
        """
        mock_loader = MagicMock()
        mock_get_instance.side_effect = [mock_loader, MagicMock(), MagicMock()]

        orchestrator = Orchestrator(config=self.mock_config)
        date_range = orchestrator.get_date_range()

        mock_loader.load_symbols.assert_not_called()
        self.assertEqual(date_range, {"start_date": "2023-01-01", "end_date": "2023-01-02"})

    @patch("src.application.orchestrator.get_instance")
    async def test_retrieve_and_process_data(self, mock_get_instance: MagicMock) -> None:
        """
        Test that retrieve_and_process_data calls fetcher, cleaner, and a
        freshly-built inserter in sequence, and closes that inserter.
        """
        mock_fetcher = MagicMock()
        mock_fetcher.retrieve = AsyncMock(return_value=MagicMock(
            to_dict=MagicMock(return_value=[{"time": "2023-01-01", "symbol": "ES", "open": 100.5}])
        ))
        mock_cleaner = MagicMock()
        mock_cleaner.clean.return_value = [{"time": "2023-01-01"}]
        mock_inserter = MagicMock()

        # __init__ consumes loader/fetcher/cleaner; retrieve_and_process_data
        # then requests one more instance (the per-symbol inserter).
        mock_get_instance.side_effect = [MagicMock(), mock_fetcher, mock_cleaner, mock_inserter]

        orchestrator = Orchestrator(config=self.mock_config)
        result = await orchestrator.retrieve_and_process_data(
            {"dataSymbol": "ES", "instrumentType": "FUTURE"}, "2023-01-01", "2023-01-02"
        )

        self.assertTrue(result)
        mock_fetcher.retrieve.assert_called_once_with(
            symbol="ES",
            loaded_asset_type="FUTURE",
            start_date="2023-01-01",
            end_date="2023-01-02",
            batch_config=self.mock_config["batch_downloading"],
        )
        mock_cleaner.clean.assert_called_once()
        mock_inserter.connect.assert_called_once()
        self.assertEqual(mock_inserter.insert_data.call_count, 2)
        mock_inserter.insert_data.assert_any_call(
            data=[{"time": "2023-01-01"}],
            schema="futures_data",
            table="ohlcv_1d",
        )
        mock_inserter.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
