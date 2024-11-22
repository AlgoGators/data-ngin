import unittest
import tempfile
import os
from unittest.mock import patch, MagicMock, AsyncMock
from data.orchestrator import Orchestrator
from utils.dynamic_loader import load_config
from typing import Dict, List, Any


class TestIntegrationPipeline(unittest.IsolatedAsyncioTestCase):
    """
    Integration tests for the full data pipeline using the Orchestrator class.
    """

    def setUp(self) -> None:
        """
        Set up mock configuration and create temporary test files.
        """
        # Mock configuration for the pipeline
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
                    "datasets": {
                        "GLOBEX": {
                            "aggregation_levels": ["ohlcv-1d"],
                            "table_prefix": "ohlcv_",
                        }
                    },
                    "roll_type": ["c"],
                    "contract_type": ["front"],
                }
            },
            "database": {
                "target_schema": "futures_data"
            },
        }

        # Create a temporary CSV file to simulate the contracts CSV
        self.temp_csv = tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".csv")
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

    @patch("data.modules.csv_loader.CSVLoader.load_symbols", return_value=[
        {"dataSymbol": "ES"}, {"dataSymbol": "NQ"}
    ])
    @patch("data.modules.databento_fetcher.DatabentoFetcher.fetch_data", new_callable=AsyncMock)
    @patch("data.modules.databento_cleaner.DatabentoCleaner.clean", return_value=[{"time": "2023-01-01"}])
    @patch("data.modules.timescaledb_inserter.TimescaleDBInserter.insert_data")
    async def test_pipeline_run(
        self,
        mock_insert_data: MagicMock,
        mock_clean: MagicMock,
        mock_fetch_data: AsyncMock,
        mock_load_symbols: MagicMock
    ) -> None:
        """
        Test that the pipeline processes symbols end-to-end.

        Args:
            mock_insert_data (MagicMock): Mocked insert_data method.
            mock_clean (MagicMock): Mocked clean method.
            mock_fetch_data (AsyncMock): Mocked fetch_data method.
            mock_load_symbols (MagicMock): Mocked load_symbols method.
        """
        # Mock fetcher return data
        mock_fetch_data.return_value = [{"time": "2023-01-01", "symbol": "ES", "open": 100.5}]

        # Initialize the Orchestrator
        orchestrator = Orchestrator(config=self.mock_config)

        # Run the pipeline
        await orchestrator.run()

        # Verify loader was called
        mock_load_symbols.assert_called_once_with(self.temp_csv.name)

        # Verify fetcher was called twice (once for each symbol)
        self.assertEqual(mock_fetch_data.call_count, 2)
        mock_fetch_data.assert_any_call(
            symbol="ES",
            start_date="2023-01-01",
            end_date="2023-01-02",
            schema="ohlcv-1d",
            roll_type="c",
            contract_type="front"
        )
        mock_fetch_data.assert_any_call(
            symbol="NQ",
            start_date="2023-01-01",
            end_date="2023-01-02",
            schema="ohlcv-1d",
            roll_type="c",
            contract_type="front"
        )

        # Verify cleaner and inserter were called
        self.assertEqual(mock_clean.call_count, 2)
        self.assertEqual(mock_insert_data.call_count, 2)
        mock_clean.assert_any_call([{"time": "2023-01-01", "symbol": "ES", "open": 100.5}])
        mock_insert_data.assert_any_call(
            data=[{"time": "2023-01-01"}],
            schema="futures_data",
            table="ohlcv_1d"
        )


if __name__ == "__main__":
    unittest.main()
