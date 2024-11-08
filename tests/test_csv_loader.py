import unittest
import pandas as pd
from data.modules.csv_loader import CSVLoader
from typing import Dict, List, Any
import os

class TestCSVLoader(unittest.TestCase):
    """
    Unit tests for the CSVLoader class.
    """

    def setUp(self) -> None:
        """
        Set up a CSVLoader instance with a test configuration and sample CSV file.
        """
        # Create a sample test CSV file
        self.test_csv_path: str = 'data/contracts/contract.csv'
        sample_data: pd.DataFrame = pd.DataFrame({
            'dataSymbol': ['AAPL', 'ES', 'BTC'],
            'instrumentType': ['EQUITY', 'FUTURES', 'CRYPTO']
        })
        sample_data.to_csv(self.test_csv_path, index=False)

        # Set up CSVLoader with a test configuration
        self.loader: CSVLoader = CSVLoader(config_path='data/config/config.yaml', contract_path=self.test_csv_path)
        self.loader.config = {
            "providers": {
                "databento": {
                    "supported_assets": ["EQUITY", "FUTURES"]
                }
            }
        }

    def tearDown(self) -> None:
        """
        Clean up by removing the test CSV file.
        """
        if os.path.exists(self.test_csv_path):
            os.remove(self.test_csv_path)

    def test_load_symbols(self) -> None:
        """
        Test if symbols are loaded correctly from the CSV file.
        """
        symbols: Dict[str, str] = self.loader.load_symbols()
        expected_symbols: Dict[str, str] = {
            "AAPL": "EQUITY",
            "ES": "FUTURES",
            "BTC": "CRYPTO"
        }
        self.assertEqual(symbols, expected_symbols, "Loaded symbols do not match expected values")

    def test_validate_symbols(self) -> None:
        """
        Test if validate_symbols correctly filters supported symbols.
        """
        symbols: Dict[str, str] = self.loader.load_symbols()
        validated_symbols: Dict[str, str] = self.loader.validate_symbols(symbols)
        self.assertIn("AAPL", validated_symbols, "AAPL should be validated as supported")
        self.assertIn("ES", validated_symbols, "ES should be validated as supported")
        self.assertNotIn("BTC", validated_symbols, "BTC should not be validated as supported")

    def test_prepare_for_ingestion(self) -> None:
        """
        Test if ingestion jobs are prepared correctly for validated symbols.
        """
        ingestion_jobs: List[Dict[str, Any]] = self.loader.prepare_for_ingestion()
        expected_jobs: List[Dict[str, Any]] = [
            {"symbol": "AAPL", "asset_type": "EQUITY", "provider": "databento", "aggregation_level": "ohlcv-1d"},
            {"symbol": "ES", "asset_type": "FUTURES", "provider": "databento", "aggregation_level": "ohlcv-1d"}
        ]
        self.assertEqual(len(ingestion_jobs), 2, "Only 2 ingestion jobs should be prepared for supported symbols")
        self.assertEqual(ingestion_jobs, expected_jobs, "Ingestion jobs do not match expected jobs")

if __name__ == "__main__":
    unittest.main()
