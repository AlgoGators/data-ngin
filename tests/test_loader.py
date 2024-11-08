import unittest
from data.modules.loader import Loader
from typing import Dict, List, Any

class MockLoader(Loader):
    """
    Mock subclass of Loader for testing purposes. This class simulates
    loading symbols and asset types from a predefined dictionary.
    """
    
    def load_symbols(self) -> Dict[str, str]:
        """
        Mock method to load symbols and asset types for testing.
        
        Returns:
            Dict[str, str]: A dictionary of symbols and their asset types.
        """
        return {
            "AAPL": "EQUITY",
            "ES": "FUTURES",
            "BTC": "CRYPTO"
        }

class TestLoader(unittest.TestCase):
    """
    Unit tests for the Loader base class using the MockLoader subclass.
    """
    
    def setUp(self) -> None:
        """
        Set up a mock loader instance with a test configuration.
        """
        self.loader: Loader = MockLoader(config_path='data\config\config.yaml')
        self.loader.config = {
            "providers": {
                "databento": {
                    "supported_assets": ["EQUITY", "FUTURES"]
                }
            }
        }

    def test_load_config(self) -> None:
        """
        Test if the configuration loads correctly.
        """
        config: Dict[str, Any] = self.loader.load_config()
        self.assertIn("providers", config, "Config should contain 'providers' key")

    def test_validate_symbols(self) -> None:
        """
        Test the validate_symbols method to ensure only supported symbols are validated.
        """
        symbols: Dict[str, str] = self.loader.load_symbols()
        validated_symbols: List[str] = self.loader.validate_symbols(symbols)
        self.assertIn("AAPL", validated_symbols, "AAPL should be supported")
        self.assertIn("ES", validated_symbols, "ES should be supported")
        self.assertNotIn("BTC", validated_symbols, "BTC should not be supported")

    def test_prepare_for_ingestion(self) -> None:
        """
        Test the prepare_for_ingestion method to check job preparation.
        """
        ingestion_jobs: List[Dict[str, Any]] = self.loader.prepare_for_ingestion()
        expected_jobs: List[Dict[str, Any]] = [
            {"symbol": "AAPL", "asset_type": "EQUITY", "provider": "databento", "aggregation_level": "ohlcv-1d"},
            {"symbol": "ES", "asset_type": "FUTURES", "provider": "databento", "aggregation_level": "ohlcv-1d"}
        ]
        self.assertEqual(len(ingestion_jobs), 2, "Only 2 jobs should be prepared for supported symbols")
        self.assertEqual(ingestion_jobs, expected_jobs, "Prepared ingestion jobs do not match expected jobs")

if __name__ == "__main__":
    unittest.main()
