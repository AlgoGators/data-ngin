import unittest
from typing import List, Dict, Any
from data.modules.fetcher import Fetcher


class MockFetcher(Fetcher):
    """
    A mock subclass of Fetcher to test base functionality.
    """

    def fetch_data(self, symbol: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Dummy implementation of the abstract method for testing.

        Args:
            symbol (str): The symbol to fetch data for.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.

        Returns:
            List[Dict[str, Any]]: Mock data for testing purposes.
        """
        return [{"time": start_date, "open": 100.0}]


class TestFetcher(unittest.TestCase):
    """
    Unit tests for the Fetcher base class using MockFetcher.
    """

    def setUp(self) -> None:
        """
        Set up a mock fetcher instance with a test configuration.
        """
        self.config: Dict[str, Any] = {
            "fetcher": {
                "batch_size_days": 30
            },
            "providers": {
                "databento": {
                    "supported_assets": ["EQUITY", "FUTURES"]
                }
            }
        }
        self.fetcher: MockFetcher = MockFetcher(config=self.config)

    def test_config_storage(self) -> None:
        """
        Test that the configuration is stored correctly in the fetcher instance.
        """
        self.assertEqual(self.fetcher.config, self.config, "Fetcher config storage failed.")
        self.assertIn("fetcher", self.fetcher.config)
        self.assertEqual(self.fetcher.config["fetcher"]["batch_size_days"], 30)
        self.assertIn("providers", self.fetcher.config)

    def test_supported_assets(self) -> None:
        """
        Test that supported assets are defined correctly.
        """
        supported_assets: List[str] = self.fetcher.config["providers"]["databento"]["supported_assets"]
        self.assertIn("EQUITY", supported_assets, "EQUITY should be a supported asset.")
        self.assertIn("FUTURES", supported_assets, "FUTURES should be a supported asset.")
        self.assertNotIn("OPTIONS", supported_assets, "OPTIONS should not be supported.")

    def test_invalid_initialization(self) -> None:
        """
        Test that initializing without a config raises an error.
        """
        with self.assertRaises(TypeError):
            MockFetcher()  # Missing config should raise an error.


if __name__ == "__main__":
    unittest.main()
