import unittest
from typing import List, Dict, Any
from data.modules.fetcher import Fetcher


class MockFetcher(Fetcher):
    """
    A mock subclass of Fetcher to test base functionality without implementing
    the abstract fetch_data method.
    """

    def fetch_data(self, symbol: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        A dummy implementation of the abstract method, not used in tests.

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
    Unit tests for the Fetcher base class, using the MockFetcher subclass to test methods.
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

    def test_config(self) -> None:
        """
        Test that the configuration is stored correctly in the fetcher instance.
        """
        # Check the full configuration
        self.assertEqual(self.fetcher.config, self.config)

        # Validate specific configuration fields
        self.assertIn("fetcher", self.fetcher.config)
        self.assertEqual(self.fetcher.config["fetcher"]["batch_size_days"], 30)
        self.assertIn("providers", self.fetcher.config)


if __name__ == "__main__":
    unittest.main()
