import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from data.modules.fred_fetcher import FREDFetcher


class TestFREDFetcher(unittest.TestCase):
    def setUp(self) -> None:
        """
        Initialize FREDFetcher with mock configuration.
        """
        self.mock_config = {
            "loader": {
                "series_metadata": {
                    "GDP": {
                        "index_name": "Real GDP",
                        "metadata": {
                            "description": "Real GDP in the US.",
                            "units": "Billions of Chained 2012 Dollars",
                            "frequency": "Quarterly",
                        },
                    }
                }
            }
        }

        # Patch Fred and os.getenv
        self.patches = [
            patch("data.modules.fred_fetcher.Fred"),  # Match the import path in your FREDFetcher
            patch("os.getenv", return_value="mocked_api_key"),
        ]
        self.mocks = [p.start() for p in self.patches]

        # Mock Fred client instance
        self.mock_fred = self.mocks[0]
        self.mock_fred_instance = self.mock_fred.return_value

        # Initialize FREDFetcher
        self.fetcher = FREDFetcher(config=self.mock_config)

        # Cleanup patches
        for p in self.patches:
            self.addCleanup(p.stop)

    def test_fetch_data_success(self):
        """
        Test that `fetch_data` correctly fetches and processes data.
        """
        # Mock return value for get_series
        mock_series = pd.Series(
            [100, 200, 300],
            index=pd.date_range("2020-01-01", periods=3, freq="QE")
        )
        self.mock_fred_instance.get_series.return_value = mock_series

        # Fetch data
        result = self.fetcher.fetch_data(
            symbol="GDP",
            start_date="2020-01-01",
            end_date="2020-12-31",
        )

        # Expected DataFrame
        expected_result = pd.DataFrame({
            "time": ["2020-03-31", "2020-06-30", "2020-09-30"],
            "index_name": ["Real GDP"] * 3,
            "value": [100, 200, 300],
            "metadata": [
                {"description": "Real GDP in the US.", "units": "Billions of Chained 2012 Dollars", "frequency": "Quarterly"}
            ] * 3,
        })
        expected_result["time"] = pd.to_datetime(expected_result["time"])  # Convert to datetime

        # Normalize metadata columns to JSON strings
        result["metadata"] = result["metadata"].apply(lambda x: dict(sorted(x.items())))
        expected_result["metadata"] = expected_result["metadata"].apply(lambda x: dict(sorted(x.items())))

        # Assert DataFrame equality
        pd.testing.assert_frame_equal(result.reset_index(drop=True), expected_result)

        # Assert mocks were called correctly
        self.mock_fred.assert_called_once_with(api_key="mocked_api_key")
        self.mock_fred_instance.get_series.assert_called_once_with(
            "GDP", observation_start="2020-01-01", observation_end="2020-12-31"
        )


        def test_fetch_data_no_data(self):
            """
            Test handling of no data returned from API.
            """
            # Mock empty series response
            self.mock_fred_instance.get_series.return_value = pd.Series(dtype=float)

            result = self.fetcher.fetch_data(
                symbol="GDP",
                start_date="2020-01-01",
                end_date="2020-12-31",
            )

            # Should return empty DataFrame with expected columns
            expected_columns = ["time", "index_name", "value", "metadata"]
            self.assertTrue(result.empty)
            self.assertListEqual(list(result.columns), expected_columns)

    def test_fetch_data_error_handling(self):
        """
        Test that `fetch_data` raises an exception when the API fails.
        """
        # Simulate API error
        self.mock_fred_instance.get_series.side_effect = Exception("API error")

        with self.assertRaises(Exception) as context:
            self.fetcher.fetch_data(
                symbol="GDP",
                start_date="2020-01-01",
                end_date="2020-12-31",
            )

        self.assertEqual(str(context.exception), "API error")


if __name__ == "__main__":
    unittest.main()
