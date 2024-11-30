import unittest
import pandas as pd
from typing import List, Dict, Any
from data.modules.databento_cleaner import DatabentoCleaner, RequiredFields


class TestDatabentoCleaner(unittest.TestCase):
    """
    Unit tests for the DatabentoCleaner class.
    """

    def setUp(self) -> None:
        """
        Set up a DatabentoCleaner instance with a test configuration.
        """
        self.config: Dict[str, Any] = {"drop_missing": True}
        self.cleaner: DatabentoCleaner = DatabentoCleaner(config=self.config)

    def test_clean(self) -> None:
        """
        Test the complete cleaning process using valid data.
        """
        data = pd.DataFrame({
            "ts_event": ["2023-01-01 00:00:00", "2023-01-02 00:00:00"],
            "open": [100.5, 101.0],
            "high": [101.0, 102.0],
            "low": [99.5, 100.0],
            "close": [100.0, 101.5],
            "volume": [1500, 1600],
        })

        cleaned_data = self.cleaner.clean(data)

        # Verify the output structure
        self.assertIsInstance(cleaned_data, list)
        self.assertEqual(len(cleaned_data), 2)  # Two rows should be cleaned
        self.assertIn("time", cleaned_data[0])  # Check key presence
        self.assertIn("open", cleaned_data[0])
        self.assertIn("volume", cleaned_data[0])

        # Check timestamp transformation
        self.assertEqual(cleaned_data[0]["time"], pd.Timestamp("2023-01-01T00:00:00Z"))

    def test_clean_empty_dataframe(self) -> None:
        """
        Test cleaning with an empty DataFrame.
        """
        empty_data = pd.DataFrame()
        with self.assertRaises(ValueError):
            self.cleaner.clean(empty_data)

    def test_validate_fields(self) -> None:
        """
        Test that the Cleaner validates required fields correctly.
        """
        data = pd.DataFrame({
            "time": ["2023-01-01"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
        })

        # Should not raise an exception
        validated_data = self.cleaner.validate_fields(data)
        self.assertTrue("time" in validated_data.columns)

        # Missing a required field
        data_missing = data.drop(columns=["open"])
        with self.assertRaises(ValueError):
            self.cleaner.validate_fields(data_missing)

    def test_handle_missing_data(self) -> None:
        """
        Test handling of missing data (drop or fill).
        """
        data = pd.DataFrame({
            "time": ["2023-01-01", "2023-01-02"],
            "open": [100.0, None],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, None],
            "volume": [1000, None],
        })

        # Test drop missing
        self.config["drop_missing"] = True
        cleaner = DatabentoCleaner(config=self.config)
        result = cleaner.handle_missing_data(data)
        self.assertEqual(len(result), 1)  # Only one row should remain

        # Test fill missing
        self.config["drop_missing"] = False
        cleaner = DatabentoCleaner(config=self.config)
        result = cleaner.handle_missing_data(data)
        self.assertEqual(result["volume"].iloc[1], 0)  # Missing volume filled with 0

    def test_transform_data(self) -> None:
        """
        Test transformation of raw data (e.g., timestamp conversion, sorting).
        """
        data = pd.DataFrame({
            "ts_event": ["2023-01-02 00:00:00", "2023-01-01 00:00:00"],
            "open": [101.0, 100.5],
            "high": [102.0, 101.0],
            "low": [100.0, 99.5],
            "close": [101.5, 100.0],
            "volume": [1600, 1500],
        })

        result: pd.DataFrame = self.cleaner.validate_fields(data)
        result: pd.DataFrame = self.cleaner.transform_data(result)

        # Check sorting
        self.assertTrue(result["time"].is_monotonic_increasing)

        # Check data types
        self.assertTrue(pd.api.types.is_float_dtype(result["open"]))
        self.assertTrue(pd.api.types.is_int64_dtype(result["volume"]))

    def test_clean_with_invalid_fields(self) -> None:
        """
        Test that the clean method raises an exception for missing fields.
        """
        data = pd.DataFrame({
            "ts_event": ["2023-01-01"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            # Missing "close" and "volume"
        })

        with self.assertRaises(ValueError):
            self.cleaner.clean(data)


if __name__ == "__main__":
    unittest.main()
