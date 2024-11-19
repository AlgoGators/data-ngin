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
            "volume": [1000]
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
            "volume": [1000, None]
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
            "date": ["2023-01-01", "2023-01-02"],
            "open": [100.5, 101.0],
            "high": [101.0, 102.0],
            "low": [99.5, 100.0],
            "close": [100.0, 101.5],
            "volume": [1500, 1600]
        })

        result = self.cleaner.transform_data(data)

        # Check column renaming
        self.assertTrue("time" in result.columns)
        self.assertFalse("date" in result.columns)

        # Check sorting
        self.assertTrue(result["time"].is_monotonic_increasing)

        # Check data types
        self.assertTrue(pd.api.types.is_float_dtype(result["open"]))
        self.assertTrue(pd.api.types.is_int64_dtype(result["volume"]))


if __name__ == "__main__":
    unittest.main()
