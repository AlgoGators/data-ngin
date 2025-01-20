import unittest
import pandas as pd
from data.modules.fred_cleaner import FREDCleaner


class TestFREDCleaner(unittest.TestCase):
    def setUp(self):
        self.mock_config = {
            "loader": {
                "series_metadata": {}
            },
            "missing_data": {
                "drop_nan": "True"  # Default method for handling missing data
            }
        }
        self.cleaner = FREDCleaner(config=self.mock_config)
        self.raw_data = pd.DataFrame({
            "time": ["2020-01-01", "2020-02-01", "2020-03-01"],
            "index_name": ["GDP"] * 3,
            "value": [100, None, 300],
            "metadata": ["{}"] * 3
        })

    def test_validate_fields_success(self):
        validated_data = self.cleaner.validate_fields(self.raw_data)
        pd.testing.assert_frame_equal(validated_data, self.raw_data)

    def test_validate_fields_missing_columns(self):
        incomplete_data = self.raw_data.drop(columns=["index_name"])
        with self.assertRaises(ValueError):
            self.cleaner.validate_fields(incomplete_data)

    def test_handle_missing_data(self):
        cleaned_data = self.cleaner.handle_missing_data(self.raw_data)
        expected_data = self.raw_data.dropna()
        pd.testing.assert_frame_equal(cleaned_data, expected_data)

    def test_transform_data(self):
        transformed_data = self.cleaner.transform_data(self.raw_data)
        self.raw_data["time"] = pd.to_datetime(self.raw_data["time"])
        expected_data = self.raw_data[["time", "index_name", "value", "metadata"]]
        pd.testing.assert_frame_equal(transformed_data, expected_data)


if __name__ == "__main__":
    unittest.main()