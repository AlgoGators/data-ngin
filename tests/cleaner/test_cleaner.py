import unittest
import pandas as pd
from typing import Dict, Any, Callable
from src.modules.cleaner.databento_cleaner import DatabentoCleaner


class TestDatabentoCleaner(unittest.TestCase):
    """
    Unit tests for the DatabentoCleaner class.
    """

    def setUp(self) -> None:
        """
        Set up a DatabentoCleaner instance with a default configuration.
        """
        self.config: Dict[str, Any] = {
            "missing_data": {
                "drop_nan": "False",
                "zero_fill": "False",
                "custom_fill": "False",
                "custom_value": "0",
                "forward_fill": "False",
                "backward_fill": "False",
                "interpolate": "False",
                "mean_fill": "False",
                "median_fill": "False",
            }
        }
        self.cleaner: DatabentoCleaner = DatabentoCleaner(config=self.config)

    def test_handle_missing_data_methods(self) -> None:
        """
        Test all applicable missing data handling methods.
        """
        data: pd.DataFrame = pd.DataFrame({
            "time": ["2023-01-01", "2023-01-02"],
            "open": [100.0, None],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, None],
            "volume": [1000, None],
        })

        # Force numeric conversion
        numeric_columns = ["open", "high", "low", "close", "volume"]
        data[numeric_columns] = data[numeric_columns].apply(
            pd.to_numeric, errors="coerce"
        )

        methods: Dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
            "drop_nan": lambda d: d.dropna(),
            "zero_fill": lambda d: d.fillna(0),
            "custom_fill": lambda d: d.fillna(999),
            "forward_fill": lambda d: d.ffill(),
            "backward_fill": lambda d: d.bfill(),
            "interpolate": lambda d: d.infer_objects().interpolate(),
            "mean_fill": lambda d: d.fillna({col: d[col].mean() for col in numeric_columns}),
            "median_fill": lambda d: d.fillna({col: d[col].median() for col in numeric_columns})
        }

        for method, action in methods.items():
            with self.subTest(method=method):
                self.config["missing_data"] = {method: "True"}
                if method == "custom_fill":
                    self.config["missing_data"]["custom_value"] = 999

                cleaner: DatabentoCleaner = DatabentoCleaner(config=self.config)
                result: pd.DataFrame = cleaner.handle_missing_data(data.copy())

                if method == "drop_nan":
                    self.assertEqual(len(result), 1)  # Only one row should remain
                else:
                    expected_value: Any = action(data.copy())["volume"].iloc[1]
                    actual_value: Any = result["volume"].iloc[1]

                    if pd.isna(expected_value) and pd.isna(actual_value):
                        self.assertTrue(pd.isna(actual_value))
                    else:
                        self.assertEqual(expected_value, actual_value)

    def test_clean_with_invalid_fields(self) -> None:
        """
        Test that the clean method raises an exception for missing fields.
        """
        data: pd.DataFrame = pd.DataFrame({
            "ts_event": ["2023-01-01"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
        })

        with self.assertRaises(ValueError):
            self.cleaner.clean(data)


if __name__ == "__main__":
    unittest.main()
