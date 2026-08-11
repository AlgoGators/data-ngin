import unittest
import pandas as pd
from typing import Dict, Any, Callable
from src.infrastructure.cleaner.databento_cleaner import DatabentoCleaner


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

    def test_handle_missing_data_accepts_real_booleans(self) -> None:
        """
        `handle_missing_data` now delegates to MissingDataFiller, which checks
        truthiness instead of `== "True"` (Phase 2 declarative-config fix).
        Real YAML bools now work -- config.yaml is validated through Pydantic's
        MissingDataConfig, which coerces to real bools, so this had to change
        in lockstep or every flag would have silently gone dead.
        """
        data: pd.DataFrame = pd.DataFrame({
            "time": ["2023-01-01", "2023-01-02"],
            "open": [100.0, None],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, None],
            "volume": [1000, None],
        })
        numeric_columns = ["open", "high", "low", "close", "volume"]
        data[numeric_columns] = data[numeric_columns].apply(pd.to_numeric, errors="coerce")

        self.config["missing_data"] = {"zero_fill": True}  # real bool
        cleaner: DatabentoCleaner = DatabentoCleaner(config=self.config)
        result: pd.DataFrame = cleaner.handle_missing_data(data.copy())

        self.assertEqual(result["volume"].iloc[1], 0.0)

    def test_apply_back_adjustment_no_roll(self) -> None:
        """
        Characterization test: with no symbol change (no roll), OHLC values
        are left unchanged.
        """
        data = pd.DataFrame({
            "time": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "symbol": ["MES", "MES", "MES"],
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "volume": [1000, 1100, 1200],
        })

        result = self.cleaner.apply_back_adjustment(data.copy())

        pd.testing.assert_series_equal(result["open"], data["open"], check_names=False)
        pd.testing.assert_series_equal(result["close"], data["close"], check_names=False)

    def test_apply_back_adjustment_single_roll(self) -> None:
        """
        Characterization test pinning the current volume-based roll/back-
        adjustment algorithm: on a roll (symbol change where the new
        contract's volume exceeds the old one's), every row BEFORE the roll
        gets shifted by (prev close - new open); rows at/after the roll are
        unchanged.
        """
        data = pd.DataFrame({
            "time": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "symbol": ["MESH23", "MESH23", "MESM23"],
            "open": [100.0, 101.0, 105.0],
            "high": [101.0, 102.0, 106.0],
            "low": [99.0, 100.0, 104.0],
            "close": [100.5, 101.5, 105.5],
            "volume": [1000, 1100, 1500],
        })

        result = self.cleaner.apply_back_adjustment(data.copy())

        # Roll detected at index 2: adjustment = close[1] (101.5) - open[2] (105.0) = -3.5
        adjustment = 101.5 - 105.0
        self.assertAlmostEqual(result["open"].iloc[0], 100.0 + adjustment)
        self.assertAlmostEqual(result["open"].iloc[1], 101.0 + adjustment)
        self.assertAlmostEqual(result["close"].iloc[1], 101.5 + adjustment)
        # Row at/after the roll point is unadjusted
        self.assertAlmostEqual(result["open"].iloc[2], 105.0)
        self.assertAlmostEqual(result["close"].iloc[2], 105.5)

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
