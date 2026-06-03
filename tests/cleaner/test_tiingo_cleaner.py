import unittest
import pandas as pd
from typing import Dict, Any

from src.modules.cleaner.tiingo_cleaner import TiingoCleaner


class TestTiingoCleaner(unittest.TestCase):
    """Unit tests for TiingoCleaner."""

    def setUp(self) -> None:
        self.config: Dict[str, Any] = {
            "missing_data": {
                "drop_nan": "True",
                "forward_fill": "False",
                "backward_fill": "False",
                "interpolate": "False",
                "zero_fill": "False",
                "mean_fill": "False",
                "median_fill": "False",
                "custom_fill": "False",
                "custom_value": "0",
            }
        }
        self.cleaner = TiingoCleaner(config=self.config)

    def _raw(self) -> pd.DataFrame:
        # Fetcher output shape (Tiingo fields already renamed to snake_case).
        # Includes an extra junk column (should be dropped), a duplicate row,
        # and a corrupt row (non-positive price) to exercise cleaning.
        return pd.DataFrame({
            "time": [
                "2024-01-02T00:00:00.000Z",
                "2024-01-03T00:00:00.000Z",
                "2024-01-03T00:00:00.000Z",  # duplicate of row 2
                "2024-01-04T00:00:00.000Z",  # corrupt (close <= 0)
            ],
            "open":           [187.15, 184.22, 184.22, 1.0],
            "high":           [188.44, 185.88, 185.88, 1.0],
            "low":            [183.88, 183.43, 183.43, 1.0],
            "close":          [185.64, 184.25, 184.25, -5.0],
            "volume":         [82488674, 58414460, 58414460, 100],
            "adj_open":       [185.06, 182.18, 182.18, 1.0],
            "adj_high":       [186.34, 183.81, 183.81, 1.0],
            "adj_low":        [181.83, 181.40, 181.40, 1.0],
            "adjusted_close": [183.57, 182.19, 182.19, -5.0],
            "adj_volume":     [82488674.0, 58414460.0, 58414460.0, 100.0],
            "div_cash":       [0.0, 0.0, 0.0, 0.0],
            "split_factor":   [1.0, 1.0, 1.0, 1.0],
            "symbol":         ["aapl", "aapl", "aapl", "aapl"],  # lowercase to test normalization
            "junk_extra":     [1, 2, 3, 4],                      # extra column, should be dropped
        })

    def test_clean_returns_list_of_dicts(self) -> None:
        result = self.cleaner.clean(self._raw())
        self.assertIsInstance(result, list)
        self.assertTrue(all(isinstance(r, dict) for r in result))
        # Duplicate removed + corrupt row dropped -> 2 unique valid rows.
        self.assertEqual(len(result), 2)

    def test_clean_schema_and_types(self) -> None:
        result = self.cleaner.clean(self._raw())
        expected_keys = {
            "time", "symbol", "open", "high", "low", "close", "volume",
            "adj_open", "adj_high", "adj_low", "adjusted_close", "adj_volume",
            "div_cash", "split_factor",
        }
        self.assertEqual(set(result[0].keys()), expected_keys)  # junk_extra dropped
        # symbol normalized to uppercase
        self.assertEqual(result[0]["symbol"], "AAPL")
        # time parsed to a pandas Timestamp (tz-aware UTC)
        self.assertIsInstance(result[0]["time"], pd.Timestamp)
        self.assertEqual(str(result[0]["time"].tz), "UTC")
        # sorted ascending by time
        self.assertLess(result[0]["time"], result[1]["time"])

    def test_clean_drops_nonpositive_prices(self) -> None:
        result = self.cleaner.clean(self._raw())
        # the -5.0 close row must not survive
        self.assertTrue(all(r["close"] > 0 for r in result))

    def test_missing_required_field_raises(self) -> None:
        bad = self._raw().drop(columns=["adjusted_close"])
        with self.assertRaises(ValueError):
            self.cleaner.clean(bad)

    def test_empty_input_returns_empty_list(self) -> None:
        self.assertEqual(self.cleaner.clean(pd.DataFrame()), [])


if __name__ == "__main__":
    unittest.main()
