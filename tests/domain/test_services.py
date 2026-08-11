import unittest
from datetime import datetime, timedelta
import pandas as pd
from src.domain.services import SymbolRemapper, BackAdjuster, MissingDataFiller, StalenessChecker


class TestSymbolRemapper(unittest.TestCase):
    def test_default_table_remaps_known_symbol(self) -> None:
        remapper = SymbolRemapper()
        self.assertEqual(remapper.remap("ES"), "MES")

    def test_unknown_symbol_passes_through(self) -> None:
        remapper = SymbolRemapper()
        self.assertEqual(remapper.remap("AAPL"), "AAPL")

    def test_custom_table_overrides_default(self) -> None:
        remapper = SymbolRemapper({"ES": "CUSTOM"})
        self.assertEqual(remapper.remap("ES"), "CUSTOM")
        self.assertEqual(remapper.remap("NQ"), "NQ")  # not in custom table


class TestBackAdjuster(unittest.TestCase):
    def test_no_roll_no_change(self) -> None:
        data = pd.DataFrame({
            "symbol": ["MES", "MES"],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [1000, 1100],
        })
        result = BackAdjuster().apply(data.copy())
        pd.testing.assert_series_equal(result["close"], data["close"], check_names=False)

    def test_apply_skips_when_asset_type_does_not_match(self) -> None:
        data = pd.DataFrame({
            "symbol": ["AAPL", "MSFT"],
            "open": [100.0, 200.0],
            "high": [101.0, 201.0],
            "low": [99.0, 199.0],
            "close": [100.5, 200.5],
            "volume": [1000, 2000],
        })
        result = BackAdjuster(applies_to="FUTURE").apply(data.copy(), asset_type="EQUITY")
        pd.testing.assert_frame_equal(result, data)

    def test_apply_runs_when_asset_type_matches(self) -> None:
        data = pd.DataFrame({
            "symbol": ["MESH23", "MESM23"],
            "open": [100.0, 105.0],
            "high": [101.0, 106.0],
            "low": [99.0, 104.0],
            "close": [100.5, 105.5],
            "volume": [1000, 1500],
        })
        result = BackAdjuster(applies_to="FUTURE").apply(data.copy(), asset_type="FUTURE")
        adjustment = 100.5 - 105.0
        self.assertAlmostEqual(result["open"].iloc[0], 100.0 + adjustment)

    def test_detect_rolls_finds_volume_based_roll(self) -> None:
        data = pd.DataFrame({
            "symbol": ["MESH23", "MESM23"],
            "open": [100.0, 105.0],
            "high": [101.0, 106.0],
            "low": [99.0, 104.0],
            "close": [100.5, 105.5],
            "volume": [1000, 1500],
        })
        rolls = BackAdjuster().detect_rolls(data)
        self.assertEqual(len(rolls), 1)
        self.assertEqual(rolls[0].index, 1)
        self.assertAlmostEqual(rolls[0].adjustment, 100.5 - 105.0)


class TestMissingDataFiller(unittest.TestCase):
    def test_zero_fill_string_true(self) -> None:
        data = pd.DataFrame({"volume": [1.0, None]})
        result = MissingDataFiller({"zero_fill": "True"}).fill(data)
        self.assertEqual(result["volume"].iloc[1], 0.0)

    def test_real_bool_true_triggers_fill(self) -> None:
        data = pd.DataFrame({"volume": [1.0, None]})
        result = MissingDataFiller({"zero_fill": True}).fill(data)
        self.assertEqual(result["volume"].iloc[1], 0.0)

    def test_string_false_does_not_trigger_fill(self) -> None:
        data = pd.DataFrame({"volume": [1.0, None]})
        result = MissingDataFiller({"zero_fill": "False"}).fill(data)
        self.assertTrue(pd.isna(result["volume"].iloc[1]))


class TestStalenessChecker(unittest.TestCase):
    def test_no_data_is_stale(self) -> None:
        report = StalenessChecker().check_staleness(None)
        self.assertTrue(report.is_stale)
        self.assertIn("No data found", report.message)

    def test_within_threshold_is_not_stale(self) -> None:
        now = datetime(2023, 6, 2, 12, 0, 0)
        checker = StalenessChecker(max_staleness=timedelta(days=1))
        report = checker.check_staleness("2023-06-02", now=now)
        self.assertFalse(report.is_stale)
        self.assertEqual(report.latest_date, "2023-06-02")

    def test_beyond_threshold_is_stale(self) -> None:
        now = datetime(2023, 6, 5, 12, 0, 0)
        checker = StalenessChecker(max_staleness=timedelta(days=1))
        report = checker.check_staleness("2023-06-02", now=now)
        self.assertTrue(report.is_stale)
        self.assertEqual(report.age, timedelta(days=3, hours=12))

    def test_exactly_at_threshold_is_not_stale(self) -> None:
        now = datetime(2023, 6, 3, 0, 0, 0)
        checker = StalenessChecker(max_staleness=timedelta(days=1))
        report = checker.check_staleness("2023-06-02", now=now)
        self.assertFalse(report.is_stale)

    def test_detect_date_gaps_finds_missing_day(self) -> None:
        checker = StalenessChecker()
        gaps = checker.detect_date_gaps(["2023-06-01", "2023-06-02", "2023-06-04"])
        self.assertEqual(gaps, ["2023-06-03"])

    def test_detect_date_gaps_no_gaps(self) -> None:
        checker = StalenessChecker()
        gaps = checker.detect_date_gaps(["2023-06-01", "2023-06-02", "2023-06-03"])
        self.assertEqual(gaps, [])

    def test_detect_date_gaps_empty_input(self) -> None:
        checker = StalenessChecker()
        self.assertEqual(checker.detect_date_gaps([]), [])


if __name__ == "__main__":
    unittest.main()
