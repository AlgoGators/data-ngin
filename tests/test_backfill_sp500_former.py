import csv
import os
import tempfile
import unittest
from datetime import date


TODAY = date(2026, 8, 6)


class TestFetchWindow(unittest.TestCase):
    """
    The window is [max(floor, tiingo_start) .. security's last trading day].
    Getting the END wrong is the single most damaging mistake available here.
    """

    def test_still_trading_runs_to_today(self) -> None:
        from scripts.backfill_sp500_former import fetch_window

        # WBA: left the index 2025-08-28 but kept trading.
        s, e = fetch_window(date(1990, 1, 2), None, TODAY)
        self.assertEqual(s, date(2000, 1, 1), "clamped to the dataset floor")
        self.assertEqual(e, TODAY)

    def test_end_is_last_trading_day_not_index_removal(self) -> None:
        """The failure this guards against: a company leaves the S&P 500 and keeps
        trading for years. Ending the fetch at its index-removal date would leave a
        multi-year hole. 111 of the targets are in exactly this situation."""
        from scripts.backfill_sp500_former import fetch_window

        index_removal = date(2015, 3, 31)
        last_traded = date(2026, 8, 4)

        s, e = fetch_window(date(2001, 2, 27), last_traded, TODAY)

        self.assertEqual(e, last_traded)
        self.assertNotEqual(e, index_removal)
        self.assertGreater((e - index_removal).days, 4000,
                           "over a decade of data would be lost by using the removal date")

    def test_delisted_security_stops_at_its_final_bar(self) -> None:
        from scripts.backfill_sp500_former import fetch_window

        s, e = fetch_window(date(1985, 2, 25), date(2015, 7, 13), TODAY)
        self.assertEqual((s, e), (date(2000, 1, 1), date(2015, 7, 13)))

    def test_start_after_floor_is_respected(self) -> None:
        """Requesting years before the security existed only bloats the response."""
        from scripts.backfill_sp500_former import fetch_window

        s, _ = fetch_window(date(2015, 10, 15), date(2019, 8, 5), TODAY)
        self.assertEqual(s, date(2015, 10, 15))

    def test_missing_start_falls_back_to_floor(self) -> None:
        from scripts.backfill_sp500_former import fetch_window

        s, e = fetch_window(None, date(2010, 5, 1), TODAY)
        self.assertEqual((s, e), (date(2000, 1, 1), date(2010, 5, 1)))

    def test_security_entirely_before_the_floor_is_not_inverted(self) -> None:
        """An inverted range would be sent to the vendor as start > end."""
        from scripts.backfill_sp500_former import fetch_window

        s, e = fetch_window(date(1990, 1, 1), date(1998, 6, 30), TODAY)
        self.assertLessEqual(s, e)
        self.assertEqual((s, e), (date(2000, 1, 1), date(2000, 1, 1)))


class TestIsDelisted(unittest.TestCase):
    def test_recent_last_bar_is_still_trading(self) -> None:
        from scripts.backfill_sp500_former import is_delisted

        self.assertFalse(is_delisted(date(2026, 8, 4), TODAY))

    def test_old_last_bar_is_delisted(self) -> None:
        from scripts.backfill_sp500_former import is_delisted

        self.assertTrue(is_delisted(date(2019, 7, 8), TODAY))

    def test_open_end_is_not_delisted(self) -> None:
        from scripts.backfill_sp500_former import is_delisted

        self.assertFalse(is_delisted(None, TODAY))

    def test_threshold_boundary(self) -> None:
        from scripts.backfill_sp500_former import is_delisted

        self.assertFalse(is_delisted(TODAY - __import__("datetime").timedelta(days=30), TODAY))
        self.assertTrue(is_delisted(TODAY - __import__("datetime").timedelta(days=31), TODAY))


class TestCheckpoint(unittest.TestCase):
    """Resumability matters: the run makes ~255 vendor calls and a stall partway
    through must not mean refetching everything."""

    def test_only_done_rows_count_as_complete(self) -> None:
        from scripts.backfill_sp500_former import load_done

        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "progress.csv")
            with open(p, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, lineterminator="\n")
                w.writerow(["ticker", "status", "rows", "at"])
                w.writerow(["FDO", "DONE", "3905", "2026-08-06T10:00:00"])
                w.writerow(["RHT", "FAILED", "0", "2026-08-06T10:00:01"])
                w.writerow(["LXK", "EMPTY", "0", "2026-08-06T10:00:02"])

            done = load_done(p)

        self.assertEqual(done, {"FDO"},
                         "FAILED and EMPTY must be retried, not treated as complete")

    def test_missing_checkpoint_is_empty_not_an_error(self) -> None:
        from scripts.backfill_sp500_former import load_done

        self.assertEqual(load_done("/nonexistent/path/progress.csv"), set())


if __name__ == "__main__":
    unittest.main()
