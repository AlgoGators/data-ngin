import csv
import os
import tempfile
import unittest


class TestDifferingRows(unittest.TestCase):
    """
    Decides which bars get re-adjusted. Over-reporting rewrites the whole table on
    float noise; under-reporting leaves a broken price series in place.
    """

    def test_detects_a_changed_adjustment(self) -> None:
        from scripts.refresh_adjustments import differing_rows

        ours = {"2025-06-02": {"adjusted_close": 479.17, "adj_open": 470.0}}
        theirs = {"2025-06-02": {"adjusted_close": 119.7925, "adj_open": 117.5}}

        out = differing_rows(ours, theirs)

        self.assertEqual(len(out), 1)
        day, changed = out[0]
        self.assertEqual(day, "2025-06-02")
        self.assertAlmostEqual(changed["adjusted_close"], 119.7925)

    def test_the_crwd_case(self) -> None:
        """CRWD's adjusted prices were exactly 4x the vendor's after a 4-for-1 split
        the table never applied. A backtest crossing that date sees a fabricated
        -75% move, so this must be caught."""
        from scripts.refresh_adjustments import differing_rows

        ours = {f"2025-06-{d:02d}": {"adjusted_close": v} for d, v in
                [(2, 479.17), (3, 488.76), (4, 460.56)]}
        theirs = {f"2025-06-{d:02d}": {"adjusted_close": v / 4} for d, v in
                  [(2, 479.17), (3, 488.76), (4, 460.56)]}

        self.assertEqual(len(differing_rows(ours, theirs)), 3)

    def test_identical_values_are_not_reported(self) -> None:
        from scripts.refresh_adjustments import differing_rows

        ours = {"2025-06-02": {"adjusted_close": 100.0, "div_cash": 0.0}}
        self.assertEqual(differing_rows(ours, dict(ours)), [])

    def test_float_noise_is_tolerated(self) -> None:
        """An exact != would flag every bar in the table as changed."""
        from scripts.refresh_adjustments import differing_rows

        ours = {"2025-06-02": {"adjusted_close": 100.0}}
        theirs = {"2025-06-02": {"adjusted_close": 100.0 + 1e-9}}
        self.assertEqual(differing_rows(ours, theirs), [])

    def test_day_absent_from_the_vendor_is_left_alone(self) -> None:
        """A day the vendor no longer returns is a coverage question, not an
        adjustment one. Acting on it would let a vendor blip delete our data."""
        from scripts.refresh_adjustments import differing_rows

        ours = {"2025-06-02": {"adjusted_close": 100.0}}
        self.assertEqual(differing_rows(ours, {}), [])

    def test_null_on_either_side_is_skipped(self) -> None:
        from scripts.refresh_adjustments import differing_rows

        self.assertEqual(
            differing_rows({"2025-06-02": {"adjusted_close": None}},
                           {"2025-06-02": {"adjusted_close": 100.0}}), [])
        self.assertEqual(
            differing_rows({"2025-06-02": {"adjusted_close": 100.0}},
                           {"2025-06-02": {"adjusted_close": None}}), [])

    def test_raw_ohlcv_is_never_included(self) -> None:
        """As-traded prices do not change. Rewriting them would turn a vendor hiccup
        into data loss, so they must never appear in an update payload."""
        from scripts.refresh_adjustments import differing_rows, ADJ_COLUMNS

        for c in ("open", "high", "low", "close", "volume"):
            self.assertNotIn(c, ADJ_COLUMNS)

        ours = {"2025-06-02": {"close": 100.0, "adjusted_close": 100.0}}
        theirs = {"2025-06-02": {"close": 999.0, "adjusted_close": 100.0}}
        self.assertEqual(differing_rows(ours, theirs), [],
                         "a changed raw close must not trigger an update")


class TestCheckpoint(unittest.TestCase):
    def test_only_done_counts_as_complete(self) -> None:
        from scripts.refresh_adjustments import load_done

        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "p.csv")
            with open(p, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, lineterminator="\n")
                w.writerow(["symbol", "status", "rows_updated", "at"])
                w.writerow(["AAPL", "DONE", "0", "2026-08-07T10:00:00"])
                w.writerow(["MSFT", "FAILED", "0", "2026-08-07T10:00:01"])
                w.writerow(["XLE", "EMPTY", "0", "2026-08-07T10:00:02"])

            self.assertEqual(load_done(p), {"AAPL"})

    def test_missing_file_is_empty(self) -> None:
        from scripts.refresh_adjustments import load_done

        self.assertEqual(load_done("/nonexistent/p.csv"), set())


if __name__ == "__main__":
    unittest.main()
