import unittest


class TestIndexDrift(unittest.TestCase):
    """
    Drift between the committed snapshot and the live index is how survivorship bias
    creeps back after it has been fixed once.
    """

    def test_joined_the_index_is_reported(self) -> None:
        """The MRVL/FLEX/HONA/FERG case: in the index, absent from the snapshot, so
        never fetched at all."""
        from scripts.check_sp500_drift import index_drift

        joined, left = index_drift(committed={"AAPL", "MSFT"},
                                   live={"AAPL", "MSFT", "MRVL", "FERG"})
        self.assertEqual(joined, ["FERG", "MRVL"])
        self.assertEqual(left, [])

    def test_left_the_index_is_reported(self) -> None:
        """Removals matter just as much: unrecorded, the company disappears from the
        universe instead of becoming a former member."""
        from scripts.check_sp500_drift import index_drift

        joined, left = index_drift(committed={"AAPL", "EA"}, live={"AAPL"})
        self.assertEqual(joined, [])
        self.assertEqual(left, ["EA"])

    def test_no_drift(self) -> None:
        from scripts.check_sp500_drift import index_drift

        self.assertEqual(index_drift({"AAPL"}, {"AAPL"}), ([], []))


class TestDelistedButActive(unittest.TestCase):
    def test_reports_a_symbol_the_vendor_stopped_updating(self) -> None:
        """The EA case -- acquired, still fetched daily, returning nothing."""
        from scripts.check_sp500_drift import delisted_but_active

        rows = [{"dataSymbol": "AAPL", "active": "true"},
                {"dataSymbol": "EA", "active": "true"}]
        out = delisted_but_active(rows, {"AAPL": "2026-08-05", "EA": "2026-08-04"},
                                  as_of="2026-08-05")
        self.assertEqual(out, ["EA"])

    def test_already_inactive_is_not_reported(self) -> None:
        from scripts.check_sp500_drift import delisted_but_active

        rows = [{"dataSymbol": "EA", "active": "false"}]
        self.assertEqual(
            delisted_but_active(rows, {"EA": "2026-08-04"}, as_of="2026-08-05"), [])

    def test_unknown_ticker_is_not_called_delisted(self) -> None:
        """Fail open. A missing manifest entry must never be read as 'delisted' --
        that would silently drop a live symbol from the daily run."""
        from scripts.check_sp500_drift import delisted_but_active

        rows = [{"dataSymbol": "NEWCO", "active": "true"}]
        self.assertEqual(delisted_but_active(rows, {}, as_of="2026-08-05"), [])

    def test_missing_as_of_reports_nothing(self) -> None:
        """Without a reference day there is no way to tell stale from current."""
        from scripts.check_sp500_drift import delisted_but_active

        rows = [{"dataSymbol": "EA", "active": "true"}]
        self.assertEqual(delisted_but_active(rows, {"EA": "2026-08-04"}, as_of=None), [])


class TestTiingoTicker(unittest.TestCase):
    def test_normalisation(self) -> None:
        from scripts.check_sp500_drift import tiingo_ticker

        self.assertEqual(tiingo_ticker("BRK.B"), "BRK-B")
        self.assertEqual(tiingo_ticker(" aapl "), "AAPL")
        self.assertEqual(tiingo_ticker("RVTY (Previously PKI)"), "RVTY")


if __name__ == "__main__":
    unittest.main()
