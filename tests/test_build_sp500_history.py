import unittest
from datetime import date


class TestBuildMembership(unittest.TestCase):
    """
    The reconstruction replays index changes BACKWARD from today's known members.
    These tests pin the four shapes that actually occur in the source data.
    """

    def test_untouched_current_member_spans_all_of_history(self) -> None:
        from scripts.build_sp500_history import build_membership

        intervals, warnings = build_membership({"AAPL"}, [])

        self.assertEqual(intervals, [{"ticker": "AAPL", "start_date": None, "end_date": None}])
        self.assertEqual(warnings, [])

    def test_added_ticker_gets_start_date_and_stays_open(self) -> None:
        from scripts.build_sp500_history import build_membership

        intervals, warnings = build_membership(
            {"MRVL"}, [(date(2026, 6, 22), "MRVL", "POOL")]
        )

        by = {r["ticker"]: r for r in intervals}
        self.assertEqual(by["MRVL"]["start_date"], "2026-06-22")
        self.assertIsNone(by["MRVL"]["end_date"], "MRVL is still a member")
        self.assertEqual(warnings, [])

    def test_removed_ticker_becomes_a_closed_interval(self) -> None:
        from scripts.build_sp500_history import build_membership

        intervals, _ = build_membership(
            {"MRVL"}, [(date(2026, 6, 22), "MRVL", "POOL")]
        )

        by = {r["ticker"]: r for r in intervals}
        self.assertIsNone(by["POOL"]["start_date"], "POOL predates the changes history")
        self.assertEqual(by["POOL"]["end_date"], "2026-06-22")

    def test_removed_then_readded_produces_two_intervals(self) -> None:
        """A company that leaves and later rejoins must NOT be collapsed into one
        continuous span -- a backtest would otherwise hold it during years it was
        out of the index, which is look-ahead bias."""
        from scripts.build_sp500_history import build_membership

        intervals, warnings = build_membership(
            {"X"},
            [(date(2020, 1, 1), "X", None), (date(2010, 1, 1), None, "X")],
        )

        xs = sorted([r for r in intervals if r["ticker"] == "X"],
                    key=lambda r: r["start_date"] or "")
        self.assertEqual(len(xs), 2)
        self.assertEqual((xs[0]["start_date"], xs[0]["end_date"]), (None, "2010-01-01"))
        self.assertEqual((xs[1]["start_date"], xs[1]["end_date"]), ("2020-01-01", None))
        self.assertEqual(warnings, [])

    def test_one_sided_events_are_handled(self) -> None:
        """Spin-offs and market-cap deletions are recorded with only one side."""
        from scripts.build_sp500_history import build_membership

        intervals, warnings = build_membership(
            {"HONA"},
            [(date(2026, 6, 29), "HONA", None), (date(2026, 6, 30), None, "CAG")],
        )

        by = {r["ticker"]: r for r in intervals}
        self.assertEqual(by["HONA"]["start_date"], "2026-06-29")
        self.assertEqual(by["CAG"]["end_date"], "2026-06-30")
        self.assertEqual(warnings, [])

    def test_inconsistent_source_warns_instead_of_silently_dropping(self) -> None:
        """An add with no corresponding later membership is a source defect. It must
        surface as a warning -- silently discarding it would understate the universe
        and nobody would know."""
        from scripts.build_sp500_history import build_membership

        intervals, warnings = build_membership({}, [(date(2020, 1, 1), "GHOST", None)])

        self.assertEqual(intervals, [])
        self.assertEqual(len(warnings), 1)
        self.assertIn("GHOST", warnings[0])


class TestFormerMembers(unittest.TestCase):
    def test_open_interval_means_current_not_former(self) -> None:
        from scripts.build_sp500_history import former_members

        rows = [{"ticker": "AAPL", "start_date": None, "end_date": None}]
        self.assertEqual(former_members(rows), [])

    def test_closed_interval_only_means_former(self) -> None:
        from scripts.build_sp500_history import former_members

        rows = [{"ticker": "WBA", "start_date": None, "end_date": "2025-08-28"}]
        self.assertEqual(former_members(rows), ["WBA"])

    def test_left_then_rejoined_is_not_former(self) -> None:
        from scripts.build_sp500_history import former_members

        rows = [
            {"ticker": "X", "start_date": None, "end_date": "2010-01-01"},
            {"ticker": "X", "start_date": "2020-01-01", "end_date": None},
        ]
        self.assertEqual(former_members(rows), [],
                         "a rejoined company is a current member, not a former one")


class TestTiingoTicker(unittest.TestCase):
    def test_share_class_dot_becomes_dash(self) -> None:
        from scripts.build_sp500_history import tiingo_ticker

        self.assertEqual(tiingo_ticker("BRK.B"), "BRK-B")
        self.assertEqual(tiingo_ticker(" brk.b "), "BRK-B")
        self.assertEqual(tiingo_ticker("AAPL"), "AAPL")

    def test_editorial_annotation_is_stripped(self) -> None:
        """'RVTY (PREVIOUSLY PKI)' is a real value in the upstream point-in-time
        data. Left alone it becomes a phantom former member that can never be
        fetched and permanently inflates the documented coverage gap."""
        from scripts.build_sp500_history import tiingo_ticker

        self.assertEqual(tiingo_ticker("RVTY (Previously PKI)"), "RVTY")
        self.assertEqual(tiingo_ticker("RVTY (PREVIOUSLY PKI)"), "RVTY")


class TestIsValidTicker(unittest.TestCase):
    def test_accepts_real_listings(self) -> None:
        from scripts.build_sp500_history import is_valid_ticker

        for t in ["A", "AAPL", "BRK-B", "GOOGL", "ABCDEFG"]:
            self.assertTrue(is_valid_ticker(t), t)

    def test_rejects_junk(self) -> None:
        from scripts.build_sp500_history import is_valid_ticker

        for t in ["", "RVTY (PREVIOUSLY PKI)", "TOO LONG", "lower", "TOOMANYCHARS", "1ABC"]:
            self.assertFalse(is_valid_ticker(t), t)


if __name__ == "__main__":
    unittest.main()
