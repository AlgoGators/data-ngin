import unittest
from datetime import date


class TestClassify(unittest.TestCase):
    """
    `classify` decides whether the security Tiingo returns for a ticker really is
    the company that was in the index. Getting this wrong writes a different
    company's prices under a historical symbol -- fabricated history, which is
    worse than the survivorship bias the project removes.
    """

    def test_unknown_ticker(self) -> None:
        from scripts.validate_sp500_entities import classify, NOT_FOUND

        v, _ = classify(date(2000, 1, 1), date(2010, 1, 1), None, None, 1, "NOT_FOUND")
        self.assertEqual(v, NOT_FOUND)

    def test_known_ticker_with_no_price_history(self) -> None:
        from scripts.validate_sp500_entities import classify, NO_PRICE_DATA

        v, _ = classify(date(2000, 1, 1), date(2010, 1, 1), None, None, 1, "AVAILABLE")
        self.assertEqual(v, NO_PRICE_DATA)

    def test_coverage_starting_after_removal_is_a_different_company(self) -> None:
        """Rule 1. CA: Tiingo returns a municipal bond ETF starting 2023, but the
        index member (Computer Associates) left in 2018."""
        from scripts.validate_sp500_entities import classify, NO_COVERAGE

        v, reason = classify(date(2000, 1, 3), date(2018, 10, 31),
                             date(2023, 12, 14), date(2026, 8, 4), 1, "AVAILABLE")
        self.assertEqual(v, NO_COVERAGE)
        self.assertIn("2023-12-14", reason)

    def test_reused_ticker_is_caught_even_when_dates_overlap(self) -> None:
        """Rule 2, and the whole reason it exists.

        CSR: Tiingo covers 1997-2026 and the index membership ended 2000-06-15, so
        the windows overlap by years and rule 1 passes it. But Tiingo's security is
        Centerspace, not the S&P member that held CSR in 2000. Only the manifest
        duplicate count reveals that. Rule 1 alone is not safe.
        """
        from scripts.validate_sp500_entities import classify, REUSED_TICKER, OK

        args = (date(2000, 1, 3), date(2000, 6, 15), date(1997, 10, 17), date(2026, 8, 4))

        self.assertEqual(classify(*args, 2, "AVAILABLE")[0], REUSED_TICKER)
        # Same dates, single manifest entry -> rule 1 alone would have said OK,
        # which is exactly the false negative rule 2 closes.
        self.assertEqual(classify(*args, 1, "AVAILABLE")[0], OK)

    def test_ample_overlap_is_usable(self) -> None:
        from scripts.validate_sp500_entities import classify, OK

        v, reason = classify(date(2000, 1, 3), date(2015, 7, 13),
                             date(1985, 2, 25), date(2015, 7, 13), 1, "AVAILABLE")
        self.assertEqual(v, OK)
        self.assertIn("days of overlap", reason)

    def test_sliver_of_overlap_needs_a_human(self) -> None:
        """BRCM: genuinely Broadcom Corp, but Tiingo holds only ~3 weeks of a
        16-year membership. Correct data, near-zero value -- a person should decide."""
        from scripts.validate_sp500_entities import classify, MARGINAL

        v, _ = classify(date(2000, 1, 3), date(2016, 1, 26),
                        date(2016, 1, 4), date(2016, 2, 9), 1, "AVAILABLE")
        self.assertEqual(v, MARGINAL)

    def test_still_a_member_open_end_does_not_crash(self) -> None:
        from scripts.validate_sp500_entities import classify, OK

        v, _ = classify(date(2000, 1, 3), None, date(2000, 6, 30), None, 1, "AVAILABLE")
        self.assertEqual(v, OK)


class TestMembershipWindow(unittest.TestCase):
    def test_single_closed_interval(self) -> None:
        from scripts.validate_sp500_entities import membership_window

        s, e = membership_window([{"start_date": "2015-07-02", "end_date": "2024-07-08"}])
        self.assertEqual((s, e), (date(2015, 7, 2), date(2024, 7, 8)))

    def test_open_start_stays_open(self) -> None:
        """A blank start means 'a member from before our history begins'. Collapsing
        that to a concrete date would make the window look narrower than it is and
        could wrongly reject a valid security."""
        from scripts.validate_sp500_entities import membership_window

        s, e = membership_window([{"start_date": "", "end_date": "2019-12-06"}])
        self.assertIsNone(s)
        self.assertEqual(e, date(2019, 12, 6))

    def test_open_end_means_still_a_member(self) -> None:
        from scripts.validate_sp500_entities import membership_window

        s, e = membership_window([{"start_date": "2026-06-22", "end_date": ""}])
        self.assertEqual(s, date(2026, 6, 22))
        self.assertIsNone(e)

    def test_rejoined_company_uses_outer_bounds(self) -> None:
        """Overlap with EITHER stint proves the security is the right one."""
        from scripts.validate_sp500_entities import membership_window

        s, e = membership_window([
            {"start_date": "2005-01-01", "end_date": "2010-01-01"},
            {"start_date": "2020-01-01", "end_date": "2022-01-01"},
        ])
        self.assertEqual((s, e), (date(2005, 1, 1), date(2022, 1, 1)))


if __name__ == "__main__":
    unittest.main()
