import unittest
from datetime import date, datetime, timedelta, timezone

from scripts.check_data_freshness import find_stale_tables


class TestFindStaleTables(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)  # a Saturday

    def test_fresh_table_is_not_stale(self):
        latest = {"futures_data.ohlcv_1d": self.now - timedelta(hours=2)}
        self.assertEqual(find_stale_tables(latest, self.now, threshold_hours=72), [])

    def test_missing_rows_is_stale(self):
        latest = {"futures_data.ohlcv_1d": None}
        stale = find_stale_tables(latest, self.now, threshold_hours=72)
        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0][0], "futures_data.ohlcv_1d")
        self.assertIn("no rows", stale[0][1])

    def test_normal_weekend_gap_is_not_stale(self):
        # Friday close ~60h before a Saturday-morning check -- must NOT alarm.
        latest = {"equities_data.ohlcv_1d": self.now - timedelta(hours=60)}
        self.assertEqual(find_stale_tables(latest, self.now, threshold_hours=72), [])

    def test_genuine_multi_day_outage_is_stale(self):
        latest = {"equities_data.ohlcv_1d": self.now - timedelta(hours=100)}
        stale = find_stale_tables(latest, self.now, threshold_hours=72)
        self.assertEqual(len(stale), 1)
        self.assertIn("4 days, 4:00:00", stale[0][1])

    def test_naive_timestamp_is_treated_as_utc(self):
        naive = (self.now - timedelta(hours=100)).replace(tzinfo=None)
        latest = {"futures_data.ohlcv_1d": naive}
        stale = find_stale_tables(latest, self.now, threshold_hours=72)
        self.assertEqual(len(stale), 1)

    def test_plain_date_column_is_handled(self):
        # equities_data.ohlcv_1d.date is a plain DATE column (no time-of-day),
        # confirmed against the live schema -- must not raise AttributeError.
        fresh_date = (self.now - timedelta(hours=2)).date()
        latest = {"equities_data.ohlcv_1d": fresh_date}
        self.assertEqual(find_stale_tables(latest, self.now, threshold_hours=72), [])

        stale_date = date(2026, 7, 1)
        latest = {"equities_data.ohlcv_1d": stale_date}
        stale = find_stale_tables(latest, self.now, threshold_hours=72)
        self.assertEqual(len(stale), 1)


if __name__ == "__main__":
    unittest.main()
