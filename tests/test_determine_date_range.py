import unittest
from unittest.mock import patch

from utils.dynamic_loader import determine_date_range


def make_config(start="", end="", seed=None, schema="equities_data", table="equities"):
    tr = {"start_date": start, "end_date": end}
    if seed is not None:
        tr["seed_start_date"] = seed
    return {
        "time_range": tr,
        "database": {"target_schema": schema, "table": table, "db_name": "testdb"},
    }


class TestDetermineDateRange(unittest.TestCase):
    """Covers the incremental date logic without touching a real DB (DataAccess mocked)."""

    @patch("utils.dynamic_loader.DataAccess")
    def test_explicit_start_unaffected(self, MockDA):
        """Non-blank start_date (how the futures pipelines run) => used verbatim, no DB query."""
        cfg = make_config(start="2026-02-03", end="2026-02-28")
        s, e = determine_date_range(cfg)
        self.assertEqual((s, e), ("2026-02-03", "2026-02-28"))
        MockDA.return_value.get_latest_date_for.assert_not_called()

    @patch("utils.dynamic_loader.DataAccess")
    def test_incremental_with_existing_data(self, MockDA):
        """Blank start + table has data => start = latest + 1 day, from the config's own table."""
        MockDA.return_value.get_latest_date_for.return_value = "2026-06-01"
        cfg = make_config(start="", end="2026-06-30", seed="2026-01-01")
        s, e = determine_date_range(cfg)
        self.assertEqual(s, "2026-06-02")
        self.assertEqual(e, "2026-06-30")
        # confirms it queried the equities table, not the futures one
        MockDA.return_value.get_latest_date_for.assert_called_once_with("equities_data", "equities")

    @patch("utils.dynamic_loader.DataAccess")
    def test_incremental_empty_table_uses_seed(self, MockDA):
        """Blank start + empty table => fall back to seed_start_date (first run)."""
        MockDA.return_value.get_latest_date_for.return_value = None
        cfg = make_config(start="", end="2026-06-30", seed="2026-01-01")
        s, _ = determine_date_range(cfg)
        self.assertEqual(s, "2026-01-01")

    @patch("utils.dynamic_loader.DataAccess")
    def test_clamp_when_caught_up(self, MockDA):
        """If latest+1 would exceed end, clamp start to end (no inverted range)."""
        MockDA.return_value.get_latest_date_for.return_value = "2026-06-30"
        cfg = make_config(start="", end="2026-06-15", seed="2026-01-01")
        s, e = determine_date_range(cfg)
        self.assertEqual(s, "2026-06-15")
        self.assertEqual(e, "2026-06-15")

    @patch("utils.dynamic_loader.DataAccess")
    def test_empty_table_no_seed_raises(self, MockDA):
        """Blank start, empty table, no seed => clear error rather than silent bad range."""
        MockDA.return_value.get_latest_date_for.return_value = None
        cfg = make_config(start="", end="2026-06-30")
        with self.assertRaises(ValueError):
            determine_date_range(cfg)


if __name__ == "__main__":
    unittest.main()
