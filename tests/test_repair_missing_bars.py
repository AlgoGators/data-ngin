import unittest
from datetime import date
from unittest.mock import MagicMock


class TestGroupHoles(unittest.TestCase):
    """A contiguous run of holes must collapse to a single fetch span."""

    def test_groups_contiguous_run_into_one_span(self) -> None:
        from scripts.repair_missing_bars import group_holes_by_symbol

        holes = [("SATS", date(2026, 7, 9)), ("SATS", date(2026, 7, 10)),
                 ("SATS", date(2026, 7, 28)), ("F", date(2026, 7, 29))]

        grouped = group_holes_by_symbol(holes)

        self.assertEqual(grouped["SATS"], (date(2026, 7, 9), date(2026, 7, 28)))
        self.assertEqual(grouped["F"], (date(2026, 7, 29), date(2026, 7, 29)))

    def test_empty_input(self) -> None:
        from scripts.repair_missing_bars import group_holes_by_symbol

        self.assertEqual(group_holes_by_symbol([]), {})


class TestRecordAbsent(unittest.TestCase):
    """Vendor-confirmed absences must be cached so they are never re-probed."""

    def test_record_absent_inserts_one_row_per_day(self) -> None:
        from scripts.repair_missing_bars import record_absent

        inserter = MagicMock()
        record_absent(inserter, "equities_data", "WELL",
                      [date(2003, 11, 7), date(2003, 11, 10)], note="vendor has no bar")

        inserter.insert_data.assert_called_once()
        kwargs = inserter.insert_data.call_args.kwargs
        self.assertEqual(kwargs["table"], "verified_absent_bars")
        self.assertEqual(len(kwargs["data"]), 2)
        self.assertEqual(kwargs["data"][0]["symbol"], "WELL")
        self.assertEqual(kwargs["data"][0]["bar_date"], date(2003, 11, 7))

    def test_record_absent_with_no_days_does_nothing(self) -> None:
        from scripts.repair_missing_bars import record_absent

        inserter = MagicMock()
        record_absent(inserter, "equities_data", "WELL", [], note="n/a")
        inserter.insert_data.assert_not_called()


if __name__ == "__main__":
    unittest.main()
