import asyncio
import unittest
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd


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

    def test_groups_unsorted_dates_by_true_min_and_max(self) -> None:
        from scripts.repair_missing_bars import group_holes_by_symbol

        # Deliberately unsorted input: true min (7/9) is not first, true max (7/28) is not last
        holes = [("SATS", date(2026, 7, 28)), ("SATS", date(2026, 7, 9)),
                 ("SATS", date(2026, 7, 15)), ("OTHER", date(2026, 8, 1)),
                 ("OTHER", date(2026, 7, 20))]

        grouped = group_holes_by_symbol(holes)

        # Must compute true min/max regardless of input order
        self.assertEqual(grouped["SATS"], (date(2026, 7, 9), date(2026, 7, 28)))
        self.assertEqual(grouped["OTHER"], (date(2026, 7, 20), date(2026, 8, 1)))


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


class TestRepairFailurePath(unittest.TestCase):
    """A vendor fetch failure for one symbol must show up in the summary's 'failed'
    key (not be silently absorbed into a summary that looks like a clean run), and
    must not prevent other symbols in the same run from being repaired."""

    def test_failed_symbol_reported_and_other_symbol_still_repaired(self) -> None:
        from scripts.repair_missing_bars import repair

        holes = [("BAD", date(2024, 1, 1)), ("GOOD", date(2024, 1, 2))]

        mock_dq = MagicMock()
        mock_dq.find_missing_bars.return_value = holes

        async def fake_fetch_data(symbol, loaded_asset_type, start_date, end_date):
            if symbol == "BAD":
                raise RuntimeError("simulated vendor outage")
            return pd.DataFrame()  # content is irrelevant; cleaner.clean is mocked below

        mock_fetcher = MagicMock()
        mock_fetcher.fetch_data = AsyncMock(side_effect=fake_fetch_data)

        mock_cleaner = MagicMock()
        mock_cleaner.clean.return_value = [
            {"symbol": "GOOD", "time": pd.Timestamp(date(2024, 1, 2))}
        ]

        mock_inserter = MagicMock()

        instances = {"fetcher": mock_fetcher, "cleaner": mock_cleaner, "inserter": mock_inserter}

        def fake_get_instance(config, module_key, class_key):
            return instances[module_key]

        config = {"database": {"target_schema": "equities_data", "table": "equities"}}

        with patch("src.modules.data_quality.DataQuality", return_value=mock_dq), \
             patch("utils.dynamic_loader.get_instance", side_effect=fake_get_instance):
            summary = asyncio.run(repair(config, dry_run=False))

        # BAD is named explicitly in the summary -- not silently dropped.
        self.assertEqual(summary["failed"], ["BAD"])
        # symbols reflects total scope (2), not just the ones that succeeded.
        self.assertEqual(summary["symbols"], 2)
        self.assertEqual(summary["candidates"], 2)
        # GOOD was still fetched, cleaned, and inserted despite BAD's failure.
        self.assertEqual(summary["refilled"], 1)
        mock_inserter.insert_data.assert_called_once_with(
            data=mock_cleaner.clean.return_value,
            schema="equities_data",
            table="equities",
        )
        mock_inserter.connect.assert_called_once()
        mock_inserter.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
