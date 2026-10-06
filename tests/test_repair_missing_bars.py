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
            # Must carry the day GOOD is missing: absence is now derived from the RAW
            # response, so an empty frame here would mean "vendor has no bar".
            return pd.DataFrame({"time": ["2024-01-02T00:00:00.000Z"]})

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


CONFIG = {"database": {"target_schema": "equities_data", "table": "equities"}}


def run_repair(holes, fetch_side_effect, clean_side_effect):
    """Run repair() with every collaborator mocked. No DB, no vendor call.

    Returns (summary, mock_inserter) so tests can assert on both the summary and
    exactly what was written to verified_absent_bars.
    """
    mock_dq = MagicMock()
    mock_dq.find_missing_bars.return_value = holes

    mock_fetcher = MagicMock()
    mock_fetcher.fetch_data = AsyncMock(side_effect=fetch_side_effect)

    mock_cleaner = MagicMock()
    mock_cleaner.clean.side_effect = clean_side_effect

    mock_inserter = MagicMock()
    instances = {"fetcher": mock_fetcher, "cleaner": mock_cleaner, "inserter": mock_inserter}

    with patch("src.modules.data_quality.DataQuality", return_value=mock_dq), \
         patch("utils.dynamic_loader.get_instance",
               side_effect=lambda config, module_key, class_key: instances[module_key]):
        from scripts.repair_missing_bars import repair
        summary = asyncio.run(repair(CONFIG, dry_run=False))
    return summary, mock_inserter


def absent_writes(mock_inserter):
    """Every row written to verified_absent_bars across the run."""
    rows = []
    for call in mock_inserter.insert_data.call_args_list:
        if call.kwargs.get("table") == "verified_absent_bars":
            rows.extend(call.kwargs["data"])
    return rows


class TestReturnedDaysComeFromRawNotCleaned(unittest.TestCase):
    """Absence must be decided by what the VENDOR returned, not by what survived
    the cleaner. The cleaner drops rows for non-positive prices, bad volumes,
    unparseable timestamps and -- because config sets drop_nan: "True" -- any NaN
    anywhere in the row. Caching such a day as "vendor returned no bar" would write
    a REAL gap into verified_absent_bars, hiding it from detection forever."""

    def test_cleaner_dropped_day_is_not_recorded_absent_and_is_counted(self) -> None:
        # Vendor returned both days; the cleaner keeps only 1/2 (say 1/3 had a NaN).
        holes = [("WELL", date(2024, 1, 2)), ("WELL", date(2024, 1, 3))]

        async def fetch(symbol, loaded_asset_type, start_date, end_date):
            return pd.DataFrame({"time": ["2024-01-02T00:00:00.000Z",
                                          "2024-01-03T00:00:00.000Z"]})

        def clean(raw):
            return [{"symbol": "WELL", "time": pd.Timestamp(date(2024, 1, 2))}]

        summary, inserter = run_repair(holes, fetch, clean)

        self.assertEqual(summary["refilled"], 1)
        self.assertEqual(summary["dropped_by_cleaner"], 1)
        # The critical assertion: the dropped day was NOT cached as vendor-absent.
        self.assertEqual(summary["absent"], 0)
        self.assertEqual(absent_writes(inserter), [])
        self.assertEqual(summary["failed"], [])

    def test_raw_iso_strings_are_parsed_not_assumed_to_be_timestamps(self) -> None:
        """raw["time"] holds Tiingo ISO-8601 strings; treating them as parsed
        timestamps would raise and turn a healthy symbol into a failure."""
        from scripts.repair_missing_bars import returned_days_from_raw

        raw = pd.DataFrame({"time": ["2024-01-02T00:00:00.000Z", "2024-01-03T00:00:00.000Z"]})
        days, unparseable = returned_days_from_raw(raw)
        self.assertEqual(days, {date(2024, 1, 2), date(2024, 1, 3)})
        self.assertEqual(unparseable, 0)

    def test_empty_dataframe_yields_no_days_without_raising(self) -> None:
        from scripts.repair_missing_bars import returned_days_from_raw

        for empty in (pd.DataFrame(), pd.DataFrame({"time": []}), None):
            self.assertEqual(returned_days_from_raw(empty), (set(), 0))

    def test_unparseable_timestamp_is_counted_not_silently_dropped(self) -> None:
        """A garbage date is a bar the vendor RETURNED but we cannot place in time.
        Dropping it silently would let that day fall into `missing` and be cached as
        'vendor returned no bar' -- permanently hiding a real gap."""
        from scripts.repair_missing_bars import returned_days_from_raw

        raw = pd.DataFrame({"time": ["not-a-date", "2024-01-03T00:00:00.000Z"]})
        days, unparseable = returned_days_from_raw(raw)
        self.assertEqual(days, {date(2024, 1, 3)})
        self.assertEqual(unparseable, 1)


class TestEmptyResponseHandling(unittest.TestCase):
    """A whole-span empty response over multiple days is a failure, not an absence:
    it is indistinguishable from a delisted ticker, a revoked key or a vendor
    outage, and caching it would permanently hide every real bar in the span."""

    def test_multi_day_empty_response_fails_and_caches_nothing(self) -> None:
        holes = [("SATS", date(2024, 1, 2)), ("SATS", date(2024, 1, 3)),
                 ("SATS", date(2024, 1, 4))]

        async def fetch(symbol, loaded_asset_type, start_date, end_date):
            return pd.DataFrame()

        def clean(raw):
            raise AssertionError("clean must not be reached for a multi-day blackout")

        summary, inserter = run_repair(holes, fetch, clean)

        self.assertEqual(summary["failed"], ["SATS"])
        self.assertEqual(summary["absent"], 0)
        self.assertEqual(summary["refilled"], 0)
        self.assertEqual(absent_writes(inserter), [])
        inserter.insert_data.assert_not_called()

    def test_single_day_empty_response_is_recorded_absent(self) -> None:
        # The normal case for half of all candidates -- must stay working.
        holes = [("WELL", date(2003, 11, 7))]

        async def fetch(symbol, loaded_asset_type, start_date, end_date):
            return pd.DataFrame()

        def clean(raw):
            return []

        summary, inserter = run_repair(holes, fetch, clean)

        self.assertEqual(summary["absent"], 1)
        self.assertEqual(summary["failed"], [])
        self.assertEqual(summary["dropped_by_cleaner"], 0)
        rows = absent_writes(inserter)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "WELL")
        self.assertEqual(rows[0]["bar_date"], date(2003, 11, 7))


class TestPerSymbolErrorsDoNotAbortLoop(unittest.TestCase):
    """Symbols are processed in sorted order, so an exception from clean or insert
    would otherwise let one bad symbol early in the alphabet block every later one
    and skip the summary and exit-code logic entirely."""

    def _holes(self):
        return [("AAA", date(2024, 1, 2)), ("ZZZ", date(2024, 1, 2))]

    @staticmethod
    async def _fetch(symbol, loaded_asset_type, start_date, end_date):
        return pd.DataFrame({"time": ["2024-01-02T00:00:00.000Z"]})

    def test_clean_raising_appends_to_failed_and_continues(self) -> None:
        def clean(raw):
            # First call is AAA (sorted order), second is ZZZ.
            if clean.calls == 0:
                clean.calls += 1
                raise ValueError("missing required field 'close'")
            clean.calls += 1
            return [{"symbol": "ZZZ", "time": pd.Timestamp(date(2024, 1, 2))}]
        clean.calls = 0

        summary, inserter = run_repair(self._holes(), self._fetch, clean)

        self.assertEqual(summary["failed"], ["AAA"])
        self.assertEqual(summary["refilled"], 1)      # ZZZ still processed
        self.assertEqual(summary["symbols"], 2)
        inserter.close.assert_called_once()

    def test_insert_raising_appends_to_failed_and_continues(self) -> None:
        mock_dq = MagicMock()
        mock_dq.find_missing_bars.return_value = self._holes()

        mock_fetcher = MagicMock()
        mock_fetcher.fetch_data = AsyncMock(side_effect=self._fetch)

        mock_cleaner = MagicMock()
        mock_cleaner.clean.side_effect = lambda raw: [
            {"symbol": "X", "time": pd.Timestamp(date(2024, 1, 2))}
        ]

        mock_inserter = MagicMock()
        calls = {"n": 0}

        def insert_data(data, schema, table):
            calls["n"] += 1
            if calls["n"] == 1:                        # AAA's equities insert
                raise RuntimeError("insert failed")

        mock_inserter.insert_data.side_effect = insert_data
        instances = {"fetcher": mock_fetcher, "cleaner": mock_cleaner, "inserter": mock_inserter}

        with patch("src.modules.data_quality.DataQuality", return_value=mock_dq), \
             patch("utils.dynamic_loader.get_instance",
                   side_effect=lambda config, module_key, class_key: instances[module_key]):
            from scripts.repair_missing_bars import repair
            summary = asyncio.run(repair(CONFIG, dry_run=False))

        self.assertEqual(summary["failed"], ["AAA"])
        self.assertEqual(summary["refilled"], 1)       # ZZZ still processed
        mock_inserter.close.assert_called_once()


class TestDryRunSummary(unittest.TestCase):
    def test_dry_run_reports_scope_without_touching_vendor(self) -> None:
        from scripts.repair_missing_bars import repair

        mock_dq = MagicMock()
        mock_dq.find_missing_bars.return_value = [("SATS", date(2024, 1, 2))]

        with patch("src.modules.data_quality.DataQuality", return_value=mock_dq), \
             patch("utils.dynamic_loader.get_instance") as get_instance:
            summary = asyncio.run(repair(CONFIG, dry_run=True))

        get_instance.assert_not_called()
        self.assertEqual(summary["candidates"], 1)
        self.assertEqual(summary["dropped_by_cleaner"], 0)
        self.assertEqual(summary["failed"], [])


if __name__ == "__main__":
    unittest.main()


class TestUnparseableTimestampFailsRatherThanCaching(unittest.TestCase):
    def test_symbol_with_a_bad_timestamp_fails_and_caches_nothing(self) -> None:
        holes = [("WELL", date(2024, 1, 2)), ("WELL", date(2024, 1, 3))]

        async def fetch(symbol, loaded_asset_type, start_date, end_date):
            return pd.DataFrame({"time": ["garbage", "2024-01-03T00:00:00.000Z"]})

        def clean(raw):
            raise AssertionError("clean must not be reached when timestamps are unparseable")

        summary, inserter = run_repair(holes, fetch, clean)

        self.assertEqual(summary["failed"], ["WELL"])
        self.assertEqual(summary["absent"], 0)
        self.assertEqual(absent_writes(inserter), [],
                         "a returned-but-unplaceable day must never be cached as absent")


class TestMultiDayAbsenceOverride(unittest.TestCase):
    """The default refuses to cache a wholly-empty multi-day span, because it cannot
    be undone. The override exists for a confirmed genuine halt."""

    @staticmethod
    async def _empty(symbol, loaded_asset_type, start_date, end_date):
        return pd.DataFrame()

    def _run(self, allow):
        from scripts.repair_missing_bars import repair
        holes = [("HALT", date(2024, 1, 2)), ("HALT", date(2024, 1, 3))]
        mock_dq = MagicMock(); mock_dq.find_missing_bars.return_value = holes
        fetcher = MagicMock(); fetcher.fetch_data = AsyncMock(side_effect=self._empty)
        cleaner = MagicMock(); cleaner.clean.return_value = []
        inserter = MagicMock()
        inst = {"fetcher": fetcher, "cleaner": cleaner, "inserter": inserter}
        with patch("src.modules.data_quality.DataQuality", return_value=mock_dq), \
             patch("utils.dynamic_loader.get_instance",
                   side_effect=lambda c, k, ck: inst[k]):
            return asyncio.run(repair(CONFIG, allow_multi_day_absence=allow)), inserter

    def test_default_refuses(self) -> None:
        summary, inserter = self._run(allow=False)
        self.assertEqual(summary["failed"], ["HALT"])
        self.assertEqual(absent_writes(inserter), [])

    def test_override_caches_the_absence(self) -> None:
        summary, inserter = self._run(allow=True)
        self.assertEqual(summary["failed"], [])
        self.assertEqual(summary["absent"], 2)
        self.assertEqual(len(absent_writes(inserter)), 2)


class TestRawTableIsKeptConsistent(unittest.TestCase):
    """The daily orchestrator writes both equities_raw and equities. Repairing only
    the clean table leaves the repaired bars with no raw provenance and the two
    tables disagreeing for those dates."""

    @staticmethod
    async def _fetch(symbol, loaded_asset_type, start_date, end_date):
        return pd.DataFrame({"time": ["2024-01-02T00:00:00.000Z"]})

    def _run(self, config):
        from scripts.repair_missing_bars import repair
        mock_dq = MagicMock()
        mock_dq.find_missing_bars.return_value = [("GOOD", date(2024, 1, 2))]
        fetcher = MagicMock(); fetcher.fetch_data = AsyncMock(side_effect=self._fetch)
        cleaner = MagicMock()
        cleaner.clean.return_value = [{"symbol": "GOOD", "time": pd.Timestamp(date(2024, 1, 2))}]
        inserter = MagicMock()
        inst = {"fetcher": fetcher, "cleaner": cleaner, "inserter": inserter}
        with patch("src.modules.data_quality.DataQuality", return_value=mock_dq), \
             patch("utils.dynamic_loader.get_instance",
                   side_effect=lambda c, k, ck: inst[k]):
            asyncio.run(repair(config))
        return inserter

    def test_raw_table_written_when_configured(self) -> None:
        inserter = self._run({"database": {"target_schema": "equities_data",
                                           "table": "ohlcv_1d",
                                           "raw_table": "ohlcv_1d_raw"}})
        tables = [c.kwargs.get("table") for c in inserter.insert_data.call_args_list]
        self.assertIn("ohlcv_1d_raw", tables)
        self.assertIn("ohlcv_1d", tables)

    def test_no_raw_table_configured_is_not_an_error(self) -> None:
        """The futures configs have no raw_table; absence must not break the run."""
        inserter = self._run({"database": {"target_schema": "equities_data",
                                           "table": "ohlcv_1d"}})
        tables = [c.kwargs.get("table") for c in inserter.insert_data.call_args_list]
        self.assertIn("ohlcv_1d", tables)
        self.assertNotIn(None, [t for t in tables if t == "ohlcv_1d_raw"])
