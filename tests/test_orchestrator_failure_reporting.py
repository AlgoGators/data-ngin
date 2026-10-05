"""Offline regressions for failures previously reported as successful runs."""

import asyncio
import logging
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd

from src.orchestrator import Orchestrator


class TestFailureReporting(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Bypass configuration/DB construction; exercise real orchestration with
        # fake provider and database boundaries, without credentials or writes.
        self.pipeline = Orchestrator.__new__(Orchestrator)
        self.pipeline.config = {
            "batch_downloading": {"batch": False},
            "database": {
                "target_schema": "futures_data",
                "raw_table": "ohlcv_1d_raw",
                "table": "ohlcv_1d",
            },
        }
        self.pipeline.loader = MagicMock()
        self.pipeline.loader.load_symbols.return_value = {"ES": "FUTURE", "NQ": "FUTURE"}
        self.pipeline.fetcher = MagicMock()
        self.pipeline.fetcher.fetch_data = AsyncMock(
            return_value=pd.DataFrame([{"close": 100}])
        )
        self.pipeline.cleaner = MagicMock()
        self.pipeline.cleaner.clean.return_value = [{"close": 100}]
        self.pipeline.inserter = MagicMock()
        dates = patch("src.orchestrator.determine_date_range", return_value=("2026-08-07", "2026-08-07"))
        dates.start()
        self.addCleanup(dates.stop)

    async def test_all_success(self):
        with self.assertLogs(level=logging.INFO) as logs:
            await self.pipeline.run()
        self.assertEqual(self.pipeline.inserter.insert_data.call_count, 4)
        self.assertTrue(any("completed successfully" in line for line in logs.output))

    async def test_partial_failure_waits_for_successful_symbol(self):
        async def fetch(**kwargs):
            if kwargs["symbol"] == "ES":
                raise ValueError("DBN decoder mismatch")
            await asyncio.sleep(0.01)
            return pd.DataFrame([{"close": 100}])

        self.pipeline.fetcher.fetch_data.side_effect = fetch
        with self.assertLogs(level=logging.INFO) as logs:
            with self.assertRaisesRegex(RuntimeError, "1/2 symbol.*ES: DBN decoder mismatch"):
                await self.pipeline.run()
        self.assertEqual(self.pipeline.inserter.insert_data.call_count, 2)
        self.assertEqual(self.pipeline.inserter.close.call_count, 2)
        self.assertFalse(any("completed successfully" in line for line in logs.output))

    async def test_all_symbols_fail(self):
        self.pipeline.fetcher.fetch_data.side_effect = ValueError("DBN decoder mismatch")
        with self.assertLogs(level=logging.INFO) as logs:
            with self.assertRaisesRegex(RuntimeError, "2/2 symbol") as error:
                await self.pipeline.run()
        self.assertIn("ES: DBN decoder mismatch", str(error.exception))
        self.assertIn("NQ: DBN decoder mismatch", str(error.exception))
        self.pipeline.inserter.insert_data.assert_not_called()
        self.assertFalse(any("completed successfully" in line for line in logs.output))

    async def test_cleaning_and_insertion_errors_propagate(self):
        for stage in (self.pipeline.cleaner.clean, self.pipeline.inserter.insert_data):
            with self.subTest(stage=stage):
                stage.side_effect = RuntimeError("processing failed")
                with self.assertLogs(level=logging.ERROR):
                    with self.assertRaisesRegex(RuntimeError, "2/2 symbol"):
                        await self.pipeline.run()
                stage.side_effect = None

    async def test_direct_symbol_call_raises_and_closes(self):
        self.pipeline.fetcher.fetch_data.side_effect = ValueError("decode failed")
        with self.assertLogs(level=logging.ERROR):
            with self.assertRaisesRegex(ValueError, "decode failed"):
                await self.pipeline.retrieve_and_process_data(
                    {"dataSymbol": "ES", "instrumentType": "FUTURE"},
                    "2026-08-07", "2026-08-07",
                )
        self.pipeline.inserter.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
