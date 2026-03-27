import unittest
from unittest.mock import patch, MagicMock
from datetime import date

import pandas as pd
import numpy as np

from src.modules.macro.fred_pipeline import (
    SERIES_CONFIG,
    SCHEMA,
    build_table_dataframe,
    upsert_table,
    run_pipeline,
    fetch_series,
)


class TestSeriesConfig(unittest.TestCase):
    """Verify the SERIES_CONFIG mapping is well-formed."""

    def test_all_tables_present(self):
        expected = {"inflation", "growth", "yield_curve", "credit_spreads", "liquidity", "market"}
        self.assertEqual(set(SERIES_CONFIG.keys()), expected)

    def test_gdp_nowcast_is_none(self):
        self.assertIsNone(SERIES_CONFIG["market"]["gdp_nowcast"])

    def test_all_fred_ids_are_strings_or_none(self):
        for table, cols in SERIES_CONFIG.items():
            for col_name, fred_id in cols.items():
                self.assertTrue(
                    fred_id is None or isinstance(fred_id, str),
                    f"{table}.{col_name} has invalid fred_id: {fred_id}",
                )


class TestFetchSeries(unittest.TestCase):
    """Test the fetch_series wrapper."""

    def test_successful_fetch(self):
        mock_fred = MagicMock()
        mock_fred.get_series.return_value = pd.Series(
            [2.5, 2.6], index=pd.to_datetime(["2024-01-01", "2024-02-01"])
        )
        result = fetch_series(mock_fred, "CPIAUCSL", "2000-01-01")
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)

    def test_failed_fetch_returns_none(self):
        mock_fred = MagicMock()
        mock_fred.get_series.side_effect = Exception("API error")
        result = fetch_series(mock_fred, "BADID", "2000-01-01")
        self.assertIsNone(result)


class TestBuildTableDataframe(unittest.TestCase):
    """Test building a merged DataFrame from multiple FRED series."""

    def _mock_fred(self):
        mock = MagicMock()

        def side_effect(series_id, observation_start=None):
            if series_id == "CPIAUCSL":
                return pd.Series(
                    [300.0, 301.0],
                    index=pd.to_datetime(["2024-01-01", "2024-02-01"]),
                )
            elif series_id == "T5YIE":
                return pd.Series(
                    [2.3, 2.4, 2.5],
                    index=pd.to_datetime(["2024-01-02", "2024-01-03", "2024-02-01"]),
                )
            raise Exception(f"Unknown series: {series_id}")

        mock.get_series.side_effect = side_effect
        return mock

    def test_outer_join_preserves_all_dates(self):
        columns = {"cpi": "CPIAUCSL", "breakeven_5y": "T5YIE"}
        df = build_table_dataframe(self._mock_fred(), columns, "2000-01-01")
        # 2024-01-01, 2024-01-02, 2024-01-03, 2024-02-01 = 4 unique dates
        self.assertEqual(len(df), 4)

    def test_nulls_where_series_missing(self):
        columns = {"cpi": "CPIAUCSL", "breakeven_5y": "T5YIE"}
        df = build_table_dataframe(self._mock_fred(), columns, "2000-01-01")
        # CPI has no data on 2024-01-02 or 2024-01-03
        jan02 = df[df["date"] == date(2024, 1, 2)]
        self.assertTrue(pd.isna(jan02["cpi"].iloc[0]))
        # But breakeven_5y should have data on 2024-01-02
        self.assertAlmostEqual(jan02["breakeven_5y"].iloc[0], 2.3)

    def test_none_fred_id_adds_null_column(self):
        columns = {"cpi": "CPIAUCSL", "gdp_nowcast": None}
        df = build_table_dataframe(self._mock_fred(), columns, "2000-01-01")
        self.assertIn("gdp_nowcast", df.columns)
        self.assertTrue(df["gdp_nowcast"].isna().all())

    def test_empty_when_all_fetches_fail(self):
        mock_fred = MagicMock()
        mock_fred.get_series.side_effect = Exception("fail")
        columns = {"cpi": "CPIAUCSL"}
        df = build_table_dataframe(mock_fred, columns, "2000-01-01")
        self.assertTrue(df.empty)


class TestUpsertTable(unittest.TestCase):
    """Test SQL upsert logic with a mocked psycopg2 connection."""

    @patch("src.modules.macro.fred_pipeline.execute_values")
    def test_upsert_executes_correct_row_count(self, mock_ev):
        mock_conn = MagicMock()

        df = pd.DataFrame({
            "date": [date(2024, 1, 1), date(2024, 2, 1)],
            "cpi": [300.0, 301.0],
            "core_cpi": [290.0, None],
        })
        columns = {"cpi": "CPIAUCSL", "core_cpi": "CPILFESL"}

        count = upsert_table(mock_conn, "inflation", columns, df)
        self.assertEqual(count, 2)
        mock_ev.assert_called_once()

    def test_empty_df_returns_zero(self):
        mock_conn = MagicMock()
        df = pd.DataFrame()
        count = upsert_table(mock_conn, "inflation", {"cpi": "CPIAUCSL"}, df)
        self.assertEqual(count, 0)

    def test_update_set_excludes_none_fred_id_columns(self):
        """gdp_nowcast (fred_id=None) must NOT appear in ON CONFLICT UPDATE SET,
        so it doesn't overwrite values populated by another pipeline."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        df = pd.DataFrame({
            "date": [date(2024, 1, 1)],
            "vix": [15.0],
            "dxy": [104.0],
            "tips_10y": [1.5],
            "gdp_nowcast": [None],
        })
        columns = {"vix": "VIXCLS", "dxy": "DTWEXBGS", "tips_10y": "DFII10", "gdp_nowcast": None}

        with patch("src.modules.macro.fred_pipeline.execute_values") as mock_ev:
            upsert_table(mock_conn, "market", columns, df)
            sql_arg = mock_ev.call_args[0][1]
            self.assertNotIn("gdp_nowcast = EXCLUDED.gdp_nowcast", sql_arg)


class TestRunPipeline(unittest.TestCase):
    """Test the full pipeline orchestration with mocks."""

    @patch("src.modules.macro.fred_pipeline.get_connection")
    @patch("src.modules.macro.fred_pipeline.Fred")
    @patch("src.modules.macro.fred_pipeline.create_schema_and_tables")
    @patch("src.modules.macro.fred_pipeline.build_table_dataframe")
    @patch("src.modules.macro.fred_pipeline.upsert_table")
    @patch.dict("os.environ", {"FRED_API_KEY": "test-key"})
    def test_pipeline_processes_all_tables(
        self, mock_upsert, mock_build, mock_create, mock_fred_cls, mock_conn
    ):
        mock_build.return_value = pd.DataFrame({"date": [date(2024, 1, 1)], "dummy": [1.0]})
        mock_upsert.return_value = 1
        mock_conn.return_value = MagicMock()

        stats = run_pipeline()
        self.assertEqual(len(stats), len(SERIES_CONFIG))
        self.assertEqual(mock_upsert.call_count, len(SERIES_CONFIG))

    @patch.dict("os.environ", {}, clear=True)
    def test_pipeline_raises_without_api_key(self):
        with self.assertRaises(ValueError):
            run_pipeline()

    @patch("src.modules.macro.fred_pipeline.get_connection")
    @patch("src.modules.macro.fred_pipeline.Fred")
    @patch("src.modules.macro.fred_pipeline.create_schema_and_tables")
    @patch("src.modules.macro.fred_pipeline.build_table_dataframe")
    @patch("src.modules.macro.fred_pipeline.upsert_table")
    @patch.dict("os.environ", {"FRED_API_KEY": "test-key"})
    def test_pipeline_raises_on_zero_total_rows(
        self, mock_upsert, mock_build, mock_create, mock_fred_cls, mock_conn
    ):
        mock_build.return_value = pd.DataFrame()
        mock_upsert.return_value = 0
        mock_conn.return_value = MagicMock()

        with self.assertRaises(RuntimeError):
            run_pipeline()


if __name__ == "__main__":
    unittest.main()
