import unittest
from unittest.mock import patch, MagicMock
from datetime import date

import pandas as pd
import numpy as np

from src.modules.macro.bsts_etf_pipeline import (
    BSTS_ETF_TICKERS,
    SCHEMA,
    TABLE,
    fetch_etf,
    clean_etf_data,
    upsert_etf_prices,
    run_pipeline,
)


class TestBstsEtfConfig(unittest.TestCase):
    """Verify the ETF ticker list is well-formed."""

    def test_expected_tickers(self):
        symbols = [t.split(".")[0] for t in BSTS_ETF_TICKERS]
        expected = {"SPY", "EEM", "TLT", "HYG", "GLD", "UUP", "USO", "CPER"}
        self.assertEqual(set(symbols), expected)

    def test_all_tickers_are_us_exchange(self):
        for ticker in BSTS_ETF_TICKERS:
            self.assertTrue(ticker.endswith(".US"), f"{ticker} is not a .US ticker")

    def test_ticker_count(self):
        self.assertEqual(len(BSTS_ETF_TICKERS), 8)


class TestFetchEtf(unittest.TestCase):
    """Test the EODHD fetch wrapper."""

    @patch("src.modules.macro.bsts_etf_pipeline.requests.get")
    def test_successful_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = [
            {"date": "2024-01-02", "open": 470.0, "high": 472.0, "low": 469.0,
             "close": 471.0, "adjusted_close": 471.0, "volume": 50000000},
        ]
        mock_get.return_value = mock_resp

        df = fetch_etf("test-key", "SPY.US", "2024-01-01", "2024-12-31")
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 1)
        self.assertEqual(df["symbol"].iloc[0], "SPY")

    @patch("src.modules.macro.bsts_etf_pipeline.requests.get")
    def test_failed_fetch_returns_none(self, mock_get):
        mock_get.side_effect = Exception("Connection error")
        result = fetch_etf("test-key", "SPY.US", "2024-01-01", "2024-12-31")
        self.assertIsNone(result)

    @patch("src.modules.macro.bsts_etf_pipeline.requests.get")
    def test_empty_response_returns_none(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = []
        mock_get.return_value = mock_resp

        result = fetch_etf("test-key", "SPY.US", "2024-01-01", "2024-12-31")
        self.assertIsNone(result)

    @patch("src.modules.macro.bsts_etf_pipeline.requests.get")
    def test_warning_response_returns_none(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = [{"warning": "API limit reached"}]
        mock_get.return_value = mock_resp

        result = fetch_etf("test-key", "SPY.US", "2024-01-01", "2024-12-31")
        self.assertIsNone(result)


class TestCleanEtfData(unittest.TestCase):
    """Test ETF data cleaning logic."""

    def _sample_df(self):
        return pd.DataFrame({
            "date": ["2024-01-02", "2024-01-03"],
            "open": [470.0, 471.0],
            "high": [472.0, 473.0],
            "low": [469.0, 470.0],
            "close": [471.0, 472.0],
            "adjusted_close": [471.0, 472.0],
            "volume": [50000000, 45000000],
            "symbol": ["SPY", "SPY"],
        })

    def test_dates_converted_to_date_objects(self):
        df = clean_etf_data(self._sample_df())
        self.assertEqual(df["date"].iloc[0], date(2024, 1, 2))

    def test_drops_negative_prices(self):
        df = self._sample_df()
        df.loc[0, "close"] = -1.0
        cleaned = clean_etf_data(df)
        self.assertEqual(len(cleaned), 1)

    def test_drops_duplicate_symbol_date(self):
        df = self._sample_df()
        dup = df.iloc[[0]].copy()
        df = pd.concat([df, dup], ignore_index=True)
        cleaned = clean_etf_data(df)
        self.assertEqual(len(cleaned), 2)

    def test_empty_df_returns_empty(self):
        result = clean_etf_data(pd.DataFrame())
        self.assertTrue(result.empty)


class TestUpsertEtfPrices(unittest.TestCase):
    """Test SQL upsert logic with a mocked connection."""

    @patch("src.modules.macro.bsts_etf_pipeline.execute_values")
    def test_upsert_correct_row_count(self, mock_ev):
        mock_conn = MagicMock()

        df = pd.DataFrame({
            "date": [date(2024, 1, 2), date(2024, 1, 3)],
            "symbol": ["SPY", "SPY"],
            "open": [470.0, 471.0],
            "high": [472.0, 473.0],
            "low": [469.0, 470.0],
            "close": [471.0, 472.0],
            "adjusted_close": [471.0, 472.0],
            "volume": [50000000, 45000000],
        })

        count = upsert_etf_prices(mock_conn, df)
        self.assertEqual(count, 2)
        mock_ev.assert_called_once()

    def test_empty_df_returns_zero(self):
        mock_conn = MagicMock()
        count = upsert_etf_prices(mock_conn, pd.DataFrame())
        self.assertEqual(count, 0)


class TestRunPipeline(unittest.TestCase):
    """Test the full ETF pipeline orchestration with mocks."""

    @patch("src.modules.macro.bsts_etf_pipeline.get_connection")
    @patch("src.modules.macro.bsts_etf_pipeline.fetch_etf")
    @patch("src.modules.macro.bsts_etf_pipeline.upsert_etf_prices")
    @patch.dict("os.environ", {"EODHD_API_KEY": "test-key"})
    def test_pipeline_fetches_all_tickers(self, mock_upsert, mock_fetch, mock_conn):
        mock_conn.return_value = MagicMock()
        mock_upsert.return_value = 1

        mock_fetch.return_value = pd.DataFrame({
            "date": ["2024-01-02"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "adjusted_close": [100.5],
            "volume": [1000000],
            "symbol": ["TEST"],
        })

        stats = run_pipeline()
        self.assertEqual(mock_fetch.call_count, len(BSTS_ETF_TICKERS))
        self.assertEqual(len(stats), len(BSTS_ETF_TICKERS))

    @patch("src.modules.macro.bsts_etf_pipeline.load_dotenv")
    @patch.dict("os.environ", {}, clear=True)
    def test_pipeline_raises_without_api_key(self, mock_dotenv):
        with self.assertRaises(ValueError):
            run_pipeline()

    @patch("src.modules.macro.bsts_etf_pipeline.get_connection")
    @patch("src.modules.macro.bsts_etf_pipeline.fetch_etf")
    @patch.dict("os.environ", {"EODHD_API_KEY": "test-key"})
    def test_pipeline_raises_on_zero_rows(self, mock_fetch, mock_conn):
        mock_conn.return_value = MagicMock()
        mock_fetch.return_value = None

        with self.assertRaises(RuntimeError):
            run_pipeline()


if __name__ == "__main__":
    unittest.main()
