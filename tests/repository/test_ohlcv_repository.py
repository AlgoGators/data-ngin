import re
import unittest
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

from src.infrastructure.repository.ohlcv_repository import OhlcvRepository


class TestOhlcvRepositoryWrites(unittest.TestCase):
    """
    Write-path tests -- migrated from the old TimescaleDBInserter test suite.
    Assertions target the connection pool (getconn/putconn) rather than
    psycopg2.connect directly now, since connect()/close() borrow/return a
    pooled connection instead of opening/closing a real one each time --
    see OhlcvRepository's class docstring.
    """

    def setUp(self) -> None:
        self.config: Dict[str, Any] = {
            "database": {
                "target_schema": "futures_data",
                "table": "ohlcv_1d",
            }
        }
        self.repository = OhlcvRepository(config=self.config)

        self.mock_pool = MagicMock()
        patcher = patch.object(OhlcvRepository, "_get_pool", return_value=self.mock_pool)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(OhlcvRepository.reset_pool_for_testing)
        OhlcvRepository.reset_pool_for_testing()

    def _mock_pool_connection(self, extra_fetchone: list = None):
        mock_connection = MagicMock()
        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value
        mock_cursor.fetchone.side_effect = [
            ("127.0.0.1", 5432, "futures_data_db", "app_user"),
            ("public",),
        ] + (extra_fetchone or [])
        mock_cursor.fetchall.return_value = [("futures_data",), ("public",)]
        self.mock_pool.getconn.return_value = mock_connection
        return mock_connection, mock_cursor

    def test_connect(self) -> None:
        self._mock_pool_connection()
        self.repository.connect()
        self.mock_pool.getconn.assert_called_once()
        self.assertIsNotNone(self.repository.connection)

    def test_connect_only_logs_diagnostics_once_per_process(self) -> None:
        """
        Diagnostic queries (server endpoint, search_path, schema list) must
        run once per process, not once per connect() -- with pooling,
        connect() can now happen once per symbol/request instead of once ever.
        """
        mock_connection, mock_cursor = self._mock_pool_connection()
        self.repository.connect()
        self.repository.close()

        second_repository = OhlcvRepository(config=self.config)
        mock_cursor.fetchone.side_effect = None  # no more diagnostic fetchone calls expected
        second_repository.connect()

        # Only the first connect()'s cursor ran the 3-query diagnostic block.
        self.assertEqual(mock_cursor.execute.call_count, 3)

    def test_insert_data(self) -> None:
        _mock_connection, mock_cursor = self._mock_pool_connection(extra_fetchone=[(1,), (1,)])

        data: List[Dict[str, Any]] = [{
            "time": "2023-01-01 00:00:00",
            "symbol": "ES",
            "open": 100.5,
            "high": 101.0,
            "low": 99.5,
            "close": 100.0,
            "volume": 1500,
        }]

        self.repository.connect()
        self.repository.insert_data(data, schema="futures_data", table="ohlcv_1d")

        expected_query = re.sub(r"\s+", " ", """
            INSERT INTO futures_data.ohlcv_1d (time, symbol, open, high, low, close, volume)
            VALUES (%(time)s, %(symbol)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
            ON CONFLICT DO NOTHING;
        """).strip()

        actual_query, actual_data = mock_cursor.executemany.call_args[0]
        self.assertEqual(re.sub(r"\s+", " ", actual_query.strip()), expected_query)
        self.assertEqual(actual_data, data)

    def test_insert_data_only_verifies_schema_table_once(self) -> None:
        """
        The schema/table-exists check must run once per (schema, table), not
        once per insert_data() call -- the orchestrator calls insert_data
        twice per symbol (raw + cleaned tables) for every symbol in a run.
        """
        _mock_connection, mock_cursor = self._mock_pool_connection(extra_fetchone=[(1,), (1,), (1,), (1,)])
        data = [{"time": "2023-01-01", "symbol": "ES", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1}]

        self.repository.connect()
        self.repository.insert_data(data, schema="futures_data", table="ohlcv_1d")
        self.repository.insert_data(data, schema="futures_data", table="ohlcv_1d")

        # 3 diagnostic queries (connect) + 2 verification queries (first insert only) = 5
        self.assertEqual(mock_cursor.execute.call_count, 5)
        self.assertEqual(mock_cursor.executemany.call_count, 2)

    def test_insert_data_no_connection(self) -> None:
        with self.assertRaises(RuntimeError):
            self.repository.insert_data([{"time": "2023-01-01"}], schema="futures_data", table="ohlcv_1d")

    def test_insert_data_empty_list_is_a_no_op(self) -> None:
        """
        Empty data is routine (a symbol with no rows for a holiday-only
        range, delisted contract, or empty batch window), not an error --
        must not raise, and must not require a connection.
        """
        self.repository.insert_data([], schema="futures_data", table="ohlcv_1d")
        self.assertIsNone(self.repository.connection)

    def test_close_connection(self) -> None:
        mock_connection, _mock_cursor = self._mock_pool_connection()
        self.repository.connect()
        self.repository.close()
        self.mock_pool.putconn.assert_called_once_with(mock_connection)
        self.assertIsNone(self.repository.connection)


class TestOhlcvRepositoryConnectionPool(unittest.TestCase):
    """Tests the pool-management machinery itself, separate from connect()/close()'s use of it."""

    def setUp(self) -> None:
        self.addCleanup(OhlcvRepository.reset_pool_for_testing)
        OhlcvRepository.reset_pool_for_testing()

    @patch("src.infrastructure.repository.ohlcv_repository.psycopg2_pool.ThreadedConnectionPool")
    def test_get_pool_builds_once_and_caches(self, mock_pool_cls: MagicMock) -> None:
        pool_a = OhlcvRepository._get_pool()
        pool_b = OhlcvRepository._get_pool()

        mock_pool_cls.assert_called_once()
        self.assertIs(pool_a, pool_b)

    @patch("src.infrastructure.repository.ohlcv_repository.psycopg2_pool.ThreadedConnectionPool")
    def test_reset_pool_for_testing_forces_rebuild(self, mock_pool_cls: MagicMock) -> None:
        OhlcvRepository._get_pool()
        OhlcvRepository.reset_pool_for_testing()
        OhlcvRepository._get_pool()

        self.assertEqual(mock_pool_cls.call_count, 2)

    @patch.dict("os.environ", {"DB_POOL_MIN_CONN": "2", "DB_POOL_MAX_CONN": "20"})
    @patch("src.infrastructure.repository.ohlcv_repository.psycopg2_pool.ThreadedConnectionPool")
    def test_get_pool_reads_size_from_env(self, mock_pool_cls: MagicMock) -> None:
        OhlcvRepository._get_pool()
        args, _kwargs = mock_pool_cls.call_args
        self.assertEqual(args[0], 2)
        self.assertEqual(args[1], 20)


class TestOhlcvRepositoryReads(unittest.TestCase):
    """
    Read-path tests for the methods reimplemented from the old ORM-based
    DataAccess against a raw psycopg2 connection.
    """

    def setUp(self) -> None:
        self.config: Dict[str, Any] = {
            "database": {
                "target_schema": "futures_data",
                "table": "ohlcv_1d",
            }
        }
        self.repository = OhlcvRepository(config=self.config)
        # Pretend we're already connected so read methods skip auto-connect.
        self.repository.connection = MagicMock()
        self.mock_cursor = self.repository.connection.cursor.return_value.__enter__.return_value

    def test_get_ohlcv_data_builds_query_with_symbols(self) -> None:
        self.mock_cursor.description = [("time",), ("symbol",), ("open",), ("high",), ("low",), ("close",), ("volume",)]
        self.mock_cursor.fetchall.return_value = [("2023-01-01", "ES", 100.0, 101.0, 99.0, 100.5, 1000)]

        result = self.repository.get_ohlcv_data("2023-01-01", "2023-01-02", symbols=["ES"])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["symbol"], "ES")
        query, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("WHERE time BETWEEN", query)
        self.assertIn("AND symbol = ANY(%s)", query)
        self.assertEqual(params, ["2023-01-01", "2023-01-02", ["ES"]])

    def test_get_symbols(self) -> None:
        self.mock_cursor.fetchall.return_value = [("ES",), ("NQ",)]
        result = self.repository.get_symbols()
        self.assertEqual(result, ["ES", "NQ"])

    def test_get_latest_date_empty_table(self) -> None:
        self.mock_cursor.fetchone.return_value = (None,)
        self.assertIsNone(self.repository.get_latest_date())

    def test_get_latest_date_returns_formatted_string(self) -> None:
        mock_date = MagicMock()
        mock_date.strftime.return_value = "2023-06-01"
        self.mock_cursor.fetchone.return_value = (mock_date,)
        self.assertEqual(self.repository.get_latest_date(), "2023-06-01")

    def test_get_latest_data_no_row(self) -> None:
        self.mock_cursor.fetchone.return_value = None
        self.assertIsNone(self.repository.get_latest_data("ES"))

    def test_default_schema_table_from_config(self) -> None:
        schema, table = self.repository._default_schema_table(None, None)
        self.assertEqual(schema, "futures_data")
        self.assertEqual(table, "ohlcv_1d")

    def test_default_schema_table_override(self) -> None:
        schema, table = self.repository._default_schema_table("other_schema", "other_table")
        self.assertEqual(schema, "other_schema")
        self.assertEqual(table, "other_table")

    def test_delete_data_without_symbols(self) -> None:
        self.mock_cursor.rowcount = 3
        self.repository.delete_data("2023-01-01", "2023-01-02")

        query, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("DELETE FROM futures_data.ohlcv_1d", query)
        self.assertIn("WHERE time BETWEEN", query)
        self.assertNotIn("symbol = ANY", query)
        self.assertEqual(params, ["2023-01-01", "2023-01-02"])

    def test_delete_data_with_symbols(self) -> None:
        self.mock_cursor.rowcount = 1
        self.repository.delete_data("2023-01-01", "2023-01-02", symbols=["ES", "NQ"])

        query, params = self.mock_cursor.execute.call_args[0]
        self.assertIn("AND symbol = ANY(%s)", query)
        self.assertEqual(params, ["2023-01-01", "2023-01-02", ["ES", "NQ"]])

    def test_delete_data_uses_configured_schema_table_by_default(self) -> None:
        self.mock_cursor.rowcount = 0
        self.repository.delete_data("2023-01-01", "2023-01-02", schema="custom_schema", table="custom_table")

        query, _params = self.mock_cursor.execute.call_args[0]
        self.assertIn("FROM custom_schema.custom_table", query)


if __name__ == "__main__":
    unittest.main()
