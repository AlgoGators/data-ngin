import unittest
from unittest.mock import patch, MagicMock
from src.modules.inserter.timescaledb_inserter import TimescaleDBInserter
from typing import List, Dict, Any
import re


class TestTimescaleDBInserter(unittest.TestCase):
    """
    Unit tests for the TimescaleDBInserter class.
    """

    def setUp(self) -> None:
        """
        Set up a TimescaleDBInserter instance with mock configuration.
        """
        self.config: Dict[str, Any] = {
            "database": {
                "target_schema": "futures_data",
                "table": "ohlcv_1d",
                "db_name": "test_db",
            }
        }
        self.inserter = TimescaleDBInserter(config=self.config)

    def _setup_mock_connection(self, mock_connect: MagicMock) -> None:
        """
        Configure mock connection and cursor with proper return values for connect() queries.
        """
        mock_connection = mock_connect.return_value
        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value

        # Mock the responses for connect() queries
        mock_cursor.fetchone.side_effect = [
            ("127.0.0.1", 5432, "test_db", "test_user"),  # inet_server_addr query
            ("public",),  # search_path query
        ]
        mock_cursor.fetchall.return_value = [("public",), ("futures_data",)]

    @patch("src.modules.inserter.timescaledb_inserter.psycopg2.connect")
    def test_connect(self, mock_connect: MagicMock) -> None:
        """
        Test that the connect method establishes a database connection.
        """
        self._setup_mock_connection(mock_connect)
        self.inserter.connect()
        mock_connect.assert_called_once()
        self.assertIsNotNone(self.inserter.connection, "Database connection should not be None")

    @patch("src.modules.inserter.timescaledb_inserter.execute_values")
    @patch("src.modules.inserter.timescaledb_inserter.psycopg2.connect")
    def test_insert_data(self, mock_connect: MagicMock, mock_execute_values: MagicMock) -> None:
        """
        Test that data is inserted into the database using executemany.
        """
        self._setup_mock_connection(mock_connect)
        mock_connection = mock_connect.return_value
        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value

        # Reset side effects to return True for schema/table existence checks
        mock_cursor.fetchone.side_effect = [
            ("127.0.0.1", 5432, "test_db", "test_user"),  # inet_server_addr query
            ("public",),  # search_path query
            (1,),  # schema exists check
            (1,),  # table exists check
        ]

        data: List[Dict[str, Any]] = [
            {
                "time": "2023-01-01 00:00:00",
                "symbol": "ES",
                "open": 100.5,
                "high": 101.0,
                "low": 99.5,
                "close": 100.0,
                "volume": 1500,
            }
        ]

        self.inserter.connect()
        self.inserter.insert_data(data, schema="futures_data", table="ohlcv_1d")

        # execute_values batches every row into ONE multi-row INSERT. The literal
        # "VALUES %s" is the marker of that -- psycopg2 expands it. Seeing a
        # per-column VALUES tuple here would mean we had regressed to executemany,
        # which costs a round-trip per row and made a bulk backfill take ~21 hours.
        expected_query = re.sub(r"\s+", " ", """
            INSERT INTO futures_data.ohlcv_1d (time, symbol, open, high, low, close, volume)
            VALUES %s ON CONFLICT DO NOTHING
        """).strip()

        mock_execute_values.assert_called_once()
        _cursor, actual_query, actual_data = mock_execute_values.call_args[0]
        kwargs = mock_execute_values.call_args.kwargs

        self.assertEqual(re.sub(r"\s+", " ", actual_query.strip()), expected_query)
        self.assertEqual(actual_data, data)
        self.assertEqual(
            kwargs["template"],
            "(%(time)s, %(symbol)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)")
        self.assertGreater(kwargs["page_size"], 1, "batching must actually be on")

        mock_cursor.executemany.assert_not_called()

    @patch("src.modules.inserter.timescaledb_inserter.psycopg2.connect")
    def test_insert_data_empty_is_a_noop(self, mock_connect: MagicMock) -> None:
        """
        Empty data must be a logged no-op, never an exception.

        Tiingo legitimately returns zero rows for a symbol with no bars in the
        requested window. Raising here would make a normal condition look like a
        failure in the orchestrator's per-symbol except handler.
        """
        self._setup_mock_connection(mock_connect)
        self.inserter.connect()
        mock_cursor = self.inserter.connection.cursor.return_value.__enter__.return_value

        result = self.inserter.insert_data([], schema="futures_data", table="ohlcv_1d")

        self.assertIsNone(result)
        mock_cursor.executemany.assert_not_called()

    @patch("src.modules.inserter.timescaledb_inserter.psycopg2.connect")
    def test_insert_data_no_connection(self, mock_connect: MagicMock) -> None:
        """
        Test inserting data without a database connection, expecting RuntimeError.
        """
        with self.assertRaises(RuntimeError, msg="Database connection is not established."):
            self.inserter.insert_data([{"time": "2023-01-01"}], schema="futures_data", table="ohlcv_1d")

    @patch("src.modules.inserter.timescaledb_inserter.psycopg2.connect")
    def test_close_connection(self, mock_connect: MagicMock) -> None:
        """
        Test closing an active database connection.
        """
        self._setup_mock_connection(mock_connect)
        mock_connection = mock_connect.return_value
        self.inserter.connect()
        self.inserter.close()
        mock_connection.close.assert_called_once()
        self.assertIsNone(self.inserter.connection, "Database connection should be closed after close()")


if __name__ == "__main__":
    unittest.main()
