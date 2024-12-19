import unittest
from unittest.mock import patch, MagicMock
from data.modules.timescaledb_inserter import TimescaleDBInserter
from typing import List, Dict, Any
import textwrap
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
            }
        }
        self.inserter = TimescaleDBInserter(config=self.config)

    @patch("data.modules.timescaledb_inserter.psycopg2.connect")
    def test_connect(self, mock_connect: MagicMock) -> None:
        """
        Test that the connect method establishes a database connection.
        """
        self.inserter.connect()
        mock_connect.assert_called_once()
        self.assertIsNotNone(self.inserter.connection, "Database connection should not be None")

    @patch("data.modules.timescaledb_inserter.psycopg2.connect")
    def test_insert_data(self, mock_connect: MagicMock) -> None:
        """
        Test that data is inserted into the database using executemany.
        """
        mock_connection = mock_connect.return_value
        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value

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
        self.inserter.insert_data(data)

        # Compare queries without extra whitespace
        expected_query = re.sub(r"\s+", " ", """
            INSERT INTO futures_data.ohlcv_1d (time, symbol, open, high, low, close, volume)
            VALUES (%(time)s, %(symbol)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
            ON CONFLICT (time, symbol) DO NOTHING;
        """).strip()

        # Extract the actual query from the call arguments
        actual_query, actual_data = mock_cursor.executemany.call_args[0]

        # Assert that the queries are equivalent
        self.assertEqual(re.sub(r"\s+", " ", actual_query.strip()), expected_query)
        self.assertEqual(actual_data, data)

    @patch("data.modules.timescaledb_inserter.psycopg2.connect")
    def test_insert_data_empty(self, mock_connect: MagicMock) -> None:
        """
        Test inserting empty data, expecting ValueError.
        """
        self.inserter.connect()
        with self.assertRaises(ValueError, msg="No data provided for insertion."):
            self.inserter.insert_data([])

    @patch("data.modules.timescaledb_inserter.psycopg2.connect")
    def test_insert_data_no_connection(self, mock_connect: MagicMock) -> None:
        """
        Test inserting data without a database connection, expecting RuntimeError.
        """
        with self.assertRaises(RuntimeError, msg="Database connection is not established."):
            self.inserter.insert_data([{"time": "2023-01-01"}])

    @patch("data.modules.timescaledb_inserter.psycopg2.connect")
    def test_close_connection(self, mock_connect: MagicMock) -> None:
        """
        Test closing an active database connection.
        """
        mock_connection = mock_connect.return_value
        self.inserter.connect()
        self.inserter.close()
        mock_connection.close.assert_called_once()
        self.assertIsNone(self.inserter.connection, "Database connection should be closed after close()")


if __name__ == "__main__":
    unittest.main()
