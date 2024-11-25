import unittest
from unittest.mock import patch, MagicMock
from data.modules.timescaledb_inserter import TimescaleDBInserter
from typing import List, Dict, Any


class TestTimescaleDBInserter(unittest.TestCase):
    """
    Unit tests for the TimescaleDBInserter class.
    """

    def setUp(self) -> None:
        """
        Set up a TimescaleDBInserter instance with mock configuration.
        """
        self.config: Dict[str, Any] = {"schema": "futures_data", "table": "ohlcv_1d"}
        self.inserter = TimescaleDBInserter(config=self.config)

    @patch("data.modules.timescaledb_inserter.psycopg2.connect")
    def test_connect(self, mock_connect: MagicMock) -> None:
        """
        Test that the connect method establishes a database connection.
        """
        self.inserter.connect()
        mock_connect.assert_called_once()

    @patch("data.modules.timescaledb_inserter.psycopg2.connect")
    def test_insert_data(self, mock_connect: MagicMock) -> None:
        """
        Test that data is inserted into the database using executemany.
        """
        # Mock the connection object returned by psycopg2.connect
        mock_connection = mock_connect.return_value
        
        # Mock the cursor object and its context manager behavior
        mock_cursor = mock_connection.cursor.return_value
        mock_cursor.__enter__.return_value = mock_cursor  # Make __enter__ return the mock_cursor itself

        # Sample data to insert
        data: List[Dict[str, Any]] = [
            {"time": "2023-01-01 00:00:00", "symbol": "ES", "open": 100.5, "high": 101.0, "low": 99.5, "close": 100.0, "volume": 1500}
        ]

        # Call the inserter logic
        self.inserter.connect()  # Ensure the mock connection is used
        self.inserter.insert_data(data, schema="futures_data", table="ohlcv_1d")

        # Verify the cursor's executemany method was called with the correct query and data
        mock_cursor.executemany.assert_called_once_with(
        """
        INSERT INTO futures_data.ohlcv_1d (time, symbol, open, high, low, close, volume)
        VALUES (%(time)s, %(symbol)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
        ON CONFLICT (time, symbol) DO NOTHING;
        """.strip(),
        data
        )


if __name__ == "__main__":
    unittest.main()
