import unittest
from unittest.mock import MagicMock, patch
from typing import List, Dict, Any, Optional
from sqlalchemy.exc import SQLAlchemyError
from data.modules.data_access import DataAccess
from data.modules.db_models import OHLCV
from datetime import datetime
import logging


class TestDataAccess(unittest.TestCase):
    """
    Unit tests for the `DataAccess` class using mocked database sessions.
    """

    def setUp(self) -> None:
        """
        Set up a mock DataAccess instance and mock session before each test.
        """
        self.data_access: DataAccess = DataAccess()
        self.mock_session = MagicMock()
        self.mock_session.__enter__.return_value = self.mock_session
        self.mock_session.__exit__.return_value = False
        self.data_access.Session = MagicMock(return_value=self.mock_session)

    def test_insert_data(self) -> None:
        """
        Test that inserting OHLCV records works correctly using a mock.
        """
        records: List[Dict[str, Any]] = [
            {
                "time": "2023-01-01T00:00:00Z",
                "symbol": "ES",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000,
            }
        ]

        self.data_access.insert_data(records)

        self.mock_session.add_all.assert_called_once()

        args, kwargs = self.mock_session.add_all.call_args
        inserted_objs = args[0]
        self.assertEqual(len(inserted_objs), 1)
        inserted_obj = inserted_objs[0]

        self.assertIsInstance(inserted_obj, OHLCV)
        self.assertEqual(inserted_obj.symbol, records[0]["symbol"])
        self.assertEqual(inserted_obj.time, records[0]["time"])
        self.assertEqual(inserted_obj.open, records[0]["open"])
        self.assertEqual(inserted_obj.high, records[0]["high"])
        self.assertEqual(inserted_obj.low, records[0]["low"])
        self.assertEqual(inserted_obj.close, records[0]["close"])
        self.assertEqual(inserted_obj.volume, records[0]["volume"])

        self.mock_session.commit.assert_called_once()
        logging.info("Insert data test passed.")


    def test_get_ohlcv_data(self) -> None:
        """
        Test retrieving OHLCV data within a date range using a mock.
        """
        mock_query_result = [
            OHLCV(
                time=datetime.fromisoformat("2023-01-01T00:00:00+00:00"),
                symbol="NQ",
                open=200.0,
                high=201.0,
                low=199.0,
                close=200.5,
                volume=2000,
            )
        ]

        self.mock_session.query.return_value.filter.return_value.all.return_value = mock_query_result

        result: List[Dict[str, Any]] = self.data_access.get_ohlcv_data(
            start_date="2023-01-01", end_date="2023-01-01"
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["symbol"], "NQ")
        logging.info("Get OHLCV data test passed.")

    def test_get_symbols(self) -> None:
        """
        Test retrieving unique symbols from the OHLCV table using a mock.
        """
        mock_query_result = [("AAPL",), ("MSFT",)]
        self.mock_session.query.return_value.distinct.return_value.all.return_value = mock_query_result

        symbols: List[str] = self.data_access.get_symbols()
        self.assertIn("AAPL", symbols)
        self.assertIn("MSFT", symbols)
        logging.info("Get symbols test passed.")

    def test_get_latest_data(self) -> None:
        """
        Test retrieving the latest OHLCV data for a specific symbol using a mock.
        """
        mock_query_result = OHLCV(
            time=datetime.fromisoformat("2023-01-02T00:00:00+00:00"),
            symbol="GOOG",
            open=1010.0,
            high=1015.0,
            low=1005.0,
            close=1012.0,
            volume=1600,
        )

        self.mock_session.query.return_value.filter_by.return_value.order_by.return_value.first.return_value = mock_query_result

        latest_data: Optional[Dict[str, Any]] = self.data_access.get_latest_data("GOOG")
        self.assertIsNotNone(latest_data)
        self.assertEqual(latest_data["close"], 1012.0)
        logging.info("Get latest data test passed.")

    def test_delete_data(self) -> None:
        """
        Test deleting OHLCV data within a date range using a mock.
        """
        self.mock_session.query.return_value.filter.return_value.filter.return_value.delete.return_value = 1

        self.data_access.delete_data(
            start_date="2023-01-01", end_date="2023-01-01", symbols=["TSLA"]
        )

        self.mock_session.query.return_value.filter.return_value.filter.return_value.delete.assert_called_once()
        self.mock_session.commit.assert_called_once()
        logging.info("Delete data test passed.")

    def test_insert_data_error(self) -> None:
        """
        Test that insertion errors are handled gracefully using a mock.
        """
        self.mock_session.add_all.side_effect = SQLAlchemyError("Insert failed.")

        invalid_records: List[Dict[str, Any]] = [
            {
                "time": "invalid-date-format",
                "symbol": "FAIL",
                "open": "NaN",
                "high": "NaN",
                "low": "NaN",
                "close": "NaN",
                "volume": "NaN",
            }
        ]

        with self.assertRaises(SQLAlchemyError):
            self.data_access.insert_data(invalid_records)
        self.mock_session.rollback.assert_called_once()
        logging.info("Insert error test passed.")


if __name__ == "__main__":
    unittest.main()
