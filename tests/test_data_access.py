import unittest
from unittest.mock import MagicMock, patch
from typing import List, Dict, Any, Optional
from sqlalchemy.exc import SQLAlchemyError
from src.modules.data_access import DataAccess
from src.modules.db_models import OHLCV
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
        self.mock_session: MagicMock = MagicMock()
        self.mock_session.__enter__.return_value = self.mock_session
        self.mock_session.__exit__.return_value = False
        self.data_access.Session = MagicMock(return_value=self.mock_session)

    def test_insert_data(self) -> None:
        """
        Test inserting OHLCV records using a mock session.
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

        inserted_objs: List[OHLCV] = self.mock_session.add_all.call_args[0][0]
        self.assertEqual(len(inserted_objs), 1)
        inserted_obj: OHLCV = inserted_objs[0]

        self.assertIsInstance(inserted_obj, OHLCV)
        self.assertEqual(inserted_obj.symbol, records[0]["symbol"])
        self.assertEqual(inserted_obj.time, records[0]["time"])
        self.mock_session.commit.assert_called_once()

    @patch("data.modules.data_access.sessionmaker")
    def test_get_ohlcv_data(self, mock_session_factory):
        # Create DataAccess after patch is in place
        data_access = DataAccess()
        with patch.object(data_access, 'Session', return_value=MagicMock()) as mock_session_factory:
            mock_session = mock_session_factory.return_value.__enter__.return_value
            # Mock chain, run test...

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

            # Set return values on the mock query chain
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = mock_query_result

            result: List[Dict[str, Any]] = data_access.get_ohlcv_data("2023-01-01", "2023-01-01")
            print(f"Mock Query Call Chain: {mock_session.query.return_value.filter.return_value.order_by.return_value.all.call_args_list}")
            print(f"Returned Data: {result}")

            self.assertEqual(len(result), 1, "Expected one record to be returned")
            self.assertEqual(result[0]["symbol"], "NQ", "Expected symbol 'NQ' to be returned")

    def test_get_symbols(self) -> None:
        """
        Test retrieving unique symbols from the OHLCV table.
        """
        mock_query_result: List[tuple[str]] = [("AAPL",), ("MSFT",)]
        self.mock_session.query.return_value.distinct.return_value.all.return_value = mock_query_result

        symbols: List[str] = self.data_access.get_symbols()
        self.assertIn("AAPL", symbols)
        self.assertIn("MSFT", symbols)

    def test_get_latest_data(self) -> None:
        """
        Test retrieving the latest OHLCV record for a specific symbol.
        """
        mock_query_result: OHLCV = OHLCV(
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

    def test_delete_data(self) -> None:
        """
        Test deleting OHLCV records within a date range.
        """
        self.mock_session.query.return_value.filter.return_value.filter.return_value.delete.return_value = 1

        self.data_access.delete_data(
            start_date="2023-01-01", end_date="2023-01-01", symbols=["TSLA"]
        )

        self.mock_session.query.return_value.filter.return_value.filter.return_value.delete.assert_called_once()
        self.mock_session.commit.assert_called_once()

    def test_insert_data_error(self) -> None:
        """
        Test handling insertion errors using a mock session.
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


if __name__ == "__main__":
    unittest.main()
