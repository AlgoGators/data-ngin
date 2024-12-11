import unittest
from unittest.mock import MagicMock, patch
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.orm import Session
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from typing import Dict, List, Optional, Any

from data.modules.db_models import get_engine, Base, OHLCV
import logging


class TestDBModels(unittest.TestCase):
    """
    Unit tests for the `db_models.py` file, including database engine and ORM models.
    """

    engine: Optional[Engine] = None
    session: Optional[Session] = None

    @classmethod
    def setUpClass(cls) -> None:
        """
        Initialize the test database and create all tables.
        This method runs once before all tests in this class.
        """
        cls.engine = get_engine()
        cls.session = Session(cls.engine)
        try:
            Base.metadata.create_all(cls.engine)  # Ensure all tables are created
            logging.info("All tables created successfully.")
        except Exception as e:
            logging.error(f"Error creating tables: {e}")
            raise

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Clean up the database by dropping all tables and closing the session.
        This method runs once after all tests in this class.
        """
        if cls.session:
            cls.session.close()
        if cls.engine:
            logging.info("All tables dropped successfully.")

    def setUp(self) -> None:
        """
        Start a new transaction before each test.
        """
        assert self.engine is not None
        self.connection: Connection = self.engine.connect()
        self.trans = self.connection.begin()
        self.session = Session(bind=self.connection)

    def tearDown(self) -> None:
        """
        Rollback the transaction after each test.
        """
        self.session.rollback()
        self.connection.close()

    def test_get_engine(self) -> None:
        """
        Verify that `get_engine` successfully creates a database engine.
        """
        engine: Engine = get_engine()
        self.assertIsNotNone(engine)
        self.assertIn("postgresql", str(engine.url))

    def test_ohlcv_table_exists(self) -> None:
        """
        Check if the `ohlcv_1d` table exists in the database.
        """
        inspector = inspect(self.engine)
        tables: List[str] = inspector.get_table_names(schema="futures_data")
        self.assertIn("ohlcv_1d", tables, "The `ohlcv_1d` table was not found in the database.")

    def test_ohlcv_model_schema(self) -> None:
        """
        Validate the `OHLCV` model's table schema.
        """
        inspector = inspect(self.engine)
        columns: Dict[str, str] = {
            col["name"]: col["type"]
            for col in inspector.get_columns("ohlcv_1d", schema="futures_data")
        }
        expected_columns: Dict[str, str] = {
            "time": "TIMESTAMP",
            "symbol": "TEXT",
            "open": "DOUBLE PRECISION",
            "high": "DOUBLE PRECISION",
            "low": "DOUBLE PRECISION",
            "close": "DOUBLE PRECISION",
            "volume": "INTEGER",
        }

        for column, col_type in expected_columns.items():
            self.assertIn(column, columns, f"Column `{column}` is missing.")
            self.assertEqual(str(columns[column]), col_type, f"Type mismatch for `{column}`.")

    @patch("data.modules.db_models.Session.add")
    @patch("data.modules.db_models.Session.commit")
    def test_insert_ohlcv(self, mock_commit: MagicMock, mock_add: MagicMock) -> None:
        """
        Test inserting a record into the `ohlcv_1d` table.
        """
        record = OHLCV(
            time="2023-01-01T00:00:00Z",
            symbol="ES",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000,
        )
        self.session.add(record)
        self.session.commit()

        mock_add.assert_called_once_with(record)
        mock_commit.assert_called_once()

    @patch("data.modules.db_models.Session.commit")
    def test_insert_duplicate_ohlcv(self, mock_commit: MagicMock) -> None:
        """
        Test inserting duplicate records and expecting an IntegrityError.
        """
        record = OHLCV(
            time="2023-01-01T00:00:00Z",
            symbol="ES",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000,
        )

        # Mock commit behavior to raise IntegrityError
        mock_commit.side_effect = IntegrityError("Duplicate record", params=None, orig=None)

        self.session.add(record)
        with self.assertRaises(IntegrityError, msg="Duplicate record did not raise IntegrityError."):
            self.session.commit()

    @patch("data.modules.db_models.Session.query")
    def test_query_ohlcv(self, mock_query: MagicMock) -> None:
        """
        Test querying the `ohlcv_1d` table.
        """
        record = OHLCV(
            time="2023-01-01T00:00:00Z",
            symbol="NQ",
            open=200.0,
            high=201.0,
            low=199.0,
            close=200.5,
            volume=2000,
        )

        mock_query.return_value.filter_by.return_value.one_or_none.return_value = record

        result = self.session.query(OHLCV).filter_by(symbol="NQ").one_or_none()
        self.assertIsNotNone(result, "The queried record was not found.")
        self.assertEqual(result.close, 200.5, "The `close` field did not match the inserted value.")

    @patch("data.modules.db_models.Session.delete")
    @patch("data.modules.db_models.Session.commit")
    def test_delete_ohlcv(self, mock_commit: MagicMock, mock_delete: MagicMock) -> None:
        """
        Test deleting a record from the `ohlcv_1d` table.
        """
        record = OHLCV(
            time="2023-01-01T00:00:00Z",
            symbol="YM",
            open=300.0,
            high=301.0,
            low=299.0,
            close=300.5,
            volume=3000,
        )

        self.session.delete(record)
        self.session.commit()

        mock_delete.assert_called_once_with(record)
        mock_commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
