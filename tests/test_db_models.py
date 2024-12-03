from sqlalchemy.engine import Engine, Connection
from sqlalchemy.orm import Session
from sqlalchemy import inspect, Table
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import DeclarativeMeta
from typing import Dict, List
import unittest
from data.modules.db_models import get_engine, Base, OHLCV
import logging


class TestDBModels(unittest.TestCase):
    """
    Unit tests for the `db_models.py` file, including database engine and ORM models.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """
        Set up the database and create tables for testing.
        This method runs once before all tests in this class.
        """
        cls.engine: Engine = get_engine()
        cls.session: Session = Session(cls.engine)
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
        cls.session.close()
        Base.metadata.drop_all(cls.engine)

    def setUp(self) -> None:
        """
        Begin a transaction for each test.
        """
        self.connection: Connection = self.engine.connect()
        self.trans = self.connection.begin()
        self.session: Session = Session(bind=self.connection)

    def tearDown(self) -> None:
        """
        Rollback the transaction after each test.
        """
        self.session.rollback()
        self.connection.close()

    def test_get_engine(self) -> None:
        """
        Test that `get_engine` successfully creates a database engine.
        """
        engine: Engine = get_engine()
        self.assertIsNotNone(engine)
        self.assertIn("postgresql", str(engine.url))

    def test_ohlcv_table_exists(self) -> None:
        """
        Test that the `ohlcv` table exists in the database.
        """
        inspector = inspect(self.engine)
        tables: List[str] = inspector.get_table_names()
        print(tables)
        self.assertIn("ohlcv_1d", tables, "The `ohlcv_1d` table was not found in the database.")

    def test_ohlcv_model_schema(self) -> None:
        """
        Test the `OHLCV` model schema for correct columns and types.
        """
        inspector = inspect(self.engine)
        print(inspector.get_table_names())
        if "ohlcv_1d" not in inspector.get_table_names():
            self.fail("The `ohlcv_1d` table does not exist in the database.")

        columns: Dict[str, str] = {col["name"]: col["type"] for col in inspector.get_columns("ohlcv")}
        expected_columns: Dict[str, str] = {
            "time": "TIMESTAMP",
            "symbol": "VARCHAR",
            "open": "FLOAT",
            "high": "FLOAT",
            "low": "FLOAT",
            "close": "FLOAT",
            "volume": "INTEGER",
        }

        for column, col_type in expected_columns.items():
            self.assertIn(column, columns, f"Column `{column}` is missing from the `ohlcv` table.")
            self.assertEqual(str(columns[column]), col_type, f"Column `{column}` has an incorrect type.")

    def test_insert_ohlcv(self) -> None:
        """
        Test inserting a record into the `ohlcv_1d` table.
        """
        record: OHLCV = OHLCV(
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

        result: OHLCV = self.session.query(OHLCV).filter_by(symbol="ES").one_or_none()
        self.assertIsNotNone(result, "Inserted record was not found.")
        self.assertEqual(result.open, 100.0, "The `open` field did not match the inserted value.")

    def test_insert_duplicate_ohlcv(self) -> None:
        """
        Test inserting duplicate records into the `ohlcv` table.
        """
        record: OHLCV = OHLCV(
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

        duplicate_record: OHLCV = OHLCV(
            time="2023-01-01T00:00:00Z",
            symbol="ES",
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000,
        )
        self.session.add(duplicate_record)

        with self.assertRaises(IntegrityError, msg="Duplicate record insertion did not raise IntegrityError."):
            self.session.commit()

    def test_query_ohlcv(self) -> None:
        """
        Test querying the `ohlcv_1d` table.
        """
        record: OHLCV = OHLCV(
            time="2023-01-01T00:00:00Z",
            symbol="NQ",
            open=200.0,
            high=201.0,
            low=199.0,
            close=200.5,
            volume=2000,
        )
        self.session.add(record)
        self.session.commit()

        result: OHLCV = self.session.query(OHLCV).filter_by(symbol="NQ").one_or_none()
        self.assertIsNotNone(result, "The queried record was not found.")
        self.assertEqual(result.close, 200.5, "The `close` field did not match the inserted value.")

    def test_delete_ohlcv(self) -> None:
        """
        Test deleting a record from the `ohlcv_1d` table.
        """
        record: OHLCV = OHLCV(
            time="2023-01-01T00:00:00Z",
            symbol="YM",
            open=300.0,
            high=301.0,
            low=299.0,
            close=300.5,
            volume=3000,
        )
        self.session.add(record)
        self.session.commit()

        self.session.delete(record)
        self.session.commit()

        result: OHLCV = self.session.query(OHLCV).filter_by(symbol="YM").one_or_none()
        self.assertIsNone(result, "The deleted record was still found in the database.")


if __name__ == "__main__":
    unittest.main()
