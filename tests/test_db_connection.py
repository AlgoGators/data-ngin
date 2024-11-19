import unittest
import yaml
import psycopg2
import os
from dotenv import load_dotenv
from typing import Dict, Any

load_dotenv()


class TestDatabaseConnection(unittest.TestCase):
    """
    Unit tests for verifying database connection and existence of required schemas and tables.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """
        This connection will be shared across all test methods.
        """
        
        # Establish a database connection
        cls.conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Close the database connection after all tests are completed.
        """
        cls.conn.close()

    def test_schema_exists(self) -> None:
        """
        Test to check if the 'futures_data' schema exists in the database.
        """
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'futures_data';")
            result = cursor.fetchone()
            self.assertIsNotNone(result, "Schema 'futures_data' does not exist")

    def test_table_exists(self) -> None:
        """
        Test to check if the 'ohlcv_1d' table exists in the 'futures_data' schema.
        """
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'futures_data' AND table_name = 'ohlcv_1d';")
            result = cursor.fetchone()
            self.assertIsNotNone(result, "Table 'futures_data.ohlcv_1d' does not exist")

    def test_table_is_hypertable(self) -> None:
        """
        Test to verify if 'futures_data.ohlcv_1d' is configured as a hypertable in TimescaleDB.
        """
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM timescaledb_information.hypertables WHERE hypertable_schema = 'futures_data' AND hypertable_name = 'ohlcv_1d';")
            result = cursor.fetchone()
            self.assertIsNotNone(result, "'futures_data.ohlcv_1d' is not configured as a hypertable")

if __name__ == "__main__":
    unittest.main()
