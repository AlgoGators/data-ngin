import unittest
import psycopg2
import os
from dotenv import load_dotenv
from typing import Optional, Tuple, Any


# Load environment variables from .env
load_dotenv()


class TestDatabaseConnection(unittest.TestCase):
    """
    Unit tests for verifying database connection, schema, and table configurations.
    """

    conn: Optional[psycopg2.extensions.connection] = None

    @classmethod
    def setUpClass(cls) -> None:
        """
        Establish a database connection shared across all test methods.

        Raises:
            ConnectionError: If the database connection fails.
        """
        try:
            cls.conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
            )
            print("Database connection established successfully.")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to the database: {e}")

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Close the database connection after all tests are completed.
        """
        if cls.conn:
            cls.conn.close()
            print("Database connection closed.")

    def test_connection_success(self) -> None:
        """
        Test that the database connection is successfully established.
        """
        self.assertIsNotNone(self.conn, "Database connection not established")

    def test_schema_exists(self) -> None:
        """
        Verify that the target schema exists in the database.
        """
        schema_name: str = os.getenv("DB_SCHEMA", "futures_data")
        query: str = """
            SELECT schema_name 
            FROM information_schema.schemata
            WHERE schema_name = %s;
        """
        result: Optional[Tuple[Any,]] = None

        with self.conn.cursor() as cursor:
            cursor.execute(query, (schema_name,))
            result = cursor.fetchone()

        self.assertIsNotNone(result, f"Schema '{schema_name}' does not exist")

    def test_table_exists(self) -> None:
        """
        Verify that the target table exists within the schema.
        """
        schema_name: str = os.getenv("DB_SCHEMA", "futures_data")
        table_name: str = os.getenv("DB_TABLE", "ohlcv_1d")
        query: str = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s;
        """
        result: Optional[Tuple[Any,]] = None

        with self.conn.cursor() as cursor:
            cursor.execute(query, (schema_name, table_name))
            result = cursor.fetchone()

        self.assertIsNotNone(result, f"Table '{schema_name}.{table_name}' does not exist")

    def test_table_is_hypertable(self) -> None:
        """
        Ensure that the target table is registered as a hypertable in TimescaleDB.
        """
        schema_name: str = os.getenv("DB_SCHEMA", "futures_data")
        table_name: str = os.getenv("DB_TABLE", "ohlcv_1d")
        query: str = """
            SELECT * 
            FROM timescaledb_information.hypertables 
            WHERE hypertable_schema = %s AND hypertable_name = %s;
        """
        result: Optional[Tuple[Any,]] = None

        with self.conn.cursor() as cursor:
            cursor.execute(query, (schema_name, table_name))
            result = cursor.fetchone()

        self.assertIsNotNone(result, f"'{schema_name}.{table_name}' is not configured as a hypertable")


if __name__ == "__main__":
    unittest.main()
