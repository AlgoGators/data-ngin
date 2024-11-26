import os
import psycopg2
from typing import List, Dict, Any
from data.modules.inserter import Inserter


class TimescaleDBInserter(Inserter):
    """
    Inserter subclass for inserting data into TimescaleDB.

    Methods:
        connect: Establish a connection to the TimescaleDB database.
        insert_data: Insert cleaned data into the appropriate schema and table.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the TimescaleDBInserter with configuration settings.

        Args:
            config (Dict[str, Any]): Configuration settings.
        """
        super().__init__(config=config)
        self.config: Dict[str, Any] = config

    def connect(self) -> None:
        """
        Establishes a connection to the TimescaleDB database.

        Raises:
            ConnectionError: If the connection to the database fails.
        """
        try:
            self.connection = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT")
            )
            self.connection.autocommit = True
        except psycopg2.OperationalError as e:
            raise ConnectionError(f"Failed to connect to TimescaleDB: {e}")

    def insert_data(self, data: List[Dict[str, Any]], schema: str, table: str) -> None:
        """
        Inserts cleaned data into the specified TimescaleDB table.

        Args:
            data (List[Dict[str, Any]]): A list of dictionaries representing cleaned data rows.
            schema (str): The target schema in TimescaleDB.
            table (str): The target table in TimescaleDB.

        Raises:
            ValueError: If the data is empty.
            RuntimError: If the insertion into the database fails.
        """
        if not data:
            raise ValueError("No data provided for insertion.")

        query: str = f"""
        INSERT INTO {schema}.{table} (time, symbol, open, high, low, close, volume)
        VALUES (%(time)s, %(symbol)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
        ON CONFLICT (time, symbol) DO NOTHING;
        """.strip()

        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(query, data)
        except Exception as e:
            self.connection.rollback()
            raise RuntimeError(f"Failed to insert data into {schema}.{table}: {e}")
        
    def close(self) -> None:
        """
        Closes the database connection if it is open.
        """
        if self.connection:
            self.connection.close()
            self.connection = None
