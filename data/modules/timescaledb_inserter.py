import os
import psycopg2
from typing import List, Dict, Any, Optional, Tuple
from data.modules.inserter import Inserter
import logging


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
        self.connection = None
        self.logger = logging.getLogger(self.__class__.__name__)

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
            self.connection = None
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
        if not self.connection:
            raise RuntimeError("Database connection is not established.")
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
            self.logger.info(f"Inserted {len(data)} rows into {schema}.{table}")
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

    def get_date_range(self, schema: str, table: str) -> Optional[Tuple[str, str]]:
        """
        Retrieves the earliest and latest dates from the specified schema and table.

        Args:
            schema (str): The schema name (e.g., 'futures_data').
            table (str): The table name (e.g., 'ohlcv_1d').

        Returns:
            Optional[Tuple[str, str]]: A tuple containing the earliest and latest dates in 'YYYY-MM-DD' format,
            or None if the table is empty.
        """
        query: str = f"""
        SELECT 
            MIN(time) AS earliest_date,
            MAX(time) AS latest_date
        FROM {schema}.{table};
        """.strip()

        try:
            connection: psycopg2.extensions.connection = self.connection
            with connection.cursor() as cursor:  # Cursor type is inferred by psycopg2
                cursor.execute(query)
                result: Optional[Tuple[Optional[str], Optional[str]]] = cursor.fetchone()

            # If result contains valid dates, return them directly
            if result and result[0] and result[1]:
                earliest_date: str = result[0]
                latest_date: str = result[1]
                return earliest_date, latest_date

            # If no dates are found, return None
            return None

        except Exception as e:
            raise RuntimeError(f"Failed to retrieve date range from {schema}.{table}: {e}")
