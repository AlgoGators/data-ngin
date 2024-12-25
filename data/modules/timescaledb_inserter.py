import os
import psycopg2
from typing import List, Dict, Any, Optional
from data.modules.inserter import Inserter
import logging

class TimescaleDBInserter(Inserter):
    """
    Inserter subclass for dynamically inserting data into TimescaleDB.

    Methods:
        connect: Establish a connection to the TimescaleDB database.
        insert_data: Insert data into the specified schema and table dynamically.
        close: Close the database connection.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the TimescaleDBInserter with configuration settings.

        Args:
            config (Dict[str, Any]): Configuration settings.
        """
        super().__init__(config=config)
        self.connection = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

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
            self.logger.info("Connected to TimescaleDB successfully.")
        except psycopg2.OperationalError as e:
            self.connection = None
            raise ConnectionError(f"Failed to connect to TimescaleDB: {e}")

    def insert_data(self, data: List[Dict[str, Any]], schema: str, table: str) -> None:
        """
        Inserts data into the specified TimescaleDB schema and table dynamically.

        Args:
            data (List[Dict[str, Any]]): A list of dictionaries representing data rows.
            schema (str): The target schema in TimescaleDB.
            table (str): The target table in TimescaleDB.

        Raises:
            ValueError: If the data is empty or columns are not specified.
            RuntimeError: If the insertion into the database fails.
        """
        if not self.connection:
            raise RuntimeError("Database connection is not established.")
        
        # Determine columns based on first row of data 
        columns = list(data[0].keys())

        # Dynamically construct query based on provided columns
        column_names = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])
        query = f"""
        INSERT INTO {schema}.{table} ({column_names})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING;
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
            self.logger.info("Database connection closed.")