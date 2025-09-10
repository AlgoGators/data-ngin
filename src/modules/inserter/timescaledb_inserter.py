import os
import psycopg2
from typing import List, Dict, Any, Optional
from src.modules.inserter.inserter import Inserter
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
            with self.connection.cursor() as cur:
                cur.execute("SELECT inet_server_addr(), inet_server_port(), current_database(), current_user")
                host, port, dbname, dbuser = cur.fetchone()
                self.logger.info("DB endpoint -> host=%s port=%s db=%s user=%s", host, port, dbname, dbuser)

                cur.execute("SHOW search_path")
                sp = cur.fetchone()[0]
                self.logger.info("search_path=%s", sp)

                cur.execute("""
                    SELECT schema_name FROM information_schema.schemata ORDER BY schema_name
                """)
                schemas = [r[0] for r in cur.fetchall()]
                self.logger.info("Schemas present: %s", ", ".join(schemas))
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
        schema_exists_sql = """
        SELECT 1 FROM information_schema.schemata WHERE schema_name = %s
        """
        table_exists_sql = """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """

        with self.connection.cursor() as cur:
            cur.execute(schema_exists_sql, (schema,))
            if cur.fetchone() is None:
                raise RuntimeError(
                    f"Target schema '{schema}' not found. You are likely on the wrong DB/host/port. "
                    "Check DB_HOST/DB_PORT in docker-compose for data-engine."
                )
            cur.execute(table_exists_sql, (schema, table))
            if cur.fetchone() is None:
                cur.execute("""
                    SELECT table_schema||'.'||table_name
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog','information_schema')
                    ORDER BY 1
                """)
                existing = [r[0] for r in cur.fetchall()]
                raise RuntimeError(
                    f"Target table '{schema}.{table}' not found. Existing tables: {existing[:50]}"
                )

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
        except psycopg2.Error as e:
            self.connection.rollback()
            self.logger.error("PG error code=%s detail=%s", getattr(e, "pgcode", None), getattr(getattr(e, "diag", None), "message_detail", None))
            raise RuntimeError(f"Failed to insert into {schema}.{table}: {e}")
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