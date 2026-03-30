import os
import psycopg2
from typing import List, Dict, Any
from src.modules.inserter.inserter import Inserter
import logging

class EIAInserter(Inserter):
    """
    Specialized Inserter for EIA Data.
    Handles dynamic table creation and multi-dimensional Primary Keys 
    to ensure granular data (by country, grade, etc.) is preserved.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config=config)
        self.connection = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def connect(self) -> None:
        """Establishes connection using environment variables."""
        try:
            self.connection = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT")
            )
            self.connection.autocommit = True
            self.logger.info("EIA Inserter connected to database.")
        except psycopg2.OperationalError as e:
            self.connection = None
            self.logger.error(f"Database connection failed: {e}")
            raise ConnectionError(f"Database connection failed: {e}")

    def insert_data(self, data: List[Dict[str, Any]], schema: str = "eia", table: str = "market_data") -> None:
        if not self.connection or not data:
            self.logger.warning("No connection or no data to insert.")
            return

        # 1. Standardize identifier names
        schema_table = f'"{schema}"."{table}"'
        columns = list(data[0].keys())
        column_names = ", ".join([f'"{col}"' for col in columns])
        placeholders = ", ".join([f"%({col})s" for col in columns])

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

                # 2. Build Table Definition & Dynamic Primary Key
                col_defs = []
                pk_cols = []
                
                # Columns that should NOT be part of the Primary Key
                # We want everything else (dimensions) to be part of the PK
                exclude_from_pk = ['value', 'period-description', 'units']

                for col in columns:
                    if col == "time":
                        col_defs.append(f'"{col}" TIMESTAMPTZ')
                        pk_cols.append(f'"{col}"')
                    elif col == "value":
                        col_defs.append(f'"{col}" DOUBLE PRECISION')
                    else:
                        col_defs.append(f'"{col}" TEXT')
                        # If it's a descriptive dimension (origin, grade, area), 
                        # it must be in the PK so rows remain unique.
                        if col not in exclude_from_pk:
                            pk_cols.append(f'"{col}"')

                pk_string = ", ".join(pk_cols)
                
                # 3. Create Table with Multi-Column Primary Key
                create_query = f"""
                CREATE TABLE IF NOT EXISTS {schema_table} (
                    {", ".join(col_defs)},
                    PRIMARY KEY ({pk_string})
                );
                """
                cursor.execute(create_query)

                # 4. UPSERT logic (ON CONFLICT DO NOTHING)
                # Since the PK now includes all dimensions, only true duplicates 
                # (same time, same country, same grade) will be ignored.
                insert_query = f"""
                INSERT INTO {schema_table} ({column_names})
                VALUES ({placeholders})
                ON CONFLICT ({pk_string}) DO NOTHING;
                """
                
                cursor.executemany(insert_query, data)
                
            self.logger.info(f"Successfully synced {len(data)} rows to {schema_table}")

        except Exception as e:
            self.logger.error(f"EIA Insertion failed for {table}: {e}")
            raise RuntimeError(f"EIA Inserter Error: {e}")

    def close(self) -> None:
        if self.connection:
            self.connection.close()
            self.connection = None