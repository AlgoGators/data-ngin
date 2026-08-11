import os
import logging
import threading
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
from psycopg2 import pool as psycopg2_pool

from src.infrastructure.inserter import Inserter


class OhlcvRepository(Inserter):
    """
    Consolidated repository for the OHLCV tables: reads (previously
    `DataAccess`, SQLAlchemy ORM) and writes (previously `TimescaleDBInserter`,
    raw psycopg2 with `ON CONFLICT DO NOTHING` upsert) reimplemented against a
    pooled psycopg2 connection.

    Consolidated *toward* the psycopg2/upsert implementation deliberately --
    it's the one with correct semantics (idempotent multi-table writes for
    raw vs. cleaned data); the ORM side never had upsert handling and was
    bound to a single table, so merging the other way would have regressed
    write idempotency.

    Connections are pooled (class-level `psycopg2.pool.ThreadedConnectionPool`,
    shared by every `OhlcvRepository` instance in the process): the
    orchestrator builds a fresh repository per symbol and the API server
    builds one per GraphQL request, and both used to pay a full TCP+auth
    handshake plus 3 diagnostic queries on every single `connect()`.
    connect()/close() now borrow/return a connection from the pool instead of
    opening/closing a real one each time; the diagnostic queries only run
    once per process (see `_diagnostics_logged`), not once per connect().
    """

    _pool: Optional["psycopg2_pool.ThreadedConnectionPool"] = None
    _pool_lock = threading.Lock()
    _diagnostics_logged: bool = False
    _verified_tables: Set[Tuple[str, str]] = set()

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config=config)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    @classmethod
    def _get_pool(cls) -> "psycopg2_pool.ThreadedConnectionPool":
        if cls._pool is None:
            with cls._pool_lock:
                if cls._pool is None:
                    cls._pool = psycopg2_pool.ThreadedConnectionPool(
                        int(os.getenv("DB_POOL_MIN_CONN", "1")),
                        int(os.getenv("DB_POOL_MAX_CONN", "10")),
                        dbname=os.getenv("DB_NAME"),
                        user=os.getenv("DB_USER"),
                        password=os.getenv("DB_PASSWORD"),
                        host=os.getenv("DB_HOST"),
                        port=os.getenv("DB_PORT"),
                    )
        return cls._pool

    @classmethod
    def reset_pool_for_testing(cls) -> None:
        """
        Test-only: drop the cached pool and cached diagnostic/verification
        state. The pool and its caches are process-lifetime singletons by
        design (that's the point -- one real pool, not one per instance);
        tests that patch psycopg2.connect need a clean slate per test rather
        than reusing whatever pool an earlier test's mock built.
        """
        if cls._pool is not None:
            try:
                cls._pool.closeall()
            except Exception:
                pass
        cls._pool = None
        cls._diagnostics_logged = False
        cls._verified_tables = set()

    def connect(self) -> None:
        """
        Borrows a connection from the shared pool (creating the pool on first
        use). Diagnostic queries (server endpoint, search_path, schema list)
        only run once per process, the first time any repository connects --
        not on every connect() call, since with pooling that could be once
        per symbol/request instead of once ever.

        Raises:
            ConnectionError: If a connection cannot be acquired.
        """
        try:
            self.connection = self._get_pool().getconn()
            self.connection.autocommit = True

            if not OhlcvRepository._diagnostics_logged:
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
                OhlcvRepository._diagnostics_logged = True

            self.logger.info("Connected to TimescaleDB successfully (pooled).")
        except psycopg2.OperationalError as e:
            self.connection = None
            raise ConnectionError(f"Failed to connect to TimescaleDB: {e}")
        except psycopg2_pool.PoolError as e:
            self.connection = None
            raise ConnectionError(f"Failed to acquire a pooled connection: {e}")

    def _ensure_connected(self) -> None:
        if not self.connection:
            self.connect()

    def insert_data(self, data: List[Dict[str, Any]], schema: str, table: str) -> None:
        """
        Inserts data into the specified TimescaleDB schema and table dynamically.

        Args:
            data (List[Dict[str, Any]]): A list of dictionaries representing data rows.
            schema (str): The target schema in TimescaleDB.
            table (str): The target table in TimescaleDB.

        Raises:
            RuntimeError: If the connection is missing or the insertion into the database fails.
        """
        if not data:
            # Routine, not an error: a symbol can legitimately have no rows
            # for a given window (holiday-only range, newly-listed/delisted
            # contract, empty batch). No DB work needed either -- skip before
            # the connection check so this no-op doesn't require a connection.
            self.logger.info(f"No rows to insert into {schema}.{table}; skipping.")
            return
        if not self.connection:
            raise RuntimeError("Database connection is not established.")

        # Schema/table existence only needs checking once per (schema, table)
        # for the process's lifetime, not on every insert -- these don't
        # change between calls, and the orchestrator calls insert_data twice
        # per symbol (raw + cleaned tables) for every symbol in a run.
        table_key = (schema, table)
        if table_key not in OhlcvRepository._verified_tables:
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
            OhlcvRepository._verified_tables.add(table_key)

        columns = list(data[0].keys())
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
        Returns the connection to the shared pool (rather than closing the
        underlying TCP connection) so the next connect() -- by this instance
        or another -- can reuse it.
        """
        if self.connection:
            try:
                self._get_pool().putconn(self.connection)
                self.logger.info("Connection returned to pool.")
            except Exception:
                # Pool may be unusable (e.g. already closed) -- fall back to
                # closing the connection directly so it isn't leaked.
                try:
                    self.connection.close()
                except Exception:
                    pass
            self.connection = None

    def _default_schema_table(self, schema: Optional[str], table: Optional[str]) -> tuple:
        database_config = self.config.get("database", {})
        return (
            schema or database_config.get("target_schema"),
            table or database_config.get("table"),
        )

    def get_ohlcv_data(
        self,
        start_date: str,
        end_date: str,
        symbols: Optional[List[str]] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves OHLCV rows for the given date range and (optional) symbols,
        ordered by symbol then time -- same contract as the old
        DataAccess.get_ohlcv_data.
        """
        self._ensure_connected()
        schema, table = self._default_schema_table(schema, table)

        query = f"""
            SELECT time, symbol, open, high, low, close, volume
            FROM {schema}.{table}
            WHERE time BETWEEN %s AND %s
        """
        params: List[Any] = [start_date, end_date]
        if symbols:
            query += " AND symbol = ANY(%s)"
            params.append(symbols)
        query += " ORDER BY symbol, time"

        with self.connection.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_symbols(self, schema: Optional[str] = None, table: Optional[str] = None) -> List[str]:
        """Retrieves all unique symbols in the target table."""
        self._ensure_connected()
        schema, table = self._default_schema_table(schema, table)

        with self.connection.cursor() as cur:
            cur.execute(f"SELECT DISTINCT symbol FROM {schema}.{table}")
            return [row[0] for row in cur.fetchall()]

    def get_earliest_date(self, schema: Optional[str] = None, table: Optional[str] = None) -> Optional[str]:
        """Retrieves the earliest date in the target table, or None if empty."""
        self._ensure_connected()
        schema, table = self._default_schema_table(schema, table)

        with self.connection.cursor() as cur:
            cur.execute(f"SELECT MIN(time) FROM {schema}.{table}")
            row = cur.fetchone()
            if row and row[0]:
                self.logger.info(f"Earliest available date in the database: {row[0]}")
                return row[0].strftime("%Y-%m-%d")
            return None

    def get_latest_date(self, schema: Optional[str] = None, table: Optional[str] = None) -> Optional[str]:
        """Retrieves the latest date in the target table, or None if empty."""
        self._ensure_connected()
        schema, table = self._default_schema_table(schema, table)

        with self.connection.cursor() as cur:
            cur.execute(f"SELECT MAX(time) FROM {schema}.{table}")
            row = cur.fetchone()
            if row and row[0]:
                self.logger.info(f"Latest available date in the database: {row[0]}")
                return row[0].strftime("%Y-%m-%d")
            return None

    def get_latest_data(
        self, symbol: str, schema: Optional[str] = None, table: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Retrieves the most recent OHLCV row for a given symbol."""
        self._ensure_connected()
        schema, table = self._default_schema_table(schema, table)

        query = f"""
            SELECT time, symbol, open, high, low, close, volume
            FROM {schema}.{table}
            WHERE symbol = %s
            ORDER BY time DESC
            LIMIT 1
        """
        with self.connection.cursor() as cur:
            cur.execute(query, (symbol,))
            row = cur.fetchone()
            if not row:
                return None
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))

    def delete_data(
        self,
        start_date: str,
        end_date: str,
        symbols: Optional[List[str]] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> None:
        """Deletes rows in the given date range and (optional) symbols."""
        self._ensure_connected()
        schema, table = self._default_schema_table(schema, table)

        query = f"DELETE FROM {schema}.{table} WHERE time BETWEEN %s AND %s"
        params: List[Any] = [start_date, end_date]
        if symbols:
            query += " AND symbol = ANY(%s)"
            params.append(symbols)

        with self.connection.cursor() as cur:
            cur.execute(query, params)
            self.logger.info(f"Deleted {cur.rowcount} records successfully.")
