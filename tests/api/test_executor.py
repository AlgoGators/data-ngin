import os
import unittest

import psycopg2

from src.api.executor import PermissionDenied, execute_as
from src.api.keys import Caller

REQUIRED = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")


def _dsn():
    return dict(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ["DB_NAME"],
    )


def _caller(role):
    return Caller(
        email="test-exec@x.com",
        name="Exec",
        db_role=role,
        key_prefix="ag_xx_1",
        max_concurrent=1,
        statement_timeout_ms=5000,
    )


@unittest.skipUnless(
    all(os.environ.get(k) for k in REQUIRED),
    "database env vars not set; integration test skipped",
)
class TestExecuteAs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        admin = psycopg2.connect(**_dsn())
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS research")
            cur.execute("CREATE SCHEMA IF NOT EXISTS trading")
            cur.execute("CREATE TABLE IF NOT EXISTS research.probe (id int)")
            cur.execute("CREATE TABLE IF NOT EXISTS trading.probe (id int)")
            cur.execute(
                "GRANT USAGE ON SCHEMA research, trading"
                " TO db_readonly, db_readwrite, db_readwrite_all"
            )
            cur.execute(
                "GRANT SELECT ON research.probe, trading.probe"
                " TO db_readonly, db_readwrite, db_readwrite_all"
            )
            cur.execute("GRANT INSERT ON research.probe TO db_readwrite")
            cur.execute("GRANT INSERT ON trading.probe TO db_readwrite_all")
        admin.close()

    def setUp(self):
        self.conn = psycopg2.connect(**_dsn())

    def tearDown(self):
        self.conn.close()

    def test_select_returns_rows(self):
        result = execute_as(self.conn, _caller("db_readonly"), "SELECT 1 AS n", 100)
        self.assertEqual(result.columns, ["n"])
        self.assertEqual(result.rows, [[1]])
        self.assertEqual(result.row_count, 1)
        self.assertFalse(result.truncated)

    def test_readonly_cannot_write(self):
        with self.assertRaises(PermissionDenied):
            execute_as(
                self.conn, _caller("db_readonly"),
                "INSERT INTO research.probe VALUES (1)", 100,
            )

    def test_readwrite_can_write_research(self):
        execute_as(
            self.conn, _caller("db_readwrite"),
            "INSERT INTO research.probe VALUES (1)", 100,
        )

    def test_readwrite_cannot_write_trading(self):
        """The carve-out that matters most: quant dev may correct market data
        but not live positions."""
        with self.assertRaises(PermissionDenied):
            execute_as(
                self.conn, _caller("db_readwrite"),
                "INSERT INTO trading.probe VALUES (1)", 100,
            )

    def test_sql_hidden_in_a_cte_is_still_refused(self):
        """The reason enforcement is Postgres's job: this is one of several
        forms a parser would have to recognise, and it does not have to."""
        with self.assertRaises(PermissionDenied):
            execute_as(
                self.conn, _caller("db_readwrite"),
                "WITH x AS (SELECT 1) INSERT INTO trading.probe SELECT * FROM x",
                100,
            )

    def test_role_does_not_leak_to_the_next_query(self):
        """SET LOCAL is scoped to the transaction. If it leaked, a low-privilege
        caller could inherit a previous caller's role on a pooled connection."""
        execute_as(self.conn, _caller("db_readwrite_all"), "SELECT 1", 100)
        with self.assertRaises(PermissionDenied):
            execute_as(
                self.conn, _caller("db_readonly"),
                "INSERT INTO research.probe VALUES (2)", 100,
            )

    def test_row_limit_truncates_and_reports_it(self):
        result = execute_as(
            self.conn, _caller("db_readonly"),
            "SELECT generate_series(1, 500) AS n", 10,
        )
        self.assertEqual(result.row_count, 10)
        self.assertTrue(result.truncated)

    def test_statement_without_rows_reports_zero(self):
        result = execute_as(
            self.conn, _caller("db_readwrite"),
            "DELETE FROM research.probe WHERE false", 100,
        )
        self.assertEqual(result.rows, [])
        self.assertFalse(result.truncated)

    def test_syntax_error_is_not_a_permission_error(self):
        with self.assertRaises(psycopg2.Error):
            execute_as(self.conn, _caller("db_readonly"), "SELEC 1", 100)
