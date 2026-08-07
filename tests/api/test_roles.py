import os
import unittest

import psycopg2

REQUIRED = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")


def _dsn():
    return dict(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ["DB_NAME"],
    )


@unittest.skipUnless(
    all(os.environ.get(k) for k in REQUIRED),
    "database env vars not set; integration test skipped",
)
class TestRoleGrants(unittest.TestCase):
    """The roles are the only thing enforcing permissions, so these assertions
    are the security boundary. Mocking them would prove nothing."""

    @classmethod
    def setUpClass(cls):
        cls.conn = psycopg2.connect(**_dsn())
        cls.conn.autocommit = True
        with cls.conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS equities_data")
            cur.execute("CREATE SCHEMA IF NOT EXISTS trading")
            cur.execute("CREATE SCHEMA IF NOT EXISTS auth")
            cur.execute(
                "CREATE TABLE IF NOT EXISTS equities_data.probe (id int)"
            )
            cur.execute("CREATE TABLE IF NOT EXISTS trading.probe (id int)")

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def _write_allowed(self, role, table):
        """True if `role` may INSERT into `table`."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT has_table_privilege(%s, %s, 'INSERT')", (role, table))
            return cur.fetchone()[0]

    def _read_allowed(self, role, table):
        with self.conn.cursor() as cur:
            cur.execute("SELECT has_table_privilege(%s, %s, 'SELECT')", (role, table))
            return cur.fetchone()[0]

    def test_readonly_can_read_market_data(self):
        self.assertTrue(self._read_allowed("db_readonly", "equities_data.probe"))

    def test_readonly_cannot_write_market_data(self):
        self.assertFalse(self._write_allowed("db_readonly", "equities_data.probe"))

    def test_readwrite_can_write_market_data(self):
        self.assertTrue(self._write_allowed("db_readwrite", "equities_data.probe"))

    def test_readwrite_cannot_write_trading(self):
        self.assertFalse(self._write_allowed("db_readwrite", "trading.probe"))

    def test_readwrite_all_can_write_trading(self):
        self.assertTrue(self._write_allowed("db_readwrite_all", "trading.probe"))

    def test_api_service_has_no_inherited_privileges(self):
        """api_service is NOINHERIT: it must SET ROLE to do anything, so a bug
        that skips SET ROLE fails closed rather than running with full access."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT rolinherit FROM pg_roles WHERE rolname = 'api_service'"
            )
            row = cur.fetchone()
        self.assertIsNotNone(row, "api_service role does not exist")
        self.assertFalse(row[0], "api_service must be NOINHERIT")
