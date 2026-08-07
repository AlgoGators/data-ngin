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
class TestApiGatewayTables(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg2.connect(**_dsn())
        self.conn.autocommit = True

    def tearDown(self):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM auth.api_keys WHERE email LIKE 'test-%'")
        self.conn.close()

    def test_rejects_unknown_role(self):
        """A typo in db_role must be impossible to store, not something that
        surfaces at request time."""
        with self.assertRaises(psycopg2.errors.CheckViolation):
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO auth.api_keys (email, name, db_role, key_hash, key_prefix)"
                    " VALUES (%s, %s, %s, %s, %s)",
                    ("test-a@x.com", "A", "db_readwrit", "h1", "ag_rw_1"),
                )

    def test_accepts_valid_role(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO auth.api_keys (email, name, db_role, key_hash, key_prefix)"
                " VALUES (%s, %s, %s, %s, %s)",
                ("test-b@x.com", "B", "db_readwrite", "h2", "ag_rw_2"),
            )
            cur.execute(
                "SELECT active, max_concurrent, statement_timeout_ms"
                " FROM auth.api_keys WHERE email = %s",
                ("test-b@x.com",),
            )
            active, max_concurrent, timeout = cur.fetchone()
        self.assertTrue(active)
        self.assertEqual(max_concurrent, 1)
        self.assertEqual(timeout, 120000)

    def test_key_hash_is_unique(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO auth.api_keys (email, name, db_role, key_hash, key_prefix)"
                " VALUES (%s, %s, %s, %s, %s)",
                ("test-c@x.com", "C", "db_readonly", "dup", "ag_ro_1"),
            )
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO auth.api_keys (email, name, db_role, key_hash, key_prefix)"
                    " VALUES (%s, %s, %s, %s, %s)",
                    ("test-d@x.com", "D", "db_readonly", "dup", "ag_ro_2"),
                )

    def test_audit_log_accepts_a_row(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO auth.audit_log"
                " (actor_email, actor_name, actor_role, statement, outcome)"
                " VALUES (%s, %s, %s, %s, %s) RETURNING id, occurred_at",
                ("test-e@x.com", "E", "db_readonly", "SELECT 1", "success"),
            )
            row_id, occurred = cur.fetchone()
            cur.execute("DELETE FROM auth.audit_log WHERE id = %s", (row_id,))
        self.assertIsNotNone(occurred)
