import os
import unittest

import psycopg2

from src.api.keys import authenticate, generate_key

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
class TestAuthenticate(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg2.connect(**_dsn())
        self.conn.autocommit = True
        self.plaintext, key_hash, prefix = generate_key("db_readwrite")
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO auth.api_keys"
                " (email, name, db_role, key_hash, key_prefix, max_concurrent)"
                " VALUES (%s, %s, %s, %s, %s, %s)",
                ("test-auth@x.com", "Test Person", "db_readwrite", key_hash, prefix, 3),
            )

    def tearDown(self):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM auth.api_keys WHERE email LIKE 'test-%'")
        self.conn.close()

    def test_valid_key_returns_the_caller(self):
        caller = authenticate(self.conn, self.plaintext)
        self.assertIsNotNone(caller)
        self.assertEqual(caller.email, "test-auth@x.com")
        self.assertEqual(caller.name, "Test Person")
        self.assertEqual(caller.db_role, "db_readwrite")
        self.assertEqual(caller.max_concurrent, 3)
        self.assertEqual(caller.statement_timeout_ms, 120000)

    def test_unknown_key_returns_none(self):
        self.assertIsNone(authenticate(self.conn, "ag_rw_notarealkey"))

    def test_inactive_key_returns_none(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE auth.api_keys SET active = false WHERE email = %s",
                ("test-auth@x.com",),
            )
        self.assertIsNone(authenticate(self.conn, self.plaintext))

    def test_empty_key_returns_none(self):
        self.assertIsNone(authenticate(self.conn, ""))

    def test_successful_auth_records_last_used(self):
        authenticate(self.conn, self.plaintext)
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT last_used_at FROM auth.api_keys WHERE email = %s",
                ("test-auth@x.com",),
            )
            self.assertIsNotNone(cur.fetchone()[0])
