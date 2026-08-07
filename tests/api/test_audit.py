import os
import unittest

import psycopg2

from src.api.audit import record, record_anonymous
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


CALLER = Caller(
    email="test-audit@x.com",
    name="Audit Person",
    db_role="db_readwrite",
    key_prefix="ag_rw_9",
    max_concurrent=1,
    statement_timeout_ms=1000,
)


@unittest.skipUnless(
    all(os.environ.get(k) for k in REQUIRED),
    "database env vars not set; integration test skipped",
)
class TestAudit(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg2.connect(**_dsn())
        self.conn.autocommit = True

    def tearDown(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM auth.audit_log WHERE actor_email LIKE 'test-%'"
                " OR actor_email = 'unknown'"
            )
        self.conn.close()

    def _latest(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT actor_email, actor_name, actor_role, statement, outcome,"
                "       row_count, error_message, key_prefix"
                "  FROM auth.audit_log ORDER BY id DESC LIMIT 1"
            )
            return cur.fetchone()

    def test_success_is_recorded_with_identity(self):
        record(self.conn, CALLER, "SELECT 1", "success", row_count=1, duration_ms=5)
        email, name, role, stmt, outcome, rows, err, prefix = self._latest()
        self.assertEqual(email, "test-audit@x.com")
        self.assertEqual(name, "Audit Person")
        self.assertEqual(role, "db_readwrite")
        self.assertEqual(stmt, "SELECT 1")
        self.assertEqual(outcome, "success")
        self.assertEqual(rows, 1)

    def test_denial_is_recorded(self):
        """A log recording only successes says nothing about someone probing
        for access."""
        record(
            self.conn, CALLER, "INSERT INTO trading.probe VALUES (1)", "denied",
            error_message="permission denied for table probe",
        )
        _, _, _, _, outcome, _, err, _ = self._latest()
        self.assertEqual(outcome, "denied")
        self.assertIn("permission denied", err)

    def test_rate_limit_is_recorded(self):
        record(self.conn, CALLER, "SELECT 1", "rate_limited")
        self.assertEqual(self._latest()[4], "rate_limited")

    def test_unauthenticated_attempt_is_recorded(self):
        record_anonymous(self.conn, "denied", error_message="unknown key")
        email, name, role, _, outcome, _, _, _ = self._latest()
        self.assertEqual(email, "unknown")
        self.assertEqual(outcome, "denied")

    def test_anonymous_record_never_stores_a_key(self):
        """key_prefix identifies which key was tried; the key itself must never
        reach the log."""
        record_anonymous(
            self.conn, "denied", key_prefix="ag_rw_123", error_message="unknown key"
        )
        self.assertEqual(self._latest()[7], "ag_rw_123")

    def test_a_full_key_passed_as_prefix_is_truncated(self):
        """key_prefix identifies which key was tried; it must never be able to
        hold a working one. The column is unbounded TEXT, so a caller passing
        the raw Authorization header would otherwise write a valid key into the
        one table an attacker would most want to read. Truncation happens in
        audit.py so it cannot be forgotten at a call site."""
        from src.api.keys import PREFIX_LENGTH, generate_key

        full_key, _, _ = generate_key("db_readonly")
        record_anonymous(self.conn, "denied", key_prefix=full_key)

        stored = self._latest()[7]
        self.assertEqual(len(stored), PREFIX_LENGTH)
        self.assertNotEqual(stored, full_key)
        self.assertTrue(full_key.startswith(stored))

    def test_a_full_key_on_an_authenticated_row_is_truncated_too(self):
        from src.api.keys import PREFIX_LENGTH, Caller, generate_key

        full_key, _, _ = generate_key("db_readwrite")
        oversized = Caller(
            email="test-audit@x.com", name="Audit Person",
            db_role="db_readwrite", key_prefix=full_key,
            max_concurrent=1, statement_timeout_ms=1000,
        )
        record(self.conn, oversized, "SELECT 1", "success")
        self.assertEqual(len(self._latest()[7]), PREFIX_LENGTH)
