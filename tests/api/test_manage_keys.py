import os
import unittest

import psycopg2
from fastapi.testclient import TestClient

from scripts.manage_api_keys import create_key, list_keys, revoke_key
from src.api.app import LOGIN_BY_ROLE, app
from src.api.keys import authenticate

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
class TestManageKeys(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg2.connect(**_dsn())
        self.conn.autocommit = True

    def tearDown(self):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM auth.api_keys WHERE email LIKE 'test-%'")
        self.conn.close()

    def test_created_key_authenticates(self):
        plaintext = create_key(
            self.conn, "test-cli@x.com", "CLI Person", "db_readwrite", "admin@x.com"
        )
        caller = authenticate(self.conn, plaintext)
        self.assertIsNotNone(caller)
        self.assertEqual(caller.name, "CLI Person")
        self.assertEqual(caller.db_role, "db_readwrite")

    def test_plaintext_is_not_stored(self):
        plaintext = create_key(
            self.conn, "test-cli2@x.com", "P", "db_readonly", "admin@x.com"
        )
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT key_hash, key_prefix FROM auth.api_keys WHERE email = %s",
                ("test-cli2@x.com",),
            )
            key_hash, prefix = cur.fetchone()
        self.assertNotEqual(key_hash, plaintext)
        self.assertNotIn(plaintext, key_hash)
        self.assertTrue(plaintext.startswith(prefix))

    def test_rotating_invalidates_the_old_key(self):
        first = create_key(
            self.conn, "test-rot@x.com", "R", "db_readonly", "admin@x.com"
        )
        second = create_key(
            self.conn, "test-rot@x.com", "R", "db_readonly", "admin@x.com"
        )
        self.assertIsNone(authenticate(self.conn, first))
        self.assertIsNotNone(authenticate(self.conn, second))

    def test_revoking_invalidates_the_key(self):
        plaintext = create_key(
            self.conn, "test-rev@x.com", "V", "db_readonly", "admin@x.com"
        )
        self.assertTrue(revoke_key(self.conn, "test-rev@x.com"))
        self.assertIsNone(authenticate(self.conn, plaintext))

    def test_revoking_someone_absent_returns_false(self):
        self.assertFalse(revoke_key(self.conn, "test-nobody@x.com"))

    def test_revoked_row_is_retained(self):
        """Deleting the row would lose the answer to 'who had access in March?'"""
        create_key(self.conn, "test-keep@x.com", "K", "db_readonly", "admin@x.com")
        revoke_key(self.conn, "test-keep@x.com")
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT active, revoked_at FROM auth.api_keys WHERE email = %s",
                ("test-keep@x.com",),
            )
            active, revoked_at = cur.fetchone()
        self.assertFalse(active)
        self.assertIsNotNone(revoked_at)

    def test_list_omits_hashes(self):
        create_key(self.conn, "test-list@x.com", "L", "db_readonly", "admin@x.com")
        rows = [r for r in list_keys(self.conn) if r["email"] == "test-list@x.com"]
        self.assertEqual(len(rows), 1)
        self.assertNotIn("key_hash", rows[0])

    def test_invalid_role_is_rejected(self):
        with self.assertRaises(ValueError):
            create_key(self.conn, "test-bad@x.com", "B", "db_root", "admin@x.com")


@unittest.skipUnless(
    all(os.environ.get(k) for k in REQUIRED),
    "database env vars not set; integration test skipped",
)
class TestCliKeysWorkOverHttp(unittest.TestCase):
    """The CLI's output has to be usable by the service, not merely by
    authenticate().

    A key that resolves to a Caller but is rejected at the HTTP layer -- wrong
    role name, a prefix the service truncates differently, a row the service
    cannot read -- would look correct in every unit test and fail for the person
    it was issued to. So these go through the app the way a caller does.
    """

    PASSWORD = "cli_probe_password"

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)
        cls.admin = psycopg2.connect(**_dsn())
        cls.admin.autocommit = True
        with cls.admin.cursor() as cur:
            for login, _ in LOGIN_BY_ROLE.values():
                cur.execute(f"ALTER ROLE {login} WITH PASSWORD %s", (cls.PASSWORD,))

        cls._saved_env = {}
        env = {
            "API_DB_HOST": os.environ["DB_HOST"],
            "API_DB_PORT": os.environ["DB_PORT"],
            "API_DB_NAME": os.environ["DB_NAME"],
        }
        for _, var in LOGIN_BY_ROLE.values():
            env[var] = cls.PASSWORD
        for var, value in env.items():
            cls._saved_env[var] = os.environ.get(var)
            os.environ[var] = value

    @classmethod
    def tearDownClass(cls):
        with cls.admin.cursor() as cur:
            cur.execute("DELETE FROM auth.api_keys WHERE email LIKE 'test-%'")
            cur.execute(
                "DELETE FROM auth.audit_log WHERE actor_email LIKE 'test-%'"
                " OR actor_email = 'unknown'"
            )
            for login, _ in LOGIN_BY_ROLE.values():
                cur.execute(
                    f"ALTER ROLE {login} WITH PASSWORD %s",
                    ("CHANGE_ME_BEFORE_DEPLOY",),
                )
        cls.admin.close()
        for var, value in cls._saved_env.items():
            if value is None:
                os.environ.pop(var, None)
            else:
                os.environ[var] = value

    def setUp(self):
        self.conn = psycopg2.connect(**_dsn())
        self.conn.autocommit = True

    def tearDown(self):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM auth.api_keys WHERE email LIKE 'test-%'")
        self.conn.close()

    def _post(self, sql, key):
        return self.client.post(
            "/v1/query", json={"sql": sql},
            headers={"Authorization": f"Bearer {key}"},
        )

    def test_a_cli_key_queries_the_api(self):
        plaintext = create_key(
            self.conn, "test-http@x.com", "HTTP", "db_readonly", "admin@x.com"
        )
        response = self._post("SELECT 1 AS n", plaintext)
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["columns"], ["n"])
        self.assertEqual(body["rows"], [[1]])
        self.assertEqual(body["row_count"], 1)

    def test_revocation_takes_effect_on_the_next_request(self):
        """Revoking has to lock someone out now, not at the next restart. There
        is no cache to invalidate precisely because of this."""
        plaintext = create_key(
            self.conn, "test-http2@x.com", "HTTP2", "db_readonly", "admin@x.com"
        )
        self.assertEqual(self._post("SELECT 1", plaintext).status_code, 200)

        self.assertTrue(revoke_key(self.conn, "test-http2@x.com"))
        self.assertEqual(self._post("SELECT 1", plaintext).status_code, 401)

    def test_plaintext_reaches_neither_the_key_table_nor_the_audit_log(self):
        """Checked against the whole row, not the columns we expect to be safe.
        A key that survives anywhere persistent is a working credential sitting
        in the two tables most worth reading.
        """
        plaintext = create_key(
            self.conn, "test-http3@x.com", "HTTP3", "db_readonly", "admin@x.com"
        )
        self._post("SELECT 1", plaintext)

        pattern = f"%{plaintext}%"
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM auth.api_keys k WHERE k::text LIKE %s",
                (pattern,),
            )
            self.assertEqual(cur.fetchone()[0], 0, "key found in auth.api_keys")

            cur.execute(
                "SELECT count(*) FROM auth.audit_log a WHERE a::text LIKE %s",
                (pattern,),
            )
            self.assertEqual(cur.fetchone()[0], 0, "key found in auth.audit_log")
