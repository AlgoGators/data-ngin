import os
import unittest

import psycopg2
from fastapi.testclient import TestClient

from src.api.app import LOGIN_BY_ROLE, _client_ip, _connect, app
from src.api.keys import generate_key

REQUIRED = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME")


def _dsn():
    return dict(
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ["DB_NAME"],
    )


class TestConnectionSelection(unittest.TestCase):
    """The connection is chosen by the caller's role, not shared between roles.

    A single login that was a member of all three roles could be escalated by
    prefixing SET ROLE to the caller's SQL, because Postgres authorises SET ROLE
    against session_user. Selecting the login per role is what removes that, so
    the mapping itself is a security boundary and is asserted directly.
    """

    def test_each_role_maps_to_its_own_login(self):
        self.assertEqual(LOGIN_BY_ROLE["db_readonly"][0], "api_service_ro")
        self.assertEqual(LOGIN_BY_ROLE["db_readwrite"][0], "api_service_rw")
        self.assertEqual(LOGIN_BY_ROLE["db_readwrite_all"][0], "api_service_all")

    def test_no_login_serves_two_roles(self):
        logins = [login for login, _ in LOGIN_BY_ROLE.values()]
        self.assertEqual(len(logins), len(set(logins)))

    def test_unknown_role_is_refused_rather_than_defaulted(self):
        """Defaulting an unrecognised role to any login would silently run that
        caller's SQL with someone else's privileges."""
        with self.assertRaises(ValueError):
            _connect("db_superuser")

    def test_unknown_role_raises_before_reading_any_password(self):
        """The refusal must not depend on environment configuration, or a
        deployment with all three passwords set would connect anyway."""
        saved = {
            var: os.environ.pop(var, None)
            for _, var in LOGIN_BY_ROLE.values()
        }
        try:
            with self.assertRaises(ValueError):
                _connect("")
        finally:
            for var, value in saved.items():
                if value is not None:
                    os.environ[var] = value


class _FakeClient:
    def __init__(self, host):
        self.host = host


class _FakeRequest:
    def __init__(self, host):
        self.client = _FakeClient(host) if host is not None else None


class TestClientIp(unittest.TestCase):
    """audit_log.client_ip is INET and audit writes are swallowed, so a peer
    address Postgres cannot parse would silently discard the whole audit row --
    who ran what, and whether it was denied. Only the address may be lost."""

    def test_real_addresses_are_kept(self):
        self.assertEqual(_client_ip(_FakeRequest("10.1.2.3")), "10.1.2.3")
        self.assertEqual(_client_ip(_FakeRequest("::1")), "::1")

    def test_unparseable_address_becomes_none_rather_than_breaking_the_row(self):
        self.assertIsNone(_client_ip(_FakeRequest("testclient")))
        self.assertIsNone(_client_ip(_FakeRequest("")))

    def test_missing_client_is_tolerated(self):
        self.assertIsNone(_client_ip(_FakeRequest(None)))


@unittest.skipUnless(
    all(os.environ.get(k) for k in REQUIRED),
    "database env vars not set; integration test skipped",
)
class TestQueryEndpoint(unittest.TestCase):
    PASSWORD = "app_probe_password"

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)
        cls.admin = psycopg2.connect(**_dsn())
        cls.admin.autocommit = True
        with cls.admin.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS research")
            cur.execute("CREATE SCHEMA IF NOT EXISTS trading")
            cur.execute("CREATE TABLE IF NOT EXISTS research.probe (id int)")
            cur.execute("CREATE TABLE IF NOT EXISTS trading.probe (id int)")
            cur.execute("GRANT USAGE ON SCHEMA research TO db_readonly, db_readwrite")
            cur.execute("GRANT SELECT ON research.probe TO db_readonly, db_readwrite")
            cur.execute("GRANT INSERT ON research.probe TO db_readwrite")

            # The service connects as its own logins, so they need passwords for
            # the duration of this class. Reset in tearDownClass.
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

        cls.ro_key, ro_hash, ro_prefix = generate_key("db_readonly")
        cls.rw_key, rw_hash, rw_prefix = generate_key("db_readwrite")
        with cls.admin.cursor() as cur:
            cur.execute(
                "INSERT INTO auth.api_keys (email, name, db_role, key_hash, key_prefix)"
                " VALUES (%s,%s,%s,%s,%s), (%s,%s,%s,%s,%s)",
                ("test-ro@x.com", "RO", "db_readonly", ro_hash, ro_prefix,
                 "test-rw@x.com", "RW", "db_readwrite", rw_hash, rw_prefix),
            )

    @classmethod
    def tearDownClass(cls):
        with cls.admin.cursor() as cur:
            cur.execute("DELETE FROM auth.api_keys WHERE email LIKE 'test-%'")
            cur.execute("DELETE FROM auth.audit_log WHERE actor_email LIKE 'test-%'"
                        " OR actor_email = 'unknown'")
            cur.execute("DELETE FROM research.probe")
            cur.execute("DELETE FROM trading.probe")
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

    def _post(self, sql, key):
        return self.client.post(
            "/v1/query", json={"sql": sql},
            headers={"Authorization": f"Bearer {key}"},
        )

    def _trading_probe_rows(self):
        with self.admin.cursor() as cur:
            cur.execute("SELECT count(*) FROM trading.probe")
            return cur.fetchone()[0]

    def test_healthz_needs_no_key(self):
        self.assertEqual(self.client.get("/healthz").status_code, 200)

    def test_missing_key_is_401(self):
        r = self.client.post("/v1/query", json={"sql": "SELECT 1"})
        self.assertEqual(r.status_code, 401)

    def test_unknown_key_is_401(self):
        r = self._post("SELECT 1", "ag_ro_nope")
        self.assertEqual(r.status_code, 401)

    def test_valid_read_returns_rows(self):
        r = self._post("SELECT 1 AS n", self.ro_key)
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["columns"], ["n"])
        self.assertEqual(body["rows"], [[1]])
        self.assertFalse(body["truncated"])

    def test_write_by_readonly_is_403(self):
        r = self._post("INSERT INTO research.probe VALUES (1)", self.ro_key)
        self.assertEqual(r.status_code, 403)

    def test_write_by_readwrite_succeeds(self):
        r = self._post("INSERT INTO research.probe VALUES (1)", self.rw_key)
        self.assertEqual(r.status_code, 200)

    def test_write_by_readwrite_is_committed(self):
        """A write that returns 200 but rolls back would be worse than a
        refusal, because the caller is told it happened."""
        with self.admin.cursor() as cur:
            cur.execute("SELECT count(*) FROM research.probe")
            before = cur.fetchone()[0]
        self._post("INSERT INTO research.probe VALUES (7)", self.rw_key)
        with self.admin.cursor() as cur:
            cur.execute("SELECT count(*) FROM research.probe")
            self.assertEqual(cur.fetchone()[0], before + 1)

    def test_sql_runs_on_the_login_matching_the_callers_role(self):
        """session_user proves which login was used, current_user proves the
        SET LOCAL ROLE took effect. Both matter: the first is what blocks
        escalation, the second is what grants any privilege at all."""
        for key, login, role in (
            (self.ro_key, "api_service_ro", "db_readonly"),
            (self.rw_key, "api_service_rw", "db_readwrite"),
        ):
            with self.subTest(role=role):
                r = self._post(
                    "SELECT session_user AS s, current_user AS c", key
                )
                self.assertEqual(r.status_code, 200)
                self.assertEqual(r.json()["rows"][0], [login, role])

    def test_set_role_escalation_is_refused_and_writes_nothing(self):
        """The escalation this design was changed to close, exercised through
        the HTTP layer exactly as a caller would send it.

        The service never inspects SQL, so nothing here recognises the SET ROLE.
        It fails because api_service_ro is not a member of db_readwrite_all and
        Postgres refuses to grant it.
        """
        before = self._trading_probe_rows()
        r = self._post(
            "SET ROLE db_readwrite_all; INSERT INTO trading.probe VALUES (1)",
            self.ro_key,
        )
        self.assertNotEqual(
            r.status_code, 200,
            "a db_readonly caller escalated to db_readwrite_all over HTTP",
        )
        self.assertEqual(r.status_code, 403)
        self.assertEqual(
            self._trading_probe_rows(), before,
            "the escalated INSERT reached trading.probe",
        )

    def test_escalation_attempt_is_audited_as_denied(self):
        """An attempt to escalate is the single most important thing for the
        log to contain."""
        self._post(
            "SET ROLE db_readwrite_all; INSERT INTO trading.probe VALUES (1)",
            self.ro_key,
        )
        with self.admin.cursor() as cur:
            cur.execute(
                "SELECT outcome, actor_email FROM auth.audit_log"
                " WHERE statement LIKE 'SET ROLE%%' ORDER BY id DESC LIMIT 1"
            )
            row = cur.fetchone()
        self.assertIsNotNone(row, "the escalation attempt was not audited")
        self.assertEqual(row[0], "denied")
        self.assertEqual(row[1], "test-ro@x.com")

    def test_syntax_error_is_400_not_500(self):
        r = self._post("SELEC 1", self.ro_key)
        self.assertEqual(r.status_code, 400)

    def test_every_outcome_is_audited(self):
        self._post("SELECT 1", self.ro_key)
        self._post("INSERT INTO research.probe VALUES (1)", self.ro_key)
        self._post("SELECT 1", "ag_ro_nope")
        with self.admin.cursor() as cur:
            cur.execute(
                "SELECT outcome, count(*) FROM auth.audit_log"
                " WHERE actor_email LIKE 'test-%' OR actor_email = 'unknown'"
                " GROUP BY outcome"
            )
            outcomes = dict(cur.fetchall())
        self.assertGreaterEqual(outcomes.get("success", 0), 1)
        self.assertGreaterEqual(outcomes.get("denied", 0), 2)

    def test_error_response_does_not_echo_the_key(self):
        r = self._post("SELECT 1", "ag_ro_secretvalue")
        self.assertNotIn("secretvalue", r.text)

    def test_rejected_key_is_not_stored_whole_in_the_audit_log(self):
        """The audit log records which key was tried, never the key. Storing it
        would put a working credential in the table most worth reading."""
        key = "ag_ro_" + "z" * 40
        self._post("SELECT 1", key)
        with self.admin.cursor() as cur:
            cur.execute(
                "SELECT key_prefix FROM auth.audit_log"
                " WHERE actor_email = 'unknown' ORDER BY id DESC LIMIT 1"
            )
            prefix = cur.fetchone()[0]
        self.assertLessEqual(len(prefix), 10)
        self.assertNotEqual(prefix, key)
        self.assertTrue(key.startswith(prefix))
