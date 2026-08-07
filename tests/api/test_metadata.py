import os
import unittest

import psycopg2
from fastapi.testclient import TestClient

from src.api import app as app_module
from src.api.app import LOGIN_BY_ROLE, app
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


@unittest.skipUnless(
    all(os.environ.get(k) for k in REQUIRED),
    "database env vars not set; integration test skipped",
)
class TestMetadataEndpoints(unittest.TestCase):
    """These endpoints have no permission logic of their own. They run as the
    caller, and information_schema is filtered by Postgres to what that role may
    see. The tests therefore assert the filtering from both sides: a read-only
    caller cannot see auth, and a db_readwrite_all caller can. Only asserting the
    first would pass equally well against a hardcoded exclusion of auth, which
    would hide nothing else the caller has no rights to."""

    PASSWORD = "metadata_probe_password"

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)
        cls.admin = psycopg2.connect(**_dsn())
        cls.admin.autocommit = True
        with cls.admin.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS research")
            # Its own table rather than the shared research.probe: these tests
            # assert exact column names and types, so they must not depend on
            # what another test class happened to create first.
            cur.execute("DROP TABLE IF EXISTS research.meta_probe")
            cur.execute("CREATE TABLE research.meta_probe (id int, label text)")
            cur.execute("GRANT USAGE ON SCHEMA research TO db_readonly")
            cur.execute("GRANT SELECT ON research.meta_probe TO db_readonly")

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
        cls.all_key, all_hash, all_prefix = generate_key("db_readwrite_all")
        with cls.admin.cursor() as cur:
            cur.execute(
                "INSERT INTO auth.api_keys (email, name, db_role, key_hash, key_prefix)"
                " VALUES (%s,%s,%s,%s,%s), (%s,%s,%s,%s,%s)",
                ("test-meta@x.com", "Meta", "db_readonly", ro_hash, ro_prefix,
                 "test-meta-all@x.com", "MetaAll", "db_readwrite_all",
                 all_hash, all_prefix),
            )

    @classmethod
    def tearDownClass(cls):
        with cls.admin.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS research.meta_probe")
            cur.execute("DELETE FROM auth.api_keys WHERE email LIKE 'test-%'")
            cur.execute("DELETE FROM auth.audit_log WHERE actor_email LIKE 'test-%'"
                        " OR actor_email = 'unknown'")
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

    def _get(self, path, key=None):
        return self.client.get(
            path, headers={"Authorization": f"Bearer {key or self.ro_key}"}
        )

    def test_schemas_lists_research(self):
        r = self._get("/v1/schemas")
        self.assertEqual(r.status_code, 200)
        self.assertIn("research", r.json()["schemas"])

    def test_schemas_excludes_internal_postgres_schemas(self):
        schemas = self._get("/v1/schemas").json()["schemas"]
        self.assertNotIn("information_schema", schemas)
        self.assertFalse([s for s in schemas if s.startswith("pg_")], schemas)

    def test_tables_lists_the_readable_table(self):
        r = self._get("/v1/tables?schema=research")
        self.assertEqual(r.status_code, 200)
        self.assertIn("meta_probe", r.json()["tables"])

    def test_columns_returns_names_and_types(self):
        r = self._get("/v1/columns?schema=research&table=meta_probe")
        self.assertEqual(r.status_code, 200)
        cols = {c["name"]: c["type"] for c in r.json()["columns"]}
        self.assertEqual(cols["id"], "integer")
        self.assertEqual(cols["label"], "text")

    def test_metadata_requires_a_key(self):
        self.assertEqual(self.client.get("/v1/schemas").status_code, 401)
        self.assertEqual(
            self.client.get("/v1/tables?schema=research").status_code, 401
        )
        self.assertEqual(
            self.client.get("/v1/columns?schema=research&table=meta_probe").status_code,
            401,
        )

    def test_unknown_key_is_401(self):
        self.assertEqual(self._get("/v1/schemas", key="ag_ro_nope").status_code, 401)

    def test_readonly_cannot_see_the_auth_schema(self):
        """auth holds the key table and the audit log. db_readonly has no USAGE
        on it, so information_schema omits it with no help from this service."""
        self.assertNotIn("auth", self._get("/v1/schemas").json()["schemas"])

    def test_readonly_cannot_see_auth_tables(self):
        """Naming the schema explicitly must not route around the filtering:
        the tables are hidden by privilege, not by the schema list."""
        tables = self._get("/v1/tables?schema=auth").json()["tables"]
        self.assertEqual(tables, [])

    def test_readonly_cannot_see_auth_columns(self):
        """The column names of the key table are the shape of the credential
        store; a read-only caller has no business enumerating them."""
        cols = self._get("/v1/columns?schema=auth&table=api_keys").json()["columns"]
        self.assertEqual(cols, [])

    def test_readwrite_all_can_see_the_auth_schema(self):
        """The other side of the same assertion. If auth were excluded by a
        hardcoded rule rather than by the caller's privileges, this would fail
        -- and every other permission the endpoints claim to respect would be
        equally fictional."""
        self.assertIn("auth", self._get("/v1/schemas", key=self.all_key).json()["schemas"])

    def test_readwrite_all_can_see_auth_tables(self):
        tables = self._get("/v1/tables?schema=auth", key=self.all_key).json()["tables"]
        self.assertIn("api_keys", tables)
        self.assertIn("audit_log", tables)

    def test_readwrite_all_can_see_auth_columns(self):
        cols = {
            c["name"]
            for c in self._get(
                "/v1/columns?schema=auth&table=api_keys", key=self.all_key
            ).json()["columns"]
        }
        self.assertIn("key_hash", cols)

    def test_the_two_roles_see_different_schema_lists(self):
        """Stated as one assertion so a change that made both endpoints return
        everything, or nothing, cannot pass."""
        ro = set(self._get("/v1/schemas").json()["schemas"])
        rw_all = set(self._get("/v1/schemas", key=self.all_key).json()["schemas"])
        self.assertTrue(ro, "read-only caller saw no schemas at all")
        self.assertLess(ro, rw_all)

    def test_query_parameters_are_not_interpolated(self):
        """The caller's role bounds the damage either way, but the parameters
        must reach psycopg2 rather than being pasted into the statement."""
        r = self._get(
            "/v1/tables?schema=research'; DROP TABLE research.meta_probe; --"
        )
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["tables"], [])
        with self.admin.cursor() as cur:
            cur.execute("SELECT to_regclass('research.meta_probe') IS NOT NULL")
            self.assertTrue(cur.fetchone()[0], "research.meta_probe was dropped")

    def test_metadata_query_runs_on_the_login_matching_the_callers_role(self):
        """Authentication runs on api_service_ro because the caller's role is
        not yet known; the caller's own statement must then move to their login.
        Reusing the authentication connection would run every caller's metadata
        lookup as api_service_ro, which for a db_readwrite_all caller is not an
        escalation but is still the wrong session_user to be auditing."""
        original = app_module._connect
        roles = []

        def recording_connect(db_role):
            roles.append(db_role)
            return original(db_role)

        app_module._connect = recording_connect
        try:
            self._get("/v1/schemas", key=self.all_key)
        finally:
            app_module._connect = original

        self.assertEqual(roles, ["db_readonly", "db_readwrite_all"])
