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

    def test_service_logins_have_no_inherited_privileges(self):
        """Each service login is NOINHERIT: it must SET ROLE to do anything, so
        a bug that skips SET ROLE fails closed rather than running with its
        role's access."""
        for login in ("api_service_ro", "api_service_rw", "api_service_all"):
            with self.subTest(login=login):
                with self.conn.cursor() as cur:
                    cur.execute(
                        "SELECT rolinherit FROM pg_roles WHERE rolname = %s",
                        (login,),
                    )
                    row = cur.fetchone()
                self.assertIsNotNone(row, f"{login} does not exist")
                self.assertFalse(row[0], f"{login} must be NOINHERIT")

    def test_the_single_login_form_is_gone(self):
        """An earlier revision used one login that was a member of all three
        roles, which allowed any caller to escalate via SET ROLE. If it still
        exists it must at least hold no access to the key table, since it could
        otherwise read every key hash in the system."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'api_service'")
            if cur.fetchone() is None:
                return  # dropped outright, which is the intended end state
            cur.execute(
                "SELECT has_table_privilege('api_service','auth.api_keys','SELECT')"
            )
            self.assertFalse(
                cur.fetchone()[0],
                "the retired api_service login can still read the key table",
            )


@unittest.skipUnless(
    all(os.environ.get(k) for k in REQUIRED),
    "database env vars not set; integration test skipped",
)
class TestRoleEscalationIsRefused(unittest.TestCase):
    """The service login must not be able to reach a role other than its own.

    Postgres authorises SET ROLE against session_user, not current_user. An
    earlier revision used a single login that was a member of all three roles;
    SET LOCAL ROLE narrowed current_user but left session_user a member of
    everything, so any caller escalated by prefixing their SQL with

        SET ROLE db_readwrite_all;

    That was verified working from db_readonly against a real database. Because
    the service deliberately never inspects the SQL it is given, nothing else
    would have caught it. One login per role is what closes it, and this test is
    what stops it reopening.
    """

    LOGINS = {
        "api_service_ro": "db_readonly",
        "api_service_rw": "db_readwrite",
        "api_service_all": "db_readwrite_all",
    }
    PASSWORD = "escalation_probe_password"

    @classmethod
    def setUpClass(cls):
        cls.admin = psycopg2.connect(**_dsn())
        cls.admin.autocommit = True
        with cls.admin.cursor() as cur:
            for login in cls.LOGINS:
                cur.execute(
                    f"ALTER ROLE {login} WITH PASSWORD %s", (cls.PASSWORD,)
                )

    @classmethod
    def tearDownClass(cls):
        with cls.admin.cursor() as cur:
            for login in cls.LOGINS:
                cur.execute(
                    f"ALTER ROLE {login} WITH PASSWORD %s",
                    ("CHANGE_ME_BEFORE_DEPLOY",),
                )
        cls.admin.close()

    def _connect_as(self, login):
        params = _dsn()
        params["user"] = login
        params["password"] = self.PASSWORD
        return psycopg2.connect(**params)

    def test_each_login_can_assume_only_its_own_role(self):
        for login, own_role in self.LOGINS.items():
            others = [r for r in self.LOGINS.values() if r != own_role]
            conn = self._connect_as(login)
            try:
                with conn.cursor() as cur:
                    cur.execute(f"SET ROLE {own_role}")
                conn.rollback()

                for other in others:
                    with self.subTest(login=login, target=other):
                        with self.assertRaises(
                            psycopg2.errors.InsufficientPrivilege,
                            msg=f"{login} was able to SET ROLE {other}",
                        ):
                            with conn.cursor() as cur:
                                cur.execute(f"SET ROLE {other}")
                        conn.rollback()
            finally:
                conn.close()

    def test_no_login_is_a_member_of_more_than_one_role(self):
        """Membership in two roles would reopen the escalation regardless of
        what SET ROLE is issued."""
        with self.admin.cursor() as cur:
            cur.execute(
                "SELECT m.rolname, count(*)"
                "  FROM pg_auth_members am"
                "  JOIN pg_roles m ON m.oid = am.member"
                "  JOIN pg_roles r ON r.oid = am.roleid"
                " WHERE m.rolname LIKE 'api_service%'"
                "   AND r.rolname IN ('db_readonly','db_readwrite','db_readwrite_all')"
                " GROUP BY m.rolname"
            )
            for login, count in cur.fetchall():
                with self.subTest(login=login):
                    self.assertEqual(
                        count, 1, f"{login} is a member of {count} gateway roles"
                    )
