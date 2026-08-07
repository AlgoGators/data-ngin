# Database API Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a FastAPI service that lets fund members run SQL against Postgres using an API key, where Postgres itself enforces what each caller may do and every request is recorded.

**Architecture:** The service authenticates an API key against `auth.api_keys`, acquires a concurrency slot, opens a transaction, issues `SET LOCAL ROLE` for that caller's Postgres role, runs their SQL, and writes an audit row. Enforcement is entirely Postgres's: the service never inspects the SQL. `SET LOCAL` is used rather than `SET` so an exception cannot leave a pooled connection holding an elevated role.

**Tech Stack:** Python 3.11, FastAPI 0.115.6, psycopg2 2.9.10, Postgres 16, pytest with `unittest.TestCase` classes (the existing convention in this repo), Caddy for TLS.

**Design spec:** `docs/superpowers/specs/2026-08-06-database-api-gateway-design.md`

## Global Constraints

- Python `^3.11, <3.13`. Do not add dependencies — `fastapi`, `uvicorn`, `psycopg2`, `httpx` and `anyio` are already in `pyproject.toml` and `poetry.lock`.
- Tests are `unittest.TestCase` subclasses run under pytest, placed in `tests/<area>/test_<thing>.py`, matching every existing test in this repo.
- Migrations are numbered SQL files in `migrations/`, wrapped in `BEGIN`/`COMMIT`, with guards that fail loudly rather than silently no-op. `001` and `002` already exist.
- Integration tests requiring Postgres read `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` from the environment and **skip** when unset, so the suite still passes on a laptop. CI supplies these via its `postgres:16` service.
- Three Postgres roles exist and are the sole enforcement mechanism: `db_readonly`, `db_readwrite`, `db_readwrite_all`.
- `trading` and `auth` are writable only by `db_readwrite_all`.
- API keys are stored only as SHA-256 hashes. Plaintext is displayed once, at creation, and never persisted or logged.
- Never write an API key, a password, or a connection string into the audit log, an exception message, or application logs.
- No commit message may reference Claude, Anthropic, or AI assistance.

---

## File Structure

**Created:**
- `migrations/003_api_gateway_roles.sql` — the three roles, their grants, and the `api_service` login
- `migrations/004_api_gateway_tables.sql` — `auth.api_keys` and `auth.audit_log`
- `src/api/__init__.py` — package marker
- `src/api/keys.py` — key generation, hashing, and lookup. No FastAPI imports.
- `src/api/limits.py` — per-user and global concurrency. No FastAPI or database imports.
- `src/api/audit.py` — writes audit rows. Database only.
- `src/api/executor.py` — runs a caller's SQL under their role. Database only.
- `src/api/app.py` — FastAPI application; wires the above together. The only file importing FastAPI.
- `scripts/manage_api_keys.py` — admin CLI for creating, rotating and revoking keys
- `Caddyfile` — TLS termination
- `tests/api/__init__.py`
- `tests/api/test_keys.py`, `test_limits.py`, `test_audit.py`, `test_executor.py`, `test_app.py`, `test_roles.py`

**Modified:**
- `docker-compose.yml` — add the `api` service with a memory limit

Each module has one responsibility and no circular imports: `app.py` depends on the other four; none of them depend on each other except `executor.py` and `audit.py`, which share a connection helper defined in `executor.py`.

---

### Task 1: Postgres roles and grants

**Files:**
- Create: `migrations/003_api_gateway_roles.sql`
- Test: `tests/api/test_roles.py`
- Create: `tests/api/__init__.py`

**Interfaces:**
- Consumes: nothing
- Produces: Postgres roles `db_readonly`, `db_readwrite`, `db_readwrite_all`, and login role `api_service`. Later tasks connect as `api_service` and `SET LOCAL ROLE` to one of the other three.

- [ ] **Step 1: Write the failing test**

Create `tests/api/__init__.py` as an empty file, then `tests/api/test_roles.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test poetry run pytest tests/api/test_roles.py -v`

Start a throwaway Postgres first if you do not have one:
`docker run -d --name pgdev -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=data_ngin_test -p 5432:5432 postgres:16`

Expected: FAIL — `role "db_readonly" does not exist`.

- [ ] **Step 3: Write the migration**

Create `migrations/003_api_gateway_roles.sql`:

```sql
-- Migration 003: roles for the database API gateway.
--
-- These three roles are the entire permission model. The API service does not
-- inspect anyone's SQL; it switches to the caller's role and lets Postgres
-- accept or reject the statement. See
-- docs/superpowers/specs/2026-08-06-database-api-gateway-design.md
--
-- Idempotent: safe to re-run. Uses DO blocks because CREATE ROLE has no
-- IF NOT EXISTS.

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'db_readonly') THEN
        CREATE ROLE db_readonly NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'db_readwrite') THEN
        CREATE ROLE db_readwrite NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'db_readwrite_all') THEN
        CREATE ROLE db_readwrite_all NOLOGIN;
    END IF;
END $$;

-- Schemas everyone may read. auth is deliberately absent: it holds the key
-- table and the audit log.
DO $$
DECLARE s text;
BEGIN
    FOREACH s IN ARRAY ARRAY['equities_data','futures_data','backtest','research',
                             'synthetic','macro_data','eia','metadata','trading']
    LOOP
        IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = s) THEN
            EXECUTE format('GRANT USAGE ON SCHEMA %I TO db_readonly, db_readwrite, db_readwrite_all', s);
            EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO db_readonly, db_readwrite, db_readwrite_all', s);
            EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO db_readonly, db_readwrite, db_readwrite_all', s);
        END IF;
    END LOOP;
END $$;

-- Schemas db_readwrite may modify. trading is excluded: it holds live positions
-- and executions, where a wrong UPDATE misstates what the fund holds rather than
-- producing research that can be re-derived.
DO $$
DECLARE s text;
BEGIN
    FOREACH s IN ARRAY ARRAY['equities_data','futures_data','backtest','research',
                             'synthetic','macro_data','eia','metadata']
    LOOP
        IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = s) THEN
            EXECUTE format('GRANT CREATE ON SCHEMA %I TO db_readwrite', s);
            EXECUTE format('GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %I TO db_readwrite', s);
            EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT INSERT, UPDATE, DELETE ON TABLES TO db_readwrite', s);
        END IF;
    END LOOP;
END $$;

-- db_readwrite_all may modify everything, including trading and auth.
DO $$
DECLARE s text;
BEGIN
    FOREACH s IN ARRAY ARRAY['equities_data','futures_data','backtest','research',
                             'synthetic','macro_data','eia','metadata','trading','auth']
    LOOP
        IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = s) THEN
            EXECUTE format('GRANT USAGE, CREATE ON SCHEMA %I TO db_readwrite_all', s);
            EXECUTE format('GRANT ALL ON ALL TABLES IN SCHEMA %I TO db_readwrite_all', s);
            EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON TABLES TO db_readwrite_all', s);
        END IF;
    END LOOP;
END $$;

-- The login the service uses. NOINHERIT means it holds no privileges of its own
-- despite being a member of all three roles -- it must SET ROLE explicitly. A
-- bug that forgets to SET ROLE therefore fails closed instead of running with
-- full access.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_service') THEN
        CREATE ROLE api_service LOGIN NOINHERIT PASSWORD 'CHANGE_ME_BEFORE_DEPLOY';
    END IF;
END $$;

GRANT db_readonly, db_readwrite, db_readwrite_all TO api_service;

COMMIT;

-- After running, set a real password:
--   ALTER ROLE api_service WITH PASSWORD '<generated>';
-- and put it in the service's environment as API_DB_PASSWORD.
```

- [ ] **Step 4: Apply the migration and run the test**

Run:
```bash
PGPASSWORD=postgres psql -h localhost -U postgres -d data_ngin_test -v ON_ERROR_STOP=1 -f migrations/003_api_gateway_roles.sql
DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test poetry run pytest tests/api/test_roles.py -v
```
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add migrations/003_api_gateway_roles.sql tests/api/__init__.py tests/api/test_roles.py
git commit -m "Add the three Postgres roles that enforce API permissions

The service will not inspect anyone's SQL. It switches to the caller's role
and lets Postgres decide, so these grants are the entire permission model.

api_service is NOINHERIT deliberately: it is a member of all three roles but
holds none of their privileges until it issues SET ROLE, so a code path that
forgets to switch fails closed rather than running with full access.

Tests assert the boundary directly -- db_readwrite can write equities_data but
not trading -- against a real Postgres, because mocking a permission system
proves nothing about it."
```

---

### Task 2: The key and audit tables

**Files:**
- Create: `migrations/004_api_gateway_tables.sql`
- Test: `tests/api/test_tables.py`

**Interfaces:**
- Consumes: roles from Task 1
- Produces: `auth.api_keys` (columns `email`, `name`, `db_role`, `key_hash`, `key_prefix`, `max_concurrent`, `statement_timeout_ms`, `active`, `created_at`, `created_by`, `last_used_at`, `revoked_at`) and `auth.audit_log`. Later tasks read `api_keys` and insert into `audit_log`.

- [ ] **Step 1: Write the failing test**

Create `tests/api/test_tables.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test poetry run pytest tests/api/test_tables.py -v`
Expected: FAIL — `relation "auth.api_keys" does not exist`.

- [ ] **Step 3: Write the migration**

Create `migrations/004_api_gateway_tables.sql`:

```sql
-- Migration 004: API gateway key and audit tables.
--
-- Both live in auth, which no role below db_readwrite_all may touch. That is
-- what keeps the audit log out of reach of the people it records -- it is not a
-- separate rule anyone has to remember.

BEGIN;

CREATE SCHEMA IF NOT EXISTS auth;

CREATE TABLE IF NOT EXISTS auth.api_keys (
    email                 TEXT PRIMARY KEY,
    name                  TEXT NOT NULL,
    db_role               TEXT NOT NULL
        CHECK (db_role IN ('db_readonly','db_readwrite','db_readwrite_all')),

    key_hash              TEXT NOT NULL UNIQUE,
    key_prefix            TEXT NOT NULL,

    max_concurrent        INT NOT NULL DEFAULT 1 CHECK (max_concurrent > 0),
    statement_timeout_ms  INT NOT NULL DEFAULT 120000 CHECK (statement_timeout_ms > 0),

    active                BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by            TEXT,
    last_used_at          TIMESTAMPTZ,
    revoked_at            TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS auth.audit_log (
    id             BIGSERIAL PRIMARY KEY,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Actor identity is snapshotted rather than joined. Removing someone from
    -- api_keys must not erase their name from history.
    actor_email    TEXT NOT NULL,
    actor_name     TEXT NOT NULL,
    actor_role     TEXT NOT NULL,
    key_prefix     TEXT,

    statement      TEXT,
    row_count      INTEGER,

    outcome        TEXT NOT NULL
        CHECK (outcome IN ('success','denied','error','rate_limited')),
    error_message  TEXT,
    duration_ms    INTEGER,
    client_ip      INET
);

CREATE INDEX IF NOT EXISTS audit_log_occurred_idx
    ON auth.audit_log (occurred_at DESC);
CREATE INDEX IF NOT EXISTS audit_log_actor_idx
    ON auth.audit_log (actor_email, occurred_at DESC);

-- api_service needs these directly rather than through role membership, because
-- it is NOINHERIT and authentication happens before any SET ROLE.
GRANT USAGE ON SCHEMA auth TO api_service;
GRANT SELECT ON auth.api_keys TO api_service;
GRANT UPDATE (last_used_at) ON auth.api_keys TO api_service;
GRANT INSERT ON auth.audit_log TO api_service;
GRANT USAGE ON SEQUENCE auth.audit_log_id_seq TO api_service;

COMMIT;
```

- [ ] **Step 4: Apply and run**

Run:
```bash
PGPASSWORD=postgres psql -h localhost -U postgres -d data_ngin_test -v ON_ERROR_STOP=1 -f migrations/004_api_gateway_tables.sql
DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test poetry run pytest tests/api/test_tables.py -v
```
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add migrations/004_api_gateway_tables.sql tests/api/test_tables.py
git commit -m "Add the API key and audit tables

Both live in auth, which only db_readwrite_all can write. That is what keeps
the audit log out of reach of the people it records, without a separate rule
anyone has to remember.

Actor name and email are stored on each audit row rather than joined from the
key table, so removing someone does not erase them from history.

api_service gets SELECT on api_keys and INSERT on audit_log as direct grants,
because it is NOINHERIT and authentication happens before any SET ROLE."
```

---

### Task 3: Key generation and hashing

**Files:**
- Create: `src/api/__init__.py`, `src/api/keys.py`
- Test: `tests/api/test_keys.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `generate_key(db_role: str) -> tuple[str, str, str]` returning `(plaintext, sha256_hex, prefix)`
  - `hash_key(plaintext: str) -> str`
  - `ROLE_PREFIXES: dict[str, str]`

- [ ] **Step 1: Write the failing test**

Create `tests/api/test_keys.py`:

```python
import unittest

from src.api.keys import ROLE_PREFIXES, generate_key, hash_key


class TestKeyGeneration(unittest.TestCase):
    def test_prefix_reflects_role(self):
        plaintext, _, _ = generate_key("db_readwrite")
        self.assertTrue(plaintext.startswith("ag_rw_"), plaintext)

    def test_every_role_has_a_prefix(self):
        for role in ("db_readonly", "db_readwrite", "db_readwrite_all"):
            self.assertIn(role, ROLE_PREFIXES)

    def test_unknown_role_is_rejected(self):
        with self.assertRaises(ValueError):
            generate_key("db_superuser")

    def test_keys_are_unique(self):
        keys = {generate_key("db_readonly")[0] for _ in range(200)}
        self.assertEqual(len(keys), 200)

    def test_hash_matches_the_plaintext(self):
        plaintext, key_hash, _ = generate_key("db_readonly")
        self.assertEqual(hash_key(plaintext), key_hash)

    def test_hash_is_sha256_hex(self):
        self.assertEqual(len(hash_key("anything")), 64)
        int(hash_key("anything"), 16)  # raises if not hex

    def test_prefix_is_stored_form_not_the_key(self):
        """The prefix is for display. It must never be long enough to be
        useful to someone who obtains it."""
        plaintext, _, prefix = generate_key("db_readonly")
        self.assertTrue(plaintext.startswith(prefix))
        self.assertLessEqual(len(prefix), 12)
        self.assertLess(len(prefix), len(plaintext))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/api/test_keys.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.api'`.

- [ ] **Step 3: Write the implementation**

Create `src/api/__init__.py` as an empty file, then `src/api/keys.py`:

```python
"""API key generation and hashing.

Keys are stored only as SHA-256 hashes. SHA-256 rather than bcrypt because
bcrypt exists to slow brute-forcing of guessable secrets such as human-chosen
passwords; a 32-character random token has enough entropy that brute force is
irrelevant, and this runs on every request.
"""

import hashlib
import secrets

ROLE_PREFIXES = {
    "db_readonly": "ro",
    "db_readwrite": "rw",
    "db_readwrite_all": "ad",
}

# Long enough to identify a key in a log line, short enough to be useless alone.
PREFIX_LENGTH = 10


def hash_key(plaintext: str) -> str:
    """Return the hex SHA-256 of a key. The only form ever persisted."""
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


def generate_key(db_role: str) -> tuple[str, str, str]:
    """Generate a new API key.

    Returns (plaintext, key_hash, key_prefix). The plaintext is the only copy
    that will ever exist -- callers must display it and discard it.
    """
    if db_role not in ROLE_PREFIXES:
        raise ValueError(
            f"unknown role {db_role!r}; expected one of {sorted(ROLE_PREFIXES)}"
        )
    plaintext = f"ag_{ROLE_PREFIXES[db_role]}_{secrets.token_urlsafe(24)}"
    return plaintext, hash_key(plaintext), plaintext[:PREFIX_LENGTH]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/api/test_keys.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/api/__init__.py src/api/keys.py tests/api/test_keys.py
git commit -m "Add API key generation and hashing

Keys are stored only as SHA-256 hashes and the plaintext is returned once, to
be displayed and discarded. This database was internet-facing with a published
password for months; if that recurs and the key table is plaintext, every key
is immediately usable, whereas hashed it is worthless.

SHA-256 rather than bcrypt: bcrypt slows brute-forcing of guessable secrets,
and a 32-character random token is not guessable. This runs on every request."
```

---

### Task 4: Concurrency limiting

**Files:**
- Create: `src/api/limits.py`
- Test: `tests/api/test_limits.py`

**Interfaces:**
- Consumes: nothing
- Produces: `ConcurrencyLimiter(global_limit: int)` with async context manager `slot(email: str, user_limit: int)`, raising `AtCapacity` when no slot is free.

- [ ] **Step 1: Write the failing test**

Create `tests/api/test_limits.py`:

```python
import asyncio
import unittest

from src.api.limits import AtCapacity, ConcurrencyLimiter


class TestConcurrencyLimiter(unittest.IsolatedAsyncioTestCase):
    async def test_single_request_is_allowed(self):
        limiter = ConcurrencyLimiter(global_limit=2)
        async with limiter.slot("a@x.com", user_limit=1):
            pass

    async def test_second_request_from_same_user_is_refused(self):
        limiter = ConcurrencyLimiter(global_limit=5)
        async with limiter.slot("a@x.com", user_limit=1):
            with self.assertRaises(AtCapacity):
                async with limiter.slot("a@x.com", user_limit=1):
                    pass

    async def test_a_different_user_is_unaffected(self):
        limiter = ConcurrencyLimiter(global_limit=5)
        async with limiter.slot("a@x.com", user_limit=1):
            async with limiter.slot("b@x.com", user_limit=1):
                pass

    async def test_global_limit_applies_across_users(self):
        """Twenty users at one request each still overwhelms a 957MB box, so the
        per-user cap alone is not enough."""
        limiter = ConcurrencyLimiter(global_limit=2)
        async with limiter.slot("a@x.com", user_limit=1):
            async with limiter.slot("b@x.com", user_limit=1):
                with self.assertRaises(AtCapacity):
                    async with limiter.slot("c@x.com", user_limit=1):
                        pass

    async def test_slot_is_released_after_use(self):
        limiter = ConcurrencyLimiter(global_limit=1)
        async with limiter.slot("a@x.com", user_limit=1):
            pass
        async with limiter.slot("a@x.com", user_limit=1):
            pass

    async def test_slot_is_released_when_the_body_raises(self):
        """A slot leaked on the error path would degrade the service into
        permanent 429s, which is worse than the original error."""
        limiter = ConcurrencyLimiter(global_limit=1)
        with self.assertRaises(RuntimeError):
            async with limiter.slot("a@x.com", user_limit=1):
                raise RuntimeError("boom")
        async with limiter.slot("a@x.com", user_limit=1):
            pass

    async def test_user_limit_above_one_is_honoured(self):
        limiter = ConcurrencyLimiter(global_limit=5)
        async with limiter.slot("a@x.com", user_limit=2):
            async with limiter.slot("a@x.com", user_limit=2):
                with self.assertRaises(AtCapacity):
                    async with limiter.slot("a@x.com", user_limit=2):
                        pass
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/api/test_limits.py -v`
Expected: FAIL — `No module named 'src.api.limits'`.

- [ ] **Step 3: Write the implementation**

Create `src/api/limits.py`:

```python
"""Per-user and global concurrency limiting.

The server has 957MB of RAM and runs Airflow, Postgres and trade-ngin alongside
this service, so unbounded concurrency is not a theoretical concern. Both caps
matter: the per-user cap stops one person queuing work, and the global cap stops
twenty people each sending one request from doing the same thing collectively.

Counters are in-process. That is correct for a single-container deployment; a
second replica would need shared state.
"""

import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager


class AtCapacity(Exception):
    """No slot is available. The caller should return 429."""


class ConcurrencyLimiter:
    def __init__(self, global_limit: int):
        if global_limit < 1:
            raise ValueError("global_limit must be at least 1")
        self._global_limit = global_limit
        self._global_in_flight = 0
        self._per_user: dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def slot(self, email: str, user_limit: int):
        """Hold a slot for the duration of the block, or raise AtCapacity.

        Refuses immediately rather than queueing: a caller waiting behind a slow
        query is worse served by a hanging request than by a prompt 429.
        """
        async with self._lock:
            if self._global_in_flight >= self._global_limit:
                raise AtCapacity("service at capacity")
            if self._per_user[email] >= user_limit:
                raise AtCapacity("you already have a request in flight")
            self._global_in_flight += 1
            self._per_user[email] += 1
        try:
            yield
        finally:
            # In finally so an exception in the body cannot leak a slot and
            # degrade the service into permanent 429s.
            async with self._lock:
                self._global_in_flight -= 1
                self._per_user[email] -= 1
                if self._per_user[email] <= 0:
                    del self._per_user[email]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/api/test_limits.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/api/limits.py tests/api/test_limits.py
git commit -m "Add per-user and global concurrency limiting

Both caps are needed. The per-user cap stops one person queueing work; the
global cap stops twenty people each sending one request from overwhelming a
957MB box that also runs Airflow and Postgres.

Slots are released in a finally block: a slot leaked on the error path would
degrade the service into permanent 429s, which is worse than the error that
caused it.

Requests are refused rather than queued, because a caller waiting behind a slow
query is better served by a prompt 429 than a hanging connection."
```

---

### Task 5: Authentication against the key table

**Files:**
- Modify: `src/api/keys.py` (append)
- Test: `tests/api/test_auth_lookup.py`

**Interfaces:**
- Consumes: `hash_key` from Task 3, `auth.api_keys` from Task 2
- Produces:
  - `Caller` dataclass with fields `email: str`, `name: str`, `db_role: str`, `key_prefix: str`, `max_concurrent: int`, `statement_timeout_ms: int`
  - `authenticate(conn, plaintext: str) -> Caller | None`

- [ ] **Step 1: Write the failing test**

Create `tests/api/test_auth_lookup.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test poetry run pytest tests/api/test_auth_lookup.py -v`
Expected: FAIL — `ImportError: cannot import name 'authenticate'`.

- [ ] **Step 3: Append to `src/api/keys.py`**

Add these imports at the top of the existing file, below the current imports:

```python
from dataclasses import dataclass
```

Then append:

```python
@dataclass(frozen=True)
class Caller:
    """An authenticated person. Carries everything the request needs, so no
    later stage has to query the key table again."""

    email: str
    name: str
    db_role: str
    key_prefix: str
    max_concurrent: int
    statement_timeout_ms: int


def authenticate(conn, plaintext: str) -> "Caller | None":
    """Resolve an API key to a Caller, or None if it is unknown or revoked.

    Looks up by hash, so the plaintext key is never compared against stored
    data and never appears in a query log.
    """
    if not plaintext:
        return None

    with conn.cursor() as cur:
        cur.execute(
            "SELECT email, name, db_role, key_prefix, max_concurrent,"
            "       statement_timeout_ms"
            "  FROM auth.api_keys"
            " WHERE key_hash = %s AND active",
            (hash_key(plaintext),),
        )
        row = cur.fetchone()
        if row is None:
            return None

        # Best-effort: a failure here must not deny an otherwise valid request.
        try:
            cur.execute(
                "UPDATE auth.api_keys SET last_used_at = now() WHERE email = %s",
                (row[0],),
            )
        except Exception:  # noqa: BLE001 - deliberately swallowed
            pass

    return Caller(
        email=row[0],
        name=row[1],
        db_role=row[2],
        key_prefix=row[3],
        max_concurrent=row[4],
        statement_timeout_ms=row[5],
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test poetry run pytest tests/api/test_auth_lookup.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/api/keys.py tests/api/test_auth_lookup.py
git commit -m "Resolve API keys to callers

Lookup is by hash, so the plaintext key never appears in a query and never
reaches a database log.

The Caller carries everything the rest of the request needs, so no later stage
queries the key table again -- one lookup per request.

Recording last_used_at is best-effort: a failure writing it must not deny an
otherwise valid request."
```

---

### Task 6: Executing SQL under the caller's role

**Files:**
- Create: `src/api/executor.py`
- Test: `tests/api/test_executor.py`

**Interfaces:**
- Consumes: `Caller` from Task 5
- Produces:
  - `QueryResult` dataclass with `columns: list[str]`, `rows: list[list]`, `row_count: int`, `truncated: bool`
  - `PermissionDenied` exception
  - `execute_as(conn, caller: Caller, sql: str, row_limit: int, params: tuple | None = None) -> QueryResult`

- [ ] **Step 1: Write the failing test**

Create `tests/api/test_executor.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test poetry run pytest tests/api/test_executor.py -v`
Expected: FAIL — `No module named 'src.api.executor'`.

- [ ] **Step 3: Write the implementation**

Create `src/api/executor.py`:

```python
"""Runs a caller's SQL under their Postgres role.

The service never inspects the SQL. It switches to the caller's role and lets
Postgres accept or reject the statement, because determining what a statement
writes to requires a full SQL parser plus search_path resolution plus following
functions and triggers -- and every gap in that is a permission bypass.
"""

from dataclasses import dataclass

import psycopg2
from psycopg2 import sql as pgsql


class PermissionDenied(Exception):
    """Postgres refused the statement for the caller's role."""


@dataclass
class QueryResult:
    columns: list
    rows: list
    row_count: int
    truncated: bool


def execute_as(conn, caller, sql: str, row_limit: int, params=None) -> QueryResult:
    """Execute `sql` as `caller`, returning at most `row_limit` rows.

    `params` is for SQL this service constructs itself, such as the metadata
    endpoints. Caller-supplied SQL arrives fully formed and passes params=None;
    it needs no parameterisation because the caller's role, not string hygiene,
    is what bounds it.

    Raises PermissionDenied if the role lacks permission, or psycopg2.Error for
    anything else (syntax errors, timeouts, constraint violations).
    """
    try:
        with conn:  # transaction: commits on success, rolls back on exception
            with conn.cursor() as cur:
                # SET LOCAL, not SET: both revert when the transaction ends, so
                # an exception cannot leave a pooled connection holding an
                # elevated role for the next caller.
                cur.execute(
                    pgsql.SQL("SET LOCAL ROLE {}").format(
                        pgsql.Identifier(caller.db_role)
                    )
                )
                cur.execute(
                    "SET LOCAL statement_timeout = %s",
                    (caller.statement_timeout_ms,),
                )

                cur.execute(sql, params) if params else cur.execute(sql)

                if cur.description is None:
                    # No result set: INSERT, UPDATE, DELETE, DDL.
                    return QueryResult([], [], cur.rowcount, False)

                columns = [d.name for d in cur.description]
                # Fetch one extra to detect truncation without counting the
                # whole result, which would defeat the limit's purpose.
                fetched = cur.fetchmany(row_limit + 1)
                truncated = len(fetched) > row_limit
                rows = [list(r) for r in fetched[:row_limit]]
                return QueryResult(columns, rows, len(rows), truncated)

    except psycopg2.errors.InsufficientPrivilege as exc:
        raise PermissionDenied(str(exc).strip()) from exc
```

- [ ] **Step 4: Run test to verify it passes**

Run: `DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test poetry run pytest tests/api/test_executor.py -v`
Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add src/api/executor.py tests/api/test_executor.py
git commit -m "Execute caller SQL under their Postgres role

SET LOCAL ROLE rather than SET ROLE: both revert when the transaction ends, so
an exception cannot leave a pooled connection holding an elevated role for
whoever gets it next. There is a test for exactly that.

Truncation is detected by fetching one row beyond the limit rather than
counting the result set, which would defeat the purpose of having a limit.

A test asserts that a write hidden inside a CTE is still refused. That is the
argument for this design: we do not recognise the pattern, and we do not have
to, because Postgres does."
```

---

### Task 7: Writing audit rows

**Files:**
- Create: `src/api/audit.py`
- Test: `tests/api/test_audit.py`

**Interfaces:**
- Consumes: `Caller` from Task 5
- Produces: `record(conn, caller, statement, outcome, *, row_count=None, error_message=None, duration_ms=None, client_ip=None) -> None` and `record_anonymous(conn, outcome, *, key_prefix=None, error_message=None, client_ip=None) -> None`

- [ ] **Step 1: Write the failing test**

Create `tests/api/test_audit.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test poetry run pytest tests/api/test_audit.py -v`
Expected: FAIL — `No module named 'src.api.audit'`.

- [ ] **Step 3: Write the implementation**

Create `src/api/audit.py`:

```python
"""Writes audit rows.

Every request produces one, including authentication failures, permission
denials and rate limits. A log recording only successes says nothing about
someone probing for access.

Actor identity is written onto each row rather than joined from api_keys, so
removing someone does not erase them from history.
"""

import logging

logger = logging.getLogger(__name__)

_INSERT = (
    "INSERT INTO auth.audit_log"
    " (actor_email, actor_name, actor_role, key_prefix, statement,"
    "  row_count, outcome, error_message, duration_ms, client_ip)"
    " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
)


def record(
    conn,
    caller,
    statement: str,
    outcome: str,
    *,
    row_count=None,
    error_message=None,
    duration_ms=None,
    client_ip=None,
) -> None:
    """Record a request made by an authenticated caller."""
    _write(
        conn,
        (
            caller.email,
            caller.name,
            caller.db_role,
            caller.key_prefix,
            statement,
            row_count,
            outcome,
            error_message,
            duration_ms,
            client_ip,
        ),
    )


def record_anonymous(
    conn,
    outcome: str,
    *,
    key_prefix=None,
    error_message=None,
    client_ip=None,
) -> None:
    """Record an attempt that never authenticated.

    key_prefix identifies which key was presented. The key itself is never
    stored -- only the caller-supplied prefix, which is not usable alone.
    """
    _write(
        conn,
        (
            "unknown",
            "unknown",
            "none",
            key_prefix,
            None,
            None,
            outcome,
            error_message,
            None,
            client_ip,
        ),
    )


def _write(conn, params) -> None:
    """Insert one row. Never raises.

    A failure to log must not turn a successful query into an error for the
    caller, so the exception is logged locally and swallowed. That is a
    deliberate trade: losing one audit row is preferable to failing the request
    that produced it.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(_INSERT, params)
        if not conn.autocommit:
            conn.commit()
    except Exception:  # noqa: BLE001 - deliberately swallowed
        logger.exception("failed to write audit row")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test poetry run pytest tests/api/test_audit.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/api/audit.py tests/api/test_audit.py
git commit -m "Record every request, including the ones that fail

Denials, rate limits and unknown keys all produce rows. A log that records only
successes says nothing about someone probing for access, which is most of what
an audit trail is for.

Actor name and email are written onto each row rather than joined from the key
table, so removing someone does not erase them from history.

Audit writes never raise. Losing one row is preferable to failing the request
that produced it, so the exception is logged locally and swallowed."
```

---

### Task 8: The FastAPI application

**Files:**
- Create: `src/api/app.py`
- Test: `tests/api/test_app.py`

**Interfaces:**
- Consumes: everything from Tasks 3–7
- Produces: `app` (FastAPI instance), `POST /v1/query`, `GET /healthz`

- [ ] **Step 1: Write the failing test**

Create `tests/api/test_app.py`:

```python
import os
import unittest

import psycopg2
from fastapi.testclient import TestClient

from src.api.app import app
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
class TestQueryEndpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)
        cls.admin = psycopg2.connect(**_dsn())
        cls.admin.autocommit = True
        with cls.admin.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS research")
            cur.execute("CREATE TABLE IF NOT EXISTS research.probe (id int)")
            cur.execute("GRANT USAGE ON SCHEMA research TO db_readonly, db_readwrite")
            cur.execute("GRANT SELECT ON research.probe TO db_readonly, db_readwrite")
            cur.execute("GRANT INSERT ON research.probe TO db_readwrite")

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
        cls.admin.close()

    def _post(self, sql, key):
        return self.client.post(
            "/v1/query", json={"sql": sql},
            headers={"Authorization": f"Bearer {key}"},
        )

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test poetry run pytest tests/api/test_app.py -v`
Expected: FAIL — `No module named 'src.api.app'`.

- [ ] **Step 3: Write the implementation**

Create `src/api/app.py`:

```python
"""The database API gateway.

Authenticates a key, takes a concurrency slot, runs the caller's SQL under their
Postgres role, and records the outcome. Enforcement is Postgres's throughout --
this module never inspects the SQL it is given.
"""

import os
import time

import psycopg2
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.api import audit
from src.api.executor import PermissionDenied, execute_as
from src.api.keys import authenticate
from src.api.limits import AtCapacity, ConcurrencyLimiter

ROW_LIMIT = int(os.environ.get("API_ROW_LIMIT", "100000"))
GLOBAL_CONCURRENCY = int(os.environ.get("API_GLOBAL_CONCURRENCY", "2"))

app = FastAPI(title="data-ngin database API", version="1.0")
limiter = ConcurrencyLimiter(global_limit=GLOBAL_CONCURRENCY)


class QueryRequest(BaseModel):
    sql: str


def _connect():
    """Open a connection as api_service.

    api_service is NOINHERIT, so this connection holds no table privileges of
    its own -- every statement runs under a role chosen by SET LOCAL ROLE.
    """
    return psycopg2.connect(
        host=os.environ["API_DB_HOST"],
        port=os.environ.get("API_DB_PORT", "5432"),
        user=os.environ.get("API_DB_USER", "api_service"),
        password=os.environ["API_DB_PASSWORD"],
        dbname=os.environ["API_DB_NAME"],
    )


def _bearer(authorization: str | None) -> str:
    if not authorization:
        return ""
    parts = authorization.split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return authorization.strip()


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/v1/query")
async def query(
    body: QueryRequest,
    request: Request,
    authorization: str | None = Header(default=None),
):
    key = _bearer(authorization)
    client_ip = request.client.host if request.client else None
    conn = _connect()
    conn.autocommit = True

    try:
        caller = authenticate(conn, key)
        if caller is None:
            # The prefix identifies which key was tried without storing it.
            audit.record_anonymous(
                conn, "denied",
                key_prefix=key[:10] or None,
                error_message="unknown or revoked key",
                client_ip=client_ip,
            )
            return JSONResponse({"detail": "invalid API key"}, status_code=401)

        try:
            async with limiter.slot(caller.email, caller.max_concurrent):
                started = time.monotonic()
                try:
                    result = execute_as(conn, caller, body.sql, ROW_LIMIT)
                except PermissionDenied as exc:
                    audit.record(
                        conn, caller, body.sql, "denied",
                        error_message=str(exc),
                        duration_ms=int((time.monotonic() - started) * 1000),
                        client_ip=client_ip,
                    )
                    return JSONResponse({"detail": str(exc)}, status_code=403)
                except psycopg2.Error as exc:
                    audit.record(
                        conn, caller, body.sql, "error",
                        error_message=str(exc).strip(),
                        duration_ms=int((time.monotonic() - started) * 1000),
                        client_ip=client_ip,
                    )
                    return JSONResponse(
                        {"detail": str(exc).strip()}, status_code=400
                    )

                audit.record(
                    conn, caller, body.sql, "success",
                    row_count=result.row_count,
                    duration_ms=int((time.monotonic() - started) * 1000),
                    client_ip=client_ip,
                )
                return {
                    "columns": result.columns,
                    "rows": result.rows,
                    "row_count": result.row_count,
                    "truncated": result.truncated,
                }

        except AtCapacity as exc:
            audit.record(
                conn, caller, body.sql, "rate_limited",
                error_message=str(exc), client_ip=client_ip,
            )
            return JSONResponse({"detail": str(exc)}, status_code=429)

    finally:
        conn.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
API_DB_HOST=localhost API_DB_USER=postgres API_DB_PASSWORD=postgres API_DB_NAME=data_ngin_test \
DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test \
poetry run pytest tests/api/test_app.py -v
```
Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add src/api/app.py tests/api/test_app.py
git commit -m "Add the query endpoint

One endpoint, because the caller's role decides what their SQL may do. There is
no read/write split in the API surface -- that distinction belongs to the
database.

Outcomes map to status codes deliberately: 401 unknown key, 403 refused by the
role, 429 at capacity, 400 for anything else Postgres rejected. A syntax error
is the caller's mistake, not a server fault, so it is not a 500.

Every branch writes an audit row before returning, including the ones that fail
authentication. A test asserts that a rejected key is not echoed back in the
error response."
```

---

### Task 9: Schema metadata endpoints

**Files:**
- Modify: `src/api/app.py` (append endpoints)
- Test: `tests/api/test_metadata.py`

**Interfaces:**
- Consumes: `authenticate` from Task 5, `execute_as` from Task 6
- Produces: `GET /v1/schemas`, `GET /v1/tables?schema=`, `GET /v1/columns?schema=&table=`

- [ ] **Step 1: Write the failing test**

Create `tests/api/test_metadata.py`:

```python
import os
import unittest

import psycopg2
from fastapi.testclient import TestClient

from src.api.app import app
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
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)
        cls.admin = psycopg2.connect(**_dsn())
        cls.admin.autocommit = True
        with cls.admin.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS research")
            cur.execute("CREATE TABLE IF NOT EXISTS research.probe (id int, label text)")
            cur.execute("GRANT USAGE ON SCHEMA research TO db_readonly")
            cur.execute("GRANT SELECT ON research.probe TO db_readonly")
        cls.key, key_hash, prefix = generate_key("db_readonly")
        with cls.admin.cursor() as cur:
            cur.execute(
                "INSERT INTO auth.api_keys (email, name, db_role, key_hash, key_prefix)"
                " VALUES (%s,%s,%s,%s,%s)",
                ("test-meta@x.com", "Meta", "db_readonly", key_hash, prefix),
            )

    @classmethod
    def tearDownClass(cls):
        with cls.admin.cursor() as cur:
            cur.execute("DELETE FROM auth.api_keys WHERE email LIKE 'test-%'")
            cur.execute("DELETE FROM auth.audit_log WHERE actor_email LIKE 'test-%'")
        cls.admin.close()

    def _get(self, path):
        return self.client.get(
            path, headers={"Authorization": f"Bearer {self.key}"}
        )

    def test_schemas_lists_research(self):
        r = self._get("/v1/schemas")
        self.assertEqual(r.status_code, 200)
        self.assertIn("research", r.json()["schemas"])

    def test_schemas_excludes_auth(self):
        """auth holds the key table and the audit log; db_readonly has no
        access, so information_schema filters it out automatically."""
        self.assertNotIn("auth", self._get("/v1/schemas").json()["schemas"])

    def test_tables_lists_probe(self):
        r = self._get("/v1/tables?schema=research")
        self.assertIn("probe", r.json()["tables"])

    def test_columns_returns_names_and_types(self):
        r = self._get("/v1/columns?schema=research&table=probe")
        cols = {c["name"]: c["type"] for c in r.json()["columns"]}
        self.assertEqual(cols["id"], "integer")
        self.assertEqual(cols["label"], "text")

    def test_metadata_requires_a_key(self):
        self.assertEqual(self.client.get("/v1/schemas").status_code, 401)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test API_DB_HOST=localhost API_DB_USER=postgres API_DB_PASSWORD=postgres API_DB_NAME=data_ngin_test poetry run pytest tests/api/test_metadata.py -v`
Expected: FAIL — 404 on `/v1/schemas`.

- [ ] **Step 3: Append to `src/api/app.py`**

Add at the end of the file:

```python
def _metadata_query(authorization, sql, params=None):
    """Run a service-constructed metadata query as the caller.

    information_schema is filtered by Postgres to what the calling role may see,
    so these endpoints need no permission logic of their own -- a general member
    simply does not see tables they cannot read.

    Query parameters are passed to psycopg2 rather than interpolated. The
    caller's role would bound the damage either way, but building SQL out of
    user input by hand is a habit worth not having.
    """
    key = _bearer(authorization)
    conn = _connect()
    conn.autocommit = True
    try:
        caller = authenticate(conn, key)
        if caller is None:
            return None, JSONResponse(
                {"detail": "invalid API key"}, status_code=401
            )
        return execute_as(conn, caller, sql, ROW_LIMIT, params), None
    finally:
        conn.close()


@app.get("/v1/schemas")
def list_schemas(authorization: str | None = Header(default=None)):
    result, error = _metadata_query(
        authorization,
        "SELECT schema_name FROM information_schema.schemata"
        " WHERE schema_name NOT LIKE 'pg\\_%'"
        "   AND schema_name <> 'information_schema'"
        " ORDER BY schema_name",
    )
    if error is not None:
        return error
    return {"schemas": [r[0] for r in result.rows]}


@app.get("/v1/tables")
def list_tables(
    schema: str, authorization: str | None = Header(default=None)
):
    result, error = _metadata_query(
        authorization,
        "SELECT table_name FROM information_schema.tables"
        " WHERE table_schema = %s ORDER BY table_name",
        (schema,),
    )
    if error is not None:
        return error
    return {"schema": schema, "tables": [r[0] for r in result.rows]}


@app.get("/v1/columns")
def list_columns(
    schema: str, table: str, authorization: str | None = Header(default=None)
):
    result, error = _metadata_query(
        authorization,
        "SELECT column_name, data_type FROM information_schema.columns"
        " WHERE table_schema = %s AND table_name = %s"
        " ORDER BY ordinal_position",
        (schema, table),
    )
    if error is not None:
        return error
    return {
        "schema": schema,
        "table": table,
        "columns": [{"name": r[0], "type": r[1]} for r in result.rows],
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test API_DB_HOST=localhost API_DB_USER=postgres API_DB_PASSWORD=postgres API_DB_NAME=data_ngin_test poetry run pytest tests/api/test_metadata.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/api/app.py tests/api/test_metadata.py
git commit -m "Add schema discovery endpoints

Tooling needs to read structure programmatically rather than a person reading
it out of pgAdmin. These are wrappers over information_schema, which Postgres
already filters to what the calling role may see -- so they need no permission
logic of their own, and a test asserts a read-only caller cannot see auth."
```

---

### Task 10: Key management CLI

**Files:**
- Create: `scripts/manage_api_keys.py`
- Test: `tests/api/test_manage_keys.py`

**Interfaces:**
- Consumes: `generate_key`, `hash_key` from Task 3
- Produces: `create_key(conn, email, name, db_role, created_by) -> str`, `revoke_key(conn, email) -> bool`, `list_keys(conn) -> list[dict]`, and a `main()` argparse entry point.

- [ ] **Step 1: Write the failing test**

Create `tests/api/test_manage_keys.py`:

```python
import os
import unittest

import psycopg2

from scripts.manage_api_keys import create_key, list_keys, revoke_key
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test poetry run pytest tests/api/test_manage_keys.py -v`
Expected: FAIL — `No module named 'scripts.manage_api_keys'`.

- [ ] **Step 3: Write the implementation**

Create `scripts/manage_api_keys.py`:

```python
#!/usr/bin/env python3
"""Admin CLI for API keys.

A script rather than an endpoint, to avoid a bootstrap problem: an endpoint
would require a key in order to create the first key.

Usage:
    python -m scripts.manage_api_keys create --email a@b.com --name "A B" --role db_readwrite
    python -m scripts.manage_api_keys revoke --email a@b.com
    python -m scripts.manage_api_keys list
"""

import argparse
import os
import sys

import psycopg2

from src.api.keys import ROLE_PREFIXES, generate_key


def _connect():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ["DB_NAME"],
    )


def create_key(conn, email: str, name: str, db_role: str, created_by: str) -> str:
    """Create or rotate a key. Returns the plaintext, which is the only copy.

    Rotating overwrites the hash, so the previous key stops working at once.
    """
    if db_role not in ROLE_PREFIXES:
        raise ValueError(
            f"unknown role {db_role!r}; expected one of {sorted(ROLE_PREFIXES)}"
        )
    plaintext, key_hash, prefix = generate_key(db_role)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO auth.api_keys"
            " (email, name, db_role, key_hash, key_prefix, created_by,"
            "  active, revoked_at)"
            " VALUES (%s, %s, %s, %s, %s, %s, TRUE, NULL)"
            " ON CONFLICT (email) DO UPDATE SET"
            "   name = EXCLUDED.name,"
            "   db_role = EXCLUDED.db_role,"
            "   key_hash = EXCLUDED.key_hash,"
            "   key_prefix = EXCLUDED.key_prefix,"
            "   active = TRUE,"
            "   revoked_at = NULL",
            (email, name, db_role, key_hash, prefix, created_by),
        )
    if not conn.autocommit:
        conn.commit()
    return plaintext


def revoke_key(conn, email: str) -> bool:
    """Deactivate a key. Returns False if there was no such person.

    The row is kept rather than deleted, so 'who had access in March?' stays
    answerable.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE auth.api_keys"
            "   SET active = FALSE, revoked_at = now()"
            " WHERE email = %s AND active",
            (email,),
        )
        changed = cur.rowcount
    if not conn.autocommit:
        conn.commit()
    return changed > 0


def list_keys(conn) -> list:
    """List keys. Never returns key_hash."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT email, name, db_role, key_prefix, active, created_at,"
            "       last_used_at"
            "  FROM auth.api_keys ORDER BY email"
        )
        return [
            {
                "email": r[0], "name": r[1], "db_role": r[2], "key_prefix": r[3],
                "active": r[4], "created_at": r[5], "last_used_at": r[6],
            }
            for r in cur.fetchall()
        ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage data-ngin API keys")
    sub = parser.add_subparsers(dest="command", required=True)

    c = sub.add_parser("create", help="create or rotate a key")
    c.add_argument("--email", required=True)
    c.add_argument("--name", required=True)
    c.add_argument("--role", required=True, choices=sorted(ROLE_PREFIXES))
    c.add_argument("--created-by", default=os.environ.get("USER", "unknown"))

    r = sub.add_parser("revoke", help="deactivate a key")
    r.add_argument("--email", required=True)

    sub.add_parser("list", help="list keys")

    args = parser.parse_args()
    conn = _connect()
    conn.autocommit = True
    try:
        if args.command == "create":
            plaintext = create_key(
                conn, args.email, args.name, args.role, args.created_by
            )
            print(f"Key for {args.email} ({args.role}):\n")
            print(f"    {plaintext}\n")
            print("This is the only time it will be shown. Send it to them")
            print("privately -- not in the repo, not in a public channel.")
        elif args.command == "revoke":
            if revoke_key(conn, args.email):
                print(f"Revoked {args.email}")
            else:
                print(f"No active key for {args.email}")
                return 1
        else:
            for row in list_keys(conn):
                state = "active" if row["active"] else "revoked"
                print(
                    f"{row['email']:<32} {row['name']:<20} {row['db_role']:<18} "
                    f"{row['key_prefix']:<12} {state}"
                )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `DB_HOST=localhost DB_PORT=5432 DB_USER=postgres DB_PASSWORD=postgres DB_NAME=data_ngin_test poetry run pytest tests/api/test_manage_keys.py -v`
Expected: 8 passed.

- [ ] **Step 5: Commit**

```bash
git add scripts/manage_api_keys.py tests/api/test_manage_keys.py
git commit -m "Add the key management CLI

A script rather than an endpoint, because an endpoint would need a key in order
to create the first key.

Rotation is an upsert that overwrites the hash, so the previous key stops
working immediately. Revocation deactivates rather than deletes -- a deleted row
cannot answer 'who had access in March?'

list never returns key_hash, and create prints the plaintext once with an
instruction not to put it in the repo."
```

---

### Task 11: Deployment

**Files:**
- Modify: `docker-compose.yml`
- Create: `Caddyfile`
- Create: `docs/api-gateway-deployment.md`

**Interfaces:**
- Consumes: everything above
- Produces: a running service on `data-ngin.algogators.com`

- [ ] **Step 1: Add the service to `docker-compose.yml`**

Append to the `services:` block, before `volumes:`:

```yaml
  api:
    build:
      context: .
      dockerfile: DockerFile
    container_name: data-ngin-api
    restart: always
    # Hard cap so the API is what dies under memory pressure. Without it the
    # kernel OOM killer picks by its own scoring, and the largest consumer on
    # this box is often Postgres -- the database could be lost to an API bug.
    mem_limit: 200m
    environment:
      API_DB_HOST: ${API_DB_HOST:?API_DB_HOST must be set in .env}
      API_DB_PORT: "5432"
      API_DB_USER: api_service
      API_DB_PASSWORD: ${API_DB_PASSWORD:?API_DB_PASSWORD must be set in .env}
      API_DB_NAME: ${API_DB_NAME:?API_DB_NAME must be set in .env}
      API_ROW_LIMIT: "100000"
      API_GLOBAL_CONCURRENCY: "2"
      PYTHONPATH: /opt/airflow/data_engine
    volumes:
      - .:/opt/airflow/data_engine
    working_dir: /opt/airflow/data_engine
    # Bound to localhost only. Caddy is the only thing on a public port.
    ports:
      - "127.0.0.1:8000:8000"
    command: ["uvicorn", "src.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 2: Verify compose parses**

Run: `docker compose config > /dev/null && echo OK`
Expected: `OK`. If it errors about a missing variable, that is the `:?` guard working — add the variables to `.env` first.

- [ ] **Step 3: Create the Caddyfile**

Create `Caddyfile`:

```
# TLS for the database API.
#
# Callers send their API key in a header; over plain HTTP anyone on the network
# path can read it. Caddy obtains and renews a Let's Encrypt certificate itself,
# rather than certbot plus a cron job -- this deployment already lost nine
# months of database backups to a scheduled job that failed silently, and
# certificate renewal has the same failure shape.

data-ngin.algogators.com {
    reverse_proxy localhost:8000
}
```

- [ ] **Step 4: Write the deployment runbook**

Create `docs/api-gateway-deployment.md`:

```markdown
# Deploying the database API gateway

Prerequisites are ordered. Each depends on the one before it.

## 1. Elastic IP

Allocate one and associate it with the instance. Without it the public address
changes whenever the instance is stopped and started -- which happens on resize
-- and the DNS record silently breaks.

Associating changes the server's public address, which breaks every member's
pgAdmin connection. Do it in the same window as a password rotation so the team
absorbs one disruption rather than two.

## 2. DNS

Add an A record: `data-ngin.algogators.com` -> the Elastic IP.

`algogators.com` is on GitHub Pages. This is an independent record; no traffic
passes through that site.

## 3. Database roles and tables

    psql "$DATABASE_URL" -f migrations/003_api_gateway_roles.sql
    psql "$DATABASE_URL" -f migrations/004_api_gateway_tables.sql

Then set a real password for the service account:

    ALTER ROLE api_service WITH PASSWORD '<generated>';

Put it in `.env` on the server as `API_DB_PASSWORD`, alongside `API_DB_HOST`
and `API_DB_NAME`.

## 4. Caddy

    sudo apt install -y caddy
    sudo cp Caddyfile /etc/caddy/Caddyfile
    sudo systemctl reload caddy

Caddy obtains the certificate on first start. Open 443 in the security group.

## 5. The service

    docker compose up -d api
    curl https://data-ngin.algogators.com/healthz

## 6. First key

    python -m scripts.manage_api_keys create \
        --email you@example.com --name "Your Name" --role db_readwrite_all

The plaintext is printed once.

## Verifying it works

    curl -s https://data-ngin.algogators.com/v1/query \
      -H "Authorization: Bearer $KEY" \
      -H "Content-Type: application/json" \
      -d '{"sql":"SELECT count(*) FROM equities_data.ohlcv_1d"}'

Then confirm it was recorded:

    SELECT actor_name, outcome, statement, occurred_at
      FROM auth.audit_log ORDER BY id DESC LIMIT 5;

## Rolling back

    docker compose stop api

The roles and tables are additive and harm nothing if left in place. Removing
them entirely:

    DROP TABLE auth.audit_log, auth.api_keys;
    DROP ROLE api_service, db_readwrite_all, db_readwrite, db_readonly;

`DROP ROLE` fails if a role owns objects. Reassign them first:

    REASSIGN OWNED BY db_readwrite TO postgres;
```

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml Caddyfile docs/api-gateway-deployment.md
git commit -m "Add deployment for the API service

The container gets a hard 200MB memory limit so that it is what dies under
pressure. Without one the kernel OOM killer chooses by its own scoring, and the
largest consumer on this box is often Postgres -- an API bug could cost the
database.

The service binds to 127.0.0.1 only; Caddy is the sole process on a public
port. Caddy rather than certbot because certificate renewal is a scheduled job
that can fail silently, and this deployment already lost nine months of backups
to exactly that.

The runbook orders the prerequisites, because each depends on the one before:
Elastic IP, then DNS, then roles, then Caddy, then the service."
```

---

## Verification

After all tasks, from a clean checkout with a throwaway Postgres running:

```bash
docker run -d --name pgverify -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=data_ngin_test -p 5432:5432 postgres:16

export DB_HOST=localhost DB_PORT=5432 DB_USER=postgres \
       DB_PASSWORD=postgres DB_NAME=data_ngin_test
export API_DB_HOST=localhost API_DB_USER=postgres \
       API_DB_PASSWORD=postgres API_DB_NAME=data_ngin_test

for f in migrations/003_api_gateway_roles.sql migrations/004_api_gateway_tables.sql; do
    PGPASSWORD=postgres psql -h localhost -U postgres -d data_ngin_test \
        -v ON_ERROR_STOP=1 -f "$f"
done

poetry run pytest tests/api/ -v
docker rm -f pgverify
```

Expected: 65 passed.

Also confirm the suite still passes with no database available, since that is how it runs on a laptop:

```bash
env -u DB_HOST -u DB_PORT -u DB_USER -u DB_PASSWORD -u DB_NAME \
    poetry run pytest tests/api/ -v
```

Expected: the pure-logic tests pass and the integration tests report as skipped, not failed.
