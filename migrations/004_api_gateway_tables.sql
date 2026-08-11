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

-- Authentication happens before the caller's role is known, so it cannot run
-- under any of the three roles -- it needs a login with direct access to the
-- key table. api_service_ro carries that, and the service uses it for both the
-- key lookup and for writing audit rows.
--
-- Only api_service_ro, not all three. Each login is NOINHERIT, so a caller
-- cannot reach these grants without a SET ROLE that Postgres refuses; giving
-- the same access to _rw and _all would widen the blast radius of a leaked
-- password for no gain.
--
-- Direct grants, not role membership: NOINHERIT suppresses privileges inherited
-- through a role, but not privileges granted to the login itself.
GRANT USAGE ON SCHEMA auth TO api_service_ro;
GRANT SELECT ON auth.api_keys TO api_service_ro;
GRANT UPDATE (last_used_at) ON auth.api_keys TO api_service_ro;
GRANT INSERT ON auth.audit_log TO api_service_ro;
GRANT USAGE ON SEQUENCE auth.audit_log_id_seq TO api_service_ro;

-- audit_log.id is BIGSERIAL, so INSERT on the table is not sufficient on its
-- own -- writing a row also needs USAGE on the backing sequence. Without this,
-- migration 003's "GRANT ALL ON ALL TABLES IN SCHEMA auth TO db_readwrite_all"
-- does not actually permit an insert, which is a confusing state to leave for
-- whoever reads the grants and believes them.
GRANT USAGE ON SEQUENCE auth.audit_log_id_seq TO db_readwrite_all;

-- Remove the single-login form from any earlier revision. It was granted SELECT
-- on auth.api_keys, so anyone still holding its password could read every key
-- hash in the system. Revoked before dropping, so the grants are gone even if
-- the DROP is refused because the role owns an object.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_service') THEN
        REVOKE ALL ON auth.api_keys FROM api_service;
        REVOKE ALL ON auth.audit_log FROM api_service;
        REVOKE ALL ON SCHEMA auth FROM api_service;
        REVOKE ALL ON SEQUENCE auth.audit_log_id_seq FROM api_service;
        BEGIN
            DROP ROLE api_service;
        EXCEPTION WHEN dependent_objects_still_exist THEN
            RAISE NOTICE 'api_service still owns objects; grants revoked but role retained';
        END;
    END IF;
END $$;

COMMIT;
