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
