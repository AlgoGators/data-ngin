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
        -- Created rather than guarded on. A guard would skip the grants entirely
        -- on a database where the schema does not exist yet -- and because
        -- ALTER DEFAULT PRIVILEGES would be skipped too, tables created later
        -- would also have no grants. That makes the migration order-dependent,
        -- which a migration must not be. CREATE SCHEMA IF NOT EXISTS is a no-op
        -- in production, where all of these already exist.
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', s);
        EXECUTE format('GRANT USAGE ON SCHEMA %I TO db_readonly, db_readwrite, db_readwrite_all', s);
        EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO db_readonly, db_readwrite, db_readwrite_all', s);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO db_readonly, db_readwrite, db_readwrite_all', s);
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
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', s);
        EXECUTE format('GRANT CREATE ON SCHEMA %I TO db_readwrite', s);
        EXECUTE format('GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %I TO db_readwrite', s);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT INSERT, UPDATE, DELETE ON TABLES TO db_readwrite', s);
    END LOOP;
END $$;

-- db_readwrite_all may modify everything, including trading and auth.
DO $$
DECLARE s text;
BEGIN
    FOREACH s IN ARRAY ARRAY['equities_data','futures_data','backtest','research',
                             'synthetic','macro_data','eia','metadata','trading','auth']
    LOOP
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', s);
        EXECUTE format('GRANT USAGE, CREATE ON SCHEMA %I TO db_readwrite_all', s);
        EXECUTE format('GRANT ALL ON ALL TABLES IN SCHEMA %I TO db_readwrite_all', s);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON TABLES TO db_readwrite_all', s);
    END LOOP;
END $$;

-- THREE logins, one per role. This is not redundancy -- a single login that was
-- a member of all three roles would be trivially escalatable.
--
-- Postgres authorises SET ROLE against session_user, not current_user. So with
-- one shared login, SET LOCAL ROLE db_readonly narrows current_user but leaves
-- session_user a member of every role, and any caller escalates by prefixing
-- nine characters to their SQL:
--
--     SET ROLE db_readwrite_all; INSERT INTO trading.positions ...
--
-- That was verified working from db_readonly against a real database before
-- this was changed. Because the service deliberately never inspects the SQL it
-- is given, there is no layer that would catch it.
--
-- With one login per role, the same statement fails: api_service_ro is not a
-- member of db_readwrite_all, so Postgres refuses the SET ROLE outright.
--
-- NOINHERIT is retained on top of that. It means each login holds none of its
-- role's privileges until it issues SET ROLE, so a code path that forgets to
-- switch fails closed rather than running with that role's access.
DO $$
DECLARE r record;
BEGIN
    FOR r IN
        SELECT * FROM (VALUES
            ('api_service_ro',  'db_readonly'),
            ('api_service_rw',  'db_readwrite'),
            ('api_service_all', 'db_readwrite_all')
        ) AS t(login, granted)
    LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r.login) THEN
            EXECUTE format(
                'CREATE ROLE %I LOGIN NOINHERIT PASSWORD %L',
                r.login, 'CHANGE_ME_BEFORE_DEPLOY'
            );
        END IF;
        EXECUTE format('GRANT %I TO %I', r.granted, r.login);
    END LOOP;
END $$;

-- Remove the single-login form if an earlier revision of this migration created
-- it. Leaving it in place would leave the escalation path open.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_service') THEN
        REVOKE db_readonly, db_readwrite, db_readwrite_all FROM api_service;
    END IF;
END $$;

COMMIT;

-- After running, set a real password for each and put them in the service's
-- environment as API_DB_PASSWORD_RO / _RW / _ALL:
--   ALTER ROLE api_service_ro  WITH PASSWORD '<generated>';
--   ALTER ROLE api_service_rw  WITH PASSWORD '<generated>';
--   ALTER ROLE api_service_all WITH PASSWORD '<generated>';
