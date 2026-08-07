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
