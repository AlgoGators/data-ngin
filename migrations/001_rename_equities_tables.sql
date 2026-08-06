-- Migration 001: rename equities tables to the cross-repo naming contract.
--
-- Context: adr/ADR-000-cross-repo-contracts.md (C-2) and adr/ADR-002-data-ngin.md (D-1).
-- trade-ngin's build_table_name(asset_class, data_type, freq) resolves equities to
-- "equities_data.ohlcv_1d" / "equities_data.ohlcv_1d_raw" -- it can never find a table
-- named "equities" / "equities_raw", so equities backtests fail with DATA_NOT_FOUND
-- until this migration runs. This is a pure rename: no data is copied or dropped.
--
-- NAME COLLISION -- read this before running.
-- equities_data.ohlcv_1d was ALREADY TAKEN when this migration was written, by a
-- retired Sharadar-sourced feed (ticker/date/close/closeadj/closeunadj/lastupdated;
-- 45,488,166 rows spanning 1997-12-31..2025-08-28). The original version of this
-- migration aborted on its own guard because of it. That table is NOT dropped here --
-- the fund still wants the history -- it is renamed to equities_data.sharadar_ohlcv_1d
-- so the canonical name is free for the live Tiingo feed. Step 1 below does that;
-- step 2 is the original rename.
--
-- Naming rationale: trade-ngin's build_table_name() composes {schema}.{data_type}_{freq}
-- from an enum, so it can only ever generate "ohlcv_1d". Prefixing the retired feed with
-- its source makes it structurally unreachable by the backtester rather than relying on
-- convention. Retirement status is recorded in a COMMENT rather than in the table name,
-- so the name stays descriptive instead of carrying a status that goes stale.
--
-- Run against: new_algo_data (the database config_tiingo.yaml's database.db_name points at)
-- Prerequisite: config_tiingo.yaml has already been updated to raw_table=ohlcv_1d_raw,
--               table=ohlcv_1d (done in the same change as this file).
-- Safe to run while the Tiingo DAG is scheduled but NOT while a run is in-flight --
-- take the 07:15 ET tiingo_data_dag run window into account when scheduling this.
-- Deploy scripts/check_data_freshness.py in the same window: it reads
-- equities_data.ohlcv_1d, whose timestamp column becomes "time" (was "date").
--
-- All renames are catalog-only -- no data is copied, scanned, or dropped -- so the
-- 45M-row Sharadar rename is as instant as the others. All of it is one transaction.
--
-- Usage:
--   psql "$DATABASE_URL" -f migrations/001_rename_equities_tables.sql
--
-- Rollback: see 001_rename_equities_tables.rollback.sql in this directory.

BEGIN;

-- Guard: fail loudly instead of silently no-op'ing if the source tables are already gone
-- (e.g. this migration already ran), or if ohlcv_1d is occupied by something other than
-- the retired Sharadar feed we expect to move aside in step 1.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'equities_data' AND table_name = 'equities'
    ) THEN
        RAISE EXCEPTION 'equities_data.equities does not exist -- migration already applied, or wrong database';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'equities_data' AND table_name = 'sharadar_ohlcv_1d'
    ) THEN
        RAISE EXCEPTION 'equities_data.sharadar_ohlcv_1d already exists -- step 1 already ran, or the name is taken';
    END IF;

    -- If ohlcv_1d exists it must be the retired Sharadar feed, identified by its
    -- closeadj column (the Tiingo shape uses adjusted_close and has no closeadj).
    -- Anything else means the database is not in the state this migration expects.
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'equities_data' AND table_name = 'ohlcv_1d'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'equities_data' AND table_name = 'ohlcv_1d'
          AND column_name = 'closeadj'
    ) THEN
        RAISE EXCEPTION 'equities_data.ohlcv_1d exists but is not the retired Sharadar table (no closeadj column) -- refusing to touch it';
    END IF;
END $$;

-- Step 1: move the retired Sharadar feed off the canonical name. Skipped cleanly if it
-- was already moved by hand. The index is renamed alongside it so nothing is left
-- claiming an ohlcv_1d-derived name -- both for clarity and so a future index on the
-- real ohlcv_1d can use the obvious name without colliding.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'equities_data' AND table_name = 'ohlcv_1d'
    ) THEN
        ALTER TABLE equities_data.ohlcv_1d RENAME TO sharadar_ohlcv_1d;

        IF EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'equities_data' AND indexname = 'idx_ohlcv_1d_ticker_date'
        ) THEN
            ALTER INDEX equities_data.idx_ohlcv_1d_ticker_date
                RENAME TO idx_sharadar_ohlcv_1d_ticker_date;
        END IF;

        COMMENT ON TABLE equities_data.sharadar_ohlcv_1d IS
            'Retired Sharadar-sourced equities EOD feed (45.5M rows, 1997-12-31..2025-08-28). '
            'Not written to by any active pipeline; superseded by equities_data.ohlcv_1d, '
            'which the Tiingo pipeline populates. Retained for historical reference -- do '
            'not drop without checking with the team. Renamed from equities_data.ohlcv_1d '
            'by migrations/001_rename_equities_tables.sql.';
    END IF;
END $$;

-- Step 2: the original rename -- put the live Tiingo tables on the contract names.
ALTER TABLE equities_data.equities      RENAME TO ohlcv_1d;
ALTER TABLE equities_data.equities_raw  RENAME TO ohlcv_1d_raw;

-- Rename the primary-key / index constraints too, so their names don't keep referencing
-- the old table (cosmetic, but avoids confusing `\d equities_data.ohlcv_1d` output later).
DO $$
DECLARE
    old_pk_name text;
    old_pk_name_raw text;
BEGIN
    SELECT conname INTO old_pk_name
    FROM pg_constraint
    WHERE conrelid = 'equities_data.ohlcv_1d'::regclass AND contype = 'p';
    IF old_pk_name IS NOT NULL AND old_pk_name LIKE '%equities%' THEN
        EXECUTE format('ALTER TABLE equities_data.ohlcv_1d RENAME CONSTRAINT %I TO ohlcv_1d_pkey', old_pk_name);
    END IF;

    SELECT conname INTO old_pk_name_raw
    FROM pg_constraint
    WHERE conrelid = 'equities_data.ohlcv_1d_raw'::regclass AND contype = 'p';
    IF old_pk_name_raw IS NOT NULL AND old_pk_name_raw LIKE '%equities%' THEN
        EXECUTE format('ALTER TABLE equities_data.ohlcv_1d_raw RENAME CONSTRAINT %I TO ohlcv_1d_raw_pkey', old_pk_name_raw);
    END IF;
END $$;

COMMIT;

-- Verification (run manually after commit). Pre-migration values as of 2026-08-05:
--   SELECT COUNT(*) FROM equities_data.ohlcv_1d;           -- expect 3,200,770 (was equities)
--   SELECT COUNT(*) FROM equities_data.ohlcv_1d_raw;       -- expect 3,896,875 (was equities_raw)
--   SELECT MAX(time) FROM equities_data.ohlcv_1d;          -- expect 2026-08-04 or later
--   SELECT COUNT(*) FROM equities_data.sharadar_ohlcv_1d;  -- expect 45,488,166, unchanged
--   \dt equities_data.*   -- expect corporate_action, ohlcv_1d, ohlcv_1d_raw, sharadar_ohlcv_1d
--
-- The Tiingo table is the one that must show a current MAX(time); sharadar_ohlcv_1d is
-- frozen at 2025-08-28 by design.
