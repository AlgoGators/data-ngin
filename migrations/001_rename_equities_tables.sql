-- Migration 001: rename equities tables to the cross-repo naming contract.
--
-- Context: adr/ADR-000-cross-repo-contracts.md (C-2) and adr/ADR-002-data-ngin.md (D-1).
-- trade-ngin's build_table_name(asset_class, data_type, freq) resolves equities to
-- "equities_data.ohlcv_1d" / "equities_data.ohlcv_1d_raw" -- it can never find a table
-- named "equities" / "equities_raw", so equities backtests fail with DATA_NOT_FOUND
-- until this migration runs. This is a pure rename: no data is copied or dropped.
--
-- Run against: new_algo_data (the database config_tiingo.yaml's database.db_name points at)
-- Prerequisite: config_tiingo.yaml has already been updated to raw_table=ohlcv_1d_raw,
--               table=ohlcv_1d (done in the same change as this file).
-- Safe to run while the Tiingo DAG is scheduled but NOT while a run is in-flight --
-- take the 07:15 ET tiingo_data_dag run window into account when scheduling this.
--
-- Usage:
--   psql "$DATABASE_URL" -f migrations/001_rename_equities_tables.sql
--
-- Rollback: see 001_rename_equities_tables.rollback.sql in this directory.

BEGIN;

-- Guard: fail loudly instead of silently no-op'ing if the source tables are already gone
-- (e.g. this migration already ran) or the targets already exist (name collision).
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
        WHERE table_schema = 'equities_data' AND table_name = 'ohlcv_1d'
    ) THEN
        RAISE EXCEPTION 'equities_data.ohlcv_1d already exists -- refusing to overwrite';
    END IF;
END $$;

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

-- Verification (run manually after commit):
--   SELECT COUNT(*) FROM equities_data.ohlcv_1d;
--   SELECT COUNT(*) FROM equities_data.ohlcv_1d_raw;
--   SELECT MAX(time) FROM equities_data.ohlcv_1d;   -- should match pre-migration MAX(time)
--   \dt equities_data.*                             -- should show ohlcv_1d / ohlcv_1d_raw only
