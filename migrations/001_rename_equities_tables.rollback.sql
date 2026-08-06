-- Rollback for 001_rename_equities_tables.sql.
-- Reverses both steps: the Tiingo tables go back to equities/equities_raw, and the
-- retired Sharadar feed goes back to equities_data.ohlcv_1d (reclaiming the name it
-- held before the forward migration). Also reverses the index rename and drops the
-- retirement COMMENT, so the schema is byte-for-byte where it started.
-- Reverses the rename only. Safe to run at any point after the forward migration,
-- as long as no new rows have been inserted under names that depend on the new schema
-- (the Tiingo pipeline itself is agnostic -- it reads table names from config_tiingo.yaml,
-- so revert that file's raw_table/table keys back to equities_raw/equities BEFORE running
-- this, or the next DAG run will insert into equities_data.ohlcv_1d_raw again and this
-- rollback will find the wrong table already occupying the "raw" name).

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'equities_data' AND table_name = 'ohlcv_1d'
    ) THEN
        RAISE EXCEPTION 'equities_data.ohlcv_1d does not exist -- nothing to roll back';
    END IF;

    -- Symmetric with the forward guard: the target names must be free before we rename
    -- onto them, so a partial or repeated rollback fails with a clear message rather
    -- than a bare Postgres "relation already exists".
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'equities_data' AND table_name = 'equities'
    ) THEN
        RAISE EXCEPTION 'equities_data.equities already exists -- rollback already ran, or the name is taken';
    END IF;

    -- ohlcv_1d must be the Tiingo table (adjusted_close, no closeadj), not the retired
    -- Sharadar feed -- otherwise the forward migration never completed and renaming it
    -- to "equities" would mislabel Sharadar data as the live feed.
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'equities_data' AND table_name = 'ohlcv_1d'
          AND column_name = 'adjusted_close'
    ) THEN
        RAISE EXCEPTION 'equities_data.ohlcv_1d is not the Tiingo table (no adjusted_close column) -- refusing to roll back';
    END IF;
END $$;

ALTER TABLE equities_data.ohlcv_1d      RENAME TO equities;
ALTER TABLE equities_data.ohlcv_1d_raw  RENAME TO equities_raw;

-- Reverse step 1: put the retired Sharadar feed back on the canonical name. Only runs
-- if the forward migration actually moved it; skipped cleanly otherwise.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'equities_data' AND table_name = 'sharadar_ohlcv_1d'
    ) THEN
        ALTER TABLE equities_data.sharadar_ohlcv_1d RENAME TO ohlcv_1d;

        IF EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'equities_data' AND indexname = 'idx_sharadar_ohlcv_1d_ticker_date'
        ) THEN
            ALTER INDEX equities_data.idx_sharadar_ohlcv_1d_ticker_date
                RENAME TO idx_ohlcv_1d_ticker_date;
        END IF;

        COMMENT ON TABLE equities_data.ohlcv_1d IS NULL;
    END IF;
END $$;

DO $$
DECLARE
    pk_name text;
    pk_name_raw text;
BEGIN
    SELECT conname INTO pk_name
    FROM pg_constraint
    WHERE conrelid = 'equities_data.equities'::regclass AND contype = 'p';
    IF pk_name IS NOT NULL AND pk_name LIKE '%ohlcv_1d%' THEN
        EXECUTE format('ALTER TABLE equities_data.equities RENAME CONSTRAINT %I TO equities_pkey', pk_name);
    END IF;

    SELECT conname INTO pk_name_raw
    FROM pg_constraint
    WHERE conrelid = 'equities_data.equities_raw'::regclass AND contype = 'p';
    IF pk_name_raw IS NOT NULL AND pk_name_raw LIKE '%ohlcv_1d%' THEN
        EXECUTE format('ALTER TABLE equities_data.equities_raw RENAME CONSTRAINT %I TO equities_raw_pkey', pk_name_raw);
    END IF;
END $$;

COMMIT;
