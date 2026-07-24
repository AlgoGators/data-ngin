-- Rollback for 001_rename_equities_tables.sql.
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
END $$;

ALTER TABLE equities_data.ohlcv_1d      RENAME TO equities;
ALTER TABLE equities_data.ohlcv_1d_raw  RENAME TO equities_raw;

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
