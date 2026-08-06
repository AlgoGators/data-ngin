-- Rollback for 003_survivorship_schema.sql.
--
-- Reverses the survivorship schema: drops the three new tables and the
-- delisting_date column, leaving equities_data byte-for-byte where 001 left it.
--
-- READ THIS BEFORE RUNNING -- unlike 001, this rollback DESTROYS DATA.
-- 001 was a pure rename, so reversing it lost nothing. This one drops tables that
-- may hold real work:
--   * sp500_membership   -- regenerable from committed snapshots via
--                           scripts/build_sp500_history.py, so cheap to lose.
--   * ticker_aliases     -- HAND-CURATED. Each row was verified individually against
--                           the live table; they are not reproducible by re-running
--                           anything. Back this table up before rolling back.
--   * coverage_gaps      -- regenerable via scripts/validate_sp500_entities.py, but
--                           only if the probe cache is still present.
--   * delisting_date     -- populated by the backfill; dropping the column discards
--                           it for every row and a full re-backfill would be needed.
--
-- Back up first if any of that matters:
--   pg_dump -t equities_data.ticker_aliases -t equities_data.sp500_membership \
--           -t equities_data.coverage_gaps "$DATABASE_URL" > survivorship_backup.sql
--
-- Usage:
--   psql "$DATABASE_URL" -f migrations/003_survivorship_schema.rollback.sql

BEGIN;

-- Guard: refuse if 002 was never applied, so a mistaken run says so plainly
-- instead of silently succeeding on nothing.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'equities_data'
          AND table_name = 'ohlcv_1d'
          AND column_name = 'delisting_date'
    ) THEN
        RAISE EXCEPTION 'equities_data.ohlcv_1d has no delisting_date column -- 002 was never applied, or already rolled back';
    END IF;
END $$;

DROP TABLE IF EXISTS equities_data.coverage_gaps;
DROP TABLE IF EXISTS equities_data.ticker_aliases;
DROP TABLE IF EXISTS equities_data.sp500_membership;

ALTER TABLE equities_data.ohlcv_1d DROP COLUMN IF EXISTS delisting_date;

COMMIT;

-- Verification (run manually after commit):
--   \d equities_data.ohlcv_1d   -- delisting_date should be gone
--   \dt equities_data.*         -- expect corporate_action, ohlcv_1d, ohlcv_1d_raw,
--                                  sharadar_ohlcv_1d, verified_absent_bars
