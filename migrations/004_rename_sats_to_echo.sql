-- Migration 004: rename the SATS price series to ECHO.
--
-- EchoStar changed its ticker from SATS to ECHO. Tiingo has already moved: the
-- metadata endpoint for SATS returns 404 while ECHO resolves to "EchoStar Corp -
-- Class A" (2008-01-02..). Only the prices endpoint still answers to the old
-- string, and that will not last.
--
-- WHY THIS IS A MIGRATION AND NOT JUST A CONTRACT EDIT
-- contract_tiingo.csv now carries ECHO instead of SATS. Without this file the
-- daily run would start a NEW series under ECHO from today forward while 4,677
-- rows of history sat orphaned under SATS -- one company split across two symbols,
-- with neither half complete. Any backtest joining on ticker would see a stock
-- that IPO'd today and another that vanished yesterday.
--
-- Verified before writing (2026-08-06):
--   equities_data.ohlcv_1d      SATS 4,677 rows 2008-01-02..2026-08-05, ECHO 0 rows
--   equities_data.ohlcv_1d_raw  SATS 4,653 rows 2008-01-02..2026-08-05, ECHO 0 rows
--   overlapping (symbol, time) keys between SATS and ECHO: 0
-- The zero overlap is what makes a plain UPDATE safe: nothing can collide with the
-- (symbol, time) primary key. The guard below re-checks it at run time rather than
-- trusting this comment, since rows keep arriving daily.
--
-- Prerequisite: 001 (tables must already be ohlcv_1d / ohlcv_1d_raw).
-- Run it BEFORE deploying the new contract, or the split described above begins.
--
-- Usage:
--   psql "$DATABASE_URL" -f migrations/004_rename_sats_to_echo.sql
--
-- Rollback: see 004_rename_sats_to_echo.rollback.sql in this directory.

BEGIN;

DO $$
DECLARE
    collisions bigint;
    sats_rows  bigint;
BEGIN
    SELECT count(*) INTO sats_rows FROM equities_data.ohlcv_1d WHERE symbol = 'SATS';
    IF sats_rows = 0 THEN
        RAISE EXCEPTION 'no SATS rows in equities_data.ohlcv_1d -- migration already applied, or wrong database';
    END IF;

    -- A collision means both symbols hold a bar for the same day, so the UPDATE
    -- would violate the (symbol, time) primary key. Refuse rather than let Postgres
    -- fail mid-statement with a less obvious message.
    SELECT count(*) INTO collisions
    FROM equities_data.ohlcv_1d a
    JOIN equities_data.ohlcv_1d b ON a."time" = b."time"
    WHERE a.symbol = 'SATS' AND b.symbol = 'ECHO';
    IF collisions > 0 THEN
        RAISE EXCEPTION
            'SATS and ECHO both hold bars for % day(s) -- resolve the overlap before renaming',
            collisions;
    END IF;
END $$;

UPDATE equities_data.ohlcv_1d     SET symbol = 'ECHO' WHERE symbol = 'SATS';
UPDATE equities_data.ohlcv_1d_raw SET symbol = 'ECHO' WHERE symbol = 'SATS';

COMMIT;

-- Verification (run manually after commit):
--   SELECT count(*) FROM equities_data.ohlcv_1d WHERE symbol='ECHO';  -- expect 4,677+
--   SELECT count(*) FROM equities_data.ohlcv_1d WHERE symbol='SATS';  -- expect 0
--   SELECT min(time)::date, max(time)::date FROM equities_data.ohlcv_1d WHERE symbol='ECHO';
--       -- expect 2008-01-02 .. current, one continuous series
