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
--
-- ORDER-INDEPENDENT. Run it before or after deploying the new contract, and re-run
-- it if the two got out of step -- day-level overlaps are merged, not refused. The
-- earlier version refused on any overlap, which meant getting the order wrong left
-- the series split with no clean way forward.
--
-- Usage:
--   psql "$DATABASE_URL" -f migrations/004_rename_sats_to_echo.sql
--
-- Rollback: see 004_rename_sats_to_echo.rollback.sql in this directory.

BEGIN;

DO $$
DECLARE
    sats_rows bigint;
BEGIN
    SELECT count(*) INTO sats_rows FROM equities_data.ohlcv_1d WHERE symbol = 'SATS';
    IF sats_rows = 0 THEN
        RAISE EXCEPTION 'no SATS rows in equities_data.ohlcv_1d -- migration already applied, or wrong database';
    END IF;
END $$;

-- Drop any SATS bar for a day ECHO already holds, THEN rename the rest.
--
-- This is what makes the migration order-independent. Either half of the change
-- landing alone splits EchoStar across two symbols:
--   * migration first, deploy later -> new bars keep arriving as SATS
--   * deploy first, migration later -> new bars arrive as ECHO, history stays SATS
-- Both are repaired by (re-)running this file, because the day-level overlap is
-- resolved instead of refused. Re-running once both sides are aligned is a no-op
-- beyond the guard above.
--
-- Deleting the SATS side of an overlap is safe: both rows describe the same
-- security on the same day, and the ECHO row is the one the live pipeline wrote.
DELETE FROM equities_data.ohlcv_1d s
 WHERE s.symbol = 'SATS'
   AND EXISTS (SELECT 1 FROM equities_data.ohlcv_1d e
                WHERE e.symbol = 'ECHO' AND e."time" = s."time");

DELETE FROM equities_data.ohlcv_1d_raw s
 WHERE s.symbol = 'SATS'
   AND EXISTS (SELECT 1 FROM equities_data.ohlcv_1d_raw e
                WHERE e.symbol = 'ECHO' AND e."time" = s."time");

UPDATE equities_data.ohlcv_1d     SET symbol = 'ECHO' WHERE symbol = 'SATS';
UPDATE equities_data.ohlcv_1d_raw SET symbol = 'ECHO' WHERE symbol = 'SATS';

COMMIT;

-- Verification (run manually after commit):
--   SELECT count(*) FROM equities_data.ohlcv_1d WHERE symbol='ECHO';  -- expect 4,677+
--   SELECT count(*) FROM equities_data.ohlcv_1d WHERE symbol='SATS';  -- expect 0
--   SELECT min(time)::date, max(time)::date FROM equities_data.ohlcv_1d WHERE symbol='ECHO';
--       -- expect 2008-01-02 .. current, one continuous series
