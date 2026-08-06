-- Rollback for 004_rename_sats_to_echo.sql.
--
-- Puts the EchoStar series back under SATS.
--
-- CAVEAT -- this is only a clean reversal if run BEFORE the pipeline has written
-- any ECHO rows of its own. Once the new contract is deployed, the daily run
-- appends genuine ECHO bars, and this rollback cannot tell those apart from the
-- rows 004 renamed: it moves ALL of them back to SATS. That is still coherent
-- (one continuous series under the old ticker) but it is a rename of everything,
-- not an undo of just what 004 touched.
--
-- Also revert contract_tiingo.csv to SATS before running this, or the next DAG run
-- will immediately start writing ECHO rows again. In build_tiingo_universe.py,
-- remove the SATS entry from TICKER_RENAMES and rebuild.
--
-- Usage:
--   psql "$DATABASE_URL" -f migrations/004_rename_sats_to_echo.rollback.sql

BEGIN;

DO $$
DECLARE
    collisions bigint;
    echo_rows  bigint;
BEGIN
    SELECT count(*) INTO echo_rows FROM equities_data.ohlcv_1d WHERE symbol = 'ECHO';
    IF echo_rows = 0 THEN
        RAISE EXCEPTION 'no ECHO rows in equities_data.ohlcv_1d -- 004 was never applied, or already rolled back';
    END IF;

    SELECT count(*) INTO collisions
    FROM equities_data.ohlcv_1d a
    JOIN equities_data.ohlcv_1d b ON a."time" = b."time"
    WHERE a.symbol = 'ECHO' AND b.symbol = 'SATS';
    IF collisions > 0 THEN
        RAISE EXCEPTION
            'ECHO and SATS both hold bars for % day(s) -- resolve the overlap before renaming back',
            collisions;
    END IF;
END $$;

UPDATE equities_data.ohlcv_1d     SET symbol = 'SATS' WHERE symbol = 'ECHO';
UPDATE equities_data.ohlcv_1d_raw SET symbol = 'SATS' WHERE symbol = 'ECHO';

COMMIT;

-- Verification (run manually after commit):
--   SELECT count(*) FROM equities_data.ohlcv_1d WHERE symbol='SATS';  -- expect the full series
--   SELECT count(*) FROM equities_data.ohlcv_1d WHERE symbol='ECHO';  -- expect 0
