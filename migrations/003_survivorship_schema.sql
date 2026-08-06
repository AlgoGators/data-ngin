-- Migration 003: schema for the S&P 500 survivorship-bias backfill.
--
-- Follows the convention established by migrations/001_rename_equities_tables.sql:
-- numbered, hand-applied, with a matching .rollback.sql.
--
-- Prerequisite: 001 must already have run. This file targets equities_data.ohlcv_1d,
-- the name 001 moved the live Tiingo table onto. Applying it against a database still
-- holding equities_data.equities will fail on the ALTER below -- which is the correct
-- outcome, since the rest of the pipeline would be reading the wrong table anyway.
--
-- Usage:
--   psql "$DATABASE_URL" -f migrations/003_survivorship_schema.sql
--
-- Rollback: see 003_survivorship_schema.rollback.sql in this directory.
-- Every statement is idempotent, so re-running is safe.

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. delisting_date on the price table.
--
-- Plain NULLABLE metadata, deliberately NOT part of the primary key.
--
-- An earlier draft made it a key column to stop two companies that share a
-- ticker from merging into one series. Entity validation removes that need:
-- Tiingo serves exactly ONE security per ticker string (the current one), so the
-- old incarnation of a reused ticker can never be fetched from this source and
-- two companies can never land under one symbol. (symbol, time) stays correct,
-- and the 3.2M-row PK does not have to be rebuilt.
--
-- It still earns its place: it drives the `active` flag that keeps delisted
-- names out of the daily run, records WHY a symbol stopped updating, and
-- future-proofs a second data source that might not share Tiingo's behaviour.
-- ---------------------------------------------------------------------------
ALTER TABLE equities_data.ohlcv_1d
    ADD COLUMN IF NOT EXISTS delisting_date DATE;

COMMENT ON COLUMN equities_data.ohlcv_1d.delisting_date IS
    'Last trading day of this security, NULL if still listed. Metadata only -- not part of the primary key.';

-- ---------------------------------------------------------------------------
-- 2. Point-in-time index membership.
--
-- Price data alone does not remove survivorship bias. Without knowing WHO was in
-- the index on a given date, a backtest either silently drops former members
-- (survivorship bias) or holds companies before they joined (look-ahead bias) --
-- the same error in the opposite direction.
--
-- A ticker may appear more than once: companies get removed and later re-added.
-- end_date NULL means "still a member".
-- ---------------------------------------------------------------------------
-- NOTE ON THE KEY: this deliberately uses a UNIQUE constraint, not a PRIMARY KEY.
-- Postgres forces every PRIMARY KEY column to NOT NULL, which would make
-- start_date mandatory -- but 468 of 1,076 intervals legitimately have no start
-- date, because those companies were already index members before the source's
-- history begins. A PK here silently rejects 43% of the data. NULLS NOT DISTINCT
-- (PG15+) keeps NULL meaningful while still refusing genuine duplicates.
CREATE TABLE IF NOT EXISTS equities_data.sp500_membership (
    ticker      TEXT NOT NULL,
    start_date  DATE,               -- NULL = member from before the source's history begins
    end_date    DATE,               -- NULL = still a member
    UNIQUE NULLS NOT DISTINCT (ticker, start_date)
);

CREATE INDEX IF NOT EXISTS sp500_membership_window_idx
    ON equities_data.sp500_membership (start_date, end_date);

COMMENT ON TABLE equities_data.sp500_membership IS
    'Point-in-time S&P 500 membership intervals. Query: SELECT ticker WHERE <date> BETWEEN COALESCE(start_date,''-infinity'') AND COALESCE(end_date,''infinity''). '
    'KNOWN LIMITATION: the upstream point-in-time source is incomplete before ~2019 -- it carries ~450 names where the index held 500 '
    '(2007-03-14: 449 in source). The reconstruction is faithful to it (matches within 1 ticker at every date checked), so the gap is '
    'upstream, not in the rebuild. A historical backtest therefore selects from ~90%% of the real index in 2000-2015. Still far better '
    'than selecting from today''s members, but not complete -- do not describe it as a full point-in-time universe.';

-- ---------------------------------------------------------------------------
-- 3. Historical ticker -> current symbol.
--
-- Tiingo follows the SECURITY, not the ticker string: a rename carries the full
-- pre-rename history forward under the new symbol. Verified -- RTX, LHX, J, GEN,
-- DD, RVTY, BALL, COR, ELV, BNY and MRSH all reach back to 2000-01-03.
--
-- So the price data is right but the JOIN breaks: membership says UTX was a
-- member 2000-2020, and the price table has no symbol UTX at all. A backtest
-- joining membership -> prices on the raw string finds nothing and silently
-- drops the name -- reintroducing the exact bias this project removes.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS equities_data.ticker_aliases (
    historical_ticker TEXT NOT NULL,
    current_symbol    TEXT NOT NULL,
    effective_until   DATE,          -- last day the historical ticker was in use
    note              TEXT,
    PRIMARY KEY (historical_ticker, current_symbol)
);

COMMENT ON TABLE equities_data.ticker_aliases IS
    'Maps a historical ticker to the symbol Tiingo now serves its history under. Apply when resolving sp500_membership to prices.';

-- ---------------------------------------------------------------------------
-- 4. Coverage gaps.
--
-- Only ~47% of identifiable former members are recoverable from Tiingo. That
-- number must be auditable rather than folded into a vague caveat, so every
-- ticker we could NOT backfill is recorded here with the evidence for why --
-- including the security name Tiingo actually returned, which is what
-- distinguishes "ticker was reused by a different company" from "right company,
-- but coverage starts too late".
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS equities_data.coverage_gaps (
    ticker           TEXT PRIMARY KEY,
    reason           TEXT NOT NULL,   -- NO_COVERAGE | REUSED_TICKER | NO_PRICE_DATA | NOT_FOUND
    membership_from  DATE,
    membership_to    DATE,
    tiingo_name      TEXT,            -- what Tiingo returned, as evidence
    tiingo_start     DATE,
    tiingo_end       DATE,
    first_attempted  TIMESTAMPTZ NOT NULL DEFAULT now(),
    note             TEXT
);

COMMENT ON TABLE equities_data.coverage_gaps IS
    'Former index members that could NOT be backfilled, with evidence. Read this before describing the dataset as survivorship-bias-free.';

COMMIT;

-- Verification (run manually after commit):
--   \d equities_data.ohlcv_1d   -- delisting_date column present
--   \dt equities_data.*         -- expect sp500_membership, ticker_aliases, coverage_gaps added
