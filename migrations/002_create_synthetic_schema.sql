-- Migration: Create synthetic schema and tables
--
-- The synthetic schema holds generated market data for stress-testing strategies
-- against conditions real history never contained: flash crashes, limit-up/down days,
-- liquidity gaps.
--
-- ISOLATION BOUNDARY: Synthetic data MUST NEVER reach production tables. The structural
-- guarantee is that synthetics write ONLY to the synthetic schema, never to
-- futures_data / equities_data / options_data. This schema separation is the
-- security boundary; code paths that could write synthetic rows to production
-- schemas are defects.
--
-- The schema-ownership-guard CI workflow (line 33) allows synthetic as a valid
-- target_schema value. All other code must only read from futures_data /
-- equities_data / options_data.

CREATE SCHEMA IF NOT EXISTS synthetic;

-- OHLCV table: daily OHLCV bars for synthetic series.
--
-- Column types are matched against the LIVE production tables, not assumed:
-- futures_data.ohlcv_1d and equities_data both store `time` as TIMESTAMPTZ, which is
-- also what ADR-000 C-2 specifies. A naive TIMESTAMP here would be a real hazard
-- rather than a cosmetic mismatch -- comparing naive against tz-aware values makes
-- Postgres apply the session TimeZone, so the same range query would return
-- different bars depending on who ran it, and any UNION with real data would need a
-- cast. adj_volume is DOUBLE PRECISION to match equities_data (volume stays BIGINT,
-- which equities_data also uses; futures_data narrows it to INTEGER).
CREATE TABLE IF NOT EXISTS synthetic.ohlcv_1d (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    adj_open DOUBLE PRECISION NOT NULL,
    adj_high DOUBLE PRECISION NOT NULL,
    adj_low DOUBLE PRECISION NOT NULL,
    adjusted_close DOUBLE PRECISION NOT NULL,
    adj_volume DOUBLE PRECISION NOT NULL,
    div_cash DOUBLE PRECISION NOT NULL,
    split_factor DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (symbol, time)
);

-- Index for efficient range queries on time (common in backtesting)
CREATE INDEX IF NOT EXISTS idx_synthetic_ohlcv_1d_time
    ON synthetic.ohlcv_1d (time);

-- Index for efficient symbol lookups (common in strategy screening)
CREATE INDEX IF NOT EXISTS idx_synthetic_ohlcv_1d_symbol
    ON synthetic.ohlcv_1d (symbol);
