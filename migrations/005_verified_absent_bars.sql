-- Migration 005: cache of bars the vendor has confirmed do not exist.
--
-- Numbered 005, not 003, because 003 and 004 are already claimed by the
-- survivorship branch (PR #78) and 002 by the synthetic-data schema on main.
-- Out-of-order numbering beats a collision between two open PRs.
--
-- Detection excludes these pairs, so genuinely-absent days (halts, pre-listing,
-- vendor coverage limits) are probed once and then never again. Without it the
-- weekly gap check re-reports the same non-existent bars forever -- half of all
-- candidates are days the vendor simply does not have.
--
-- ALREADY APPLIED to new_algo_data (out-of-band, 2026-08-05, before the
-- migrations/ convention existed). This file records it so a fresh environment
-- can reproduce the schema; CREATE TABLE IF NOT EXISTS makes re-running a no-op.
--
-- Usage:
--   psql "$DATABASE_URL" -f migrations/005_verified_absent_bars.sql
--
-- Rollback: DROP TABLE equities_data.verified_absent_bars;
--   Safe in the sense that detection simply re-reports the cached days again --
--   nothing else reads it -- but the probe results are lost and must be re-earned
--   with vendor calls.

CREATE TABLE IF NOT EXISTS equities_data.verified_absent_bars (
    symbol      TEXT        NOT NULL,
    bar_date    DATE        NOT NULL,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    note        TEXT,
    PRIMARY KEY (symbol, bar_date)
);
