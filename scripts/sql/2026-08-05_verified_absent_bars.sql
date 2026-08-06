-- Bars the vendor has confirmed do not exist. Detection excludes these so that
-- genuinely-absent days (halts, pre-listing, vendor coverage limits) are probed
-- once and then never again.
CREATE TABLE IF NOT EXISTS equities_data.verified_absent_bars (
    symbol      TEXT        NOT NULL,
    bar_date    DATE        NOT NULL,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    note        TEXT,
    PRIMARY KEY (symbol, bar_date)
);
