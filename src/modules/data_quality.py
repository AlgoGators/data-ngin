"""Data-quality checks for the equities tables.

The daily pipeline cannot self-heal: determine_date_range (utils/dynamic_loader.py)
derives one window from the GLOBAL max(time) across the table, so once any symbol
advances past a failed day, that day is never requested again for any symbol. This
module finds those permanent holes so a repair pass can fill them.
"""
import logging
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from src.modules.db_models import get_engine

# A "candidate hole" is a (symbol, day) where:
#   - the day is a real trading day (more than min_symbols symbols have a bar), and
#   - the day falls inside that symbol's own first..last date range, and
#   - the symbol has no bar, and
#   - the vendor has not already confirmed the bar does not exist.
#
# The span join is what stops a newly-added symbol from reporting a hole for every
# day before it existed. Schema/table come from trusted config, matching the
# f-string pattern already used in data_access.get_latest_date_for (quoted
# "{schema}"."{table}").
#
# Bounding every symbol by its OWN last bar would make the exact incident this
# module exists to catch invisible: a symbol that stops updating and never resumes
# has no days past its own hi, so it yields zero candidates. The span CTE therefore
# extends the upper bound to the table-wide maximum trading day for symbols that are
# still active (last bar within :active_within_days of that maximum). Genuinely
# delisted symbols keep their own hi and stay quiet.
#
# days/have are filtered by :since inside the CTEs so the planner does not scan the
# full 3.2M-row table before the outer WHERE applies. span is deliberately NOT
# filtered: it must see each symbol's true first and last bar.
MISSING_BARS_SQL = """
WITH days AS (
    SELECT time::date AS d
    FROM "{schema}"."{table}"
    WHERE time::date >= :since
    GROUP BY 1
    HAVING count(DISTINCT symbol) > :min_symbols
),
raw_span AS (
    SELECT symbol, min(time)::date AS lo, max(time)::date AS hi
    FROM "{schema}"."{table}"
    GROUP BY 1
),
bounds AS (
    SELECT max(hi) AS table_hi
    FROM raw_span
),
span AS (
    SELECT r.symbol,
           r.lo AS lo,
           CASE
               WHEN r.hi >= b.table_hi - make_interval(days => :active_within_days)
                   THEN b.table_hi
               ELSE r.hi
           END AS hi
    FROM raw_span r
    CROSS JOIN bounds b
),
have AS (
    SELECT symbol, time::date AS d
    FROM "{schema}"."{table}"
    WHERE time::date >= :since
)
SELECT sp.symbol, d.d
FROM span sp
CROSS JOIN days d
LEFT JOIN have h
       ON h.symbol = sp.symbol AND h.d = d.d
LEFT JOIN "{schema}"."verified_absent_bars" v
       ON v.symbol = sp.symbol AND v.bar_date = d.d
WHERE d.d BETWEEN sp.lo AND sp.hi
  AND d.d >= :since
  AND h.symbol IS NULL
  AND v.symbol IS NULL
ORDER BY d.d, sp.symbol
"""


class DataQuality:
    """Finds permanent holes in an OHLCV table."""

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.engine: Engine = get_engine(config=config)
        self.Session = sessionmaker(bind=self.engine)
        self.logger: logging.Logger = logging.getLogger("DataQuality")
        self.logger.setLevel(logging.INFO)

    def find_missing_bars(
        self,
        schema: str,
        table: str,
        since: str = "2000-01-01",
        min_symbols: int = 100,
        active_within_days: int = 30,
    ) -> List[Tuple[str, date]]:
        """
        Return every candidate missing (symbol, day), oldest first.

        These are CANDIDATES, not confirmed gaps: about half of them are days the
        vendor genuinely has no bar for. Always verify against the vendor before
        treating one as a defect.

        Args:
            schema: Target schema, from config['database']['target_schema'].
            table: Target table, from config['database']['table'].
            since: Ignore holes before this date (YYYY-MM-DD).
            min_symbols: A day counts as a trading day only if more than this many
                symbols have a bar. Filters out partial/aborted runs.
            active_within_days: A symbol whose last bar is within this many days of
                the table-wide maximum trading day is treated as still active, and
                its candidate window is extended to that table-wide maximum. Without
                this, a symbol that stops updating and never resumes produces no
                candidates at all and is invisible to the check. Symbols with an
                older last bar (genuinely delisted) stay bounded by their own last
                bar and report nothing.

        Returns:
            List of (symbol, date) tuples.
        """
        query = text(MISSING_BARS_SQL.format(schema=schema, table=table))
        with self.Session() as session:
            rows = session.execute(
                query,
                {
                    "since": since,
                    "min_symbols": min_symbols,
                    "active_within_days": active_within_days,
                },
            )
            holes = [(r[0], r[1]) for r in rows]
        self.logger.info("Found %d candidate missing bars in %s.%s", len(holes), schema, table)
        return holes
