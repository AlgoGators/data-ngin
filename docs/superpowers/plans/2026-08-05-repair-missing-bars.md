# Missing Bar Detection & Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Repair the 25 price bars missing from `equities_data.equities`, and add automated detection + repair so future gaps surface within a week instead of never.

**Architecture:** A `DataQuality` class finds candidate holes with one SQL query; a repair script verifies each candidate against Tiingo (because half of all candidates are not real gaps), refetches the confirmed ones through the existing fetcher/cleaner/inserter chain, and caches vendor-confirmed absences so they are never re-probed. A weekly Airflow task runs detection and fails loudly when new holes appear.

**Tech Stack:** Python 3.11/3.12, psycopg2, SQLAlchemy 1.4, pandas, aiohttp, unittest + unittest.mock, pytest, Airflow 2.10.

## Global Constraints

- Python `^3.11, <3.13`; add **no new dependencies** — everything needed is already in `pyproject.toml`.
- Tests use `unittest.TestCase` + `unittest.mock`, run via `pytest`. Match the style in `tests/test_data_access.py`.
- Schema and table names always come from `config['database']`, never hardcoded. They are trusted config, so f-string interpolation into SQL is the established pattern (see `data_access.py:131`).
- New modules live under `src/modules/`; new one-off tooling under `scripts/`; new DAGs under `dags/`.
- The Tiingo `endDate` parameter is **inclusive**. Dates are `YYYY-MM-DD` strings.
- `equities_data.equities` stores `time` as `timestamptz` at `00:00:00 UTC`. Compare with `time::date`.

## Baseline: the test suite is already partly broken

Before starting, know what failure is pre-existing versus caused by you:

```
python3 -m pytest tests/ -q
  -> 2 collection ERRORS (test_batch_download_databento_fetcher.py,
     test_data_staleness.py) because `airflow` is not installed locally.

python3 -m pytest tests/ -q --ignore=tests/test_batch_download_databento_fetcher.py \
                            --ignore=tests/test_data_staleness.py
  -> 20 failed, 57 passed, 13 errors
```

All 5 tests in `tests/inserter/test_timescaledb_inserter.py` fail because they patch
`data.modules.timescaledb_inserter.psycopg2.connect` — a module path that no longer exists.
The correct path is `src.modules.inserter.timescaledb_inserter.psycopg2.connect`.
**Task 1 fixes those 5.** Do not attempt to fix the other 20 failures; they are out of scope.

Relevant tests that DO pass and must keep passing:

```
python3 -m pytest tests/fetcher/test_tiingo_fetcher.py tests/cleaner/test_tiingo_cleaner.py \
                  tests/test_contract_tiingo_universe.py tests/test_determine_date_range.py -q
  -> 31 passed
```

## The problem, established by measurement

`equities_data.equities` holds 3,200,770 rows over 570 symbols, 2000-01-03 → 2026-08-04.
A cross-join of every symbol against every trading day inside that symbol's own first/last
date range finds **50 candidate holes**. Probing Tiingo shows only half are real:

| Symbol | Bars | Range | Tiingo has it? |
|---|---|---|---|
| `SATS` | 24 | 2026-06-24 → 2026-07-28 | **yes — real gap** |
| `F` | 1 | 2026-07-29 | **yes — real gap** |
| `WELL` | 12 | 2003-11-07 → 2008-01-23 | no — not a real gap |
| `SPGI` | 6 | 2003-03-21 → 2008-01-24 | no — not a real gap |
| `KIM` | 3 | 2006-05-25 → 2006-09-29 | no — not a real gap |
| `WBD` | 2 | 2022-04-06 → 2022-04-07 | no — not a real gap |
| `VST` | 1 | 2016-11-25 | no — not a real gap |
| `DOC` | 1 | 2023-02-13 | no — not a real gap |

**25 real, 25 false positives.** This is why Task 3 exists: a repair tool that trusts the
detection query blindly would hammer Tiingo forever for 25 bars that do not exist.

**Why the gaps are permanent.** `determine_date_range` (`utils/dynamic_loader.py:99-148`)
computes one window from the **global** `MAX(time)` across the whole table. Once any symbol
advances past a failed day, that day is never requested again for any symbol. Nothing in the
pipeline can self-heal.

**Why they were silent.** `equities_raw` also has zero `SATS` rows for the gap window, so the
data never arrived. Either Tiingo returned an empty list at fetch time (it has the bars now —
vendor backfill is the likely cause) or the request errored. Both paths end the same way:
`TimescaleDBInserter.insert_data` crashes on empty input at
`timescaledb_inserter.py:109` (`columns = list(data[0].keys())` → `IndexError`), the
orchestrator's per-symbol `except` at `orchestrator.py:108` logs it, and the run continues.
A symbol can fail for 24 consecutive days and nothing surfaces.

Note `insert_data`'s own docstring promises `ValueError: If the data is empty` — that check
was lost. `tests/inserter/test_timescaledb_inserter.py:72` still asserts it, but the test
cannot run because of the stale mock path.

## File Structure

| File | Responsibility |
|---|---|
| `src/modules/inserter/timescaledb_inserter.py` *(modify)* | Empty input becomes a logged no-op, not a crash |
| `tests/inserter/test_timescaledb_inserter.py` *(modify)* | Fix 5 stale mock paths; assert no-op instead of `ValueError` |
| `src/modules/data_quality.py` *(create)* | `DataQuality.find_missing_bars()` — candidate hole detection |
| `tests/test_data_quality.py` *(create)* | Unit tests for detection, mocked session |
| `scripts/sql/2026-08-05_verified_absent_bars.sql` *(create)* | DDL for the absence cache |
| `scripts/repair_missing_bars.py` *(create)* | Verify candidates against Tiingo, refetch, record absences |
| `tests/test_repair_missing_bars.py` *(create)* | Unit tests for grouping + absence recording |
| `dags/tiingo_gap_check_dag.py` *(create)* | Weekly detection, fails loudly on new holes |

---

### Task 1: Make empty inserts a no-op instead of a crash

**Files:**
- Modify: `src/modules/inserter/timescaledb_inserter.py:65-118`
- Test: `tests/inserter/test_timescaledb_inserter.py` (fix 5 stale patch targets)

**Interfaces:**
- Consumes: nothing.
- Produces: `TimescaleDBInserter.insert_data(data, schema, table)` returns `None` and logs at
  INFO when `data` is empty, instead of raising. Task 4 relies on this so a symbol with no
  new bars does not abort a repair run.

- [ ] **Step 1: Fix the 5 stale mock paths so the tests can run at all**

In `tests/inserter/test_timescaledb_inserter.py`, replace every occurrence of:

```python
@patch("data.modules.timescaledb_inserter.psycopg2.connect")
```

with:

```python
@patch("src.modules.inserter.timescaledb_inserter.psycopg2.connect")
```

There are 5, at lines 25, 34, 71, 80, 88.

- [ ] **Step 2: Run the inserter tests to see the real failures**

Run: `python3 -m pytest tests/inserter/test_timescaledb_inserter.py -q`
Expected: the patch errors are gone. `test_insert_data_empty` now fails with `IndexError:
list index out of range` (not the `ValueError` it asserts) — that is the bug.

- [ ] **Step 3: Rewrite the empty-data test to assert the behaviour we want**

An empty response from Tiingo is normal (a symbol with no bars in the requested range), so it
must be a quiet no-op, not an exception. Replace `test_insert_data_empty` entirely:

```python
    @patch("src.modules.inserter.timescaledb_inserter.psycopg2.connect")
    def test_insert_data_empty_is_a_noop(self, mock_connect: MagicMock) -> None:
        """
        Empty data must be a logged no-op, never an exception.

        Tiingo legitimately returns zero rows for a symbol with no bars in the
        requested window. Raising here would make a normal condition look like a
        failure in the orchestrator's per-symbol except handler.
        """
        self.inserter.connect()
        mock_cursor = self.inserter.connection.cursor.return_value.__enter__.return_value

        result = self.inserter.insert_data([], schema="futures_data", table="ohlcv_1d")

        self.assertIsNone(result)
        mock_cursor.executemany.assert_not_called()
```

- [ ] **Step 4: Run it to verify it fails**

Run: `python3 -m pytest tests/inserter/test_timescaledb_inserter.py::TestTimescaleDBInserter::test_insert_data_empty_is_a_noop -q`
Expected: FAIL with `IndexError: list index out of range`.

- [ ] **Step 5: Implement the guard**

In `src/modules/inserter/timescaledb_inserter.py`, inside `insert_data`, immediately after the
existing connection check and **before** the schema/table existence queries:

```python
        if not self.connection:
            raise RuntimeError("Database connection is not established.")

        # An empty payload is a normal outcome (symbol has no bars in the requested
        # window), not an error. Returning early keeps it out of the orchestrator's
        # per-symbol except handler, where it would masquerade as a fetch failure.
        if not data:
            self.logger.info("No rows to insert into %s.%s; skipping.", schema, table)
            return
```

- [ ] **Step 6: Run the full inserter test file**

Run: `python3 -m pytest tests/inserter/test_timescaledb_inserter.py -q`
Expected: 5 passed.

- [ ] **Step 7: Confirm nothing else regressed**

Run: `python3 -m pytest tests/fetcher/test_tiingo_fetcher.py tests/cleaner/test_tiingo_cleaner.py tests/test_contract_tiingo_universe.py tests/test_determine_date_range.py -q`
Expected: 31 passed.

- [ ] **Step 8: Commit**

```bash
git add src/modules/inserter/timescaledb_inserter.py tests/inserter/test_timescaledb_inserter.py
git commit -m "fix(inserter): treat empty payload as a no-op instead of IndexError

insert_data crashed on an empty list at columns = list(data[0].keys()).
Tiingo returns zero rows for a symbol with no bars in range, so the crash
surfaced a normal condition as a per-symbol failure in the orchestrator.

Also repairs 5 tests that patched a module path (data.modules.*) which has
not existed since the src/ layout landed, so the whole file was inert."
```

---

### Task 2: Detect candidate missing bars

**Files:**
- Create: `src/modules/data_quality.py`
- Test: `tests/test_data_quality.py`
- Create: `scripts/sql/2026-08-05_verified_absent_bars.sql`

**Interfaces:**
- Consumes: `get_engine(config)` from `src.modules.db_models`.
- Produces: `DataQuality(config).find_missing_bars(schema, table, since="2000-01-01",
  min_symbols=100) -> List[Tuple[str, datetime.date]]`, sorted by date then symbol.
  Task 3 and Task 5 both call exactly this.

- [ ] **Step 1: Write the DDL for the absence cache**

Create `scripts/sql/2026-08-05_verified_absent_bars.sql`. This table records
`(symbol, date)` pairs the vendor has confirmed it does not have, so the weekly job stops
re-probing the 25 known false positives forever.

```sql
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
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_data_quality.py`:

```python
import unittest
from datetime import date
from unittest.mock import MagicMock, patch
from typing import Any, Dict


class TestDataQuality(unittest.TestCase):
    """Unit tests for DataQuality.find_missing_bars with a mocked session."""

    def setUp(self) -> None:
        self.config: Dict[str, Any] = {
            "database": {
                "db_name": "new_algo_data",
                "target_schema": "equities_data",
                "table": "equities",
            }
        }

    @patch("src.modules.data_quality.get_engine")
    def test_find_missing_bars_returns_symbol_date_tuples(self, mock_engine: MagicMock) -> None:
        from src.modules.data_quality import DataQuality

        dq = DataQuality(config=self.config)
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = False
        mock_session.execute.return_value = [("SATS", date(2026, 7, 9)), ("F", date(2026, 7, 29))]
        dq.Session = MagicMock(return_value=mock_session)

        holes = dq.find_missing_bars("equities_data", "equities")

        self.assertEqual(holes, [("SATS", date(2026, 7, 9)), ("F", date(2026, 7, 29))])

    @patch("src.modules.data_quality.get_engine")
    def test_find_missing_bars_passes_since_and_min_symbols(self, mock_engine: MagicMock) -> None:
        from src.modules.data_quality import DataQuality

        dq = DataQuality(config=self.config)
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = False
        mock_session.execute.return_value = []
        dq.Session = MagicMock(return_value=mock_session)

        dq.find_missing_bars("equities_data", "equities", since="2026-01-01", min_symbols=400)

        params = mock_session.execute.call_args[0][1]
        self.assertEqual(params["since"], "2026-01-01")
        self.assertEqual(params["min_symbols"], 400)

    @patch("src.modules.data_quality.get_engine")
    def test_find_missing_bars_empty_result(self, mock_engine: MagicMock) -> None:
        from src.modules.data_quality import DataQuality

        dq = DataQuality(config=self.config)
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = False
        mock_session.execute.return_value = []
        dq.Session = MagicMock(return_value=mock_session)

        self.assertEqual(dq.find_missing_bars("equities_data", "equities"), [])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Run it to verify it fails**

Run: `python3 -m pytest tests/test_data_quality.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.modules.data_quality'`.

- [ ] **Step 4: Implement the module**

Create `src/modules/data_quality.py`:

```python
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
# f-string pattern already used in data_access.get_latest_date_for.
MISSING_BARS_SQL = """
WITH days AS (
    SELECT time::date AS d
    FROM {schema}.{table}
    GROUP BY 1
    HAVING count(DISTINCT symbol) > :min_symbols
),
span AS (
    SELECT symbol, min(time)::date AS lo, max(time)::date AS hi
    FROM {schema}.{table}
    GROUP BY 1
),
have AS (
    SELECT symbol, time::date AS d
    FROM {schema}.{table}
)
SELECT sp.symbol, d.d
FROM span sp
CROSS JOIN days d
LEFT JOIN have h
       ON h.symbol = sp.symbol AND h.d = d.d
LEFT JOIN {schema}.verified_absent_bars v
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

        Returns:
            List of (symbol, date) tuples.
        """
        query = text(MISSING_BARS_SQL.format(schema=schema, table=table))
        with self.Session() as session:
            rows = session.execute(query, {"since": since, "min_symbols": min_symbols})
            holes = [(r[0], r[1]) for r in rows]
        self.logger.info("Found %d candidate missing bars in %s.%s", len(holes), schema, table)
        return holes
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `python3 -m pytest tests/test_data_quality.py -q`
Expected: 3 passed.

- [ ] **Step 6: Apply the DDL and sanity-check against the live database**

```bash
psql "postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/new_algo_data" \
     -f scripts/sql/2026-08-05_verified_absent_bars.sql
```

Then confirm detection reproduces the known result:

```bash
python3 -c "
from utils.dynamic_loader import load_config
from src.modules.data_quality import DataQuality
c = load_config('src/config/config_tiingo.yaml')
h = DataQuality(config=c).find_missing_bars('equities_data','equities')
print(len(h), 'candidates'); print(h[:5])
"
```

Expected: `50 candidates` — the 25 real plus the 25 not-yet-cached false positives.

- [ ] **Step 7: Commit**

```bash
git add src/modules/data_quality.py tests/test_data_quality.py scripts/sql/2026-08-05_verified_absent_bars.sql
git commit -m "feat(data-quality): detect permanent holes in the equities table

determine_date_range derives one window from the global max(time), so a day a
symbol fails is never requested again. This finds those holes. Results are
candidates only -- half of the 50 found are days the vendor has no bar for --
so verified_absent_bars caches confirmed absences to stop re-probing them."
```

---

### Task 3: Verify candidates against Tiingo and record confirmed absences

**Files:**
- Create: `scripts/repair_missing_bars.py`
- Test: `tests/test_repair_missing_bars.py`

**Interfaces:**
- Consumes: `DataQuality.find_missing_bars()` from Task 2.
- Produces:
  - `group_holes_by_symbol(holes) -> Dict[str, Tuple[date, date]]` — collapses each symbol's
    holes to one `(min_date, max_date)` span so a 24-day run costs **one** request, not 24.
  - `record_absent(inserter, schema, symbol, days, note)` — writes to `verified_absent_bars`.
  Task 4 calls both.

- [ ] **Step 1: Write the failing test**

Create `tests/test_repair_missing_bars.py`:

```python
import unittest
from datetime import date
from unittest.mock import MagicMock


class TestGroupHoles(unittest.TestCase):
    """A contiguous run of holes must collapse to a single fetch span."""

    def test_groups_contiguous_run_into_one_span(self) -> None:
        from scripts.repair_missing_bars import group_holes_by_symbol

        holes = [("SATS", date(2026, 7, 9)), ("SATS", date(2026, 7, 10)),
                 ("SATS", date(2026, 7, 28)), ("F", date(2026, 7, 29))]

        grouped = group_holes_by_symbol(holes)

        self.assertEqual(grouped["SATS"], (date(2026, 7, 9), date(2026, 7, 28)))
        self.assertEqual(grouped["F"], (date(2026, 7, 29), date(2026, 7, 29)))

    def test_empty_input(self) -> None:
        from scripts.repair_missing_bars import group_holes_by_symbol

        self.assertEqual(group_holes_by_symbol([]), {})


class TestRecordAbsent(unittest.TestCase):
    """Vendor-confirmed absences must be cached so they are never re-probed."""

    def test_record_absent_inserts_one_row_per_day(self) -> None:
        from scripts.repair_missing_bars import record_absent

        inserter = MagicMock()
        record_absent(inserter, "equities_data", "WELL",
                      [date(2003, 11, 7), date(2003, 11, 10)], note="vendor has no bar")

        inserter.insert_data.assert_called_once()
        kwargs = inserter.insert_data.call_args.kwargs
        self.assertEqual(kwargs["table"], "verified_absent_bars")
        self.assertEqual(len(kwargs["data"]), 2)
        self.assertEqual(kwargs["data"][0]["symbol"], "WELL")
        self.assertEqual(kwargs["data"][0]["bar_date"], date(2003, 11, 7))

    def test_record_absent_with_no_days_does_nothing(self) -> None:
        from scripts.repair_missing_bars import record_absent

        inserter = MagicMock()
        record_absent(inserter, "equities_data", "WELL", [], note="n/a")
        inserter.insert_data.assert_not_called()


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run it to verify it fails**

Run: `python3 -m pytest tests/test_repair_missing_bars.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.repair_missing_bars'`.

- [ ] **Step 3: Create the script with just these two helpers**

Create `scripts/repair_missing_bars.py`:

```python
"""Repair permanently-missing bars in equities_data.equities.

Run from the repo root:
    python3 scripts/repair_missing_bars.py --dry-run
    python3 scripts/repair_missing_bars.py

Half of all detected candidates are days the vendor genuinely has no bar for, so
every candidate is verified against Tiingo before being treated as a defect, and
confirmed absences are cached in verified_absent_bars so they are probed once and
never again.
"""
import argparse
import asyncio
import logging
from datetime import date
from typing import Any, Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("repair_missing_bars")


def group_holes_by_symbol(holes: List[Tuple[str, date]]) -> Dict[str, Tuple[date, date]]:
    """
    Collapse each symbol's holes into a single (min_date, max_date) span.

    A symbol with 24 consecutive missing days becomes ONE request covering the whole
    range rather than 24 separate ones. Refetching days we already have is harmless:
    the inserter uses ON CONFLICT DO NOTHING.
    """
    spans: Dict[str, Tuple[date, date]] = {}
    for symbol, day in holes:
        if symbol not in spans:
            spans[symbol] = (day, day)
        else:
            lo, hi = spans[symbol]
            spans[symbol] = (min(lo, day), max(hi, day))
    return spans


def record_absent(inserter: Any, schema: str, symbol: str, days: List[date], note: str) -> None:
    """Cache days the vendor confirmed it has no bar for, so detection stops reporting them."""
    if not days:
        return
    inserter.insert_data(
        data=[{"symbol": symbol, "bar_date": d, "note": note} for d in days],
        schema=schema,
        table="verified_absent_bars",
    )
    logger.info("Recorded %d vendor-absent bars for %s", len(days), symbol)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m pytest tests/test_repair_missing_bars.py -q`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add scripts/repair_missing_bars.py tests/test_repair_missing_bars.py
git commit -m "feat(repair): add hole grouping and vendor-absence caching helpers

Grouping collapses a symbol's contiguous holes into one request span (SATS's 24
missing days cost 1 request, not 24). record_absent caches vendor-confirmed
absences so the 25 known false positives are never re-probed."
```

---

### Task 4: Wire up the end-to-end repair run

**Files:**
- Modify: `scripts/repair_missing_bars.py` (add `repair()` and `main()`)

**Interfaces:**
- Consumes: `group_holes_by_symbol`, `record_absent` (Task 3); `DataQuality.find_missing_bars`
  (Task 2); `TimescaleDBInserter.insert_data` no-op on empty (Task 1).
- Produces: a runnable CLI. No further task depends on it.

- [ ] **Step 1: Add the repair routine**

Append to `scripts/repair_missing_bars.py`:

```python
async def repair(config: Dict[str, Any], dry_run: bool = False) -> Dict[str, int]:
    """
    Detect candidate holes, verify each against Tiingo, refill the real ones, and
    cache the confirmed absences.

    Returns a summary dict: {"candidates", "refilled", "absent", "symbols"}.
    """
    from utils.dynamic_loader import get_instance
    from src.modules.data_quality import DataQuality

    schema = config["database"]["target_schema"]
    table = config["database"]["table"]

    holes = DataQuality(config=config).find_missing_bars(schema, table)
    spans = group_holes_by_symbol(holes)
    logger.info("%d candidate bars across %d symbols", len(holes), len(spans))
    if dry_run or not spans:
        for sym, (lo, hi) in sorted(spans.items()):
            logger.info("  DRY RUN %s %s..%s", sym, lo, hi)
        return {"candidates": len(holes), "refilled": 0, "absent": 0, "symbols": len(spans)}

    fetcher = get_instance(config, "fetcher", "class")
    cleaner = get_instance(config, "cleaner", "class")
    inserter = get_instance(config, "inserter", "class")

    wanted: Dict[str, set] = {}
    for symbol, day in holes:
        wanted.setdefault(symbol, set()).add(day)

    refilled = absent = 0
    inserter.connect()
    try:
        for symbol, (lo, hi) in sorted(spans.items()):
            try:
                raw = await fetcher.fetch_data(
                    symbol=symbol,
                    loaded_asset_type="EQUITY",
                    start_date=lo.strftime("%Y-%m-%d"),
                    end_date=hi.strftime("%Y-%m-%d"),
                )
            except Exception as exc:                      # noqa: BLE001 - report and continue
                logger.error("Fetch failed for %s (%s..%s): %s", symbol, lo, hi, exc)
                continue

            rows = cleaner.clean(raw)
            returned_days = {r["time"].date() for r in rows}
            filled = wanted[symbol] & returned_days
            missing = sorted(wanted[symbol] - returned_days)

            if rows:
                inserter.insert_data(data=rows, schema=schema, table=table)
            refilled += len(filled)
            logger.info("%s: refilled %d, vendor has no bar for %d",
                        symbol, len(filled), len(missing))

            record_absent(inserter, schema, symbol, missing, note="vendor returned no bar")
            absent += len(missing)
    finally:
        inserter.close()

    return {"candidates": len(holes), "refilled": refilled,
            "absent": absent, "symbols": len(spans)}


def main() -> None:
    from utils.dynamic_loader import load_config

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="src/config/config_tiingo.yaml")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be fetched without calling the vendor.")
    args = parser.parse_args()

    summary = asyncio.run(repair(load_config(args.config), dry_run=args.dry_run))
    logger.info("SUMMARY %s", summary)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Confirm the unit tests still pass**

Run: `python3 -m pytest tests/test_repair_missing_bars.py tests/test_data_quality.py -q`
Expected: 7 passed.

- [ ] **Step 3: Dry run against the live database**

The config's `loader.file_path` is a container path; that does not matter here because the
repair path never uses the loader. It does need DB env vars and Tiingo keys from `.env`.

Run: `python3 scripts/repair_missing_bars.py --dry-run`
Expected: `50 candidate bars across 8 symbols`, then one DRY RUN line per symbol, including:

```
  DRY RUN F 2026-07-29..2026-07-29
  DRY RUN SATS 2026-06-24..2026-07-28
```

- [ ] **Step 4: Run for real**

Run: `python3 scripts/repair_missing_bars.py`
Expected: 8 requests total. Summary should read approximately
`{'candidates': 50, 'refilled': 25, 'absent': 25, 'symbols': 8}`.

- [ ] **Step 5: Verify the holes are gone**

```bash
python3 -c "
from utils.dynamic_loader import load_config
from src.modules.data_quality import DataQuality
c = load_config('src/config/config_tiingo.yaml')
print(DataQuality(config=c).find_missing_bars('equities_data','equities'))
"
```

Expected: `[]` — the 25 real bars are filled, the 25 vendor-absent ones are cached and
therefore excluded.

Also spot-check the two known symbols:

```bash
python3 -c "
import os; from dotenv import load_dotenv; load_dotenv('.env')
import psycopg2
c=psycopg2.connect(dbname='new_algo_data',user=os.getenv('DB_USER'),password=os.getenv('DB_PASSWORD'),
                   host=os.getenv('DB_HOST'),port=os.getenv('DB_PORT'))
cur=c.cursor()
cur.execute(\"SELECT count(*) FROM equities_data.equities WHERE symbol='SATS' AND time::date BETWEEN '2026-06-24' AND '2026-07-28'\")
print('SATS bars in gap window (expect 24):', cur.fetchone()[0])
cur.execute(\"SELECT count(*) FROM equities_data.equities WHERE symbol='F' AND time::date='2026-07-29'\")
print('F bar on 2026-07-29 (expect 1):', cur.fetchone()[0])
"
```

- [ ] **Step 6: Commit**

```bash
git add scripts/repair_missing_bars.py
git commit -m "feat(repair): end-to-end missing-bar repair run

Detects candidates, verifies each span against Tiingo, refills real gaps through
the existing cleaner/inserter chain, and caches vendor-confirmed absences.
Repairs the 25 real bars (SATS 24, F 1) and caches the 25 that never existed."
```

---

### Task 5: Weekly gap check in Airflow

**Files:**
- Create: `dags/tiingo_gap_check_dag.py`

**Interfaces:**
- Consumes: `DataQuality.find_missing_bars()` (Task 2).
- Produces: nothing consumed downstream.

- [ ] **Step 1: Write the DAG**

Follows the deferred-import pattern in `dags/tiingo_data_dag.py` — heavy imports go inside
the callable so DAG parsing stays under the import timeout on the t2.micro.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging, pendulum

local_tz = pendulum.timezone("America/New_York")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

CONFIG_PATH = "/opt/airflow/data_engine/src/config/config_tiingo.yaml"

# Only look at recent history. Older holes are already cached in
# verified_absent_bars, and a full-history scan is pointless weekly work.
LOOKBACK_DAYS = 120


def check_for_gaps(**kwargs):
    """Fail the task if any unexplained hole exists in the recent window.

    Failing loudly is the point: a symbol previously went missing for 24
    consecutive days without anyone noticing, because the orchestrator logs
    per-symbol failures and moves on.
    """
    from datetime import date, timedelta as td
    from utils.dynamic_loader import load_config
    from src.modules.data_quality import DataQuality

    config = load_config(CONFIG_PATH)
    schema = config["database"]["target_schema"]
    table = config["database"]["table"]
    since = (date.today() - td(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    holes = DataQuality(config=config).find_missing_bars(schema, table, since=since)
    if not holes:
        logging.info("Gap check clean: no missing bars since %s", since)
        return

    for symbol, day in holes[:50]:
        logging.error("MISSING BAR %s %s", symbol, day)
    raise ValueError(
        f"{len(holes)} missing bar(s) in {schema}.{table} since {since}. "
        "Run: python3 scripts/repair_missing_bars.py"
    )


with DAG(
    "tiingo_gap_check_dag",
    default_args=default_args,
    description="Weekly check for permanently-missing equity bars",
    schedule_interval="0 9 * * 6",   # Saturdays 9:00 AM ET, after the week's runs
    start_date=datetime(2026, 8, 1, tzinfo=local_tz),
    catchup=False,
    tags=["tiingo", "equity", "data_quality"],
    max_active_runs=1,
) as dag:

    check_for_gaps_task = PythonOperator(
        task_id="check_for_gaps",
        python_callable=check_for_gaps,
    )
```

- [ ] **Step 2: Verify the DAG file parses**

`airflow` is not installed locally, so check syntax only:

Run: `python3 -m py_compile dags/tiingo_gap_check_dag.py && echo "parses OK"`
Expected: `parses OK`

- [ ] **Step 3: Commit**

```bash
git add dags/tiingo_gap_check_dag.py
git commit -m "feat(dags): weekly gap check for the equities table

Fails loudly when an unexplained hole appears in the last 120 days. SATS went
missing for 24 consecutive trading days with no alert because the orchestrator
logs per-symbol failures and continues."
```

- [ ] **Step 4: Deploy**

```bash
# on the EC2 box
git pull
docker compose down && docker compose up -d
```

Then trigger `tiingo_gap_check_dag` once from the Airflow UI. Expected: task succeeds with
`Gap check clean: no missing bars since <date>` in the log.

---

## Out of Scope

- The 20 pre-existing test failures unrelated to the inserter.
- `determine_date_range`'s global-window design. The repair script works around it; changing
  it would affect the futures pipelines too.
- The orchestrator's shared-inserter concurrency issue (`orchestrator.py:112`).
- Distinguishing a rate-limited 429 from a disabled-key 429 in `TiingoFetcher` — related
  silent-failure problem, tracked separately with the survivorship-bias work.
- The S&P 500 survivorship backfill itself
  (`docs/superpowers/specs/2026-08-05-sp500-survivorship-bias-backfill-design.md`).

## Follow-ups deferred from review (not blocking merge)

Recorded here because `.superpowers/` scratch is not committed. None of these were load-bearing;
all were adjudicated as park-with-ruling.

1. **Unparseable raw timestamp can still cache a returned day as absent.** `returned_days_from_raw`
   coerces bad dates to `NaT` and drops them, so a day the vendor *did* return lands in `missing`
   and is cached. Requires Tiingo to emit a garbage date — narrow, but it is the one remaining
   door into the bug class Fix 1 closed. A row-count check (`len(raw)` vs parsed non-NaT) closes it.
2. **No convergence path for a genuine multi-day vendor absence.** By design, a wholly-empty
   response over a multi-day span is treated as a failure rather than an absence, so such a symbol
   is marked `failed` every run and `main()` can never exit 0 for it. This is the correct trade
   (never cache a real gap), but there is no `--allow-multi-day-absence` escape hatch.
3. **Total pipeline outage is invisible to detection.** A day with zero bars for every symbol is
   not counted as a trading day, so it yields no candidates. Documented in the DAG docstring;
   solving it properly needs an NYSE market calendar.
4. **`scripts/sql/2026-08-05_verified_absent_bars.sql` is hand-run.** No migration runner exists in
   this repo. Production is already migrated; a fresh environment must apply it manually or
   `find_missing_bars` raises `UndefinedTable`.
5. **Day-convention coupling.** `returned_days` is a UTC date while `wanted` comes from Postgres
   `time::date` on a `timestamptz`, which honours the session `TimeZone`. Aligned today because
   both are UTC; a non-UTC session would diverge. Pre-existing, not introduced here.

## Verification Summary

| Check | Command | Expected |
|---|---|---|
| Inserter tests repaired | `pytest tests/inserter/test_timescaledb_inserter.py -q` | 5 passed |
| New unit tests | `pytest tests/test_data_quality.py tests/test_repair_missing_bars.py -q` | 7 passed |
| No regression | `pytest tests/fetcher/test_tiingo_fetcher.py tests/cleaner/test_tiingo_cleaner.py tests/test_contract_tiingo_universe.py tests/test_determine_date_range.py -q` | 31 passed |
| Holes repaired | `find_missing_bars('equities_data','equities')` | `[]` |
| SATS filled | SQL count 2026-06-24..07-28 | 24 |
| F filled | SQL count 2026-07-29 | 1 |
