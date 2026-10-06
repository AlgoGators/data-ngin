## The problem

`equities_data.equities` was missing 25 price bars that Tiingo has: **`SATS` for 24 consecutive trading days** (2026-06-24 → 07-28) and **`F` on 2026-07-29**. They were never going to come back on their own.

Two reasons this happened and stayed hidden:

1. **`insert_data` crashed on an empty payload.** `columns = list(data[0].keys())` raises `IndexError` on an empty list. Tiingo legitimately returns zero rows for a symbol with no bars in the requested window, so a normal condition surfaced as a per-symbol failure — which `orchestrator.py:108` catches and merely logs.
2. **Nothing can self-heal.** `determine_date_range` derives one fetch window from the **global** `MAX(time)` across the whole table. Once any symbol advances past a failed day, that day is never requested again, for any symbol.

Net effect: a symbol went missing for 24 straight days and nothing surfaced it.

## Already fixed in production

The repair has been run and verified against `new_algo_data`:

| Check | Result |
|---|---|
| `equities` row count | 3,200,770 → **3,200,795** (exactly +25) |
| `SATS` in gap window | 24 bars |
| `F` on 2026-07-29 | 1 bar |
| Detection re-run | returns `[]` |

`equities_data.verified_absent_bars` was created and holds 25 rows.

## What's in the branch

- **`src/modules/data_quality.py`** — `find_missing_bars()`. Finds `(symbol, day)` holes bounded by each symbol's own lifetime, and excludes days the vendor has confirmed it doesn't have.
- **`scripts/repair_missing_bars.py`** — verifies each candidate against Tiingo, refills the real ones, caches confirmed absences. Idempotent and resumable; a symbol's contiguous holes cost one request, not one per day.
- **`dags/tiingo_gap_check_dag.py`** — weekly (Sat 09:00 ET), raises on unexplained holes, routed through the `notify_dag_failure` callback added in #51.
- **`scripts/sql/2026-08-05_verified_absent_bars.sql`** — DDL (already applied to production).
- **`timescaledb_inserter.py`** — empty payload is now a logged no-op.

### Half of all detected "gaps" are not gaps

Naive detection returns 50 candidates; only 25 are real. The other 25 are days the vendor genuinely has no bar for (`WELL`, `SPGI`, `KIM`, `WBD`, `VST`, `DOC`). This is why repair verifies against Tiingo before treating a candidate as a defect, and why `verified_absent_bars` exists — without it the weekly job would re-probe the same non-existent bars forever.

Two review findings that could have made things worse than the original bug, both fixed:
- absence was derived from **cleaned** rows, so a row the cleaner dropped (`drop_nan: "True"` drops any row with any NaN) would be permanently cached as "vendor has no bar"
- a wholly-empty response over a multi-day span cached the whole span as absent — had that fired on `SATS`, all 24 real bars would have been permanently hidden

Detection also now catches a symbol that **stops updating and never resumes** — the original design bounded each symbol by its own last bar, so the exact `SATS` scenario was invisible unless it came back. Boundary logic verified against Postgres: stalled 15d/29d extend to the table max; 31d and a 2019 delisting stay quiet.

## Tests

`91 passed` (was 81). This also repairs 5 tests in `tests/inserter/test_timescaledb_inserter.py` that had been inert since the `src/` layout change — they patched `data.modules.timescaledb_inserter`, a path that no longer exists, so the whole file silently passed nothing.

The 15 remaining failures and 13 errors are pre-existing on `main` and untouched here (`test_orchestrator`, `test_config`, `test_db_models`, `test_integration_pipeline`).

## Please look hardest at

**The DAG has never run under Airflow.** `airflow` isn't installed locally, so it has only been syntax-checked. Everything else here was verified against the real database; that one wasn't. If you run Airflow day to day, that's the piece worth two minutes of your attention.

Also note the tests ran under SQLAlchemy 2.0.46 locally while the container pins 1.4.36. The `text()` + `session.execute` pattern is already proven in production via `data_access.py`, but this specific code hasn't run on 1.4.

## Known limitations (documented, not blocking)

Recorded in `docs/superpowers/plans/2026-08-05-repair-missing-bars.md`:

1. An unparseable timestamp from the vendor could still cache a returned day as absent.
2. A genuine multi-day vendor absence is reported as a failure every run — deliberate (never cache a real gap), but there's no override flag.
3. A **total** outage is invisible here — a day with zero bars isn't recognised as a trading day. `check_data_freshness.py` from #51 covers that from the other side; the two are complementary.
4. `verified_absent_bars.sql` is hand-run; there's no migration runner in this repo. Production is already migrated.
5. `equities_raw` does not contain the 25 repaired bars — the repair writes only to `equities`. Harmless (nothing in the pipeline reads `equities_raw`, and it already carries ~696k more rows than `equities`), but the repaired bars have no raw provenance.

Also included: the design spec for the separate S&P 500 survivorship-bias work under `docs/superpowers/specs/` — context only, nothing implemented against it here.
