# S&P 500 survivorship-bias backfill — design

**Date:** 2026-08-05
**Branch base:** fresh branch off `main`
**Status:** design approved, ready for implementation plan

## Goal

The Tiingo equities pipeline holds 570 tickers, 3,200,770 rows, 2000-01-03 → 2026-08-04.
Every one of those tickers is **still trading today** — verified against Tiingo's manifest
(569 of 570 appear in it; all 569 are live). The dataset therefore contains 100% survivors.
That is survivorship bias stated numerically.

`contracts/contract_tiingo.csv` is generated from the *current* S&P 500 snapshot, so a
company leaves the dataset the moment it leaves the index — whether it died or is still
trading happily. Backtests run against it systematically overstate returns.

This design closes that gap for the **S&P 500 universe**: backfill every company that was
in the index at any point since 2000 and whose history Tiingo can actually serve, and add a
point-in-time membership table so backtests can ask "who was in the index on 2007-03-14?"

## Scope decision: 506 candidates, not 22,105

Survivorship bias is defined **relative to the universe a strategy selects from**.

| Universe definition | Tickers to consider | Chosen |
|---|---|---|
| S&P 500 members, point-in-time | **506** | **yes** |
| Entire US equity market, ever-traded 2000+ | 22,105 (~45M rows) | no |

The pipeline is built on the S&P 500 plus ETF hedges for the futures book plus curated
liquid names. Strategies select from the S&P. The wider universe would add ~45M rows the
strategies never select from and would force a permanent Tiingo Power subscription
($30/mo), because the daily run would grow to 13,506 requests — beyond what the free tier
can serve at any speed. If research ever selects outside the S&P, that is a separate
project reusing this machinery.

## Measured coverage — 237 of 506 are usable

Universe source: [hanshof/sp500_constituents](https://github.com/hanshof/sp500_constituents),
3,482 daily point-in-time snapshots, 1996-01-02 → 2025-08-23.

```
Unique tickers ever in S&P 500, 2000-01-01 -> 2025-08-23 : 1015
Current members                                          :  503
Former members (the gap)                                 :  512
  already in contract_tiingo.csv                         :    6   BF-B BRK-B CIEN FISV GME SNDK
  NOT in the contract  -> the candidate set              :  506
```

Coverage was **measured, not estimated**. All 506 were probed against Tiingo's
`/tiingo/daily/<ticker>` metadata endpoint (~42 requests per key across 12 keys, under the
50/hr free cap), then each result was **validated against the ticker's index-membership
window**. Both steps are necessary; the second one is where most candidates fail.

| Bucket | Count | Pipeline action |
|---|---|---|
| **Usable, still trading** | **111** | backfill to 2000, then daily forever |
| **Usable, delisted** | **126** | backfill once, never fetch again |
| Marginal — <30 days overlap, needs review | 6 | manual eyeball, then reclassify |
| Tiingo's data does not cover the membership period | 106 | `coverage_gaps` |
| Known to Tiingo, no price data at all | 134 | `coverage_gaps` |
| Unknown to Tiingo | 23 | `coverage_gaps` |
| **Total** | **506** | |

**Usable: 237 (47%).** Estimated volume **~1.19M rows** (vs 3.2M existing).
Daily run grows **570 → 681**.

### Why 106 candidates fail entity validation

Tiingo serves exactly **one security per ticker string — the current one**. For any ticker
that has been reused, the API returns the modern company, not the historical index member:

```
CA   -> "XTRACKERS CALIFORNIA MUNICIPAL BOND ETF" (2023-)  NOT Computer Associates  (1984-2018)
STI  -> "Solidion Technology Inc"                 (2024-)  NOT SunTrust Banks       (1987-2019)
NFX  -> "Corgi NFLX 2x Daily ETF"                 (2026-)  NOT Newfield Exploration (1993-2019)
INFO -> "HARBOR PANAGORA DYNAMIC LARGE CAP ETF"   (2024-)  NOT IHS Markit           (2014-2022)
DYN  -> "Dyne Therapeutics"                       (2020-)  NOT Dynegy               (2012-2018)
```

A second, quieter failure lands in the same bucket: the ticker is the *right* company, but
Tiingo's coverage starts after the company left the index (e.g. `IKN` / IKON Office
Solutions — membership ends 2000-06-27, Tiingo data starts 2006-12-28). Operationally
identical: **Tiingo cannot serve the period the company was in the index.**

Detection requires **two** rules, not one:

1. `tiingo_start > membership_end` — data beginning after the company left the index cannot
   be that company's index-era history. Catches 104.
2. **Manifest-duplication check** — if `supported_tickers.zip` lists the ticker more than
   once, the string has been reused, and rule 1 can silently pass the wrong security when
   the reuser's data happens to start *before* the original left the index. Catches 2 more:

   ```
   CSR -> "Centerspace"                  Tiingo 1997-2026, membership ended 2000-06-15
   EP  -> "Empire Petroleum Corporation" Tiingo 2011-2026, membership ended 2012-05-23
   ```

   Both overlap the membership window numerically, so rule 1 passed them — but both are
   different companies from the S&P members that held those tickers.

Rule 2 was found by auditing rule 1's output, and it is the reason the usable count is 237
rather than 239. Any future validation pass must run both. Both causes are recorded in
`coverage_gaps` with Tiingo's returned security name, so a human can tell them apart later.

**Backfilling these 106 without validation would insert an unrelated modern ETF's prices
under a symbol the membership table says was an S&P 500 member in 2005 — fabricated
history, which is strictly worse than the survivorship bias being fixed.** Entity
validation is therefore a required, non-optional step, not a nice-to-have.

### The 23 unknown tickers, traced by hand

| Outcome | Count | Detail |
|---|---|---|
| History already in the DB under a successor | 7 | `UTX→RTX`, `HRS→LHX`, `JEC→J`, `SYMC→GEN`, `NLOK→GEN`, `DWDP→DD`, `RVTY` |
| Recoverable via a successor not yet loaded | 6 | `CBS→PARA`, `HFC→DINO`, `KORS→CPRI`, `ESV→VAL`, `FII→FHI`, `ATGE` |
| Genuinely lost | 10 | First Republic, SVB, JCPenney, Endo, Weatherford, Tupperware + 4 pre-2000 |

The 134 "known but no price data" were **not** individually traced. This does not block
implementation: either way they cannot be fetched, so they go into `coverage_gaps`.

## The renames hazard

Tiingo follows the security, not the string: a rename carries full pre-rename history
forward under the new symbol. Verified live — `RTX`, `LHX`, `J`, `GEN`, `DD`, `RVTY`,
`BALL`, `COR`, `ELV`, `BNY`, `MRSH` all reach back to 2000-01-03 with 6,686 rows.

The price data is correct, but the **join** breaks. Membership says `UTX` was a member
2000–2020; the prices table has no symbol `UTX`. A point-in-time backtest joining
membership → prices on ticker string finds nothing and **silently drops the name** — the
bias reintroduced through the back door.

Fix: a `ticker_aliases` table applied when resolving membership to prices.

Documented lineage caveat: `DWDP → DD` is not a clean rename. DowDuPont **split** into DD
and DOW in 2019, so `DD`'s pre-2019 history is DowDuPont's, not modern DuPont's alone.

## Why `delisting_date` is metadata, not a key

An earlier draft made `delisting_date` part of the primary key, to stop two companies
sharing a ticker from merging into one series. **Entity validation removes that need:**
since Tiingo only ever serves the current security for a given string, the old incarnation
of a reused ticker can never be fetched, so two companies can never land under one symbol
from this source. `(symbol, time)` is safe.

`delisting_date` is still worth adding as a **plain nullable column** — it drives the
`active` flag, documents why a symbol stopped updating, and future-proofs a second data
source. It just does not belong in the primary key, and the existing 3.2M-row PK stays as
it is. This removes the riskiest migration step from the plan.

## Component design

### 1. Universe + membership generation — `scripts/build_sp500_history.py`

Follows the existing `scripts/build_tiingo_universe.py` pattern: read a **committed
snapshot**, emit CSVs, no model in the loop, reproducible by re-running.

Inputs (committed to `scripts/`):
- `sp500_membership_snapshot.csv` — point-in-time membership
- `tiingo_supported_tickers.csv` — Tiingo manifest (a coverage *hint* only, see below)

Outputs:
- `contracts/contract_tiingo.csv` — gains an **`active`** column
- `contracts/sp500_membership.csv` — `(ticker, start_date, end_date)` intervals
- `contracts/ticker_aliases.csv` — hand-maintained rename map, seeded with the 13 traced

Note: `supported_tickers.zip` predicted 337 usable; the live API plus validation gives 237.
The manifest indexes ticker *strings* while the API follows the *security*. **Probe the
API; do not trust the manifest.**

### 2. Entity validation — `scripts/validate_sp500_entities.py`

The step that makes this design correct rather than actively harmful. For each candidate:

1. Probe `/tiingo/daily/<ticker>` for `name`, `startDate`, `endDate`.
2. Compare against the ticker's membership window.
3. Classify: `OK` (≥30 days overlap) / `MARGINAL` (<30 days) / `NO_COVERAGE`
   (`tiingo_start > membership_end`) / `NO_PRICE_DATA` / `NOT_FOUND`.
4. Emit `contracts/backfill_targets.csv` (the OK set) and populate `coverage_gaps` for the
   rest, recording Tiingo's returned security name as evidence.

The 6 `MARGINAL` cases (`BRCM`, `FWLT`, `WRK`, `PEAK`, `VIAC`, `FCPT`) get a human decision
before the backfill runs.

### 3. Schema changes (out-of-band SQL, matching how `equities` was created)

`equities_data.equities` is a **plain table**, not a hypertable (531 MB).

```sql
ALTER TABLE equities_data.equities ADD COLUMN delisting_date DATE;  -- nullable metadata
-- primary key (symbol, time) is UNCHANGED

CREATE TABLE equities_data.sp500_membership (
    ticker      TEXT NOT NULL,
    start_date  DATE NOT NULL,
    end_date    DATE,                      -- NULL = still a member
    PRIMARY KEY (ticker, start_date)
);

CREATE TABLE equities_data.ticker_aliases (
    historical_ticker TEXT NOT NULL,
    current_symbol    TEXT NOT NULL,
    effective_until   DATE,
    note              TEXT,
    PRIMARY KEY (historical_ticker, current_symbol)
);

CREATE TABLE equities_data.coverage_gaps (
    ticker          TEXT PRIMARY KEY,
    reason          TEXT NOT NULL,   -- NO_COVERAGE | NO_PRICE_DATA | NOT_FOUND | LOST
    membership_from DATE,
    membership_to   DATE,
    tiingo_name     TEXT,            -- evidence: what Tiingo actually returned
    tiingo_start    DATE,
    first_attempted TIMESTAMPTZ NOT NULL,
    note            TEXT
);
```

### 4. Backfill — `scripts/backfill_sp500_former.py`

Standalone and resumable. It **must not** run through the DAG:
`determine_date_range` (`utils/dynamic_loader.py:99`) computes **one global window** from
`MAX(time)` across the whole table. Since the table already reaches yesterday, every
newly-added ticker would fetch a single day (clamped at line 146) and return nothing.

Behaviour:
- Input is `backfill_targets.csv` — the validated 237 only. Never the raw candidate list.
- **Per-symbol date range**, from Tiingo metadata, not from index membership:
  - delisted → `2000-01-01` … the security's real last trading day
  - still trading → `2000-01-01` … **today**

  Using the index-removal date as the end date would manufacture a multi-year hole in every
  one of the 111 still-trading names. This is the easiest thing to get wrong.
- Reuses `TiingoFetcher` (key rotation, semaphore) and `TiingoCleaner` unchanged.
- Sets `delisting_date` for dead names; leaves it NULL for live ones.
- **Checkpointed** to a local progress file so a rate-limit stall or restart resumes.
- Bounded concurrency, well under the 570-wide `asyncio.gather` the orchestrator uses.

### 5. Daily and monthly DAG changes

**Daily** (`dags/tiingo_data_dag.py`): universe grows 570 → **681**. The 126 dead names are
excluded via the new `active` column. **This requires a code change to
`CSVLoader.load_symbols()` (`src/modules/loader/csv_loader.py`)**, which currently builds
`dict(zip(dataSymbol, instrumentType))` and would read an `active` column but ignore it —
adding the column to the CSV alone does nothing. Without the filter the daily run would
burn 126 requests every day,
forever, re-fetching immutable data.

Capacity: 681 needed vs the current 13 keys × 50/hr = **650/hr**. Two options:

- **Preferred — add 1-3 free keys.** 14 keys = 700/hr covers 681; 15 keys = 750/hr gives ~10%
  headroom. Purely a `.env` edit; the fetcher already auto-detects `TIINGO_API_KEY*`.
  At 15 keys each key issues ~45 requests in one burst — under the 50/hr cap **regardless of
  whether Tiingo's limit is a rolling window or a clock hour**, which retires that open
  question entirely.
- **Fallback — a second DAG run 90 minutes later** with its own contract file of only the
  111 active former members. Required if more keys are unavailable, because the fetcher
  disables a key permanently for the run once it 429s (`_disabled_keys`, never re-enabled),
  so one long run cannot coast across the hour boundary — it would progressively kill all
  12 keys and fail every remaining symbol. 90 minutes, not 60, in case Tiingo's limit is a
  rolling window rather than a clock hour.

**Monthly** (new task): re-read the membership snapshot, detect index changes, run entity
validation on new departures, backfill the ones that pass, and flip `active=false` for
newly-delisted names. Delistings run ~500–1,150/year market-wide, so monthly is ample.
Never daily — a delisted security's history is immutable.

### 6. Tests

Mock the HTTP and DB layers; no live calls.
- **Entity validation rejects a reused ticker** whose Tiingo `startDate` postdates its
  membership window. The highest-value test in the set.
- Backfill excludes a reused ticker flagged by the manifest-duplication check.
- Backfill picks the **correct end date** per bucket (today for live, last-trading-day for
  dead).
- Backfill refuses to run on an unvalidated candidate list.
- A 404 / empty / no-coverage result writes a `coverage_gaps` row rather than failing.
- Checkpoint resume skips already-completed tickers.
- The daily loader excludes `active=false` rows.
- `build_sp500_history.py` emits well-formed membership and alias rows.

## Out of scope

- The wider 22,105-ticker market universe.
- EODHD or any second vendor. Its free tier is past-year-only, excludes delisted data, and
  caps at 20 requests/day; its cleaner emits 8 columns against this table's 14, leaving
  `adj_open/high/low`, `adj_volume`, `div_cash`, `split_factor` NULL. The `origin/eodhd`
  branch also **deletes the entire Tiingo pipeline** (-3,438 lines) and must not be merged.
- Databento and futures pipelines; the Tiingo fetcher's rotation logic; the cleaner.
- Recovering the 106 no-coverage names from another source.

## Risks and honest caveats

- **47% coverage is the headline honest number.** Only 237 of 506 former members are
  recoverable from Tiingo. The fix is a large improvement over zero, but it is *partial*,
  and anyone using the data must read `coverage_gaps`. Do not describe the result as
  "survivorship-bias-free" — describe it as "survivorship bias corrected for 237 of 506
  identifiable former members, with the remainder documented."
- **The membership snapshot ends 2025-08-23** — roughly 11 months stale. Index changes
  since then are missing, so the 506 likely understates by ~20 names. The monthly reconcile
  fixes this going forward; the interim gap needs a fresher source. **Flagged, not solved.**
- **The 134 untraced tickers** may include renames whose history is already in the DB, so
  the effective gap is somewhat smaller than the raw count suggests.
- **Entity validation may still have residual false negatives.** Rule 2 depends on
  `supported_tickers.zip` listing both incarnations of a reused ticker. Where the manifest
  records only the current security, a reuse would go undetected. The 237 should be treated
  as "validated by two independent rules", not "provably correct". A name-based sanity pass
  over the final set before the backfill is cheap insurance.
- **The 6 recoverable-via-successor names** (`CBS→PARA`, `HFC→DINO`, `KORS→CPRI`, `ESV→VAL`,
  `FII→FHI`, `ATGE`) are not in the 681 daily count. Loading them adds ~6 to the daily run
  and needs their own alias rows.
- **Orchestrator shared-inserter concurrency**: `retrieve_and_process_data` calls
  `connect()`/`close()` per symbol on a **shared** inserter and `finally: self.inserter.close()`
  (`orchestrator.py:112`) closes the connection out from under other in-flight coroutines.
  Pre-existing, survivable at 681, **flagged not fixed** — the backfill sidesteps it by not
  using the orchestrator.
- **`equities_raw` grows too.** The orchestrator writes raw rows before cleaning; check disk
  before the run.
- **10 confirmed-lost tickers** are unrecoverable from any free source — bank failures and
  bankruptcies whose equity went to ~zero.

## Deployment

1. Merge; run the schema SQL against `new_algo_data` (all additive — no PK migration).
2. Run `scripts/build_sp500_history.py`; commit the regenerated contracts.
3. Run `scripts/validate_sp500_entities.py`; resolve the 6 `MARGINAL` cases by hand.
4. Run `scripts/backfill_sp500_former.py` — 237 tickers, ~1.19M rows, roughly an hour on
   the existing keys. Verify `coverage_gaps` is populated and row counts landed.
5. Add 1-3 more `TIINGO_API_KEY*` values to local and EC2 `.env` (or deploy the fallback
   second DAG run).
6. `git pull && docker compose down && docker compose up -d` on the box.
7. Trigger `tiingo_data_dag`; confirm 681 distinct symbols and no key exhaustion.
