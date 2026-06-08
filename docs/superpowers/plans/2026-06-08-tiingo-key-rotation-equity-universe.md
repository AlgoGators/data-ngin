# Tiingo Key Rotation + S&P 500 Universe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the Tiingo equities pipeline to ~570 symbols (S&P 500 + 44 ETFs + curated liquid extras) served by round-robin rotation across all `TIINGO_API_KEY*` env keys, so the universe lands in one daily burst once real keys are filled in.

**Architecture:** The single `TiingoFetcher` instance (created once per run, called concurrently per symbol via `asyncio.gather`) holds the key list and a per-run disabled-key set on `self`. Each symbol gets a *stable* primary key via a hashlib hash (keeps each key under its 500-symbols/month cap); on a key-level failure (429/401/403) the request fails over to the next key. The universe is a regenerated CSV the DAG re-reads every run — no config/DAG/orchestrator changes.

**Tech Stack:** Python 3, `aiohttp`, `pandas`, `hashlib`, `pytest`/`unittest.IsolatedAsyncioTestCase`, Airflow (unchanged), CSV contract file.

---

## Pre-flight

- [ ] **Step 0: Create a working branch**

```bash
cd /Users/jonahnissan/PycharmProjects/data-ngin
git checkout main && git pull
git checkout -b feature/tiingo-key-rotation
```

Expected: on a fresh branch off `main`.

---

## File Structure

- **Modify:** `src/modules/fetcher/tiingo_fetcher.py` — replace single-key auth with multi-key collection + stable assignment + failover. Sole owner of rotation logic.
- **Modify:** `tests/fetcher/test_tiingo_fetcher.py` — add env isolation (`clear=True`); add a `_RoutingSession` fake; add a rotation test class.
- **Modify:** `contracts/contract_tiingo.csv` — regenerate to ~570 symbols.
- **Create:** `tests/test_contract_tiingo_universe.py` — structural guarantees for the CSV (header, all-EQUITY, no dups, no dot share-classes, ETF baseline present, size band).
- **Untouched (do NOT edit):** orchestrator, inserter, loader, cleaner, `dynamic_loader`, `config_tiingo.yaml`, the DAG.

---

## Task 1: Multi-key rotation in the fetcher

**Files:**
- Modify: `tests/fetcher/test_tiingo_fetcher.py`
- Modify: `src/modules/fetcher/tiingo_fetcher.py`

- [ ] **Step 1: Isolate env in the existing tests (prevents real `.env` keys leaking in)**

In `tests/fetcher/test_tiingo_fetcher.py`, change the `setUp` env patch to clear the environment, and change the missing-key test the same way.

Replace in `setUp`:

```python
        self.env_patch = patch.dict(os.environ, {"TIINGO_API_KEY": "x" * 40})
```

with:

```python
        self.env_patch = patch.dict(os.environ, {"TIINGO_API_KEY": "x" * 40}, clear=True)
```

Replace in `test_missing_api_key_raises`:

```python
        with patch.dict(os.environ, {"TIINGO_API_KEY": ""}):
```

with:

```python
        with patch.dict(os.environ, {"TIINGO_API_KEY": ""}, clear=True):
```

- [ ] **Step 2: Run the existing tests to confirm they still pass (deterministic baseline)**

Run: `python -m pytest tests/fetcher/test_tiingo_fetcher.py -v`
Expected: 4 PASS (`test_fetch_data_success`, `test_fetch_data_empty`, `test_fetch_data_http_error`, `test_missing_api_key_raises`).

- [ ] **Step 3: Add the routing fake + rotation test class (failing tests)**

Append to `tests/fetcher/test_tiingo_fetcher.py` (after the existing `_FakeSession` class is fine; place the new class near the bottom, before the `if __name__` block):

```python
class _RoutingSession:
    """Fake aiohttp session that returns a different response per token,
    so multi-key rotation can be exercised. Records tokens in call order."""

    def __init__(self, by_token):
        self._by_token = by_token
        self.tokens_tried = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, params=None):
        token = params["token"]
        self.tokens_tried.append(token)
        return self._by_token[token]


class TestTiingoFetcherRotation(unittest.IsolatedAsyncioTestCase):
    config = {"provider": {"name": "tiingo", "asset": "EQUITY"}}

    def test_collects_all_tiingo_keys(self):
        env = {
            "TIINGO_API_KEY": "aaaa",
            "TIINGO_API_KEY_JHN": "bbbb",
            "TIINGO_API_KEY_3": "cccc",
            "PATH": "/usr/bin",  # non-Tiingo var must be ignored
        }
        with patch.dict(os.environ, env, clear=True):
            fetcher = TiingoFetcher(config=self.config)
        self.assertCountEqual(fetcher.api_keys, ["aaaa", "bbbb", "cccc"])

    def test_no_keys_raises(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(EnvironmentError):
                TiingoFetcher(config=self.config)

    def test_stable_assignment(self):
        env = {"TIINGO_API_KEY": "a", "TIINGO_API_KEY_2": "b", "TIINGO_API_KEY_3": "c"}
        with patch.dict(os.environ, env, clear=True):
            f1 = TiingoFetcher(config=self.config)
            f2 = TiingoFetcher(config=self.config)
        # Deterministic across instances/processes, and in range.
        self.assertEqual(f1._primary_index("AAPL"), f2._primary_index("AAPL"))
        self.assertTrue(0 <= f1._primary_index("AAPL") < 3)
        # Sanity: a sample of symbols spreads across more than one key.
        idxs = {f1._primary_index(s) for s in
                ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOG", "META", "JPM"]}
        self.assertGreater(len(idxs), 1)

    async def test_failover_on_429(self):
        env = {"TIINGO_API_KEY": "a", "TIINGO_API_KEY_2": "b"}
        with patch.dict(os.environ, env, clear=True):
            fetcher = TiingoFetcher(config=self.config)
            symbol = "AAPL"
            primary = fetcher.api_keys[fetcher._primary_index(symbol)]
            by_token = {primary: _FakeResponse(429, {"detail": "rate limit"})}
            for k in fetcher.api_keys:
                if k != primary:
                    by_token[k] = _FakeResponse(200, _sample_payload())
            session = _RoutingSession(by_token)
            with patch("src.modules.fetcher.tiingo_fetcher.aiohttp.ClientSession",
                       return_value=session):
                df = await fetcher.fetch_data(symbol, "EQUITY", "2024-01-02", "2024-01-03")
        self.assertEqual(len(df), 2)
        self.assertIn(primary, fetcher._disabled_keys)

    async def test_failover_on_401(self):
        env = {"TIINGO_API_KEY": "a", "TIINGO_API_KEY_2": "b"}
        with patch.dict(os.environ, env, clear=True):
            fetcher = TiingoFetcher(config=self.config)
            symbol = "MSFT"
            primary = fetcher.api_keys[fetcher._primary_index(symbol)]
            by_token = {primary: _FakeResponse(401, {"detail": "invalid token"})}
            for k in fetcher.api_keys:
                if k != primary:
                    by_token[k] = _FakeResponse(200, _sample_payload())
            session = _RoutingSession(by_token)
            with patch("src.modules.fetcher.tiingo_fetcher.aiohttp.ClientSession",
                       return_value=session):
                df = await fetcher.fetch_data(symbol, "EQUITY", "2024-01-02", "2024-01-03")
        self.assertEqual(len(df), 2)
        self.assertIn(primary, fetcher._disabled_keys)

    async def test_all_keys_exhausted_raises(self):
        env = {"TIINGO_API_KEY": "a", "TIINGO_API_KEY_2": "b"}
        with patch.dict(os.environ, env, clear=True):
            fetcher = TiingoFetcher(config=self.config)
            by_token = {k: _FakeResponse(429, {"detail": "rate"}) for k in fetcher.api_keys}
            session = _RoutingSession(by_token)
            with patch("src.modules.fetcher.tiingo_fetcher.aiohttp.ClientSession",
                       return_value=session):
                with self.assertRaises(RuntimeError):
                    await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-03")
            self.assertEqual(len(fetcher._disabled_keys), len(fetcher.api_keys))
```

- [ ] **Step 4: Run the new tests to verify they fail**

Run: `python -m pytest tests/fetcher/test_tiingo_fetcher.py::TestTiingoFetcherRotation -v`
Expected: FAIL — `AttributeError`/`TypeError` (no `api_keys`, `_primary_index`, or `_disabled_keys` yet; `_collect_api_keys` missing).

- [ ] **Step 5: Rewrite the fetcher with rotation (full file)**

Replace the entire contents of `src/modules/fetcher/tiingo_fetcher.py` with:

```python
import logging
import os
import hashlib
import aiohttp
import pandas as pd
from typing import Dict, Any, List
from dotenv import load_dotenv

# Reuse the existing Fetcher base class
from src.modules.fetcher.fetcher import Fetcher

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = "https://api.tiingo.com/tiingo/daily"

# Tiingo field name -> pipeline/DB column name. We keep the full raw + adjusted
# set plus the dividend/split event fields (all returned in the same response).
RENAME_MAP = {
    "date": "time",
    "adjOpen": "adj_open",
    "adjHigh": "adj_high",
    "adjLow": "adj_low",
    "adjClose": "adjusted_close",
    "adjVolume": "adj_volume",
    "divCash": "div_cash",
    "splitFactor": "split_factor",
}

# Columns persisted to the equities / equities_raw tables. The fetcher trims its
# output to exactly these so the raw insert (which inserts every column verbatim)
# aligns with the table schema.
OUTPUT_COLUMNS = [
    "time",
    "open", "high", "low", "close", "volume",            # raw / as-traded
    "adj_open", "adj_high", "adj_low", "adjusted_close", "adj_volume",  # split/div adjusted
    "div_cash", "split_factor",                           # adjustment events
    "symbol",
]

# HTTP statuses that mean "this key is unusable right now" -> rotate to another key.
# 429 = hourly/daily allocation exhausted; 401/403 = invalid/placeholder/over-quota key.
KEY_LEVEL_FAILURES = (401, 403, 429)


class TiingoFetcher(Fetcher):
    """
    Fetcher for the Tiingo End-of-Day daily prices API, with multi-key rotation.

    Endpoint: https://api.tiingo.com/tiingo/daily/{ticker}/prices
    Auth:     ?token=<key>; keys are collected from every TIINGO_API_KEY* env var.
    Dates:    startDate / endDate are INCLUSIVE (no +1-day translation).

    Rotation:
      - Each symbol gets a STABLE primary key (hashlib-based) so it lands on the
        same key every run -> keeps each key under its 500-unique-symbols/month cap.
      - On a key-level failure (429/401/403) the key is disabled FOR THIS RUN and
        the symbol is retried with the next key. If all keys are disabled, raise
        (the orchestrator's per-symbol except logs it; the symbol refills next run).

    Docs: https://www.tiingo.com/documentation/end-of-day
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self.api_keys: List[str] = self._collect_api_keys()
        if not self.api_keys:
            raise EnvironmentError(
                "No TIINGO_API_KEY* keys set in environment / .env file"
            )
        # Keys disabled for THIS run (rate-limited / invalid). A fresh fetcher is
        # built per pipeline run, so this resets every run.
        self._disabled_keys: set = set()

    @staticmethod
    def _collect_api_keys() -> List[str]:
        """Collect every non-empty env var named TIINGO_API_KEY* in deterministic
        (name-sorted) order. Adding a key is purely a .env edit -- no code change."""
        keys: List[str] = []
        for name in sorted(os.environ):
            if name.startswith("TIINGO_API_KEY"):
                value = os.environ[name].strip()
                if value:
                    keys.append(value)
        return keys

    def _primary_index(self, symbol: str) -> int:
        """Stable per-symbol key assignment. Uses hashlib (NOT built-in hash(),
        which is salted per-process via PYTHONHASHSEED) so a symbol maps to the
        same key across runs/containers."""
        digest = hashlib.sha1(symbol.encode("utf-8")).hexdigest()
        return int(digest, 16) % len(self.api_keys)

    async def fetch_data(
        self,
        symbol: str,
        loaded_asset_type: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Fetch EOD OHLCV data for a single ticker, rotating keys on key-level failures.

        Returns:
            pd.DataFrame with OUTPUT_COLUMNS (empty DataFrame with those columns if
            Tiingo returns no data).

        Raises:
            RuntimeError: on a non-key error (e.g. 5xx) or when all keys are exhausted.
        """
        url = f"{BASE_URL}/{symbol}/prices"
        base_params = {"startDate": start_date, "endDate": end_date, "format": "json"}

        n = len(self.api_keys)
        start = self._primary_index(symbol)
        last_error = None

        logger.info(f"[Tiingo] Fetching EOD data for {symbol} from {start_date} to {end_date}")

        for offset in range(n):
            idx = (start + offset) % n
            key = self.api_keys[idx]
            if key in self._disabled_keys:
                continue
            params = {**base_params, "token": key}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._to_dataframe(data, symbol)
                    body = await response.text()
                    if response.status in KEY_LEVEL_FAILURES:
                        logger.warning(
                            f"[Tiingo] key #{idx} unusable for {symbol} "
                            f"(HTTP {response.status}); rotating to next key"
                        )
                        self._disabled_keys.add(key)
                        last_error = f"HTTP {response.status}: {body}"
                        continue
                    # Non-key error (5xx, etc.): fail this symbol, don't burn the key.
                    raise RuntimeError(f"[Tiingo] HTTP {response.status} for {symbol}: {body}")

        raise RuntimeError(
            f"[Tiingo] all {n} keys exhausted for {symbol}; last error: {last_error}"
        )

    def _to_dataframe(self, data: Any, symbol: str) -> pd.DataFrame:
        """Map Tiingo JSON to the persisted column set (rename + trim + tag symbol)."""
        if not data:
            logger.warning(f"[Tiingo] No data returned for {symbol}")
            return pd.DataFrame(columns=OUTPUT_COLUMNS)

        df = pd.DataFrame(data)
        df.rename(columns=RENAME_MAP, inplace=True)
        df["symbol"] = symbol

        missing = [c for c in OUTPUT_COLUMNS if c not in df.columns]
        if missing:
            raise RuntimeError(
                f"[Tiingo] Response for {symbol} missing expected fields {missing}. "
                f"Got columns: {list(df.columns)}"
            )
        df = df[OUTPUT_COLUMNS].copy()
        logger.info(f"[Tiingo] Fetched {len(df)} rows for {symbol}")
        return df


# Optional live smoke test: run directly with real keys in .env.
#   python -m src.modules.fetcher.tiingo_fetcher
if __name__ == "__main__":
    import asyncio

    async def _smoke():
        fetcher = TiingoFetcher(config={"provider": {"asset": "EQUITY"}})
        print(f"Collected {len(fetcher.api_keys)} key(s)")
        df = await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-05")
        print(df)
        print("columns:", list(df.columns))

    asyncio.run(_smoke())
```

- [ ] **Step 6: Run the whole fetcher test file to verify all pass**

Run: `python -m pytest tests/fetcher/test_tiingo_fetcher.py -v`
Expected: all PASS — the original 4 plus `test_collects_all_tiingo_keys`, `test_no_keys_raises`, `test_stable_assignment`, `test_failover_on_429`, `test_failover_on_401`, `test_all_keys_exhausted_raises`.

- [ ] **Step 7: Commit**

```bash
git add src/modules/fetcher/tiingo_fetcher.py tests/fetcher/test_tiingo_fetcher.py
git commit -m "feat(tiingo): round-robin multi-key rotation with stable per-symbol assignment"
```

---

## Task 2: Expand the universe CSV to ~570 symbols

**Files:**
- Create: `tests/test_contract_tiingo_universe.py`
- Modify: `contracts/contract_tiingo.csv`

- [ ] **Step 1: Write the structural test (failing — CSV still has 44 rows)**

Create `tests/test_contract_tiingo_universe.py`:

```python
import csv
import os
import unittest

CSV_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "contracts", "contract_tiingo.csv")
)

# The 44 ETFs already deployed — must survive the expansion.
ETF_BASELINE = [
    "SPY", "QQQ", "IWM", "DIA", "SHY", "IEI", "IEF", "TLT", "USO", "UNG", "UGA",
    "XLE", "GLD", "SLV", "CPER", "PPLT", "CORN", "WEAT", "SOYB", "DBA", "UUP",
    "FXE", "FXB", "FXY", "FXC", "FXF", "FXA", "IBIT", "VOO", "VTI", "XLF", "XLK",
    "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC", "EFA", "EEM", "AGG", "LQD",
]


class TestContractTiingoUniverse(unittest.TestCase):
    def setUp(self):
        with open(CSV_PATH, newline="") as f:
            self.rows = list(csv.DictReader(f))
        self.symbols = [r["dataSymbol"] for r in self.rows]

    def test_header_and_all_equity(self):
        self.assertEqual(set(self.rows[0].keys()), {"dataSymbol", "instrumentType"})
        self.assertTrue(all(r["instrumentType"] == "EQUITY" for r in self.rows))

    def test_no_duplicates(self):
        self.assertEqual(len(self.symbols), len(set(self.symbols)),
                         msg="duplicate tickers in contract_tiingo.csv")

    def test_no_dot_share_classes(self):
        # Tiingo uses dashes (BRK-B), not dots (BRK.B).
        dotted = [s for s in self.symbols if "." in s]
        self.assertEqual(dotted, [], msg=f"dot-form tickers must be dashed: {dotted}")

    def test_no_blank_symbols(self):
        self.assertTrue(all(s.strip() for s in self.symbols))

    def test_etf_baseline_present(self):
        missing = [t for t in ETF_BASELINE if t not in self.symbols]
        self.assertEqual(missing, [], msg=f"missing ETFs: {missing}")

    def test_universe_size_in_range(self):
        # ~500 S&P + 44 ETFs + ~20-30 curated extras. Over 580 -> advise a 13th key.
        self.assertGreaterEqual(len(self.symbols), 540)
        self.assertLessEqual(len(self.symbols), 600)


if __name__ == "__main__":
    unittest.main()
```

Run: `python -m pytest tests/test_contract_tiingo_universe.py -v`
Expected: FAIL on `test_universe_size_in_range` and `test_etf_baseline_present`-adjacent checks (current CSV is 44 rows / size < 540).

- [ ] **Step 2: Fetch the current S&P 500 constituents**

Use WebFetch on the maintained list and extract the ticker column:
- Primary source: `https://en.wikipedia.org/wiki/List_of_S%26P_500_companies` (the "Symbol" column of the constituents table).
- Fallback source: `https://datahub.io/core/s-and-p-500-companies/r/constituents.csv` (column `Symbol`).

Capture the raw symbol list (≈500 tickers). Keep it in memory for the next step.

- [ ] **Step 3: Build the merged CSV with the generator script**

Create and run a one-off generator. Create `scripts/build_tiingo_universe.py`:

```python
"""One-off generator for contracts/contract_tiingo.csv.

Merges: existing 44 ETFs + S&P 500 constituents + curated liquid non-S&P names.
Applies the Tiingo share-class fix (BRK.B -> BRK-B) and dedupes. Run from repo root:
    python scripts/build_tiingo_universe.py
Paste the S&P symbols (one per line, dot-form ok) into SP500 below before running,
or wire in the fetched list.
"""
import csv
import os

OUT = os.path.join(os.path.dirname(__file__), "..", "contracts", "contract_tiingo.csv")

ETFS = [
    "SPY", "QQQ", "IWM", "DIA", "SHY", "IEI", "IEF", "TLT", "USO", "UNG", "UGA",
    "XLE", "GLD", "SLV", "CPER", "PPLT", "CORN", "WEAT", "SOYB", "DBA", "UUP",
    "FXE", "FXB", "FXY", "FXC", "FXF", "FXA", "IBIT", "VOO", "VTI", "XLF", "XLK",
    "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC", "EFA", "EEM", "AGG", "LQD",
]

# Curated very-liquid names that are NOT ETFs and may sit outside the S&P 500
# (large ADRs / high-volume US listings). Overlaps with S&P are deduped below.
CURATED_EXTRAS = [
    "BABA", "PDD", "NIO", "TSM", "JD", "BIDU", "LI", "XPEV", "NU", "SE", "SHOP",
    "RIVN", "LCID", "MARA", "RIOT", "GME", "AMC", "AFRM", "UPST", "RBLX", "SNAP",
    "CVNA", "IONQ", "HOOD", "SOFI",
]

# Paste S&P 500 constituent symbols here (dot-form like BRK.B is fine).
SP500 = [
    # e.g. "AAPL", "MSFT", "BRK.B", "BF.B", ...
]


def tiingo_ticker(sym: str) -> str:
    return sym.strip().upper().replace(".", "-")  # BRK.B -> BRK-B


def main() -> None:
    seen = set()
    ordered = []
    # ETFs first (preserve the deployed set), then S&P, then curated extras.
    for sym in ETFS + [tiingo_ticker(s) for s in SP500] + CURATED_EXTRAS:
        t = tiingo_ticker(sym)
        if t and t not in seen:
            seen.add(t)
            ordered.append(t)

    with open(os.path.normpath(OUT), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["dataSymbol", "instrumentType"])
        for t in ordered:
            w.writerow([t, "EQUITY"])

    print(f"Wrote {len(ordered)} symbols to {OUT}")


if __name__ == "__main__":
    main()
```

Fill `SP500` with the list from Step 2, then run:

```bash
python scripts/build_tiingo_universe.py
```

Expected: `Wrote <N> symbols ...` with N ≈ 560–575.

- [ ] **Step 4: Run the universe test to verify it passes**

Run: `python -m pytest tests/test_contract_tiingo_universe.py -v`
Expected: all PASS. If `test_universe_size_in_range` reports > 580, note that a **13th key** is advised; if it fails high, trim `CURATED_EXTRAS` or accept and add a key.

- [ ] **Step 5: Commit**

```bash
git add contracts/contract_tiingo.csv scripts/build_tiingo_universe.py tests/test_contract_tiingo_universe.py
git commit -m "feat(tiingo): expand universe to S&P 500 + ETFs + curated liquid names"
```

---

## Task 3: Full regression + finalize

**Files:** none (verification only)

- [ ] **Step 1: Run the full Tiingo-related test set**

Run:
```bash
python -m pytest tests/fetcher/test_tiingo_fetcher.py tests/cleaner/test_tiingo_cleaner.py tests/test_determine_date_range.py tests/test_contract_tiingo_universe.py -v
```
Expected: all PASS. (Pre-existing unrelated failures in `test_orchestrator.py` / `test_config.py` etc. are out of scope — do not fix here.)

- [ ] **Step 2: Confirm the exact final symbol count and record it**

Run:
```bash
tail -n +2 contracts/contract_tiingo.csv | wc -l
```
Record the number in the PR description and state how many keys it needs (`ceil(count / 50)`).

- [ ] **Step 3: Open the PR**

```bash
git push -u origin feature/tiingo-key-rotation
gh pr create --base main --title "Tiingo key rotation + S&P 500 universe" \
  --body "Round-robin rotation across all TIINGO_API_KEY* keys (stable per-symbol assignment, 429/401/403 failover) + universe expanded to <N> symbols. Needs ceil(<N>/50) working keys to clear the hourly burst on free tier."
```

---

## Task 4: Deploy (manual — after merge, requires real keys)

**Files:** none (ops)

- [ ] **Step 1: Replace the 10 placeholder key values** in the local `.env` (`TIINGO_API_KEY_3`…`_12`) with real free-tier keys, and add the same keys to the **EC2 box `.env`** (`/home/ubuntu/data-ngin/.env`).

- [ ] **Step 2: On the box, pull and reload the container** (env is read at container start):

```bash
cd /home/ubuntu/data-ngin
git pull
docker compose down && docker compose up -d
```

- [ ] **Step 3: Confirm the DAG parses**

```bash
docker compose exec airflow-scheduler airflow dags list | grep tiingo
docker compose exec airflow-scheduler airflow dags list-import-errors
```
Expected: `tiingo_data_dag` listed; no import errors.

- [ ] **Step 4: Trigger once and verify rows climb toward the full universe**

```sql
SELECT COUNT(DISTINCT symbol) AS symbols, COUNT(*) AS rows,
       MIN(time)::date AS first_day, MAX(time)::date AS last_day
FROM equities_data.equities;
```
Expected: `symbols` approaching the full count (≈570 with 12+ real keys; fewer if some keys are still placeholders or rate-limited — those refill on subsequent runs).

---

## Self-Review (completed)

- **Spec coverage:** rotation (Task 1) ✓, stable assignment for monthly cap (Task 1, `_primary_index`) ✓, universe + share-class fix + dedupe + no-bulk-validation (Task 2) ✓, tests (Tasks 1–2) ✓, deployment (Task 4) ✓, risks recorded in the design doc ✓.
- **Placeholders:** none — all code shown in full; the only intentional fill-in is the `SP500` list, which Step 2 sources explicitly and the Step 4 test enforces.
- **Type/name consistency:** `api_keys`, `_disabled_keys`, `_collect_api_keys`, `_primary_index`, `_to_dataframe`, `KEY_LEVEL_FAILURES`, `_RoutingSession`, `_FakeResponse`, `OUTPUT_COLUMNS` used consistently across tasks.
