# Tiingo key rotation + S&P 500 equity universe — design

**Date:** 2026-06-08
**Branch base:** `feature/tiingo-incremental-dates` (or a fresh branch off `main`)
**Status:** design approved, ready for implementation plan

## Goal

Expand the live Tiingo equities pipeline from 44 ETFs (one API key) to the full
S&P 500 + the 44 ETFs + a curated set of liquid non-S&P names (~570 symbols),
served by **round-robin rotation across many free-tier API keys** so the whole
universe lands in one daily 7:15 AM ET burst.

The work is two independent halves that meet at runtime:
1. **Code** — multi-key rotation in the fetcher + the expanded universe CSV + tests.
2. **Ops (user)** — fill 12 real free-tier keys into `.env` (placeholders exist now).

The code auto-detects whatever keys are present, so once the placeholder values
are replaced with real keys, it works with **zero code change**.

## The free-tier math (why 12 keys)

Per free key: **50 requests/hour**, 1,000/day, **500 unique symbols/month**.
One request per symbol per daily run.

| Limit | With 12 keys | Binding? |
|---|---|---|
| 50 req/hour | 600/hr capacity vs ~570 needed | This is the one that gates it. 12 keys clears it with ~30 headroom. |
| 1,000 req/day | 12,000/day vs 570 | No. |
| 500 unique symbols/month | ~48 symbols/key/month (stable assignment) vs 500 | No — but only if each key consistently handles the same subset. |

**Conditions for "it works":** universe stays under ~580 (else add a 13th key);
all 12 keys are real, separate, authenticating free accounts; round-robin
assignment is **stable per symbol** so no key drifts over its monthly cap.

> Honest note recorded for the team: 12 free keys is operationally heavier than
> **one Power-tier key ($10/mo, 5,000 req/hr)**, which would swallow all 570 in
> one burst with ~9× headroom and need no rotation or upkeep. The code is
> identical either way — only `.env` differs — so this is purely an ops choice.

## Component design

### 1. Key rotation — `src/modules/fetcher/tiingo_fetcher.py`

**Key collection (at `__init__`):**
- Collect every env var whose name starts with `TIINGO_API_KEY` with a non-empty
  stripped value, into a deterministically-ordered list `self.api_keys`.
- This auto-includes `TIINGO_API_KEY`, `TIINGO_API_KEY_JHN`, `TIINGO_API_KEY_3`…`_12`,
  and any key added later — no code change to scale.
- Raise `EnvironmentError` only if **zero** keys are found.

**Stable per-symbol primary assignment:**
- Each symbol maps to a primary key via a **stable** hash:
  `index = int(hashlib.sha1(symbol.encode()).hexdigest(), 16) % len(api_keys)`.
- `hashlib` (not Python's built-in `hash()`, which is salted per-process via
  `PYTHONHASHSEED` and would NOT be stable across runs/containers).
- Stability is what keeps each key under its 500-unique-symbols/month cap: a given
  symbol lands on the same key every day, so each key sees a fixed ~48 symbols/month.

**Failover + retry (replaces the current raise-on-non-200):**
- Try the primary key; on a key-level failure — HTTP **429** (rate limit) or
  **401/403** (bad/placeholder key) — mark that key disabled **for this run** and
  retry the same symbol with the next not-disabled key.
- Bounded attempts = number of keys (try each at most once). No sleep between
  attempts — the point is to switch keys, not wait.
- If all keys are disabled, raise `RuntimeError` → the orchestrator's per-symbol
  `except` (orchestrator.py:108) logs it and the symbol refills next run.
- Non-key errors (e.g. 5xx, network) still raise as today (per-symbol skip).

**Concurrency safety:** asyncio is single-threaded and cooperative; the disabled-key
set and any index reads/writes happen between `await` points, so no lock is needed.

### 2. The universe — `contracts/contract_tiingo.csv`

- Source the **current S&P 500 constituents** from a reliable list at build time
  (e.g. the maintained Wikipedia "List of S&P 500 companies" table or an equivalent
  CSV), then:
  - Apply the **share-class fix**: `BRK.B` → `BRK-B`, `BF.B` → `BF-B` (Tiingo uses
    a dash for share classes; the index list uses a dot).
  - **Dedupe** against the existing 44 ETFs (no overlap expected, but enforce it).
  - Add a **curated ~20–30** very liquid non-S&P names (large ADRs / high-volume
    NASDAQ names) — hand-picked, deduped against the S&P set.
- Output format unchanged: `dataSymbol,instrumentType`, all rows `EQUITY`.
- **No bulk live-validation** this time — validating ~570 tickers would itself burn
  the monthly unique-symbol cap. Standard S&P tickers are reliably covered; any bad
  symbol just logs + skips (orchestrator already handles that).
- Report the **exact final count** after generation; if it exceeds ~580, recommend a
  13th key.

### 3. Tests — `tests/fetcher/test_tiingo_fetcher.py` (extend)

Mock the HTTP layer (no live Tiingo calls):
- Multi-key collection: given several `TIINGO_API_KEY*` env vars, `api_keys` contains
  all of them in deterministic order; zero keys → `EnvironmentError`.
- Stable assignment: the same symbol maps to the same key index across instances.
- Failover: a 429 on the primary key → request retried on the next key and succeeds.
- Bad-key failover: a 401/403 behaves the same (survives placeholder keys).
- All-keys-exhausted: every key 429s → `RuntimeError` raised (so orchestrator skips).

## Out of scope (do NOT modify)

- Orchestrator concurrency (`asyncio.gather` over all symbols), the inserter, the
  loader, the cleaner, the incremental date logic, the DAG schedule. All unchanged.

## Risks / honest caveats

- **Inserter concurrency:** `inserter.connect()`/`close()` run per-symbol on a shared
  instance (orchestrator.py:86,112). This already exists for 44 symbols; at ~570
  concurrency a connect/close race or connection-count pressure *could* surface.
  Pre-existing, not introduced here, flagged not fixed.
- **570 concurrent `aiohttp` sessions:** the fetcher opens a `ClientSession` per
  request; 570 at once raises connection/fd pressure. Pre-existing pattern; if it
  bites, the follow-up is a shared session or a concurrency semaphore (separate change).
- **Placeholder keys:** while keys `_3`…`_12` hold placeholder values, those keys
  will 401/403 and be disabled per-run; only the 2 real keys do work until filled.
  The 401/403 failover path is specifically what keeps the pipeline running during
  this interim.

## Deployment (after merge)

1. Fill real key values into local `.env` and the **EC2 box `.env`**.
2. On the box: `git pull`, then `docker compose down && docker compose up -d` so the
   container reloads the new `.env`.
3. Trigger `tiingo_data_dag` once; verify row counts climb toward ~570 distinct symbols.
