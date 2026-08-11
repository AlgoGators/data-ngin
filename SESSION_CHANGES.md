# data-ngin: Session Changes

Refactor of the market-data ETL pipeline toward a declarative config model and a domain/application/infrastructure architecture, plus a new GraphQL query API. All changes are staged but **uncommitted** — nothing has been pushed.

---

## Bug fixes

- **Batch fetch returned data in reverse chronological order.** `BatchDownloadDatabentoFetcher.generate_and_fetch_data` concatenated each new batch *before* the accumulator (`pd.concat([data, master_df])`), so multi-batch fetches came back chronologically descending across batch boundaries. This corrupted `BackAdjuster`'s roll detection (which assumes ascending time order), producing wrong back-adjusted prices. Fixed to collect batches in order and concat once; also eliminates an O(n²) copy.
- **Orchestrator silently swallowed per-symbol failures.** `run()` used to catch and log exceptions without re-raising, so `asyncio.gather` reported success even when every symbol failed. Now re-raises and aggregates failures into a `RuntimeError` naming every failed symbol.
- **Shared DB connection raced across concurrent symbols.** A single `inserter` instance was reused across all symbols processed concurrently via `asyncio.gather`; one symbol's `finally: close()` could tear down the connection another symbol was still inserting on. Each symbol now gets its own inserter/connection (later backed by a shared pool — see below).
- **`insert_data([])` raised `IndexError`.** Empty results (holiday-only range, delisted contract, empty batch window) are routine, not errors. Now a no-op with a log line, skipped before the connection check.
- **`batch_downloading.batch` config flag was silently ignored.** Batching had become purely a function of which fetcher class was configured; the flag stopped doing anything. Restored as a real gate — `false` now falls back to a single unbatched fetch even with the batching class configured.
- **`data/config/config.yaml` path was wrong in `tests/test_config.py`.** Duplicated `load_config` logic pointed at a path that never existed, so those tests always raised `FileNotFoundError`. Now delegates to the real `load_config`.
- **`missing_data.*` string-compare bug.** `handle_missing_data` compared config values to the literal string `"True"`, so a real YAML boolean `true` was silently a no-op. Fixed in lockstep with adding Pydantic config validation (which coerces to real booleans) — checks truthiness now, accepts both bools and legacy strings.
- **Dead/broken code removed:** `fetch_data_with_limit` (undefined-variable bug, `Timestamp + int` type error, unreachable code), `data_staleness.py` (queried a nonexistent column, wrong config path, never wired anywhere — later rebuilt properly, see below).
- **Airflow DAG bugs found via live execution:** an invalid `.expand()` call mapping over a dict-subscripted XCom (Airflow only allows mapping over a task's raw return value), and deprecated `schedule_interval` param.
- **`slowapi` rate-limiting middleware silently did nothing.** `SlowAPIMiddleware`'s route-matching never recognized routes mounted via `app.include_router()` under the installed FastAPI/Starlette version — requests sailed through uncounted, with no error, looking configured but doing nothing. Replaced with a small, directly-tested in-memory fixed-window limiter.
- **`db_models.get_engine()`'s clear "missing env var" validation was lost** when the write path moved to `OhlcvRepository` (raw psycopg2) — a missing `DB_PASSWORD` now surfaced as an opaque driver error. *(Flagged; not yet reconciled — see Next Steps.)*

## Improvements

- **Config is now a validated contract**, not an untyped dict. `PipelineConfig` (Pydantic) validates `config.yaml` on load; unknown/missing/malformed keys fail loudly at startup instead of silently no-op'ing deep in the pipeline.
- **Symbol remap table and back-adjustment asset-type gating moved into config** (`symbol_remap:`, `back_adjustment.applies_to:`), consumed by domain services instead of being hardcoded/unconditional.
- **Repository consolidation.** `DataAccess` (ORM reads) and `TimescaleDBInserter` (raw-SQL writes) merged into one `OhlcvRepository`, preserving the upsert (`ON CONFLICT DO NOTHING`) semantics from the write side.
- **Connection pooling.** `OhlcvRepository` now borrows/returns connections from a shared `psycopg2.pool.ThreadedConnectionPool` instead of opening a fresh TCP connection (plus 3 diagnostic queries) on every API request and every symbol. Diagnostic queries now log once per process; schema/table-existence checks cache per `(schema, table)` instead of re-running on every insert.
- **Domain ports are now load-bearing, not decorative.** `FetcherPort`/`CleanerPort`/`RepositoryPort`/`SymbolSourcePort` are `@runtime_checkable`; `get_instance()` validates a configured class against its expected port at construction time, so a `config.yaml` typo pointing at a class missing `retrieve()`/`clean()`/etc. now fails immediately with a clear `TypeError` instead of an `AttributeError` mid-pipeline.
- **Airflow DAG rebuilt with per-symbol dynamic task mapping**, replacing one opaque `run_pipeline` task. `resolve_date_range` and `resolve_symbols` now call independent `Orchestrator` methods so neither does the other's work (symbols task no longer needs the DB reachable; date-range task no longer reloads the CSV).
- **Staleness/gap monitoring rebuilt.** New `StalenessChecker` domain service; wired into the DAG as a `check_staleness` task (runs regardless of upstream failures, warns without failing the DAG) — replaces the deleted, broken `data_staleness.py` which was never actually wired into anything.
- **GraphQL query API added** (`src/api/`) — Strawberry/FastAPI, backed by `OhlcvRepository`. API-key auth gate + custom rate limiter (both verified working, including the auth-before-DB-touch ordering and rate-limit-exceeded behavior).
- **Dead code removed:** unused domain value objects (`Symbol`, `Instrument`, `Contract`, `AssetType`, unused `OHLCVBar`) that had zero production callers; the duplicate `OHLCVBar` schema (domain vs. GraphQL) this created.
- **Test infrastructure fixes:** dozens of stale `unittest.mock.patch` targets pointed at a `data.modules.*` path that never existed (pre-existing breakage, unrelated to this session's changes, fixed as a side effect of touching these files) — test pass count went from 27 (many silently broken) to 104 over the session, zero regressions.
- **`utils/` moved into `src/utils/`**, fixing a real packaging gap — `pyproject.toml` only declared `src` as an installable package, so `utils/` was silently excluded from any real `poetry build` and only worked via a `PYTHONPATH` workaround.
- **Logging consolidated.** `setup_logging()` (previously dead code, default log path pointed at a stale scaffold directory) is now idempotent and wired into both the orchestrator and API server, replacing a duplicated inline `logging.basicConfig`.
- **`.gitignore` reorganized** into labeled sections; `poetry.lock` regenerated to match `pyproject.toml`'s new dependencies (`strawberry-graphql`); `.env.template` documents all new env vars.

## Architectural changes

- **Declarative vs. imperative split, deliberately drawn:** component selection, symbol remap, back-adjustment gating, missing-data strategies, and DB schema/table names are config-driven; pipeline stage order (fetch → insert raw → clean → insert clean) stays hardcoded — a 4-stage linear pipeline doesn't benefit from a config-driven step interpreter.
- **Domain/Application/Infrastructure layering** (hexagonal-style, 3 layers — adapters and infrastructure treated as the same concept given this codebase's size):
  - `src/domain/` — `models.py` (`RollEvent`, `BackAdjustment`, `StalenessReport`), `services.py` (`SymbolRemapper`, `BackAdjuster`, `MissingDataFiller`, `StalenessChecker`), `ports.py` (`FetcherPort`, `CleanerPort`, `RepositoryPort`, `SymbolSourcePort`, `QueryPort`)
  - `src/application/orchestrator.py` — coordinates domain + infrastructure, no business rules of its own
  - `src/infrastructure/` — `fetcher/`, `cleaner/`, `loader/`, `repository/`, `db_models.py`, `inserter.py`
  - `src/api/` — GraphQL query layer, sits atop the repository
  - `src/config/` — `config.yaml` + Pydantic schema
  - `src/utils/` — dynamic class loading, logging setup
- **Business logic extracted from infrastructure into domain services**: symbol remapping, back-adjustment algorithm, and missing-data fill strategy used to live inline inside fetcher/cleaner classes; now they're independently testable domain services those classes delegate to.
- **`dags/` and `main.py` stay outside `src/`** on purpose — Airflow scans one dedicated folder, and a plain CLI entrypoint isn't part of the importable application package.
- **`data-ngin` is a standalone service, not a shared library** — the GraphQL API is the intended integration point for other services; direct Python import of internals was explicitly rejected to avoid coupling consumers to internal domain objects and forcing synchronized deploys.

## Next steps

- **Live-DB test run.** `test_db_connection.py`/`test_db_models.py` (11 tests) never ran against a real database in this session — no reachable TimescaleDB instance here. Run these before merge; they're the only untested code paths in the whole branch.
- **Docker build verification.** `docker-compose.yml` gained a new `api` service; nobody has run `docker compose build`/`up` since the changes.
- **Security review** on `src/api/server.py` before it's reachable off localhost — the API-key gate and rate limiter are both real and tested, but explicitly starter-level (no per-key scoping/rotation, single-process-only rate limiting).
- **`db_models.get_engine()`'s env-var validation gap** — reconcile the clear "which var is missing" error message that was lost when the write path moved to `OhlcvRepository`.
- **Behavior changes to call out explicitly in the PR description:**
  - `handle_missing_data` now checks truthiness instead of `== "True"`
  - back-adjustment is now skippable per asset type via `back_adjustment.applies_to` (used to run unconditionally, including on EQUITY)
  - `batch_downloading.batch` is a real gate again (was silently dead for part of this session, now fixed)
- **CSV-based symbol source (`contracts/contract_valid.csv`) scalability** — hand-maintained, unversioned, doesn't scale. Real design decision (DB-backed registry vs. GraphQL admin mutation vs. provider-driven discovery), deliberately not built this session.
- **Commit strategy** — everything is staged, nothing committed. Decide: one commit, one per phase, or squashed by topic (restructure vs. bugfixes vs. new API vs. review fixes).
- **Optional cleanup:** delete the untracked `app/`/`data/` scaffolding directories (confirmed empty, unused, not part of the repo).
