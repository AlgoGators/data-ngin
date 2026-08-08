"""Backfill price history for validated former S&P 500 members.

Run from the repo root:
    PYTHONPATH=. python3 scripts/backfill_sp500_former.py --dry-run
    PYTHONPATH=. python3 scripts/backfill_sp500_former.py --limit 5      # smoke test
    PYTHONPATH=. python3 scripts/backfill_sp500_former.py

PREREQUISITES
    1. scripts/build_sp500_history.py    -> membership + former members
    2. scripts/validate_sp500_entities.py -> contracts/backfill_targets.csv
    3. migrations/003_survivorship_schema.sql applied (this script checks and refuses
       to run without the delisting_date column rather than failing halfway through).

Input is contracts/backfill_targets.csv ONLY -- the validated set. Never the raw
former-members list: Tiingo serves one security per ticker string, so backfilling
an unvalidated ticker can write a modern company's prices under a historical
symbol. See validate_sp500_entities.py for why that matters.

WHY THIS CANNOT RUN THROUGH THE DAG
    determine_date_range (utils/dynamic_loader.py) derives ONE window from the
    GLOBAL max(time) across the table. Since the table already reaches yesterday,
    every newly-added ticker would request a single day and get nothing useful.
    This script sets a date range PER SYMBOL, which is the whole point.

THE EASIEST THING TO GET WRONG
    The fetch window ends at the security's REAL last trading day, not at the date
    it left the index. A company can leave the S&P 500 and keep trading for years
    -- 111 of these still trade today. Ending at the index-removal date would
    manufacture a multi-year hole in every one of them.
"""
import argparse
import asyncio
import csv
import logging
import os
import sys
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("backfill_sp500_former")

HERE = os.path.dirname(__file__)
CONTRACTS = os.path.join(HERE, "..", "contracts")
TARGETS_IN = os.path.join(CONTRACTS, "backfill_targets.csv")
CHECKPOINT = os.path.join(CONTRACTS, "backfill_progress.csv")

# The dataset floor. Everything else in equities starts here.
HISTORY_FLOOR = date(2000, 1, 1)

# A security whose last bar is older than this is treated as delisted, so its
# delisting_date is stamped and it can be excluded from the daily run.
DELISTED_AFTER_DAYS = 30


def fetch_window(tiingo_start: Optional[date], tiingo_end: Optional[date],
                 today: date, floor_: date = HISTORY_FLOOR) -> Tuple[date, date]:
    """
    The date range to request for one symbol.

    Start is the later of the dataset floor and Tiingo's first bar -- asking for
    years before the security existed just wastes response size.

    End is Tiingo's LAST bar, or today when the security still trades. It is
    deliberately NOT the index-removal date: 111 of these companies left the S&P
    500 and are still trading, and ending at their removal date would leave a
    multi-year hole in each.
    """
    start = max(floor_, tiingo_start) if tiingo_start else floor_
    end = tiingo_end if tiingo_end else today
    if end < start:
        # Security's entire life predates the floor; ask for a single day and let
        # the vendor return nothing rather than sending an inverted range.
        end = start
    return start, end


def is_delisted(tiingo_end: Optional[date], today: date,
                threshold_days: int = DELISTED_AFTER_DAYS) -> bool:
    """True if the security has stopped trading, so delisting_date should be stamped."""
    if tiingo_end is None:
        return False
    return (today - tiingo_end).days > threshold_days


def _d(s: str) -> Optional[date]:
    s = (s or "").strip()
    return datetime.fromisoformat(s[:10]).date() if s else None


def _read_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_done(path: str = CHECKPOINT) -> Set[str]:
    """Tickers already completed, so a stalled or killed run resumes instead of refetching."""
    return {r["ticker"] for r in _read_csv(path) if r.get("status") == "DONE"}


def _append_checkpoint(ticker: str, status: str, rows: int, path: str = CHECKPOINT) -> None:
    new = not os.path.exists(path)
    with open(os.path.normpath(path), "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        if new:
            w.writerow(["ticker", "status", "rows", "at"])
        w.writerow([ticker, status, rows, datetime.now().isoformat(timespec="seconds")])


def _require_schema(inserter: Any, schema: str, table: str) -> None:
    """
    Refuse to start unless the survivorship DDL has been applied.

    Failing here costs nothing. Failing after 200 symbols means a partial backfill
    with no delisting_date, which is worse than not starting.
    """
    with inserter.connection.cursor() as cur:
        cur.execute("""SELECT 1 FROM information_schema.columns
                       WHERE table_schema=%s AND table_name=%s AND column_name='delisting_date'""",
                    (schema, table))
        if cur.fetchone() is None:
            raise RuntimeError(
                f"{schema}.{table} has no delisting_date column. Run "
                f"migrations/003_survivorship_schema.sql first, then re-run.")


async def backfill(config: Dict[str, Any], dry_run: bool = False,
                   limit: Optional[int] = None, today: Optional[date] = None) -> Dict[str, Any]:
    """
    Fetch, clean and insert history for every validated target not already done.

    Returns a summary dict. Every symbol is independent: one failure is recorded
    and the run continues, because a single bad symbol must not strand the other
    250-odd.
    """
    from utils.dynamic_loader import get_instance

    today = today or date.today()
    schema = config["database"]["target_schema"]
    table = config["database"]["table"]

    targets = _read_csv(TARGETS_IN)
    if not targets:
        raise RuntimeError(f"No targets in {TARGETS_IN}. Run validate_sp500_entities.py first.")
    done = load_done()
    todo = [t for t in targets if t["dataSymbol"] not in done]
    if limit:
        todo = todo[:limit]

    logger.info("%d validated targets; %d already done; %d to fetch",
                len(targets), len(done), len(todo))

    if dry_run:
        for t in todo[:15]:
            s, e = fetch_window(_d(t["tiingo_start"]), _d(t["tiingo_end"]), today)
            tag = "delisted" if is_delisted(_d(t["tiingo_end"]), today) else "active"
            logger.info("  DRY RUN %-7s %s .. %s  (%s)", t["dataSymbol"], s, e, tag)
        if len(todo) > 15:
            logger.info("  ... and %d more", len(todo) - 15)
        return {"targets": len(targets), "done": len(done), "todo": len(todo),
                "inserted": 0, "failed": []}

    fetcher = get_instance(config, "fetcher", "class")
    cleaner = get_instance(config, "cleaner", "class")
    inserter = get_instance(config, "inserter", "class")

    # Symbols already present in the raw landing table. It has NO primary key, so
    # ON CONFLICT DO NOTHING is inert there and a second insert would silently
    # duplicate every bar. Skipping symbols that already have raw rows is the only
    # thing preventing that.
    raw_table = config["database"].get("raw_table")
    raw_already: Set[str] = set()

    inserted = 0
    failed: List[str] = []
    inserter.connect()
    try:
        _require_schema(inserter, schema, table)
        if raw_table:
            with inserter.connection.cursor() as cur:
                cur.execute(f"SELECT DISTINCT symbol FROM {schema}.{raw_table}")
                raw_already = {r[0] for r in cur.fetchall()}
            logger.info("%s already holds %d symbols; those are skipped for raw",
                        raw_table, len(raw_already))
        for t in todo:
            sym = t["dataSymbol"]
            start, end = fetch_window(_d(t["tiingo_start"]), _d(t["tiingo_end"]), today)
            delisting = _d(t["tiingo_end"]) if is_delisted(_d(t["tiingo_end"]), today) else None
            try:
                raw = await fetcher.fetch_data(
                    symbol=sym, loaded_asset_type="EQUITY",
                    start_date=start.strftime("%Y-%m-%d"), end_date=end.strftime("%Y-%m-%d"))
                rows = cleaner.clean(raw)
                if not rows:
                    # Validation said this security has history in this window, so an
                    # empty result is a failure to investigate -- not a silent skip.
                    logger.error("%s: validated target returned NO rows for %s..%s", sym, start, end)
                    failed.append(sym)
                    _append_checkpoint(sym, "EMPTY", 0)
                    continue
                for r in rows:
                    r["delisting_date"] = delisting

                # Raw first, and only when this symbol has none. The daily
                # orchestrator writes both tables, so a backfill that populates only
                # the clean one leaves the two disagreeing for every backfilled date.
                if raw_table and sym not in raw_already:
                    inserter.insert_data(data=raw.to_dict(orient="records"),
                                         schema=schema, table=raw_table)
                    raw_already.add(sym)

                inserter.insert_data(data=rows, schema=schema, table=table)
                inserted += len(rows)
                _append_checkpoint(sym, "DONE", len(rows))
                logger.info("%-7s %s .. %s  %d rows%s",
                            sym, start, end, len(rows),
                            f"  delisted {delisting}" if delisting else "")
            except Exception as exc:  # noqa: BLE001 - record and keep going
                logger.error("%s: FAILED (%s..%s): %s", sym, start, end, exc)
                failed.append(sym)
                _append_checkpoint(sym, "FAILED", 0)
    finally:
        inserter.close()

    if failed:
        logger.warning("%d symbol(s) failed and remain unbackfilled: %s",
                       len(failed), ", ".join(failed))
    return {"targets": len(targets), "done": len(done), "todo": len(todo),
            "inserted": inserted, "failed": failed}


def main() -> None:
    from dotenv import load_dotenv
    from utils.dynamic_loader import load_config
    load_dotenv()

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default="src/config/config_tiingo.yaml")
    p.add_argument("--dry-run", action="store_true",
                   help="Show the per-symbol windows without calling the vendor.")
    p.add_argument("--limit", type=int, help="Only process the first N pending targets.")
    args = p.parse_args()

    summary = asyncio.run(backfill(load_config(args.config),
                                   dry_run=args.dry_run, limit=args.limit))
    logger.info("SUMMARY %s", summary)
    if summary["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
