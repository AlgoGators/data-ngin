"""Re-sync split/dividend-adjusted prices with the vendor.

Run from the repo root:
    PYTHONPATH=. python3 scripts/refresh_adjustments.py --dry-run
    PYTHONPATH=. python3 scripts/refresh_adjustments.py --limit 50
    PYTHONPATH=. python3 scripts/refresh_adjustments.py

WHY THIS EXISTS
---------------
Adjusted prices are not fixed. Every dividend and split retroactively rewrites the
adjusted series for ALL prior bars, and Tiingo recomputes them on its side. Our
pipeline never revisits a bar once written:

  * the daily run only requests days after MAX(time), so a historical bar is
    fetched exactly once, and
  * the inserter uses ON CONFLICT DO NOTHING, so even a re-fetch is discarded.

The result is that adjusted_close silently rots the moment a corporate action
occurs. Measured on 2026-08-07 against a 21-symbol sample: 29% had every 2025 bar
stale. CRWD was wrong by a factor of exactly 4 -- a 4-for-1 split that our table
never applied, so a total-return backtest crossing that date sees a fabricated
-75% move. Raw OHLCV is unaffected; only the adjustment columns rot.

WHAT IT DOES
------------
Re-fetches each symbol's full history and UPDATEs only the adjustment columns where
they disagree with the vendor. Raw OHLCV is never touched -- it does not change, and
overwriting it would risk turning a vendor hiccup into data loss.

Checkpointed per symbol, so a rate-limit stall resumes instead of restarting, and
--limit keeps a run inside the free tier's 50 requests/key/hour.
"""
import argparse
import asyncio
import csv
import logging
import os
import sys
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("refresh_adjustments")

HERE = os.path.dirname(__file__)
CHECKPOINT = os.path.join(HERE, "..", "contracts", "adjustment_refresh_progress.csv")

# Only these are rewritten by a corporate action. open/high/low/close/volume are
# as-traded and never change, so they are deliberately excluded.
ADJ_COLUMNS = ["adj_open", "adj_high", "adj_low", "adjusted_close",
               "adj_volume", "div_cash", "split_factor"]

# Vendor field -> our column.
VENDOR_MAP = {"adjOpen": "adj_open", "adjHigh": "adj_high", "adjLow": "adj_low",
              "adjClose": "adjusted_close", "adjVolume": "adj_volume",
              "divCash": "div_cash", "splitFactor": "split_factor"}

TOLERANCE = 1e-6


def differing_rows(ours: Dict[str, Dict[str, float]],
                   theirs: Dict[str, Dict[str, float]]) -> List[Tuple[str, Dict[str, float]]]:
    """
    Bars whose adjustment columns disagree with the vendor.

    Compared with a tolerance because these are floats that have been through JSON
    and a numeric column; an exact != would report every bar as changed.

    Only days present on BOTH sides are considered. A day the vendor no longer
    returns is left alone -- that is a coverage question, not an adjustment one, and
    silently deleting our copy would be the wrong response to a vendor blip.
    """
    out = []
    for day, mine in ours.items():
        vendor = theirs.get(day)
        if not vendor:
            continue
        changed = {c: vendor[c] for c in ADJ_COLUMNS
                   if c in vendor and vendor[c] is not None and mine.get(c) is not None
                   and abs(float(mine[c]) - float(vendor[c])) > TOLERANCE}
        if changed:
            out.append((day, changed))
    return out


def load_done(path: str = CHECKPOINT) -> Set[str]:
    if not os.path.exists(path):
        return set()
    with open(path, newline="", encoding="utf-8") as f:
        return {r["symbol"] for r in csv.DictReader(f) if r.get("status") == "DONE"}


def _checkpoint(symbol: str, status: str, updated: int, path: str = CHECKPOINT) -> None:
    new = not os.path.exists(path)
    with open(os.path.normpath(path), "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        if new:
            w.writerow(["symbol", "status", "rows_updated", "at"])
        w.writerow([symbol, status, updated, datetime.now().isoformat(timespec="seconds")])


def _vendor_frame_to_map(raw: Any) -> Dict[str, Dict[str, float]]:
    """Vendor response -> {YYYY-MM-DD: {our_column: value}}."""
    if raw is None or getattr(raw, "empty", True):
        return {}
    out: Dict[str, Dict[str, float]] = {}
    times = pd.to_datetime(raw["time"], utc=True, errors="coerce")
    for i, ts in enumerate(times):
        if pd.isna(ts):
            continue
        row = {}
        for col in ADJ_COLUMNS:
            if col in raw.columns:
                v = raw.iloc[i][col]
                row[col] = None if pd.isna(v) else float(v)
        out[ts.date().isoformat()] = row
    return out


async def refresh(config: Dict[str, Any], dry_run: bool = False,
                  limit: Optional[int] = None, symbols: Optional[Sequence[str]] = None
                  ) -> Dict[str, Any]:
    from utils.dynamic_loader import get_instance
    from src.modules.db_models import get_engine
    import psycopg2.extras

    schema = config["database"]["target_schema"]
    table = config["database"]["table"]

    fetcher = get_instance(config, "fetcher", "class")
    inserter = get_instance(config, "inserter", "class")
    inserter.connect()
    conn = inserter.connection

    try:
        with conn.cursor() as cur:
            if symbols:
                todo_all = list(symbols)
            else:
                cur.execute(f'SELECT DISTINCT symbol FROM {schema}.{table} ORDER BY symbol')
                todo_all = [r[0] for r in cur.fetchall()]

        done = load_done(CHECKPOINT)
        todo = [s for s in todo_all if s not in done]

        # Rolling sweep. A full pass over 852 symbols is one vendor request each,
        # which exceeds an hour of free-tier key budget, so a scheduled run takes a
        # --limit slice and the checkpoint carries progress between runs. Once every
        # symbol has been visited the checkpoint is cleared and the next run starts a
        # fresh pass -- otherwise the job would go permanently quiet after one sweep
        # while adjustments kept rotting.
        if not todo and todo_all:
            logger.info("full pass complete over %d symbols; starting a new sweep", len(todo_all))
            if os.path.exists(CHECKPOINT):
                os.remove(CHECKPOINT)
            done = set()
            todo = list(todo_all)

        if limit:
            todo = todo[:limit]
        logger.info("%d symbols in table; %d already refreshed; %d this run",
                    len(todo_all), len(done), len(todo))
        if dry_run:
            logger.info("DRY RUN: would re-fetch %s%s", todo[:10], " ..." if len(todo) > 10 else "")
            return {"symbols": len(todo_all), "checked": 0, "updated_rows": 0,
                    "updated_symbols": 0, "failed": []}

        checked = updated_rows = updated_symbols = 0
        failed: List[str] = []

        for sym in todo:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f'SELECT to_char("time",\'YYYY-MM-DD\'), '
                        f'{", ".join(ADJ_COLUMNS)} FROM {schema}.{table} WHERE symbol = %s', (sym,))
                    rows = cur.fetchall()
                if not rows:
                    _checkpoint(sym, "DONE", 0, CHECKPOINT)
                    continue
                ours = {r[0]: dict(zip(ADJ_COLUMNS, r[1:])) for r in rows}
                lo, hi = min(ours), max(ours)

                raw = await fetcher.fetch_data(symbol=sym, loaded_asset_type="EQUITY",
                                               start_date=lo, end_date=hi)
                theirs = _vendor_frame_to_map(raw)
                if not theirs:
                    logger.warning("%s: vendor returned nothing for %s..%s; leaving as-is", sym, lo, hi)
                    _checkpoint(sym, "EMPTY", 0, CHECKPOINT)
                    continue

                diffs = differing_rows(ours, theirs)
                checked += 1
                if not diffs:
                    _checkpoint(sym, "DONE", 0, CHECKPOINT)
                    continue

                payload = [(sym, d, *[vals.get(c) for c in ADJ_COLUMNS]) for d, vals in diffs]
                # Cast explicitly: a VALUES list carrying NULLs is inferred as text,
                # and COALESCE(text, double precision) will not type-check.
                sets = ", ".join(
                    f"{c} = COALESCE(v.{c}::double precision, t.{c})" for c in ADJ_COLUMNS)
                cols = ", ".join(ADJ_COLUMNS)
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        # Compare the timestamp directly, NOT via to_char(t."time",...).
                        # Wrapping the column in a function makes the (symbol, time)
                        # primary key unusable, so every update degenerated into a
                        # scan of a 4.5M-row table -- ~35s per symbol.
                        f'UPDATE {schema}.{table} t SET {sets} '
                        f'FROM (VALUES %s) AS v(symbol, d, {cols}) '
                        f'WHERE t.symbol = v.symbol '
                        f'  AND t."time" = (v.d || \' 00:00:00+00\')::timestamptz',
                        payload, page_size=1000)
                updated_rows += len(diffs)
                updated_symbols += 1
                _checkpoint(sym, "DONE", len(diffs), CHECKPOINT)
                logger.info("%-7s %d bar(s) re-adjusted", sym, len(diffs))
            except Exception as exc:  # noqa: BLE001
                logger.error("%s: FAILED: %s", sym, exc)
                failed.append(sym)
                _checkpoint(sym, "FAILED", 0, CHECKPOINT)
    finally:
        inserter.close()

    if failed:
        logger.warning("%d symbol(s) failed: %s", len(failed), ", ".join(failed))
    return {"symbols": len(todo_all), "checked": checked, "updated_rows": updated_rows,
            "updated_symbols": updated_symbols, "failed": failed}


def main() -> None:
    from dotenv import load_dotenv
    from utils.dynamic_loader import load_config
    load_dotenv()

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default="src/config/config_tiingo.yaml")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--limit", type=int,
                   help="Process at most N pending symbols. One vendor request each, so "
                        "keep runs inside 50 requests per key per hour.")
    p.add_argument("--symbols", help="Comma-separated symbols instead of the whole table.")
    args = p.parse_args()

    syms = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols else None
    summary = asyncio.run(refresh(load_config(args.config), dry_run=args.dry_run,
                                  limit=args.limit, symbols=syms))
    logger.info("SUMMARY %s", summary)
    if summary["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
