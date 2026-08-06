"""Repair permanently-missing bars in equities_data.equities.

Run from the repo root (PYTHONPATH=. is required: running the script directly puts
scripts/ on sys.path[0] instead of the repo root, so `utils` and `src` are not
importable without it):
    PYTHONPATH=. python3 scripts/repair_missing_bars.py --dry-run
    PYTHONPATH=. python3 scripts/repair_missing_bars.py

Half of all detected candidates are days the vendor genuinely has no bar for, so
every candidate is verified against Tiingo before being treated as a defect, and
confirmed absences are cached in verified_absent_bars so they are probed once and
never again.
"""
import argparse
import asyncio
import logging
import sys
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


async def repair(config: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """
    Detect candidate holes, verify each against Tiingo, refill the real ones, and
    cache the confirmed absences.

    Returns a summary dict: {"candidates", "refilled", "absent", "symbols", "failed"}.
    "symbols" is the count of symbols with candidate holes, computed up front and
    NOT reduced when a fetch fails, so it stays a measure of scope. "failed" is the
    list of symbols whose fetch raised (vendor outage, network error, etc.) -- those
    symbols contribute 0 to refilled/absent for this run and remain unrepaired.
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
        return {"candidates": len(holes), "refilled": 0, "absent": 0,
                "symbols": len(spans), "failed": []}

    fetcher = get_instance(config, "fetcher", "class")
    cleaner = get_instance(config, "cleaner", "class")
    inserter = get_instance(config, "inserter", "class")

    wanted: Dict[str, set] = {}
    for symbol, day in holes:
        wanted.setdefault(symbol, set()).add(day)

    refilled = absent = 0
    failed: List[str] = []
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
                failed.append(symbol)
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

    if failed:
        logger.warning("%d symbol(s) failed to fetch and remain unrepaired: %s",
                        len(failed), ", ".join(failed))

    return {"candidates": len(holes), "refilled": refilled,
            "absent": absent, "symbols": len(spans), "failed": failed}


def main() -> None:
    from utils.dynamic_loader import load_config

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="src/config/config_tiingo.yaml")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be fetched without calling the vendor.")
    args = parser.parse_args()

    summary = asyncio.run(repair(load_config(args.config), dry_run=args.dry_run))
    logger.info("SUMMARY %s", summary)

    if summary.get("failed"):
        sys.exit(1)


if __name__ == "__main__":
    main()
