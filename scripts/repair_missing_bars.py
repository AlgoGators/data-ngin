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
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

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


def returned_days_from_raw(raw: Any) -> Set[date]:
    """
    Days the vendor actually returned, taken from the RAW fetcher output.

    Deliberately NOT derived from cleaned rows. TiingoCleaner drops rows for reasons
    that have nothing to do with vendor absence -- non-positive price, negative or
    non-numeric volume, unparseable timestamp, and (because config_tiingo.yaml sets
    drop_nan: "True") any row containing any NaN in any column. Deriving absence from
    cleaned rows would let a bar the vendor DID return be cached in
    verified_absent_bars as "vendor returned no bar", which permanently and silently
    hides a real gap from detection and leaves a false audit record behind.

    raw["time"] holds Tiingo's ISO-8601 strings (e.g. "2024-01-02T00:00:00.000Z"),
    not parsed timestamps, so it is parsed here rather than assumed.
    """
    if raw is None or getattr(raw, "empty", True):
        return set()
    if "time" not in getattr(raw, "columns", []):
        return set()
    parsed = pd.to_datetime(raw["time"], utc=True, errors="coerce")
    return set(parsed.dropna().dt.date)


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

    Returns a summary dict with keys "candidates", "refilled", "absent",
    "dropped_by_cleaner", "symbols" and "failed".

    "symbols" is the count of symbols with candidate holes, computed up front and
    NOT reduced when a fetch fails, so it stays a measure of scope. "failed" is the
    list of symbols whose fetch, clean or insert raised (vendor outage, network
    error, malformed payload, etc.) -- those symbols contribute 0 to refilled/absent
    for this run and remain unrepaired. "dropped_by_cleaner" counts days the vendor
    DID return but the cleaner discarded: neither refilled nor recorded absent, and
    logged at WARNING because it means real vendor data is being thrown away.
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
                "dropped_by_cleaner": 0, "symbols": len(spans), "failed": []}

    fetcher = get_instance(config, "fetcher", "class")
    cleaner = get_instance(config, "cleaner", "class")
    inserter = get_instance(config, "inserter", "class")

    wanted: Dict[str, set] = {}
    for symbol, day in holes:
        wanted.setdefault(symbol, set()).add(day)

    refilled = absent = dropped_total = 0
    failed: List[str] = []
    inserter.connect()
    try:
        for symbol, (lo, hi) in sorted(spans.items()):
            # Everything per-symbol is inside the try: cleaner.clean raises ValueError
            # on missing fields and insert_data raises RuntimeError, and letting either
            # propagate would abort the loop before the summary and exit-code logic --
            # so one bad symbol early in the alphabet would block every later one.
            try:
                raw = await fetcher.fetch_data(
                    symbol=symbol,
                    loaded_asset_type="EQUITY",
                    start_date=lo.strftime("%Y-%m-%d"),
                    end_date=hi.strftime("%Y-%m-%d"),
                )

                returned_days = returned_days_from_raw(raw)

                # An entirely empty response covering more than one day is a FAILURE,
                # not an absence: a multi-day blackout is indistinguishable from a
                # delisted ticker, a revoked API key or a transient vendor problem, and
                # caching it would permanently hide every real bar in the span. A
                # single-day span returning nothing IS a legitimate absence -- that is
                # the normal case for half of all candidates -- so it falls through.
                if not returned_days and lo != hi:
                    logger.error(
                        "Vendor returned no rows at all for %s across %s..%s (%d day span); "
                        "treating as a FAILURE, not an absence, and caching nothing.",
                        symbol, lo, hi, (hi - lo).days + 1,
                    )
                    failed.append(symbol)
                    continue

                rows = cleaner.clean(raw)
                cleaned_days = {r["time"].date() for r in rows}

                filled = wanted[symbol] & cleaned_days
                # Vendor returned it, cleaner threw it away: not refilled, and NOT
                # recordable as absent.
                dropped = sorted((wanted[symbol] & returned_days) - cleaned_days)
                missing = sorted(wanted[symbol] - returned_days)

                if rows:
                    inserter.insert_data(data=rows, schema=schema, table=table)
                refilled += len(filled)
                logger.info("%s: refilled %d, vendor has no bar for %d",
                            symbol, len(filled), len(missing))

                if dropped:
                    dropped_total += len(dropped)
                    logger.warning(
                        "%s: vendor RETURNED %d bar(s) that the cleaner discarded -- real "
                        "vendor data is being dropped, and these days are neither refilled "
                        "nor recorded absent: %s",
                        symbol, len(dropped), ", ".join(str(d) for d in dropped),
                    )

                record_absent(inserter, schema, symbol, missing, note="vendor returned no bar")
                absent += len(missing)
            except Exception as exc:                      # noqa: BLE001 - report and continue
                logger.error("Repair failed for %s (%s..%s): %s", symbol, lo, hi, exc)
                failed.append(symbol)
                continue
    finally:
        inserter.close()

    if failed:
        logger.warning("%d symbol(s) failed and remain unrepaired: %s",
                        len(failed), ", ".join(failed))
    if dropped_total:
        logger.warning("%d day(s) were returned by the vendor but discarded by the cleaner.",
                       dropped_total)

    return {"candidates": len(holes), "refilled": refilled,
            "absent": absent, "dropped_by_cleaner": dropped_total,
            "symbols": len(spans), "failed": failed}


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
