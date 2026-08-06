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
