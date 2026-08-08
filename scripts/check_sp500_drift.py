"""Detect drift between the committed S&P 500 snapshots and the live index.

Run from the repo root:
    PYTHONPATH=. python3 scripts/check_sp500_drift.py

WHY THIS EXISTS
---------------
build_tiingo_universe.py and build_sp500_history.py read COMMITTED snapshots, which
is what makes them reproducible. The cost is that the snapshots go stale, and stale
snapshots are exactly how survivorship bias creeps back:

  * a company leaves the index -> it stops being a current member, and unless the
    committed changes file records the removal it never becomes a former member
    either. It simply vanishes from the universe.
  * a company joins -> it is never fetched at all. That is how MRVL, FLEX, HONA and
    FERG went missing while sitting in the index.
  * a company delists -> tiingo_ticker_status.csv still calls it active, so the
    daily run keeps spending a request on a security that returns nothing (EA).

This script compares the committed snapshots against live sources and reports what
has changed. It deliberately does NOT rewrite them: the snapshots are committed
artifacts, and silently mutating them during a scheduled run would make the
universe depend on when a DAG last fired rather than on what is in git.

Exit code 1 when drift is found, so the DAG turns red and a human refreshes.
"""
import csv
import io
import os
import sys
import zipfile
from collections import Counter
from typing import Dict, List, Optional, Sequence, Set, Tuple

import requests

HERE = os.path.dirname(__file__)
CONTRACTS = os.path.join(HERE, "..", "contracts")
SP500_SNAPSHOT = os.path.join(HERE, "sp500_constituents.csv")
STATUS = os.path.join(HERE, "tiingo_ticker_status.csv")
CONTRACT = os.path.join(CONTRACTS, "contract_tiingo.csv")

CONSTITUENTS_URL = ("https://raw.githubusercontent.com/datasets/"
                    "s-and-p-500-companies/main/data/constituents.csv")
MANIFEST_URL = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
MAJOR_EXCHANGES = {"NASDAQ", "NYSE", "NYSE ARCA", "AMEX", "NYSE MKT", "BATS", "NYSE NAT"}
_TIMEOUT = 60


def tiingo_ticker(sym: str) -> str:
    s = (sym or "").strip().upper().replace(".", "-")
    return s.split("(", 1)[0].strip() if "(" in s else s


def _read(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def index_drift(committed: Set[str], live: Set[str]) -> Tuple[List[str], List[str]]:
    """
    (joined, left) -- tickers the live index has that the snapshot lacks, and vice versa.

    `joined` is the urgent half: those are companies in the index right now that the
    daily run is not fetching at all.
    """
    return sorted(live - committed), sorted(committed - live)


def delisted_but_active(contract_rows: Sequence[Dict[str, str]],
                        live_last_day: Dict[str, str],
                        as_of: Optional[str]) -> List[str]:
    """
    Symbols the contract still fetches daily that the vendor has stopped updating.

    Each one costs a request every run and returns nothing. Unknown tickers are
    ignored rather than reported, matching the fail-open rule everywhere else: a
    missing manifest entry must never be read as 'delisted'.
    """
    if not as_of:
        return []
    out = []
    for r in contract_rows:
        if r.get("active") != "true":
            continue
        last = live_last_day.get(r["dataSymbol"])
        if last and last < as_of:
            out.append(r["dataSymbol"])
    return sorted(out)


def fetch_live_constituents(url: str = CONSTITUENTS_URL) -> Set[str]:
    resp = requests.get(url, timeout=_TIMEOUT)
    resp.raise_for_status()
    rows = csv.DictReader(io.StringIO(resp.text))
    return {tiingo_ticker(r["Symbol"]) for r in rows if (r.get("Symbol") or "").strip()}


def fetch_live_status() -> Tuple[Dict[str, str], Optional[str]]:
    """
    Per-ticker last trading day from Tiingo's free manifest, plus its as-of date.

    The as-of date is the modal last-trading-day across live US listings -- the
    manifest carries no explicit timestamp, and hardcoding "yesterday" breaks over
    weekends and holidays.
    """
    resp = requests.get(MANIFEST_URL, timeout=_TIMEOUT)
    resp.raise_for_status()
    last: Dict[str, str] = {}
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        with z.open(z.namelist()[0]) as f:
            for r in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")):
                if r.get("priceCurrency") != "USD" or r.get("exchange") not in MAJOR_EXCHANGES:
                    continue
                t = (r.get("ticker") or "").strip().upper()
                end = r.get("endDate") or ""
                if t and end > last.get(t, ""):
                    last[t] = end
    as_of = Counter(last.values()).most_common(1)[0][0] if last else None
    return last, as_of


def main() -> None:
    committed = {tiingo_ticker(r["Symbol"]) for r in _read(SP500_SNAPSHOT)
                 if (r.get("Symbol") or "").strip()}
    contract_rows = _read(CONTRACT)

    live = fetch_live_constituents()
    live_last_day, as_of = fetch_live_status()

    joined, left = index_drift(committed, live)
    stale_active = delisted_but_active(contract_rows, live_last_day, as_of)

    print(f"committed snapshot : {len(committed)} members")
    print(f"live index         : {len(live)} members")
    print(f"manifest as-of     : {as_of}")

    problems = 0
    if joined:
        problems += len(joined)
        print(f"\nJOINED the index but NOT in the committed snapshot ({len(joined)}):")
        print(f"  {joined}")
        print("  -> these are NOT being fetched. This is how MRVL/FLEX/HONA/FERG were missed.")
    if left:
        problems += len(left)
        print(f"\nLEFT the index but still in the committed snapshot ({len(left)}):")
        print(f"  {left}")
        print("  -> they must become FORMER members, or they vanish from the universe entirely.")
    if stale_active:
        problems += len(stale_active)
        print(f"\nMarked active but the vendor stopped updating them ({len(stale_active)}):")
        print(f"  {stale_active}")
        print("  -> each costs a request every run and returns nothing (this is the EA case).")

    if not problems:
        print("\nNo drift. Committed snapshots match the live index.")
        return

    print("\nTo resolve, from the repo root:")
    print("  curl -sS https://raw.githubusercontent.com/datasets/s-and-p-500-companies"
          "/main/data/constituents.csv -o scripts/sp500_constituents.csv")
    print("  PYTHONPATH=. python3 scripts/build_sp500_history.py")
    print("  PYTHONPATH=. python3 scripts/validate_sp500_entities.py")
    print("  PYTHONPATH=. python3 scripts/build_tiingo_universe.py")
    print("  # then backfill any newly-former members and commit the regenerated files")
    sys.exit(1)


if __name__ == "__main__":
    main()
