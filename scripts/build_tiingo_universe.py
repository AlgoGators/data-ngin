"""Generator for contracts/contract_tiingo.csv.

Run from the repo root:
    PYTHONPATH=. python3 scripts/build_tiingo_universe.py

The universe is the UNION of:
    1. current S&P 500 constituents
    2. FORMER S&P 500 members that entity validation cleared for backfill
    3. the 44 ETFs matching the futures book
    4. curated liquid non-S&P names

Point 2 is the one that matters, and it is why this file was rewritten.

    The previous version emitted only the CURRENT constituents. That is the
    mechanism by which survivorship bias regenerates: every time the S&P drops a
    company, the next run of this script silently removes it from the contract,
    the daily pipeline stops fetching it, and the dataset quietly becomes a
    survivors-only sample again. Backfilling history without fixing this would
    have re-accumulated the bias from the day it finished.

Each row carries an `active` flag. Delisted securities stay in the contract --
their history is still wanted -- but the daily run skips them, because a delisted
ticker returns nothing from the vendor forever. EA is the worked example: acquired
2026-08-04, still fetched every day, contributing nothing.

INPUTS (committed under scripts/)
    sp500_constituents.csv     current members
    sp500_changes.csv          index events, used to correct the snapshot's lag
    tiingo_ticker_status.csv   per-ticker first/last trading day + manifest as-of

INPUT (committed under contracts/, produced by validate_sp500_entities.py)
    backfill_targets.csv       former members Tiingo can actually serve

Refresh the sources:
    curl -sS https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv \
        -o scripts/sp500_constituents.csv
    # sp500_changes.csv and tiingo_ticker_status.csv: see build_sp500_history.py

Structural guarantees are enforced by tests/test_contract_tiingo_universe.py.
"""
import csv
import os
from typing import Dict, List, Optional, Sequence, Set, Tuple

HERE = os.path.dirname(__file__)
CONTRACTS = os.path.join(HERE, "..", "contracts")
OUT = os.path.join(CONTRACTS, "contract_tiingo.csv")
SP500_SNAPSHOT = os.path.join(HERE, "sp500_constituents.csv")
CHANGES = os.path.join(HERE, "sp500_changes.csv")
STATUS = os.path.join(HERE, "tiingo_ticker_status.csv")
BACKFILL_TARGETS = os.path.join(CONTRACTS, "backfill_targets.csv")

ETFS = [
    "SPY", "QQQ", "IWM", "DIA", "SHY", "IEI", "IEF", "TLT", "USO", "UNG", "UGA",
    "XLE", "GLD", "SLV", "CPER", "PPLT", "CORN", "WEAT", "SOYB", "DBA", "UUP",
    "FXE", "FXB", "FXY", "FXC", "FXF", "FXA", "IBIT", "VOO", "VTI", "XLF", "XLK",
    "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC", "EFA", "EEM", "AGG", "LQD",
]

# Curated very-liquid names that are NOT ETFs and may sit outside the S&P 500
# (large ADRs / high-volume US listings). Overlaps are deduped below.
CURATED_EXTRAS = [
    "BABA", "PDD", "NIO", "TSM", "JD", "BIDU", "LI", "XPEV", "NU", "SE", "SHOP",
    "RIVN", "LCID", "MARA", "RIOT", "GME", "AMC", "AFRM", "UPST", "RBLX", "SNAP",
    "CVNA", "IONQ", "HOOD", "SOFI",
]

# Historical ticker -> the symbol Tiingo now serves that security under. Applied so
# the contract carries the canonical ticker rather than a stale one that will stop
# resolving. SATS is the live case: EchoStar renamed to ECHO, Tiingo's metadata for
# SATS already 404s, and only the prices endpoint still answers to the old string.
TICKER_RENAMES: Dict[str, str] = {
    "SATS": "ECHO",
}


def tiingo_ticker(sym: str) -> str:
    """Normalise to Tiingo's convention: BRK.B -> BRK-B, and drop any annotation."""
    s = sym.strip().upper().replace(".", "-")
    if "(" in s:
        s = s.split("(", 1)[0].strip()
    return s


def _read(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_status(path: str = STATUS) -> Tuple[Dict[str, str], Optional[str]]:
    """
    Per-ticker last trading day, plus the manifest's as-of date.

    Returns ({ticker: last_trading_day}, as_of).
    """
    rows = _read(path)
    if not rows:
        return {}, None
    as_of = rows[0].get("manifest_as_of") or None
    return {r["ticker"]: r.get("last_trading_day", "") for r in rows}, as_of


def is_active(ticker: str, status: Dict[str, str], as_of: Optional[str]) -> bool:
    """
    Is this security still trading?

    A ticker is active exactly when its last trading day is the manifest's as-of
    day. Anything older means the vendor stopped receiving bars for it -- that is
    what a delisting looks like in this data.

    Unknown tickers default to ACTIVE. Failing open is deliberate: an unnecessary
    fetch costs one request, while wrongly marking a live symbol inactive removes
    it from the daily run silently, which is the failure mode this whole project
    exists to eliminate.
    """
    if not as_of:
        return True
    last = status.get(ticker)
    if not last:
        return True
    return last >= as_of


def current_members(snapshot_path: str = SP500_SNAPSHOT,
                    changes_path: str = CHANGES) -> Set[str]:
    """
    Today's index members, with the snapshot's lag corrected from the changes table.

    The constituent snapshot trails the index -- on 2026-08-06 it was still missing
    FERG, added 2026-08-05. Additions recorded on or after the snapshot's newest
    known event are folded back in, and same-day removals taken back out, so a stale
    download cannot silently drop a live member from the daily run.
    """
    members = {tiingo_ticker(r["Symbol"]) for r in _read(snapshot_path)
               if (r.get("Symbol") or "").strip()}
    changes = _read(changes_path)
    if not changes:
        return members
    latest = max((c["date"] for c in changes if c.get("date")), default=None)
    if not latest:
        return members
    for c in changes:
        if c.get("date") != latest:
            continue
        if c.get("add_t"):
            members.add(tiingo_ticker(c["add_t"]))
        if c.get("rm_t"):
            members.discard(tiingo_ticker(c["rm_t"]))
    return members


def build_universe(current: Set[str], former: Sequence[str],
                   status: Dict[str, str], as_of: Optional[str],
                   activate_former: bool = False) -> List[Tuple[str, str, str]]:
    """
    Assemble (dataSymbol, instrumentType, active), deduped, in a stable order.

    Order is ETFs, then current members, then validated former members, then curated
    extras -- ETFs first preserves the historically deployed prefix so a diff of this
    file stays readable.

    activate_former gates whether STILL-TRADING former members join the daily run.
    It defaults to False because they are a capacity decision, not a data decision:
    adding all 260 pushes the daily run to ~697 requests, which needs 14 working
    keys and there are 13. Exceeding the ceiling does not fail loudly -- the tail of
    the run just silently returns nothing, which is how symbols go missing.

    Either way every former member is WRITTEN to the contract, so its history is
    part of the dataset and the backfill can target it. The flag only controls
    whether the daily pipeline also fetches it. Flip it once the keys are in.
    """
    ordered: List[str] = []
    seen: Set[str] = set()
    for group in (ETFS, sorted(current), list(former), CURATED_EXTRAS):
        for raw in group:
            t = TICKER_RENAMES.get(tiingo_ticker(raw), tiingo_ticker(raw))
            if t and t not in seen:
                seen.add(t)
                ordered.append(t)
    former_set = {TICKER_RENAMES.get(tiingo_ticker(f), tiingo_ticker(f)) for f in former}
    core = set(ETFS) | current | set(CURATED_EXTRAS)
    rows: List[Tuple[str, str, str]] = []
    for t in ordered:
        active = is_active(t, status, as_of)
        # A former member that is not part of the core universe stays out of the
        # daily run until capacity exists, even though it is still trading.
        if active and not activate_former and t in former_set and t not in core:
            active = False
        rows.append((t, "EQUITY", "true" if active else "false"))
    return rows


def main() -> None:
    import argparse
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--activate-former", action="store_true",
                   help="Also fetch still-trading former members daily. Needs ~14 "
                        "working Tiingo keys; only enable once they are in place.")
    args = p.parse_args()

    status, as_of = load_status()
    current = current_members()
    former = [r["dataSymbol"] for r in _read(BACKFILL_TARGETS)]

    rows = build_universe(current, former, status, as_of,
                          activate_former=args.activate_former)
    with open(os.path.normpath(OUT), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["dataSymbol", "instrumentType", "active"])
        w.writerows(rows)

    active = sum(1 for _, _, a in rows if a == "true")
    print(f"Wrote {len(rows)} symbols to {OUT}")
    print(f"  current S&P members      : {len(current)}")
    print(f"  validated former members : {len(former)}")
    print(f"  ETFs / curated           : {len(ETFS)} / {len(CURATED_EXTRAS)}")
    print(f"  ACTIVE (fetched daily)   : {active}")
    print(f"  inactive (history only)  : {len(rows) - active}   [manifest as-of {as_of}]")
    if TICKER_RENAMES:
        print(f"  renames applied          : {TICKER_RENAMES}")
    if not args.activate_former:
        held = sum(1 for t, _, a in rows if a == "false" and is_active(t, status, as_of))
        print(f"  still-trading former members held OUT of the daily run: {held}")
        print(f"    (capacity, not data -- rerun with --activate-former once keys allow)")


if __name__ == "__main__":
    main()
