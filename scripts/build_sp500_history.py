"""Reconstruct point-in-time S&P 500 membership from committed snapshots.

Run from the repo root:
    PYTHONPATH=. python3 scripts/build_sp500_history.py

Reads two committed snapshots and writes three CSVs. No network, no database,
no API calls -- re-running always produces the same output from the same inputs,
which is the same reproducibility guarantee build_tiingo_universe.py gives.

INPUTS (committed under scripts/)
    sp500_changes.csv       index add/remove events -- THE AUTHORITATIVE SOURCE
    sp500_constituents.csv  today's members -- CROSS-CHECK / walk-back anchor

The changes file is built from TWO sources by scripts/refresh_sp500_sources.py,
and it has to be, because neither one alone is sufficient:

  * 1999-12 .. 2025-08  derived from daily point-in-time constituent snapshots
    by diffing consecutive days. Complete, because every change shows up as a
    difference between two days.
  * 2025-08 .. present  from Wikipedia's "Selected changes" table, which is the
    only source current to within a day.

Wikipedia alone is NOT enough: its table is explicitly "selected" and records 0
changes for 2001, 2002 and 2004, and only 7 for 2000, against a real rate of
~20-25 per year. Building the universe from it alone undercounts former members
by roughly 170 -- 355 instead of 532.

The constituent snapshot is used only to anchor the backward walk, and it lags:
on 2026-08-06 it was still missing FERG (joined 2026-08-05) and it carries FISV
rather than FI. main() reconciles that lag against the changes table before
walking, so a stale anchor cannot corrupt the reconstruction.

OUTPUTS (under contracts/)
    sp500_membership.csv     ticker, start_date, end_date  (one row per interval)
    sp500_former_members.csv tickers that were members at some point but are not now
    sp500_membership_warnings.csv  source inconsistencies, for eyeballing
"""
import csv
import os
import re
from datetime import date, datetime
from typing import Dict, List, Optional, Sequence, Set, Tuple

HERE = os.path.dirname(__file__)
CHANGES_IN = os.path.join(HERE, "sp500_changes.csv")
CONSTITUENTS_IN = os.path.join(HERE, "sp500_constituents.csv")
OUT_DIR = os.path.join(HERE, "..", "contracts")

# A real US listing: 1-7 chars, letters/digits, dash for share classes (BRK-B).
TICKER_RE = re.compile(r"^[A-Z][A-Z0-9\-]{0,6}$")


def tiingo_ticker(sym: str) -> str:
    """
    Normalise an index-list ticker to Tiingo's convention.

    Tiingo writes share classes with a dash where index lists use a dot
    (BRK.B -> BRK-B).

    The upstream point-in-time data also carries the occasional editorial
    annotation glued onto the ticker -- 'RVTY (PREVIOUSLY PKI)' is a real value
    in the source. Left alone it becomes a phantom former member that can never
    be fetched, so the parenthetical is stripped here rather than being allowed
    to propagate into the universe.
    """
    s = sym.strip().upper().replace(".", "-")
    if "(" in s:
        s = s.split("(", 1)[0].strip()
    return s


def is_valid_ticker(sym: str) -> bool:
    """True if `sym` looks like a tradable US ticker. Used to quarantine source junk."""
    return bool(TICKER_RE.fullmatch(sym))


def build_membership(
    current_members: Set[str],
    changes: Sequence[Tuple[date, Optional[str], Optional[str]]],
) -> Tuple[List[Dict[str, Optional[str]]], List[str]]:
    """
    Reconstruct membership intervals by replaying index changes BACKWARD.

    Walking backward from a known-good present is what makes this reliable: the
    current constituent list is a fact, and each change tells us what the set
    looked like immediately before it. Walking forward would instead require a
    trustworthy starting set from decades ago, which we do not have.

    For each event, moving from newest to oldest:
      - a ticker ADDED on D was NOT a member before D  -> its open interval starts at D
      - a ticker REMOVED on D WAS a member before D    -> open a new interval ending at D

    Args:
        current_members: tickers in the index today.
        changes: (effective_date, added_ticker, removed_ticker), any order.
            Either ticker may be None -- spin-offs and market-cap deletions are
            recorded one-sided.

    Returns:
        (intervals, warnings). Each interval is a dict with ticker/start_date/
        end_date as ISO strings; start_date None means "a member from before the
        changes history begins", end_date None means "still a member".
        Warnings name source inconsistencies rather than hiding them.
    """
    warnings: List[str] = []
    intervals: List[Dict[str, Optional[str]]] = []

    # ticker -> end of the interval currently open for it as we walk back.
    # None means the interval is still open today (a current member).
    open_end: Dict[str, Optional[date]] = {t: None for t in current_members}

    for eff, added, removed in sorted(changes, key=lambda c: c[0], reverse=True):
        if added:
            if added in open_end:
                intervals.append({
                    "ticker": added,
                    "start_date": eff.isoformat(),
                    "end_date": open_end[added].isoformat() if open_end[added] else None,
                })
                del open_end[added]
            else:
                # Added, but not a member at any later point we know of. Usually a
                # source error, or an add/remove pair we only have half of.
                warnings.append(
                    f"{eff.isoformat()}: '{added}' added but was not an open member "
                    f"at that point -- interval not recorded"
                )
        if removed:
            if removed in open_end:
                warnings.append(
                    f"{eff.isoformat()}: '{removed}' removed while an interval was "
                    f"already open (ending {open_end[removed]}) -- keeping the later one"
                )
            else:
                open_end[removed] = eff

    # Anything still open was a member from before the changes history begins.
    for ticker, end in open_end.items():
        intervals.append({
            "ticker": ticker,
            "start_date": None,
            "end_date": end.isoformat() if end else None,
        })

    intervals.sort(key=lambda r: (r["ticker"], r["start_date"] or ""))
    return intervals, warnings


def former_members(intervals: Sequence[Dict[str, Optional[str]]]) -> List[str]:
    """
    Tickers that were members at some point but are not members now.

    A ticker counts as current if ANY of its intervals is still open, which
    correctly keeps a company that left and later rejoined out of this list.
    """
    ever = {r["ticker"] for r in intervals}
    current = {r["ticker"] for r in intervals if r["end_date"] is None}
    return sorted(ever - current)


def _clean(raw: str, where: str, rejects: List[str]) -> Optional[str]:
    """Normalise one ticker, quarantining anything that isn't a plausible listing."""
    if not raw or not raw.strip():
        return None
    t = tiingo_ticker(raw)
    if not t:
        return None
    if not is_valid_ticker(t):
        rejects.append(f"{where}: rejected {raw.strip()!r} (normalised to {t!r})")
        return None
    return t


def _read_changes(path: str, rejects: List[str]) -> List[Tuple[date, Optional[str], Optional[str]]]:
    out: List[Tuple[date, Optional[str], Optional[str]]] = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            raw = (row.get("date") or "").strip()
            if not raw:
                continue
            eff = datetime.fromisoformat(raw[:10]).date()
            add = _clean(row.get("add_t") or "", f"changes {raw[:10]}", rejects)
            rem = _clean(row.get("rm_t") or "", f"changes {raw[:10]}", rejects)
            if add or rem:
                out.append((eff, add, rem))
    return out


def _read_constituents(path: str, rejects: List[str]) -> Set[str]:
    out: Set[str] = set()
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            t = _clean(r.get("Symbol") or "", "constituents", rejects)
            if t:
                out.add(t)
    return out


def _write_csv(path: str, header: Sequence[str], rows: Sequence[Sequence]) -> None:
    with open(os.path.normpath(path), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def main() -> None:
    rejects: List[str] = []
    changes = _read_changes(CHANGES_IN, rejects)
    current = _read_constituents(CONSTITUENTS_IN, rejects)

    # The constituent snapshot lags, so reconcile it against the changes table
    # before using it as the walk-back starting point. Without this the whole
    # reconstruction is anchored on a stale set.
    latest = max(c[0] for c in changes)
    added_recently = {a for d, a, _ in changes if a and d > date(2020, 1, 1)}
    stale = sorted(t for t in added_recently - current
                   if any(d == latest and a == t for d, a, _ in changes))
    if stale:
        print(f"NOTE: constituent snapshot is behind the changes table; "
              f"adding {stale} from the {latest} event(s)")
        current |= set(stale)

    intervals, warnings = build_membership(current, changes)
    former = former_members(intervals)

    _write_csv(os.path.join(OUT_DIR, "sp500_membership.csv"),
               ["ticker", "start_date", "end_date"],
               [[r["ticker"], r["start_date"] or "", r["end_date"] or ""] for r in intervals])
    _write_csv(os.path.join(OUT_DIR, "sp500_former_members.csv"),
               ["dataSymbol"], [[t] for t in former])
    _write_csv(os.path.join(OUT_DIR, "sp500_membership_warnings.csv"),
               ["warning"], [[w] for w in warnings + rejects])

    print(f"changes read            : {len(changes)}")
    print(f"current members         : {len(current)}")
    print(f"membership intervals    : {len(intervals)}")
    print(f"former members          : {len(former)}")
    print(f"rejected junk tickers   : {len(rejects)}")
    print(f"source warnings         : {len(warnings)}  (see sp500_membership_warnings.csv)")
    for w in warnings:
        print(f"    {w}")


if __name__ == "__main__":
    main()
