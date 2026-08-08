"""Merge the two S&P 500 change-event sources into scripts/sp500_changes.csv.

Run from the repo root:
    PYTHONPATH=. python3 scripts/build_sp500_changes.py

INPUTS (committed, small, both required)
    sp500_changes_pit.csv        events derived by diffing consecutive days of a
                                 point-in-time constituent dataset, 1999-12..2025-08
    sp500_changes_wikipedia.csv  Wikipedia's "Selected changes" table, 1976..present

NEITHER SOURCE IS SUFFICIENT ALONE
----------------------------------
The point-in-time set is dense but has holes: it never mentions 85 tickers that were
demonstrably in the index, including YHOO (Yahoo, a member 1999-2017), LEH (Lehman),
FNM and FRE, EK (Kodak), MON (Monsanto), TYC, PCLN, DTV, KFT and SHLD. A universe
built from it alone silently omits those companies from the survivorship correction
-- they are absent from the numerator AND the denominator, so they do not even show
up as documented gaps.

Wikipedia is explicitly a *selected* list and is far too sparse to stand alone: it
records 0 changes for 2001, 2002 and 2004 and only 7 for 2000, against a real rate
of roughly 20-25 per year.

THE MERGE IS DELIBERATELY SURGICAL
----------------------------------
Wikipedia events are added ONLY for tickers the point-in-time source never mentions
at all. Merging both sources wholesale would double up on companies both know about,
where the two disagree on effective dates by a day or two, and the backward walk
would then close an interval at the wrong date or emit spurious warnings.

So: point-in-time wins wherever it has an opinion; Wikipedia fills only the holes.
"""
import csv
import os
from typing import Dict, List, Sequence, Set, Tuple

HERE = os.path.dirname(__file__)
PIT_IN = os.path.join(HERE, "sp500_changes_pit.csv")
WIKI_IN = os.path.join(HERE, "sp500_changes_wikipedia.csv")
CONSTITUENTS = os.path.join(HERE, "sp500_constituents.csv")
OUT = os.path.join(HERE, "sp500_changes.csv")


def tiingo_ticker(sym: str) -> str:
    s = (sym or "").strip().upper().replace(".", "-")
    return s.split("(", 1)[0].strip() if "(" in s else s


def _read(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def tickers_in(events: Sequence[Dict[str, str]]) -> Set[str]:
    out: Set[str] = set()
    for r in events:
        for k in ("add_t", "rm_t"):
            t = tiingo_ticker(r.get(k, ""))
            if t:
                out.add(t)
    return out


def merge_changes(pit: Sequence[Dict[str, str]],
                  wiki: Sequence[Dict[str, str]],
                  current_members: Set[str]) -> Tuple[List[Dict[str, str]], List[str]]:
    """
    Point-in-time events, plus Wikipedia events for tickers PIT never mentions.

    `current_members` is part of "already known": a company that has been in the
    index continuously generates no change events at all, so judging by events alone
    would wrongly treat it as missing (Disney is the obvious case).

    Returns (merged_events, filled_tickers).
    """
    known = tickers_in(pit) | {tiingo_ticker(t) for t in current_members}
    filled: Set[str] = set()
    merged = [dict(r) for r in pit]

    for r in wiki:
        add, rm = tiingo_ticker(r.get("add_t", "")), tiingo_ticker(r.get("rm_t", ""))
        # Keep only the side of the event that concerns an unknown ticker; carrying
        # the known side across would duplicate an event PIT already recorded.
        keep_add = add if add and add not in known else ""
        keep_rm = rm if rm and rm not in known else ""
        if not (keep_add or keep_rm):
            continue
        merged.append({"date": r["date"][:10], "add_t": keep_add, "rm_t": keep_rm})
        filled.update(t for t in (keep_add, keep_rm) if t)

    merged.sort(key=lambda r: (r["date"], r.get("add_t", ""), r.get("rm_t", "")))
    return merged, sorted(filled)


def main() -> None:
    pit = _read(PIT_IN)
    wiki = _read(WIKI_IN)
    current = {tiingo_ticker(r["Symbol"]) for r in _read(CONSTITUENTS)
               if (r.get("Symbol") or "").strip()}

    merged, filled = merge_changes(pit, wiki, current)

    with open(os.path.normpath(OUT), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["date", "add_t", "rm_t"])
        for r in merged:
            w.writerow([r["date"], r.get("add_t", ""), r.get("rm_t", "")])

    print(f"point-in-time events : {len(pit)}")
    print(f"wikipedia events     : {len(wiki)}")
    print(f"merged               : {len(merged)}  -> {OUT}")
    print(f"tickers recovered from Wikipedia that PIT never mentions: {len(filled)}")
    if filled:
        print(f"  {filled}")


if __name__ == "__main__":
    main()
