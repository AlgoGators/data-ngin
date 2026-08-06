"""Decide which former S&P 500 members can actually be backfilled from Tiingo.

Run from the repo root:
    PYTHONPATH=. python3 scripts/validate_sp500_entities.py --dry-run
    PYTHONPATH=. python3 scripts/validate_sp500_entities.py

WHY THIS EXISTS
---------------
Tiingo serves exactly ONE security per ticker string -- the current one. When a
ticker has been reused, the API happily returns the modern company:

    CA   -> "XTRACKERS CALIFORNIA MUNICIPAL BOND ETF" (2023-)  NOT Computer Associates
    STI  -> "Solidion Technology Inc"                 (2024-)  NOT SunTrust Banks
    NFX  -> "Corgi NFLX 2x Daily ETF"                 (2026-)  NOT Newfield Exploration

Backfilling without checking would write a 2023 municipal bond ETF's prices under
a symbol the membership table says was an S&P 500 member in 2005. Fabricated
history is strictly worse than the survivorship bias this project removes, so
this step is mandatory, not advisory.

TWO RULES, BOTH REQUIRED
------------------------
1. Tiingo's coverage begins after the company left the index -> it cannot be that
   company's index-era history.
2. Tiingo's own manifest lists the ticker more than once -> the string has been
   reused, and rule 1 silently passes the wrong security whenever the reuser's
   data happens to start BEFORE the original left.

Rule 2 was found by auditing rule 1's output: CSR and EP both overlap their
membership window numerically but are Centerspace and Empire Petroleum, not the
S&P members that held those tickers. Running rule 1 alone is not safe.

API COST
--------
Results are cached in contracts/tiingo_coverage_probe.csv and only uncached
tickers are probed, so re-running is nearly free. Requests are spread round-robin
across every TIINGO_API_KEY* so no single key approaches its 50/hour free-tier
cap. The lightweight metadata endpoint is used, not the prices endpoint.
"""
import argparse
import asyncio
import csv
import io
import logging
import os
import zipfile
from datetime import date, datetime
from typing import Dict, List, Optional, Sequence, Tuple

import aiohttp

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("validate_sp500_entities")

HERE = os.path.dirname(__file__)
CONTRACTS = os.path.join(HERE, "..", "contracts")
FORMER_IN = os.path.join(CONTRACTS, "sp500_former_members.csv")
MEMBERSHIP_IN = os.path.join(CONTRACTS, "sp500_membership.csv")
PROBE_CACHE = os.path.join(CONTRACTS, "tiingo_coverage_probe.csv")
# Human decisions on cases the rules can't settle. Committed so the judgement is
# auditable and the run stays reproducible instead of depending on who ran it.
OVERRIDES_IN = os.path.join(CONTRACTS, "entity_overrides.csv")
TARGETS_OUT = os.path.join(CONTRACTS, "backfill_targets.csv")
GAPS_OUT = os.path.join(CONTRACTS, "coverage_gaps.csv")

MANIFEST_URL = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
META_URL = "https://api.tiingo.com/tiingo/daily/{ticker}"

# Below this much overlap between Tiingo's coverage and the index-membership
# window, a human should look before the data is trusted.
MIN_OVERLAP_DAYS = 30

OK = "OK"
MARGINAL = "MARGINAL"
NO_COVERAGE = "NO_COVERAGE"
REUSED_TICKER = "REUSED_TICKER"
NO_PRICE_DATA = "NO_PRICE_DATA"
NOT_FOUND = "NOT_FOUND"


def classify(
    membership_start: Optional[date],
    membership_end: Optional[date],
    tiingo_start: Optional[date],
    tiingo_end: Optional[date],
    manifest_count: int,
    status: str,
) -> Tuple[str, str]:
    """
    Decide whether Tiingo's security for a ticker really is the index member.

    Pure function -- no I/O -- so the decision logic is testable without network.

    Args:
        membership_start: first day in the index (None = before our history).
        membership_end: last day in the index (None = still a member).
        tiingo_start/tiingo_end: Tiingo's coverage for this ticker string.
        manifest_count: times the ticker appears in Tiingo's supported-tickers
            manifest. >1 means the string has been reused by another security.
        status: 'AVAILABLE' or 'NOT_FOUND' from the metadata probe.

    Returns:
        (verdict, human-readable reason).
    """
    if status != "AVAILABLE":
        return NOT_FOUND, "Tiingo does not know this ticker at all"
    if tiingo_start is None:
        return NO_PRICE_DATA, "ticker is known to Tiingo but carries no price history"

    # Rule 2 first: a reused string is unsafe even when the dates happen to line up.
    if manifest_count > 1:
        return (REUSED_TICKER,
                f"Tiingo's manifest lists this ticker {manifest_count} times, so the "
                f"string was reused; the security returned may not be the index member")

    end = membership_end or date.max
    start = membership_start or date.min

    # Rule 1: coverage that begins after the company left cannot be its history.
    if tiingo_start > end:
        return (NO_COVERAGE,
                f"Tiingo coverage starts {tiingo_start} but index membership ended {end}")

    overlap_end = min(tiingo_end or date.max, end)
    overlap_start = max(tiingo_start, start)
    overlap_days = (overlap_end - overlap_start).days
    if overlap_days >= MIN_OVERLAP_DAYS:
        return OK, f"{overlap_days} days of overlap with the membership window"
    return (MARGINAL,
            f"only {overlap_days} days of overlap -- needs a human decision")


def membership_window(rows: Sequence[Dict[str, str]]) -> Tuple[Optional[date], Optional[date]]:
    """
    Collapse a ticker's intervals into the outer bounds of its index membership.

    Deliberately the OUTER bounds: a company that left and rejoined should be
    judged against its whole span, because Tiingo coverage overlapping either
    stint means the security is the right one.
    """
    starts = [datetime.fromisoformat(r["start_date"]).date() for r in rows if r.get("start_date")]
    ends = [datetime.fromisoformat(r["end_date"]).date() for r in rows if r.get("end_date")]
    # A missing start means "member from before our history"; a missing end means
    # "still a member". Either way the bound is open, which None represents.
    start = min(starts) if starts and len(starts) == len(rows) else None
    end = max(ends) if ends and len(ends) == len(rows) else None
    return start, end


def _collect_keys() -> List[str]:
    keys = [os.environ[n].strip() for n in sorted(os.environ)
            if n.startswith("TIINGO_API_KEY") and os.environ[n].strip()]
    if not keys:
        raise EnvironmentError("No TIINGO_API_KEY* set in environment / .env")
    return keys


async def fetch_manifest_counts(session: aiohttp.ClientSession) -> Dict[str, int]:
    """
    How many times each ticker appears in Tiingo's supported-tickers manifest.

    This is a free static file -- no token, no rate limit -- so rule 2 costs
    nothing. A count above 1 means the string covers more than one security.
    """
    async with session.get(MANIFEST_URL) as r:
        r.raise_for_status()
        blob = await r.read()
    counts: Dict[str, int] = {}
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        with z.open(z.namelist()[0]) as f:
            for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")):
                t = (row.get("ticker") or "").strip().upper()
                if t:
                    counts[t] = counts.get(t, 0) + 1
    return counts


async def probe(session: aiohttp.ClientSession, keys: Sequence[str], idx: int,
                ticker: str) -> Dict[str, str]:
    """Probe one ticker's metadata. Round-robin key choice keeps every key under its cap."""
    key = keys[idx % len(keys)]
    for attempt in range(3):
        try:
            async with session.get(META_URL.format(ticker=ticker), params={"token": key}) as r:
                if r.status == 200:
                    d = await r.json()
                    return {"ticker": ticker, "status": "AVAILABLE",
                            "name": (d.get("name") or "")[:80],
                            "exchange": d.get("exchangeCode") or "",
                            "startDate": (d.get("startDate") or "")[:10],
                            "endDate": (d.get("endDate") or "")[:10]}
                if r.status == 404:
                    return {"ticker": ticker, "status": "NOT_FOUND", "name": "",
                            "exchange": "", "startDate": "", "endDate": ""}
                if r.status == 429:
                    key = keys[(idx + attempt + 1) % len(keys)]
                    await asyncio.sleep(1)
                    continue
                return {"ticker": ticker, "status": f"HTTP_{r.status}", "name": "",
                        "exchange": "", "startDate": "", "endDate": ""}
        except Exception as exc:  # noqa: BLE001
            if attempt == 2:
                return {"ticker": ticker, "status": f"ERR_{type(exc).__name__}", "name": "",
                        "exchange": "", "startDate": "", "endDate": ""}
            await asyncio.sleep(1)
    return {"ticker": ticker, "status": "EXHAUSTED", "name": "",
            "exchange": "", "startDate": "", "endDate": ""}


def _read_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write_csv(path: str, header: Sequence[str], rows: Sequence[Sequence]) -> None:
    with open(os.path.normpath(path), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def _d(s: str) -> Optional[date]:
    return datetime.fromisoformat(s).date() if s else None


async def run(dry_run: bool = False, concurrency: int = 12) -> Dict[str, int]:
    former = [r["dataSymbol"] for r in _read_csv(FORMER_IN)]
    membership: Dict[str, List[Dict[str, str]]] = {}
    for r in _read_csv(MEMBERSHIP_IN):
        membership.setdefault(r["ticker"], []).append(r)

    cached = {r["ticker"]: r for r in _read_csv(PROBE_CACHE)}
    todo = [t for t in former if t not in cached]
    logger.info("%d former members; %d already probed; %d to probe",
                len(former), len(former) - len(todo), len(todo))

    keys = _collect_keys()
    logger.info("%d API keys -> %.1f requests per key (free cap is 50/hour)",
                len(keys), len(todo) / len(keys) if keys else 0)

    if dry_run:
        logger.info("DRY RUN: would probe %d tickers and download the manifest", len(todo))
        return {"former": len(former), "cached": len(cached), "to_probe": len(todo),
                "probed": 0, OK: 0}

    sem = asyncio.Semaphore(concurrency)
    conn = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60),
                                     connector=conn) as session:
        logger.info("downloading Tiingo supported-tickers manifest (free, unauthenticated)")
        manifest = await fetch_manifest_counts(session)

        async def guarded(i: int, t: str) -> Dict[str, str]:
            async with sem:
                return await probe(session, keys, i, t)

        results = list(cached.values())
        if todo:
            fresh = await asyncio.gather(*[guarded(i, t) for i, t in enumerate(todo)])
            results.extend(fresh)
            _write_csv(PROBE_CACHE,
                       ["ticker", "status", "name", "exchange", "startDate", "endDate"],
                       [[r["ticker"], r["status"], r["name"], r["exchange"],
                         r["startDate"], r["endDate"]] for r in results])
            logger.info("probe cache written: %s (%d rows)", PROBE_CACHE, len(results))

    overrides = {r["ticker"]: r for r in _read_csv(OVERRIDES_IN)}
    if overrides:
        logger.info("%d human overrides loaded from %s", len(overrides), OVERRIDES_IN)

    # Classify only tickers that are actually in the current universe. The probe
    # cache outlives the universe -- a ticker cleaned up or dropped by a later
    # build_sp500_history run would otherwise keep being counted forever.
    universe = set(former)
    targets, gaps, counts = [], [], {}
    for r in results:
        t = r["ticker"]
        if t not in universe:
            continue
        ms, me = membership_window(membership.get(t, []))
        verdict, reason = classify(ms, me, _d(r["startDate"]), _d(r["endDate"]),
                                   manifest.get(t, 1), r["status"])
        if t in overrides:
            ov = overrides[t]
            verdict = ov["verdict"].strip()
            reason = f"HUMAN OVERRIDE: {ov.get('note','').strip()} (auto verdict was: {reason})"
        counts[verdict] = counts.get(verdict, 0) + 1
        if verdict == OK:
            targets.append([t, r["name"], r["startDate"], r["endDate"],
                            ms.isoformat() if ms else "", me.isoformat() if me else ""])
        else:
            gaps.append([t, verdict, ms.isoformat() if ms else "",
                         me.isoformat() if me else "", r["name"],
                         r["startDate"], r["endDate"], reason])

    _write_csv(TARGETS_OUT,
               ["dataSymbol", "tiingo_name", "tiingo_start", "tiingo_end",
                "membership_from", "membership_to"], sorted(targets))
    _write_csv(GAPS_OUT,
               ["ticker", "reason", "membership_from", "membership_to",
                "tiingo_name", "tiingo_start", "tiingo_end", "note"], sorted(gaps))

    logger.info("verdicts: %s", dict(sorted(counts.items())))
    logger.info("backfill targets -> %s (%d)", TARGETS_OUT, len(targets))
    logger.info("coverage gaps    -> %s (%d)", GAPS_OUT, len(gaps))
    return {"former": len(former), "probed": len(todo), **counts}


def main() -> None:
    from dotenv import load_dotenv
    load_dotenv()

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true",
                   help="Report how many tickers would be probed without calling the API.")
    p.add_argument("--concurrency", type=int, default=12)
    args = p.parse_args()

    summary = asyncio.run(run(dry_run=args.dry_run, concurrency=args.concurrency))
    logger.info("SUMMARY %s", summary)


if __name__ == "__main__":
    main()
