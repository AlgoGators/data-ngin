"""Generator for contracts/contract_tiingo.csv.

Merges: existing 44 ETFs + S&P 500 constituents + curated liquid non-S&P names.

The S&P 500 list is read from a committed snapshot (scripts/sp500_constituents.csv),
pulled VERBATIM from github.com/datasets/s-and-p-500-companies. Reading the raw file
(rather than an inline pasted list or a model-summarized fetch) keeps the data exact
-- it is what caught the 2026 ticker changes (MMC->MRSH, BK->BNY, FedEx Freight FDXF).

To refresh the universe, re-download the snapshot and re-run this script from repo root:
    curl -sS https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv \
        -o scripts/sp500_constituents.csv
    python scripts/build_tiingo_universe.py

Applies the Tiingo share-class fix (BRK.B -> BRK-B) and dedupes (ETFs first, then S&P,
then curated extras). The structural guarantees are enforced by
tests/test_contract_tiingo_universe.py.
"""
import csv
import os

HERE = os.path.dirname(__file__)
OUT = os.path.join(HERE, "..", "contracts", "contract_tiingo.csv")
SP500_SNAPSHOT = os.path.join(HERE, "sp500_constituents.csv")

ETFS = [
    "SPY", "QQQ", "IWM", "DIA", "SHY", "IEI", "IEF", "TLT", "USO", "UNG", "UGA",
    "XLE", "GLD", "SLV", "CPER", "PPLT", "CORN", "WEAT", "SOYB", "DBA", "UUP",
    "FXE", "FXB", "FXY", "FXC", "FXF", "FXA", "IBIT", "VOO", "VTI", "XLF", "XLK",
    "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC", "EFA", "EEM", "AGG", "LQD",
]

# Curated very-liquid names that are NOT ETFs and may sit outside the S&P 500
# (large ADRs / high-volume US listings). Overlaps with S&P are deduped below.
CURATED_EXTRAS = [
    "BABA", "PDD", "NIO", "TSM", "JD", "BIDU", "LI", "XPEV", "NU", "SE", "SHOP",
    "RIVN", "LCID", "MARA", "RIOT", "GME", "AMC", "AFRM", "UPST", "RBLX", "SNAP",
    "CVNA", "IONQ", "HOOD", "SOFI",
]


def tiingo_ticker(sym: str) -> str:
    return sym.strip().upper().replace(".", "-")  # BRK.B -> BRK-B


def load_sp500() -> list:
    """Read the S&P 500 ticker column from the committed constituents snapshot."""
    with open(os.path.normpath(SP500_SNAPSHOT), newline="") as f:
        return [row["Symbol"] for row in csv.DictReader(f) if row.get("Symbol", "").strip()]


def main() -> None:
    sp500 = load_sp500()
    seen = set()
    ordered = []
    # ETFs first (preserve the deployed set), then S&P, then curated extras.
    for sym in ETFS + sp500 + CURATED_EXTRAS:
        t = tiingo_ticker(sym)
        if t and t not in seen:
            seen.add(t)
            ordered.append(t)

    with open(os.path.normpath(OUT), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["dataSymbol", "instrumentType"])
        for t in ordered:
            w.writerow([t, "EQUITY"])

    print(
        f"Wrote {len(ordered)} symbols to {OUT} "
        f"(S&P {len(sp500)} + ETFs {len(ETFS)} + extras {len(CURATED_EXTRAS)}, deduped)"
    )


if __name__ == "__main__":
    main()
