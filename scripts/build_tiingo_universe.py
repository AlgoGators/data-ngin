"""One-off generator for contracts/contract_tiingo.csv.

Merges: existing 44 ETFs + S&P 500 constituents + curated liquid non-S&P names.
Applies the Tiingo share-class fix (BRK.B -> BRK-B) and dedupes. Run from repo root:
    python scripts/build_tiingo_universe.py
"""
import csv
import os

OUT = os.path.join(os.path.dirname(__file__), "..", "contracts", "contract_tiingo.csv")

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

# S&P 500 constituent symbols (sourced from github.com/datasets/s-and-p-500-companies, 2026-06-08).
# Dot-form share classes (BRK.B, BF.B) are converted to dashes by tiingo_ticker().
SP500 = [
    "MMM", "AOS", "ABT", "ABBV", "ACN", "ADBE", "AMD", "AES", "AFL", "A", "APD",
    "ABNB", "AKAM", "ALB", "ARE", "ALGN", "ALLE", "LNT", "ALL", "GOOGL", "GOOG",
    "MO", "AMZN", "AMCR", "AEE", "AEP", "AXP", "AIG", "AMT", "AWK", "AMP", "AME",
    "AMGN", "APH", "ADI", "AON", "APA", "APO", "AAPL", "AMAT", "APP", "APTV",
    "ACGL", "ADM", "ARES", "ANET", "AJG", "AIZ", "T", "ATO", "ADSK", "ADP", "AZO",
    "AVB", "AVY", "AXON", "BKR", "BALL", "BAC", "BAX", "BDX", "BRK.B", "BBY",
    "TECH", "BIIB", "BLK", "BX", "XYZ", "BNY", "BA", "BKNG", "BSX", "BMY", "AVGO",
    "BR", "BRO", "BF.B", "BLDR", "BG", "BXP", "CHRW", "CDNS", "CPT", "CPB", "COF",
    "CAH", "CCL", "CARR", "CVNA", "CASY", "CAT", "CBOE", "CBRE", "CDW", "COR",
    "CNC", "CNP", "CF", "CRL", "SCHW", "CHTR", "CVX", "CMG", "CB", "CHD", "CIEN",
    "CI", "CINF", "CTAS", "CSCO", "C", "CFG", "CLX", "CME", "CMS", "KO", "CTSH",
    "COHR", "COIN", "CL", "CMCSA", "FIX", "CAG", "COP", "ED", "STZ", "CEG", "COO",
    "CPRT", "GLW", "CPAY", "CTVA", "CSGP", "COST", "CRH", "CRWD", "CCI", "CSX",
    "CMI", "CVS", "DHR", "DRI", "DDOG", "DVA", "DECK", "DE", "DELL", "DAL", "DVN",
    "DXCM", "FANG", "DLR", "DG", "DLTR", "D", "DPZ", "DASH", "DOV", "DOW", "DHI",
    "DTE", "DUK", "DD", "ETN", "EBAY", "SATS", "ECL", "EIX", "EW", "EA", "ELV",
    "EME", "EMR", "ETR", "EOG", "EQT", "EFX", "EQIX", "EQR", "ERIE", "ESS", "EL",
    "EG", "EVRG", "ES", "EXC", "EXE", "EXPE", "EXPD", "EXR", "XOM", "FFIV", "FDS",
    "FICO", "FAST", "FRT", "FDX", "FDXF", "FIS", "FITB", "FSLR", "FE", "FISV", "F",
    "FTNT", "FTV", "FOXA", "FOX", "BEN", "FCX", "GRMN", "IT", "GE", "GEHC", "GEV",
    "GEN", "GNRC", "GD", "GIS", "GM", "GPC", "GILD", "GPN", "GL", "GDDY", "GS",
    "HAL", "HIG", "HAS", "HCA", "DOC", "HSIC", "HSY", "HPE", "HLT", "HD", "HON",
    "HRL", "HST", "HWM", "HPQ", "HUBB", "HUM", "HBAN", "HII", "IBM", "IEX", "IDXX",
    "ITW", "INCY", "IR", "PODD", "INTC", "IBKR", "ICE", "IFF", "IP", "INTU", "ISRG",
    "IVZ", "INVH", "IQV", "IRM", "JBHT", "JBL", "JKHY", "J", "JNJ", "JCI", "JPM",
    "KVUE", "KDP", "KEY", "KEYS", "KMB", "KIM", "KMI", "KKR", "KLAC", "KHC", "KR",
    "LHX", "LH", "LRCX", "LVS", "LDOS", "LEN", "LII", "LLY", "LIN", "LYV", "LMT",
    "L", "LOW", "LULU", "LITE", "LYB", "MTB", "MPC", "MAR", "MRSH", "MLM", "MAS",
    "MA", "MKC", "MCD", "MCK", "MDT", "MRK", "META", "MET", "MTD", "MGM", "MCHP",
    "MU", "MSFT", "MAA", "MRNA", "TAP", "MDLZ", "MPWR", "MNST", "MCO", "MS", "MOS",
    "MSI", "MSCI", "NDAQ", "NTAP", "NFLX", "NEM", "NWSA", "NWS", "NEE", "NKE", "NI",
    "NDSN", "NSC", "NTRS", "NOC", "NCLH", "NRG", "NUE", "NVDA", "NVR", "NXPI",
    "ORLY", "OXY", "ODFL", "OMC", "ON", "OKE", "ORCL", "OTIS", "PCAR", "PKG",
    "PLTR", "PANW", "PSKY", "PH", "PAYX", "PYPL", "PNR", "PEP", "PFE", "PCG", "PM",
    "PSX", "PNW", "PNC", "POOL", "PPG", "PPL", "PFG", "PG", "PGR", "PLD", "PRU",
    "PEG", "PTC", "PSA", "PHM", "PWR", "QCOM", "DGX", "Q", "RL", "RJF", "RTX", "O",
    "REG", "REGN", "RF", "RSG", "RMD", "RVTY", "HOOD", "ROK", "ROL", "ROP", "ROST",
    "RCL", "SPGI", "CRM", "SNDK", "SBAC", "SLB", "STX", "SRE", "NOW", "SHW", "SPG",
    "SWKS", "SJM", "SW", "SNA", "SOLV", "SO", "LUV", "SWK", "SBUX", "STT", "STLD",
    "STE", "SYK", "SMCI", "SYF", "SNPS", "SYY", "TMUS", "TROW", "TTWO", "TPR",
    "TRGP", "TGT", "TEL", "TDY", "TER", "TSLA", "TXN", "TPL", "TXT", "TMO", "TJX",
    "TKO", "TTD", "TSCO", "TT", "TDG", "TRV", "TRMB", "TFC", "TYL", "TSN", "USB",
    "UBER", "UDR", "ULTA", "UNP", "UAL", "UPS", "URI", "UNH", "UHS", "VLO", "VEEV",
    "VTR", "VLTO", "VRSN", "VRSK", "VZ", "VRTX", "VRT", "VTRS", "VICI", "V", "VST",
    "VMC", "WRB", "GWW", "WAB", "WMT", "DIS", "WBD", "WM", "WAT", "WEC", "WFC",
    "WELL", "WST", "WDC", "WY", "WSM", "WMB", "WTW", "WDAY", "WYNN", "XEL", "XYL",
    "YUM", "ZBRA", "ZBH", "ZTS",
]


def tiingo_ticker(sym: str) -> str:
    return sym.strip().upper().replace(".", "-")  # BRK.B -> BRK-B


def main() -> None:
    seen = set()
    ordered = []
    # ETFs first (preserve the deployed set), then S&P, then curated extras.
    for sym in ETFS + [tiingo_ticker(s) for s in SP500] + CURATED_EXTRAS:
        t = tiingo_ticker(sym)
        if t and t not in seen:
            seen.add(t)
            ordered.append(t)

    with open(os.path.normpath(OUT), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["dataSymbol", "instrumentType"])
        for t in ordered:
            w.writerow([t, "EQUITY"])

    print(f"Wrote {len(ordered)} symbols to {OUT}")


if __name__ == "__main__":
    main()
