import csv
import os
import unittest

CSV_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "contracts", "contract_tiingo.csv")
)

# The 44 ETFs already deployed — must survive the expansion.
ETF_BASELINE = [
    "SPY", "QQQ", "IWM", "DIA", "SHY", "IEI", "IEF", "TLT", "USO", "UNG", "UGA",
    "XLE", "GLD", "SLV", "CPER", "PPLT", "CORN", "WEAT", "SOYB", "DBA", "UUP",
    "FXE", "FXB", "FXY", "FXC", "FXF", "FXA", "IBIT", "VOO", "VTI", "XLF", "XLK",
    "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC", "EFA", "EEM", "AGG", "LQD",
]


class TestContractTiingoUniverse(unittest.TestCase):
    def setUp(self):
        with open(CSV_PATH, newline="") as f:
            self.rows = list(csv.DictReader(f))
        self.symbols = [r["dataSymbol"] for r in self.rows]

    def test_header_and_all_equity(self):
        self.assertEqual(set(self.rows[0].keys()), {"dataSymbol", "instrumentType"})
        self.assertTrue(all(r["instrumentType"] == "EQUITY" for r in self.rows))

    def test_no_duplicates(self):
        self.assertEqual(len(self.symbols), len(set(self.symbols)),
                         msg="duplicate tickers in contract_tiingo.csv")

    def test_no_dot_share_classes(self):
        # Tiingo uses dashes (BRK-B), not dots (BRK.B).
        dotted = [s for s in self.symbols if "." in s]
        self.assertEqual(dotted, [], msg=f"dot-form tickers must be dashed: {dotted}")

    def test_no_blank_symbols(self):
        self.assertTrue(all(s.strip() for s in self.symbols))

    def test_etf_baseline_present(self):
        missing = [t for t in ETF_BASELINE if t not in self.symbols]
        self.assertEqual(missing, [], msg=f"missing ETFs: {missing}")

    def test_universe_size_in_range(self):
        # ~500 S&P + 44 ETFs + ~20-30 curated extras. Over 580 -> advise a 13th key.
        self.assertGreaterEqual(len(self.symbols), 540)
        self.assertLessEqual(len(self.symbols), 600)


if __name__ == "__main__":
    unittest.main()
