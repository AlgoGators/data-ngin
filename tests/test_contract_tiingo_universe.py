import csv
import math
import os
import unittest

CSV_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "contracts", "contract_tiingo.csv")
)
SP500_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "scripts", "sp500_constituents.csv")
)

# The 44 ETFs already deployed — must survive every expansion.
ETF_BASELINE = [
    "SPY", "QQQ", "IWM", "DIA", "SHY", "IEI", "IEF", "TLT", "USO", "UNG", "UGA",
    "XLE", "GLD", "SLV", "CPER", "PPLT", "CORN", "WEAT", "SOYB", "DBA", "UUP",
    "FXE", "FXB", "FXY", "FXC", "FXF", "FXA", "IBIT", "VOO", "VTI", "XLF", "XLK",
    "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC", "EFA", "EEM", "AGG", "LQD",
]

# Tiingo free tier: 50 requests per key per hour, one request per active symbol.
FREE_TIER_REQUESTS_PER_KEY_PER_HOUR = 50
KEYS_CURRENTLY_WORKING = 13


class TestContractTiingoUniverse(unittest.TestCase):
    def setUp(self):
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            self.rows = list(csv.DictReader(f))
        self.symbols = [r["dataSymbol"] for r in self.rows]
        self.active = [r["dataSymbol"] for r in self.rows if r.get("active") == "true"]

    def test_header_carries_the_active_flag(self):
        self.assertEqual(set(self.rows[0].keys()),
                         {"dataSymbol", "instrumentType", "active"})
        self.assertTrue(all(r["instrumentType"] == "EQUITY" for r in self.rows))

    def test_active_values_are_canonical(self):
        """CSVLoader fails open on unrecognised values, so a typo here would silently
        keep a delisted symbol in the daily run rather than failing loudly."""
        bad = {r["active"] for r in self.rows} - {"true", "false"}
        self.assertEqual(bad, set(), msg=f"non-canonical active values: {bad}")

    def test_no_duplicates(self):
        self.assertEqual(len(self.symbols), len(set(self.symbols)),
                         msg="duplicate tickers in contract_tiingo.csv")

    def test_no_dot_share_classes(self):
        dotted = [s for s in self.symbols if "." in s]
        self.assertEqual(dotted, [], msg=f"dot-form tickers must be dashed: {dotted}")

    def test_no_blank_symbols(self):
        self.assertTrue(all(s.strip() for s in self.symbols))

    def test_etf_baseline_present_and_active(self):
        missing = [t for t in ETF_BASELINE if t not in self.symbols]
        self.assertEqual(missing, [], msg=f"missing ETFs: {missing}")
        inactive = [t for t in ETF_BASELINE if t not in self.active]
        self.assertEqual(inactive, [], msg=f"ETFs must stay active: {inactive}")

    def test_every_current_sp500_member_is_present_and_active(self):
        """The gap that let MRVL, FLEX, HONA and FERG go unfetched: current index
        members missing from the contract entirely."""
        with open(SP500_PATH, newline="", encoding="utf-8") as f:
            members = {r["Symbol"].strip().upper().replace(".", "-")
                       for r in csv.DictReader(f) if (r.get("Symbol") or "").strip()}
        missing = sorted(members - set(self.symbols))
        self.assertEqual(missing, [], msg=f"current S&P members absent: {missing}")
        inactive = sorted(members - set(self.active))
        self.assertEqual(inactive, [], msg=f"current S&P members marked inactive: {inactive}")

    def test_universe_includes_former_members(self):
        """The whole point of the rewrite. If the contract ever shrinks back to
        roughly the current-index size, survivorship bias has been reintroduced."""
        self.assertGreater(len(self.symbols), 700,
                           msg="contract looks current-members-only — former members dropped?")

    def test_delisted_names_are_retained_but_inactive(self):
        """Delisted securities keep their history in the dataset but must not be
        fetched daily — the vendor returns nothing for them, forever."""
        inactive = [r["dataSymbol"] for r in self.rows if r.get("active") == "false"]
        self.assertGreater(len(inactive), 0,
                           msg="no inactive rows — is the active flag being computed at all?")

    def test_active_count_fits_available_api_keys(self):
        """One request per active symbol per day. Exceeding the hourly ceiling means
        the tail of the run silently fails, which is how symbols go missing."""
        needed = math.ceil(len(self.active) / FREE_TIER_REQUESTS_PER_KEY_PER_HOUR)
        self.assertLessEqual(
            needed, KEYS_CURRENTLY_WORKING,
            msg=(f"{len(self.active)} active symbols need {needed} working keys at "
                 f"{FREE_TIER_REQUESTS_PER_KEY_PER_HOUR}/hr, but only "
                 f"{KEYS_CURRENTLY_WORKING} are working. Add keys before deploying "
                 f"this contract, or the last ~"
                 f"{len(self.active) - KEYS_CURRENTLY_WORKING * FREE_TIER_REQUESTS_PER_KEY_PER_HOUR}"
                 f" symbols will fail every run."))


if __name__ == "__main__":
    unittest.main()
