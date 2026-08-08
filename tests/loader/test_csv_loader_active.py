import os
import tempfile
import unittest
from typing import Dict

from src.modules.loader.csv_loader import CSVLoader


def _loader(csv_text: str, tmpdir: str) -> CSVLoader:
    path = os.path.join(tmpdir, "contract.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        f.write(csv_text)
    return CSVLoader(config={"loader": {"file_path": path}})


class TestActiveColumn(unittest.TestCase):
    """
    The 'active' column keeps delisted securities out of the daily run. Without it
    a dead symbol is fetched every day forever and returns nothing -- EA has been
    doing exactly that since it was acquired on 2026-08-04.
    """

    def test_inactive_rows_are_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            loader = _loader(
                "dataSymbol,instrumentType,active\n"
                "AAPL,EQUITY,true\n"
                "EA,EQUITY,false\n"
                "MSFT,EQUITY,true\n", d)

            symbols: Dict[str, str] = loader.load_symbols()

        self.assertEqual(set(symbols), {"AAPL", "MSFT"})
        self.assertNotIn("EA", symbols)

    def test_contract_without_active_column_is_unchanged(self) -> None:
        """The futures contracts have no 'active' column and must keep working."""
        with tempfile.TemporaryDirectory() as d:
            loader = _loader(
                "dataSymbol,instrumentType\n"
                "ES,FUTURE\n"
                "NQ,FUTURE\n", d)

            symbols = loader.load_symbols()

        self.assertEqual(set(symbols), {"ES", "NQ"})

    def test_unrecognised_value_is_treated_as_active(self) -> None:
        """A typo must never silently drop a symbol from the daily run.

        Failing open is the deliberate choice: an extra fetch costs one request,
        while a wrongly-dropped symbol goes unnoticed for weeks -- which is the
        exact failure mode this project exists to eliminate.
        """
        with tempfile.TemporaryDirectory() as d:
            loader = _loader(
                "dataSymbol,instrumentType,active\n"
                "AAPL,EQUITY,ture\n"          # typo
                "MSFT,EQUITY,\n"              # blank
                "GOOG,EQUITY,yes\n", d)

            symbols = loader.load_symbols()

        self.assertEqual(set(symbols), {"AAPL", "MSFT", "GOOG"})

    def test_recognised_false_spellings_all_exclude(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            loader = _loader(
                "dataSymbol,instrumentType,active\n"
                "A,EQUITY,false\n"
                "B,EQUITY,FALSE\n"
                "C,EQUITY,False\n"
                "D,EQUITY,no\n"
                "E,EQUITY,N\n"
                "F,EQUITY,0\n"
                "G,EQUITY,true\n", d)

            symbols = loader.load_symbols()

        self.assertEqual(set(symbols), {"G"})

    def test_all_inactive_yields_empty_not_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            loader = _loader(
                "dataSymbol,instrumentType,active\n"
                "EA,EQUITY,false\n", d)

            self.assertEqual(loader.load_symbols(), {})

    def test_duplicate_check_still_runs_before_filtering(self) -> None:
        """Validation must not be bypassable by marking the duplicate inactive."""
        with tempfile.TemporaryDirectory() as d:
            loader = _loader(
                "dataSymbol,instrumentType,active\n"
                "AAPL,EQUITY,true\n"
                "AAPL,EQUITY,false\n", d)

            with self.assertRaises(ValueError):
                loader.load_symbols()


if __name__ == "__main__":
    unittest.main()
