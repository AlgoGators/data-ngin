import unittest
import tempfile
import os
import pandas as pd
from typing import Dict, Any, List, Optional
from src.modules.loader.csv_loader import CSVLoader


class TestCSVLoader(unittest.TestCase):
    """
    Unit tests for the CSVLoader module.
    """

    def setUp(self) -> None:
        """
        Set up the test environment, including mock configuration and test data.
        """
        self.mock_config: Dict[str, Any] = {
            "loader": {
                "class": "CSVLoader",
                "module": "csv_loader",
                "file_path": "mock_path.csv",
            }
        }
        self.temp_file: Optional[tempfile.NamedTemporaryFile] = None

    def create_mock_csv(self, data: List[Dict[str, Any]]) -> str:
        """
        Create a temporary CSV file with the given data.

        Args:
            data (List[Dict[str, Any]]): A list of dictionaries where each dictionary represents a row in the CSV.

        Returns:
            str: The path to the temporary CSV file.
        """
        self.temp_file = tempfile.NamedTemporaryFile(
            delete=False, suffix=".csv", mode="w", newline=""
        )
        file_path: str = self.temp_file.name
        df: pd.DataFrame = pd.DataFrame(data)
        df.to_csv(self.temp_file, index=False)
        self.temp_file.close()
        return file_path

    def tearDown(self) -> None:
        """
        Clean up temporary files after each test.
        """
        if self.temp_file and os.path.exists(self.temp_file.name):
            os.remove(self.temp_file.name)

    def test_load_symbols_valid_csv(self) -> None:
        """
        Test that CSVLoader correctly parses a valid CSV file.
        """
        mock_csv_data: List[Dict[str, str]] = [
            {"dataSymbol": "ES", "instrumentType": "FUTURE"},
            {"dataSymbol": "NQ", "instrumentType": "FUTURE"},
        ]
        temp_csv_path: str = self.create_mock_csv(mock_csv_data)
        self.mock_config["loader"]["file_path"] = temp_csv_path

        loader: CSVLoader = CSVLoader(config=self.mock_config)
        symbols: Dict[str, str] = loader.load_symbols()

        expected_symbols: Dict[str, str] = {
            "ES": "FUTURE",
            "NQ": "FUTURE",
        }
        self.assertEqual(symbols, expected_symbols)

    def test_load_symbols_missing_columns(self) -> None:
        """
        Test that CSVLoader raises a ValueError if required columns are missing.
        """
        mock_csv_data: List[Dict[str, str]] = [{"dataSymbol": "ES"}, {"dataSymbol": "NQ"}]
        temp_csv_path: str = self.create_mock_csv(mock_csv_data)
        self.mock_config["loader"]["file_path"] = temp_csv_path

        loader: CSVLoader = CSVLoader(config=self.mock_config)
        with self.assertRaises(ValueError) as context:
            loader.load_symbols()
        self.assertIn("Missing columns", str(context.exception))

    def test_load_symbols_duplicates(self) -> None:
        """
        Test that CSVLoader raises a ValueError if duplicate symbols exist in the CSV.
        """
        mock_csv_data: List[Dict[str, str]] = [
            {"dataSymbol": "ES", "instrumentType": "FUTURE"},
            {"dataSymbol": "ES", "instrumentType": "FUTURE"},
        ]
        temp_csv_path: str = self.create_mock_csv(mock_csv_data)
        self.mock_config["loader"]["file_path"] = temp_csv_path

        loader: CSVLoader = CSVLoader(config=self.mock_config)
        with self.assertRaises(ValueError) as context:
            loader.load_symbols()
        self.assertIn("Duplicate symbols", str(context.exception))

    def test_load_symbols_null_values(self) -> None:
        """
        Test that CSVLoader raises a ValueError if null values exist in the 'dataSymbol' column.
        """
        mock_csv_data: List[Dict[str, Any]] = [
            {"dataSymbol": "ES", "instrumentType": "FUTURE"},
            {"dataSymbol": None, "instrumentType": "FUTURE"},
        ]
        temp_csv_path: str = self.create_mock_csv(mock_csv_data)
        self.mock_config["loader"]["file_path"] = temp_csv_path

        loader: CSVLoader = CSVLoader(config=self.mock_config)
        with self.assertRaises(ValueError) as context:
            loader.load_symbols()
        self.assertIn("Null values", str(context.exception))

    def test_load_symbols_missing_file(self) -> None:
        """
        Test that CSVLoader raises a FileNotFoundError if the file does not exist.
        """
        self.mock_config["loader"]["file_path"] = "non_existent_file.csv"
        loader: CSVLoader = CSVLoader(config=self.mock_config)
        with self.assertRaises(FileNotFoundError) as context:
            loader.load_symbols()
        self.assertIn("Contract file not found", str(context.exception))


if __name__ == "__main__":
    unittest.main()
