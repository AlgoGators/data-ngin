import unittest
import tempfile
import os
import pandas as pd
from typing import Dict, Any, List
from data.modules.csv_loader import CSVLoader


class TestCSVLoader(unittest.TestCase):
    """
    Unit tests for the CSVLoader module.

    This test suite includes:
    - Valid CSV loading.
    - Handling of missing or malformed data.
    - Validation of duplicate or null values in the CSV.
    """

    def setUp(self) -> None:
        """
        Set up the test environment, including mock configuration and test data.
        """
        self.mock_config: Dict[str, Any] = {
            "loader": {
                "class": "CSVLoader",
                "module": "csv_loader",
                "file_path": "mock_path.csv"
            }
        }

    def create_mock_csv(self, data: List[Dict[str, Any]]) -> str:
        """
        Create a temporary CSV file with the given data.

        Args:
            data (List[Dict[str, Any]]): A list of dictionaries where each dictionary represents a row in the CSV.

        Returns:
            str: The path to the temporary CSV file.
        """
        temp_file: tempfile.NamedTemporaryFile = tempfile.NamedTemporaryFile(
            delete=False, suffix=".csv", mode="w", newline=""
        )
        file_path: str = temp_file.name
        df: pd.DataFrame = pd.DataFrame(data)
        df.to_csv(temp_file, index=False)
        temp_file.close()
        return file_path

    def tearDown(self) -> None:
        """
        Clean up temporary files after each test.
        """
        file_path: str = self.mock_config["loader"]["file_path"]
        if os.path.exists(file_path):
            os.remove(file_path)

    def test_load_symbols_valid_csv(self) -> None:
        """
        Test that CSVLoader correctly parses a valid CSV file.

        Raises:
            AssertionError: If the test fails to validate the parsed symbols.
        """
        # Mock CSV data
        mock_csv_data: List[Dict[str, str]] = [
            {"dataSymbol": "ES", "instrumentType": "FUTURE"},
            {"dataSymbol": "NQ", "instrumentType": "FUTURE"}
        ]
        temp_csv_path: str = self.create_mock_csv(mock_csv_data)
        self.mock_config["loader"]["file_path"] = temp_csv_path

        # Initialize CSVLoader
        loader: CSVLoader = CSVLoader(config=self.mock_config)

        # Call load_symbols and verify results
        symbols: Dict[str, str] = loader.load_symbols()
        expected_symbols: Dict[str, str] = {
            "ES": "FUTURE",
            "NQ": "FUTURE"
        }
        self.assertEqual(symbols, expected_symbols)

    def test_load_symbols_missing_columns(self) -> None:
        """
        Test that CSVLoader raises a ValueError if required columns are missing.

        Raises:
            AssertionError: If the exception is not raised or contains incorrect details.
        """
        # Mock CSV data with missing 'instrumentType' column
        mock_csv_data: List[Dict[str, str]] = [
            {"dataSymbol": "ES"},
            {"dataSymbol": "NQ"}
        ]
        temp_csv_path: str = self.create_mock_csv(mock_csv_data)
        self.mock_config["loader"]["file_path"] = temp_csv_path

        # Initialize CSVLoader
        loader: CSVLoader = CSVLoader(config=self.mock_config)

        # Verify exception
        with self.assertRaises(ValueError) as context:
            loader.load_symbols()
        self.assertIn("Missing columns", str(context.exception))

    def test_load_symbols_duplicates(self) -> None:
        """
        Test that CSVLoader raises a ValueError if duplicate symbols exist in the CSV.

        Raises:
            AssertionError: If the exception is not raised or contains incorrect details.
        """
        # Mock CSV data with duplicate 'dataSymbol' entries
        mock_csv_data: List[Dict[str, str]] = [
            {"dataSymbol": "ES", "instrumentType": "FUTURE"},
            {"dataSymbol": "ES", "instrumentType": "FUTURE"}
        ]
        temp_csv_path: str = self.create_mock_csv(mock_csv_data)
        self.mock_config["loader"]["file_path"] = temp_csv_path

        # Initialize CSVLoader
        loader: CSVLoader = CSVLoader(config=self.mock_config)

        # Verify exception
        with self.assertRaises(ValueError) as context:
            loader.load_symbols()
        self.assertIn("Duplicate symbols", str(context.exception))

    def test_load_symbols_null_values(self) -> None:
        """
        Test that CSVLoader raises a ValueError if null values exist in the 'dataSymbol' column.

        Raises:
            AssertionError: If the exception is not raised or contains incorrect details.
        """
        # Mock CSV data with a null value in 'dataSymbol'
        mock_csv_data: List[Dict[str, Any]] = [
            {"dataSymbol": "ES", "instrumentType": "FUTURE"},
            {"dataSymbol": None, "instrumentType": "FUTURE"}
        ]
        temp_csv_path: str = self.create_mock_csv(mock_csv_data)
        self.mock_config["loader"]["file_path"] = temp_csv_path

        # Initialize CSVLoader
        loader: CSVLoader = CSVLoader(config=self.mock_config)

        # Verify exception
        with self.assertRaises(ValueError) as context:
            loader.load_symbols()
        self.assertIn("Null values", str(context.exception))

    def test_load_symbols_missing_file(self) -> None:
        """
        Test that CSVLoader raises a FileNotFoundError if the file does not exist.

        Raises:
            AssertionError: If the exception is not raised or contains incorrect details.
        """
        # Mock configuration with a non-existent file path
        self.mock_config["loader"]["file_path"] = "non_existent_file.csv"

        # Initialize CSVLoader
        loader: CSVLoader = CSVLoader(config=self.mock_config)

        # Verify exception
        with self.assertRaises(FileNotFoundError) as context:
            loader.load_symbols()
        self.assertIn("Contract file not found", str(context.exception))


if __name__ == "__main__":
    unittest.main()
