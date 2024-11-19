import unittest
from unittest.mock import MagicMock, patch
from data.utils.dynamic_loader import load_class, get_instance
from typing import Dict, Any


class TestDynamicLoader(unittest.TestCase):
    """
    Tests for the dynamic_loader utility functions.
    """

    def test_load_class_valid(self) -> None:
        """
        Test that load_class successfully loads a valid class from a module.
        """
        # Example: Load the CSVLoader class
        loaded_class: Any = load_class("data.modules.csv_loader", "CSVLoader")
        self.assertEqual(loaded_class.__name__, "CSVLoader")

    def test_load_class_invalid(self) -> None:
        """
        Test that load_class raises ImportError when the class or module is invalid.
        """
        with self.assertRaises(ImportError):
            load_class("data.modules.invalid_module", "InvalidClass")

    @patch("data.modules.csv_loader.CSVLoader")
    def test_get_instance_valid(self, MockCSVLoader: MagicMock) -> None:
        """
        Test that get_instance successfully creates an instance of a class with valid configuration.

        Args:
            MockCSVLoader (MagicMock): Mocked CSVLoader class.
        """
        # Mock configuration
        config: Dict[str, Any] = {
            "loader": {
                "class": "CSVLoader",
                "module": "csv_loader"  # Explicit module name
            }
        }

        # Create an instance dynamically
        instance: Any = get_instance(config, "loader", "class", file_path="data/contracts/contract.csv")

        # Verify that the mock CSVLoader was instantiated with the correct arguments
        MockCSVLoader.assert_called_once_with(config=config, file_path="data/contracts/contract.csv")
        self.assertIsInstance(instance, MagicMock)

    def test_get_instance_missing_keys(self) -> None:
        """
        Test that get_instance raises ValueError when keys are missing in the configuration.
        """
        # Missing module_key
        config: Dict[str, Any] = {}
        with self.assertRaises(ValueError):
            get_instance(config, "loader", "class")

        # Missing class_key
        config = {
            "loader": {}
        }
        with self.assertRaises(ValueError):
            get_instance(config, "loader", "class")


if __name__ == "__main__":
    unittest.main()
