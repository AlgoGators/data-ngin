import unittest
import tempfile
import os
from typing import Dict, Any
from unittest.mock import MagicMock, patch
from utils.dynamic_loader import load_config, load_class, get_instance


class TestDynamicLoader(unittest.TestCase):
    """
    Unit tests for the dynamic_loader module.
    """

    def setUp(self) -> None:
        """
        Set up mock configuration data for testing.
        """
        self.mock_config: Dict[str, Any] = {
            "loader": {"class": "CSVLoader", "module": "csv_loader"},
            "fetcher": {"class": "DatabentoFetcher", "module": "databento_fetcher"},
            "cleaner": {"class": "DatabentoCleaner", "module": "databento_cleaner"},
            "inserter": {"class": "TimescaleDBInserter", "module": "timescaledb_inserter"},
        }

    def create_temp_yaml(self, data: Dict[str, Any]) -> str:
        """
        Create a temporary YAML file with the provided data.

        Args:
            data (Dict[str, Any]): The configuration dictionary to save in YAML format.

        Returns:
            str: The path to the temporary YAML file.
        """
        import yaml

        temp_file: tempfile.NamedTemporaryFile = tempfile.NamedTemporaryFile(
            delete=False, suffix=".yaml", mode="w"
        )
        file_path: str = temp_file.name
        yaml.safe_dump(data, temp_file)
        temp_file.close()
        self.addCleanup(os.remove, file_path)
        return file_path

    def test_load_config_valid(self) -> None:
        """
        Test that `load_config` correctly loads a valid YAML configuration file.
        """
        temp_file_path: str = self.create_temp_yaml(self.mock_config)
        config: Dict[str, Any] = load_config(temp_file_path)
        self.assertEqual(config, self.mock_config, "Loaded configuration does not match expected result.")

    def test_load_config_missing_file(self) -> None:
        """
        Test that `load_config` raises FileNotFoundError for a non-existent file.
        """
        with self.assertRaises(FileNotFoundError):
            load_config("non_existent_config.yaml")

    def test_load_config_invalid_yaml(self) -> None:
        """
        Test that `load_config` raises ValueError for invalid YAML files.
        """
        temp_file: tempfile.NamedTemporaryFile = tempfile.NamedTemporaryFile(
            delete=False, mode="w", suffix=".yaml"
        )
        temp_file.write("Invalid YAML content: :::")
        temp_file.close()
        self.addCleanup(os.remove, temp_file.name)

        with self.assertRaises(ValueError) as context:
            load_config(temp_file.name)
        self.assertIn("Error parsing configuration file", str(context.exception))

    @patch("utils.dynamic_loader.importlib.import_module")
    def test_load_class_valid(self, mock_import_module: MagicMock) -> None:
        """
        Test that `load_class` correctly imports a class from a valid module.
        """
        # Mock module and class
        mock_module = MagicMock()
        mock_class = MagicMock()
        mock_import_module.return_value = mock_module
        mock_module.MockClass = mock_class

        loaded_class: Any = load_class("mock_module", "MockClass")

        mock_import_module.assert_called_once_with("mock_module")
        self.assertEqual(loaded_class, mock_class)

    @patch("utils.dynamic_loader.importlib.import_module")
    @patch("utils.dynamic_loader.getattr")
    def test_load_class_missing_class(self, mock_getattr: MagicMock, mock_import_module: MagicMock) -> None:
        """
        Test that `load_class` raises ImportError for a missing class in the module.
        """
        mock_module = MagicMock()
        mock_import_module.return_value = mock_module
        mock_getattr.side_effect = AttributeError("Mock class not found")

        with self.assertRaises(ImportError) as context:
            load_class("mock_module", "NonExistentClass")

        self.assertIn(
            "Class 'NonExistentClass' does not exist in module 'mock_module'",
            str(context.exception),
        )
        mock_getattr.assert_called_once_with(mock_module, "NonExistentClass")

    @patch("utils.dynamic_loader.load_class")
    def test_get_instance_valid(self, mock_load_class: MagicMock) -> None:
        """
        Test that `get_instance` correctly creates an instance of a dynamically loaded class.
        """
        mock_class = MagicMock()
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        mock_load_class.return_value = mock_class

        instance: Any = get_instance(self.mock_config, "loader", "class")

        mock_load_class.assert_called_once_with("data.modules.csv_loader", "CSVLoader")
        mock_class.assert_called_once_with(config=self.mock_config)
        self.assertEqual(instance, mock_instance)

    def test_get_instance_missing_module_key(self) -> None:
        """
        Test that `get_instance` raises ValueError for a missing module key in the configuration.
        """
        with self.assertRaises(ValueError):
            get_instance({}, "non_existent_key", "class")

    def test_get_instance_missing_class_key(self) -> None:
        """
        Test that `get_instance` raises ValueError for a missing class key in the module configuration.
        """
        with self.assertRaises(ValueError):
            get_instance(self.mock_config, "loader", "non_existent_key")


if __name__ == "__main__":
    unittest.main()
