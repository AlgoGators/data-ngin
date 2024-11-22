import unittest
import tempfile
import os
from typing import Dict, Any
from unittest.mock import MagicMock, patch
from utils.dynamic_loader import load_config, load_class, get_instance


class TestDynamicLoader(unittest.TestCase):
    """
    Unit tests for the dynamic_loader module.

    This test suite includes:
    - Testing `load_config` for loading YAML configuration files.
    - Testing `load_class` for dynamically importing classes.
    - Testing `get_instance` for creating instances dynamically from configurations.
    """

    def setUp(self) -> None:
        """
        Set up mock configuration data for testing.
        """
        self.mock_config: Dict[str, Any] = {
            "loader": {"class": "CSVLoader", "module": "csv_loader"},
            "fetcher": {"class": "DatabentoFetcher", "module": "databento_fetcher"},
            "cleaner": {"class": "DatabentoCleaner", "module": "databento_cleaner"},
            "inserter": {"class": "TimescaleDBInserter", "module": "timescaledb_inserter"}
        }

    def create_temp_yaml(self, data: Dict[str, Any]) -> str:
        """
        Create a temporary YAML file with the provided data.

        Args:
            data (Dict[str, Any]): The configuration dictionary to save in YAML format.

        Returns:
            str: The path to the temporary YAML file.
        """
        temp_file: tempfile.NamedTemporaryFile = tempfile.NamedTemporaryFile(delete=False, suffix=".yaml", mode="w")
        file_path: str = temp_file.name
        import yaml
        yaml.safe_dump(data, temp_file)
        temp_file.close()
        return file_path

    def tearDown(self) -> None:
        """
        Clean up temporary files created during testing.
        """
        if hasattr(self, "temp_file_path") and os.path.exists(self.temp_file_path):
            os.remove(self.temp_file_path)

    def test_load_config_valid(self) -> None:
        """
        Test that `load_config` correctly loads a valid YAML configuration file.

        Raises:
            AssertionError: If the loaded configuration does not match the expected result.
        """
        self.temp_file_path: str = self.create_temp_yaml(self.mock_config)
        config: Dict[str, Any] = load_config(self.temp_file_path)
        self.assertEqual(config, self.mock_config)

    def test_load_config_missing_file(self) -> None:
        """
        Test that `load_config` raises FileNotFoundError for a non-existent file.

        Raises:
            FileNotFoundError: If the file is not found.
        """
        with self.assertRaises(FileNotFoundError):
            load_config("non_existent_config.yaml")

    def test_load_config_invalid_yaml(self) -> None:
        """
        Test that `load_config` raises ValueError for invalid YAML files.

        Raises:
            ValueError: If the YAML file cannot be parsed.
        """
        # Create a temporary file with invalid YAML content
        temp_file: tempfile.NamedTemporaryFile = tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".yaml")
        temp_file.write("Invalid YAML content: :::")
        temp_file.close()

        try:
            with self.assertRaises(ValueError) as context:
                load_config(temp_file.name)
            
            # Validate the error message
            self.assertIn("Error parsing configuration file", str(context.exception))
        finally:
            os.remove(temp_file.name)


    @patch("utils.dynamic_loader.importlib.import_module")
    def test_load_class_valid(self, mock_import_module: MagicMock) -> None:
        """
        Test that `load_class` correctly imports a class from a valid module.

        Args:
            mock_import_module (MagicMock): Mocked `importlib.import_module`.
        """
        # Mock module and class
        mock_module = MagicMock()
        mock_class = MagicMock()
        mock_import_module.return_value = mock_module
        mock_module.MockClass = mock_class

        # Call `load_class`
        loaded_class: Any = load_class("mock_module", "MockClass")

        # Assertions
        mock_import_module.assert_called_once_with("mock_module")
        self.assertEqual(loaded_class, mock_class)

    @patch("utils.dynamic_loader.importlib.import_module")
    @patch("utils.dynamic_loader.getattr")
    def test_load_class_missing_class(self, mock_getattr: MagicMock, mock_import_module: MagicMock) -> None:
        """
        Test that `load_class` raises ImportError for a missing class in the module.

        Args:
            mock_getattr (MagicMock): Mocked `getattr` to simulate missing class behavior.
            mock_import_module (MagicMock): Mocked `importlib.import_module`.
        """
        # Mock module returned by importlib
        mock_module = MagicMock()
        mock_import_module.return_value = mock_module

        # Simulate getattr raising AttributeError when the class does not exist
        mock_getattr.side_effect = AttributeError("Mock class not found")

        # Attempt to load a non-existent class
        with self.assertRaises(ImportError) as context:
            load_class("mock_module", "NonExistentClass")

        # Verify the error message
        self.assertIn("Class 'NonExistentClass' does not exist in module 'mock_module'", str(context.exception))

        # Ensure getattr was called with the right arguments
        mock_getattr.assert_called_once_with(mock_module, "NonExistentClass")



    def test_get_instance_valid(self) -> None:
        """
        Test that `get_instance` correctly creates an instance of a dynamically loaded class.

        Raises:
            AssertionError: If the loaded instance is not as expected.
        """
        with patch("utils.dynamic_loader.load_class") as mock_load_class:
            # Mock the loaded class and its constructor
            mock_class = MagicMock()
            mock_instance = MagicMock()
            mock_class.return_value = mock_instance
            mock_load_class.return_value = mock_class

            # Call `get_instance`
            instance: Any = get_instance(self.mock_config, "loader", "class")

            # Assertions
            mock_load_class.assert_called_once_with("data.modules.csv_loader", "CSVLoader")
            mock_class.assert_called_once_with(config=self.mock_config)
            self.assertEqual(instance, mock_instance)

    def test_get_instance_missing_module_key(self) -> None:
        """
        Test that `get_instance` raises ValueError for a missing module key in the configuration.

        Raises:
            ValueError: If the module key is not found.
        """
        with self.assertRaises(ValueError):
            get_instance({}, "non_existent_key", "class")

    def test_get_instance_missing_class_key(self) -> None:
        """
        Test that `get_instance` raises ValueError for a missing class key in the module configuration.

        Raises:
            ValueError: If the class key is not found.
        """
        with self.assertRaises(ValueError):
            get_instance(self.mock_config, "loader", "non_existent_key")


if __name__ == "__main__":
    unittest.main()
