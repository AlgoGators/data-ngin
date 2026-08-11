import unittest
import tempfile
import os
from typing import Dict, Any
from unittest.mock import MagicMock, patch
from src.utils.dynamic_loader import load_config, load_class, get_instance, determine_date_range


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
            "inserter": {"class": "OhlcvRepository", "module": "repository.ohlcv_repository"},
            "provider": {
                "name": "databento",
                "asset": "FUTURE",
                "dataset": "GLBX.MDP3",
                "schema": "OHLCV_1D",
                "roll_type": "v",
                "contract_type": "0",
            },
            "database": {
                "target_schema": "futures_data",
                "raw_table": "ohlcv_1d_raw",
                "table": "ohlcv_1d",
            },
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
        Test that `load_config` correctly loads a valid YAML configuration
        file. `load_config` now validates through `PipelineConfig`, which
        fills in defaults for sections not present in the YAML (missing_data,
        logging, batch_downloading, back_adjustment, symbol_remap) -- so the
        result is a superset of the input, not an exact match.
        """
        temp_file_path: str = self.create_temp_yaml(self.mock_config)
        config: Dict[str, Any] = load_config(temp_file_path)

        for section, expected in self.mock_config.items():
            for key, value in expected.items():
                self.assertEqual(
                    config[section][key], value,
                    f"Section '{section}.{key}' does not match expected result.",
                )

        # Defaulted sections should be present even though absent from the input YAML.
        self.assertIn("missing_data", config)
        self.assertIn("symbol_remap", config)
        self.assertIn("back_adjustment", config)

    def test_load_config_fails_schema_validation(self) -> None:
        """
        Test that `load_config` raises ValueError (not a downstream KeyError
        deep in the pipeline) when a required section is missing.
        """
        incomplete_config = dict(self.mock_config)
        del incomplete_config["provider"]
        temp_file_path: str = self.create_temp_yaml(incomplete_config)

        with self.assertRaises(ValueError) as context:
            load_config(temp_file_path)
        self.assertIn("failed schema validation", str(context.exception))

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

    @patch("src.utils.dynamic_loader.importlib.import_module")
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

    @patch("src.utils.dynamic_loader.importlib.import_module")
    @patch("src.utils.dynamic_loader.getattr")
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

    @patch("src.utils.dynamic_loader.load_class")
    def test_get_instance_valid(self, mock_load_class: MagicMock) -> None:
        """
        Test that `get_instance` correctly creates an instance of a dynamically loaded class.
        """
        mock_class = MagicMock()
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        mock_load_class.return_value = mock_class

        instance: Any = get_instance(self.mock_config, "loader", "class")

        mock_load_class.assert_called_once_with("src.infrastructure.csv_loader", "CSVLoader")
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

    @patch("src.utils.dynamic_loader.load_class")
    def test_get_instance_expected_port_satisfied(self, mock_load_class: MagicMock) -> None:
        """
        A class that structurally implements the expected port constructs
        without error.
        """
        from src.domain.ports import SymbolSourcePort

        class ConformingLoader:
            def __init__(self, config):
                self.config = config

            def load_symbols(self):
                return {}

        mock_load_class.return_value = ConformingLoader

        instance = get_instance(self.mock_config, "loader", "class", expected_port=SymbolSourcePort)
        self.assertIsInstance(instance, ConformingLoader)

    @patch("src.utils.dynamic_loader.load_class")
    def test_get_instance_expected_port_violation_raises_type_error(self, mock_load_class: MagicMock) -> None:
        """
        A class configured for a role but missing that role's required
        method(s) must fail at construction (here), not mid-pipeline with an
        AttributeError once the missing method is actually called.
        """
        from src.domain.ports import SymbolSourcePort

        class NonConformingLoader:
            def __init__(self, config):
                self.config = config
            # no load_symbols() -- violates SymbolSourcePort

        mock_load_class.return_value = NonConformingLoader

        with self.assertRaises(TypeError):
            get_instance(self.mock_config, "loader", "class", expected_port=SymbolSourcePort)

    def test_determine_date_range_uses_config_dates_without_repository(self) -> None:
        """
        When both dates are present in config, determine_date_range must not
        touch the database at all -- no OhlcvRepository should be built.
        """
        config = dict(self.mock_config)
        config["time_range"] = {"start_date": "2023-01-01", "end_date": "2023-01-02"}

        with patch("src.utils.dynamic_loader.OhlcvRepository") as mock_repo_cls:
            start_date, end_date = determine_date_range(config)

        mock_repo_cls.assert_not_called()
        self.assertEqual((start_date, end_date), ("2023-01-01", "2023-01-02"))

    @patch("src.utils.dynamic_loader.OhlcvRepository")
    def test_determine_date_range_falls_back_to_repository(self, mock_repo_cls: MagicMock) -> None:
        """
        With no start_date in config, determine_date_range should query the
        (consolidated) repository for the latest date and close it afterward.
        """
        config = dict(self.mock_config)
        config["time_range"] = {"end_date": "2023-01-05"}
        mock_repository = MagicMock()
        mock_repository.get_latest_date.return_value = "2023-01-01"
        mock_repo_cls.return_value = mock_repository

        start_date, end_date = determine_date_range(config)

        mock_repository.get_latest_date.assert_called_once()
        mock_repository.close.assert_called_once()
        self.assertEqual(start_date, "2023-01-02")
        self.assertEqual(end_date, "2023-01-05")


if __name__ == "__main__":
    unittest.main()
