import unittest
import yaml
import os
from typing import Dict, Any

def load_config() -> Dict[str, Any]:
    """
    Loads the configuration settings from the config.yaml file.

    Returns:
        Dict[str, Any]: The configuration settings as a dictionary.

    Raises:
        FileNotFoundError: If the configuration file is not found.
        yaml.YAMLError: If there is an error parsing the YAML file.
    """
    config_path = os.path.join("data", "config", "config.yaml")
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    return config

class TestConfig(unittest.TestCase):
    """
    Unit tests for verifying essential fields in the configuration file.
    """
    
    @classmethod
    def setUpClass(cls) -> None:
        """
        Load the configuration once for all tests.
        """
        cls.config = load_config()
    
    def test_database_section_exists(self) -> None:
        """Test that the 'database' section exists in the configuration."""
        self.assertIn("database", self.config, "Database settings missing from config")
    
    def test_database_fields(self) -> None:
        """Test that essential fields exist in the 'database' configuration section."""
        db = self.config["database"]
        self.assertIn("host", db, "Database host missing")
        self.assertIn("port", db, "Database port missing")
        # self.assertIn("user", db, "Database user missing")  # Uncomment if 'user' is required
        self.assertIn("password", db, "Database password missing")
        self.assertIn("db_name", db, "Database name missing")

    def test_providers_section_exists(self) -> None:
        """Test that the 'providers' section exists in the configuration."""
        self.assertIn("providers", self.config, "Providers section missing from config")

    def test_databento_provider_exists(self) -> None:
        """Test that the 'databento' provider exists in the 'providers' section."""
        self.assertIn("databento", self.config["providers"], "Databento provider missing from config")

    def test_databento_fields(self) -> None:
        """Test that essential fields exist in the 'databento' provider configuration."""
        databento = self.config["providers"]["databento"]
        self.assertIn("api_key", databento, "Databento API key missing")
        self.assertIn("datasets", databento, "Databento datasets missing")
        self.assertIn("GLOBEX", databento["datasets"], "GLOBEX dataset missing for Databento")
        self.assertIn("aggregation_levels", databento["datasets"]["GLOBEX"], "Aggregation levels missing for GLOBEX")

if __name__ == "__main__":
    unittest.main()
