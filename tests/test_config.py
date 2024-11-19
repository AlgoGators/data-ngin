import unittest
import yaml
import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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
    
    def test_providers_section_exists(self) -> None:
        """Test that the 'providers' section exists in the configuration."""
        self.assertIn("providers", self.config, "Providers section missing from config")

    def test_databento_provider_exists(self) -> None:
        """Test that the 'databento' provider exists in the 'providers' section."""
        self.assertIn("databento", self.config["providers"], "Databento provider missing from config")

    def test_databento_fields(self) -> None:
        """Test that essential fields exist in the 'databento' provider configuration."""
        databento = self.config["providers"]["databento"]
        self.assertIn("supported_assets", databento, "Supported assets missing for Databento")
        self.assertIn("datasets", databento, "Databento datasets missing")
        self.assertIn("GLOBEX", databento["datasets"], "GLOBEX dataset missing for Databento")
        self.assertIn("aggregation_levels", databento["datasets"]["GLOBEX"], "Aggregation levels missing for GLOBEX")
        self.assertIn("schema_name", databento["datasets"]["GLOBEX"], "Schema name missing for GLOBEX dataset")

    def test_database_section_exists(self) -> None:
        """Test that the 'database' section exists in the configuration."""
        self.assertIn("database", self.config, "Database settings missing from config")

    def test_database_fields(self) -> None:
        """Test that essential fields exist in the 'database' configuration section."""
        db = self.config["database"]
        self.assertIn("table_prefix", db, "Database table prefix missing")
        self.assertIn("aggregation_levels", db, "Database aggregation levels missing")

class TestEnvVariables(unittest.TestCase):
    """
    Unit tests to verify that all required environment variables are loaded.
    """

    def test_databento_api_key_exists(self) -> None:
        """Test that the Databento API key is set in the environment."""
        self.assertTrue(os.getenv("DATABENTO_API_KEY"), "Databento API key missing in .env")

    def test_database_credentials_exist(self) -> None:
        """Test that database credentials are set in the environment."""
        self.assertTrue(os.getenv("DB_HOST"), "Database host missing in .env")
        self.assertTrue(os.getenv("DB_PORT"), "Database port missing in .env")
        self.assertTrue(os.getenv("DB_USER"), "Database user missing in .env")
        self.assertTrue(os.getenv("DB_PASSWORD"), "Database password missing in .env")
        self.assertTrue(os.getenv("DB_NAME"), "Database name missing in .env")

if __name__ == "__main__":
    unittest.main()
