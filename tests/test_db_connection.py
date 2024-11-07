import unittest
import yaml
import psycopg2
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

class TestDatabaseConnection(unittest.TestCase):
    """
    Unit tests for verifying database connection and existence of required schemas and tables.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """
        Set up database connection and load configuration.
        This connection will be shared across all test methods.
        """
        config = load_config()
        db_config = config["database"]
        
        # Establish a database connection
        cls.conn = psycopg2.connect(
            dbname=db_config["db_name"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Close the database connection after all tests are completed.
        """
        cls.conn.close()

    def test_schema_exists(self) -> None:
        """
        Test to check if the 'futures_data' schema exists in the database.
        """
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'futures_data';")
            result = cursor.fetchone()
            self.assertIsNotNone(result, "Schema 'futures_data' does not exist")

    def test_table_exists(self) -> None:
        """
        Test to check if the 'ohlcv_1d' table exists in the 'futures_data' schema.
        """
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'futures_data' AND table_name = 'ohlcv_1d';")
            result = cursor.fetchone()
            self.assertIsNotNone(result, "Table 'futures_data.ohlcv_1d' does not exist")

    def test_table_is_hypertable(self) -> None:
        """
        Test to verify if 'futures_data.ohlcv_1d' is configured as a hypertable in TimescaleDB.
        """
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT * FROM timescaledb_information.hypertables WHERE hypertable_schema = 'futures_data' AND hypertable_name = 'ohlcv_1d';")
            result = cursor.fetchone()
            self.assertIsNotNone(result, "'futures_data.ohlcv_1d' is not configured as a hypertable")

if __name__ == "__main__":
    unittest.main()
