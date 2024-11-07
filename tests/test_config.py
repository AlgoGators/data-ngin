import yaml
import os

def load_config():
    config_path = os.path.join("data\config", "config.yaml")
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def test_config():
    config = load_config()
    
    assert "database" in config, "Database settings missing from config"
    assert "providers" in config, "Providers section missing from config"
    assert "databento" in config["providers"], "Databento provider missing from config"
    
    # Database connection fields
    db = config["database"]
    assert "host" in db, "Database host missing"
    assert "port" in db, "Database port missing"
    # assert "user" in db, "Database user missing"
    assert "password" in db, "Database password missing"
    assert "db_name" in db, "Database name missing"

    # Databento fields
    databento = config["providers"]["databento"]
    assert "api_key" in databento, "Databento API key missing"
    assert "datasets" in databento, "Databento datasets missing"
    assert "GLOBEX" in databento["datasets"], "GLOBEX dataset missing for Databento"
    assert "aggregation_levels" in databento["datasets"]["GLOBEX"], "Aggregation levels missing for GLOBEX"
    
    print("Config tests passed!")

if __name__ == "__main__":
    test_config()
