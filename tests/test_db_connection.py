import yaml
import psycopg2
import os

def load_config():
    config_path = os.path.join("data\config", "config.yaml")
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def test_db_connection():
    config = load_config()
    db_config = config["database"]
    
    # Connect to the database
    conn = psycopg2.connect(
        dbname=db_config["db_name"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )
    
    # Check for schemas and tables
    with conn.cursor() as cursor:
        # Check if the schema exists
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'futures_data';")
        assert cursor.fetchone() is not None, "Schema 'futures_data' does not exist"
        
        # Check if the table exists and is a hypertable
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'futures_data' AND table_name = 'ohlcv_1d';")
        assert cursor.fetchone() is not None, "Table 'futures_data.ohlcv_1d' does not exist"
        
        # Verify it’s a hypertable
        cursor.execute("SELECT * FROM timescaledb_information.hypertables WHERE hypertable_schema = 'futures_data' AND hypertable_name = 'ohlcv_1d';")
        assert cursor.fetchone() is not None, "'futures_data.ohlcv_1d' is not configured as a hypertable"

    print("Database setup tests passed!")
    conn.close()

if __name__ == "__main__":
    test_db_connection()
