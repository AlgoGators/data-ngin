#17

from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import yaml

def load_config() -> dict:
    """
    Load configuration from the config.yaml file.
    """
    with open("config.yaml", "r") as file:
        return yaml.safe_load(file)

def check_data_staleness_and_gaps() -> None:
    """
    Checks data staleness and gaps in the database.
    - Data staleness is checked by comparing the latest timestamp with the current time.
    - Gaps are checked by identifying missing time intervals between rows.
    """
    try:
        # Load the config
        config = load_config()
        
        # Extract relevant settings from the config
        database_config = config['database']
        table = database_config['raw_table']
        
        # Initialize Postgres hook with connection ID from the Airflow connection
        hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        
        # Check for the latest timestamp (for staleness check)
        query_latest = f"""
        SELECT MAX(timestamp_column)
        FROM {table};
        """
        latest_timestamp = hook.get_first(query_latest)
        
        if latest_timestamp:
            latest_timestamp = latest_timestamp[0]
            logging.info(f"Latest data timestamp: {latest_timestamp}")
            
            # Check if the data is stale (comparing with the current time or configured time range)
            current_time = datetime.now()
            time_diff = current_time - latest_timestamp
            logging.info(f"Data staleness check - Time difference: {time_diff}")
            
            if time_diff > timedelta(days=1):  # If the data is older than 1 day
                logging.warning("Data is stale.")
            else:
                logging.info("Data is not stale.")
        else:
            logging.warning("No data found in the database.")
        
        # Check for gaps in data
        # Assuming the data is stored in daily frequency
        query_gaps = f"""
        SELECT timestamp_column
        FROM {table}
        ORDER BY timestamp_column;
        """
        
        data = hook.get_records(query_gaps)
        
        if data:
            # Iterate through the results and check for gaps
            previous_timestamp = None
            for row in data:
                current_timestamp = row[0]
                if previous_timestamp and (current_timestamp - previous_timestamp > timedelta(days=1)):
                    logging.warning(f"Data gap detected between {previous_timestamp} and {current_timestamp}")
                previous_timestamp = current_timestamp
        else:
            logging.warning("No records found to check for gaps.")
    except Exception as e:
        logging.error(f"Error during data staleness and gap check: {e}")
        raise
