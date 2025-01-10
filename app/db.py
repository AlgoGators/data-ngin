import os
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_connection() -> connection:
    """
    Establishes a connection to the PostgreSQL database using credentials from the .env file.

    Returns:
        connection (psycopg2.extensions.connection): A connection object for interacting with the database.

    Raises:
        ConnectionError: If the connection to the database fails.
    """
    try:
        # Connect to the PostgreSQL database using environment variables
        conn: connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            cursor_factory=DictCursor  # Use DictCursor to return query results as dictionaries
        )
        return conn
    except psycopg2.OperationalError as e:
        # Raise a more descriptive error if connection fails
        raise ConnectionError(f"Failed to connect to the database: {e}")

# Testing script to verify database connectivity
if __name__ == "__main__":
    try:
        conn: connection = get_connection()
        print("Database connection successful!")  # Output if connection is established
        conn.close()  # Close the connection when done
    except Exception as e:
        print(f"Error connecting to the database: {e}")  # Output the error if connection fails
