import os
import secrets
import psycopg2
from app.db import get_connection

def generate_api_key(user_name: str) -> str:
    """
    Generates a new API key for the specified user and stores it in the database.

    Args:
        user_name (str): The name of the user.

    Returns:
        str: The newly generated API key.
    """
    api_key = secrets.token_urlsafe(32)  # Generate a secure 32-character API key
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO admin.api_keys (user_name, api_key)
                VALUES (%s, %s)
                RETURNING api_key
                """,
                (user_name, api_key),
            )
            conn.commit()
            print(f"API key for {user_name}: {api_key}")
    except Exception as e:
        print(f"Error generating API key: {e}")
    finally:
        if conn:
            conn.close()
    return api_key

if __name__ == "__main__":
    user_name = input("Enter the user name to generate an API key for: ")
    if user_name:
        key = generate_api_key(user_name)
        print(f"API key: {key}")
    else:
        print("API_USER environment variable not set.")