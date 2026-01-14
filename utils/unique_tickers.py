import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def export_tickers_to_file(filename:str):
    conn = None #better initialization
    try:
      # Establish Connection
        conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
        )

        # Create a Cursor
        cur = conn.cursor()
        # Query to fetch unique tickers
        print("Querying unique tickers from database...")
        query = "SELECT DISTINCT ticker FROM equities_data.ohlcv_1d ORDER BY ticker;"
        cur.execute(query)

        # Extract the first element of each tuple into a list
        tickers = [row[0] for row in cur.fetchall()]

        # Save to a text file
        with open(filename, "w") as f:
            for ticker in tickers:
                f.write(f"{ticker}\n")

        print(f"Successfully saved {len(tickers)} tickers to {filename}")

    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    export_tickers_to_file("unique_tickers.txt")