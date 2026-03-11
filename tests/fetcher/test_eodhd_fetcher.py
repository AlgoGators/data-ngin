"""
Quick smoke test for EODHDFetcher.
Run this directly: python test_eodhd_fetcher.py

Does NOT require the database or cleaner to be set up yet (Deadline 1 scope).
Make sure EODHD_API_KEY is set in your .env file.
"""
import asyncio
import logging
from src.modules.fetcher.eodhd_fetcher import EODHDFetcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

MOCK_CONFIG = {
    "fetcher": {
        "batch_size_days": 30
    }
}

async def main():
    fetcher = EODHDFetcher(config=MOCK_CONFIG)

    # Free tier demo symbols that EODHD guarantees work without a paid plan
    test_cases = [
        ("AAPL.US", "EQUITY", "2024-01-01", "2024-02-01"),
        ("MSFT.US", "EQUITY", "2024-01-01", "2024-02-01"),
    ]

    for symbol, asset_type, start, end in test_cases:
        print(f"\n--- Fetching {symbol} ---")
        df = await fetcher.fetch_data(
            symbol=symbol,
            loaded_asset_type=asset_type,
            start_date=start,
            end_date=end,
        )
        if df.empty:
            print(f"WARNING: No data returned for {symbol}")
        else:
            print(df.head())
            print(f"Shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")

if __name__ == "__main__":
    asyncio.run(main())
