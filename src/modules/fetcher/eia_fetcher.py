import os
import httpx
import pandas as pd
from dotenv import load_dotenv
from typing import Dict, Any
from src.modules.fetcher.fetcher import Fetcher
from src.modules.loader.csv_loader import CSVLoader

class EIAFetcher(Fetcher):
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        load_dotenv()
        self.api_key: str = os.getenv("EIA_API_KEY")
        
        if not self.api_key:
            self.logger.error("EIA_API_KEY not found in .env file.")
            raise ValueError("Missing EIA API Key")

    async def fetch_data(self, symbol: str, asset_type: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Final, bug-fixed version for Deadline 1.
        Handles Imports (Monthly) and Futures (Daily) dynamically.
        """
        # 1. Setup variables based on the type of data
        if asset_type == "IMPORTS":
            route = "crude-oil-imports"
            frequency = "monthly"
            data_col = "quantity"
        elif asset_type == "FUTURE":
            route = "petroleum/pri/fut"
            frequency = "daily"
            data_col = "value"
        else:
            # Fallback for generic reports
            route = "petroleum/stoc/wst"
            frequency = "weekly"
            data_col = "value"

        url = f"https://api.eia.gov/v2/{route}/data/"
        
        # 2. Build the params
        params = {
            "api_key": self.api_key,
            "frequency": frequency,
            "data[0]": data_col,
            "start": start_date, 
            "end": end_date,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "offset": 0,
            "length": 5000
        }

        # Imports use a different structure and don't require the series facet
        if asset_type != "IMPORTS":
            params["facets[series][]"] = symbol

        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, timeout=30.0)
            
            if response.status_code != 200:
                print(f"❌ EIA API Error Detail: {response.text}")
                response.raise_for_status()
                
            data = response.json()

        records = data.get("response", {}).get("data", [])
        df = pd.DataFrame(records)

        if not df.empty:
            # Standardize naming to 'time' and 'value'
            df.rename(columns={"period": "time", data_col: "value"}, inplace=True)
            df["value"] = pd.to_numeric(df["value"], errors='coerce')

            # --- AGGREGATION STEP ---
            # If it's Imports, sum up all rows (countries/ports) to get one total per month
            if asset_type == "IMPORTS":
                df = df.groupby("time")["value"].sum().reset_index()
                # Ensure the time is still sorted descending after grouping
                df = df.sort_values("time", ascending=False)
            
        return df

if __name__ == "__main__":
    import asyncio

    async def test_full_flow():
        config = {
            "loader": {"file_path": "src/modules/fetcher/eia_contracts.csv"}
        }

        try:
            print("--- Step 1: Loading Symbols via CSVLoader ---")
            loader = CSVLoader(config)
            symbols_to_fetch = loader.load_symbols() 
            
            print("--- Step 2: Fetching Data from EIA ---")
            fetcher = EIAFetcher(config)
            
            for symbol, asset_type in symbols_to_fetch.items():
                # Set date formats: Monthly for Imports, Daily for others
                s_date = "2024-01" if asset_type == "IMPORTS" else "2025-10-01"
                e_date = "2024-12" if asset_type == "IMPORTS" else "2025-12-31"

                print(f"Fetching {symbol} ({asset_type})...")
                
                df = await fetcher.fetch_data(
                    symbol=symbol, 
                    asset_type=asset_type, 
                    start_date=s_date, 
                    end_date=e_date
                )

                if not df.empty:
                    print(f"✅ Success! Loaded {len(df)} aggregated rows for {symbol}")
                    # Show the final clean time-series
                    print(df[['time', 'value']].head(12))
                else:
                    print(f"❌ No data found for {symbol}")

        except Exception as e:
            print(f"❌ Pipeline Error: {e}")

    asyncio.run(test_full_flow())