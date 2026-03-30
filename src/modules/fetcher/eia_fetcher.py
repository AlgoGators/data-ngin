import os
import httpx
import pandas as pd
import logging
from typing import Dict, Any
from src.modules.fetcher.fetcher import Fetcher

class EIAFetcher(Fetcher):
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self.api_key = os.getenv("EIA_API_KEY")
        self.base_url = "https://api.eia.gov/v2"
        self.logger = logging.getLogger(__name__)

    async def fetch_data(self, symbol_metadata: Dict[str, Any], start_date: str, end_date: str) -> pd.DataFrame:
        symbol = symbol_metadata.get('dataSymbol')
        # Standardize data column (quantity for imports, value for others)
        data_col = symbol_metadata.get('dataCol', 'value')
        facet_id = symbol_metadata.get('facetId')
        facet_val = symbol_metadata.get('facetValue')
        
        url = f"{self.base_url}/{symbol}/data/"
        
        params = {
            "api_key": self.api_key,
            "frequency": "monthly",
            "data[0]": data_col,
            "start": start_date[:7], 
            "end": end_date[:7],
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "offset": 0,
            "length": 5000
        }

        # Add facets (like duoarea or originType)
        if pd.notnull(facet_id) and pd.notnull(facet_val) and str(facet_id).strip() != "":
            params[f"facets[{facet_id}][]"] = facet_val

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, params=params, timeout=120.0)
                
                if response.status_code != 200:
                    self.logger.error(f"EIA API Fail for {symbol}: Status {response.status_code} - {response.text}")
                    return pd.DataFrame()

                json_res = response.json()
                raw_data = json_res.get("response", {}).get("data", [])
                
                if not raw_data:
                    self.logger.warning(f"API returned 200 OK but 0 rows for {symbol}. Check your facets!")
                    return pd.DataFrame()

                df = pd.DataFrame(raw_data)

                # 1. Standardize core columns
                df.rename(columns={"period": "time", data_col: "value"}, inplace=True)
                
                # 2. Fix Timestamps for Postgres
                df["time"] = pd.to_datetime(df["time"]) 

                # 3. Standardize Value column
                df["value"] = pd.to_numeric(df["value"], errors='coerce')
                
                # 4. Add the Metadata/Instrument tag
                df["series_id"] = symbol_metadata.get('instrumentType', symbol)

                # 5. Clean up noise columns (desc, unit labels) to keep the table clean
                cols_to_drop = ['period-description', 'units', f"{data_col}-units"]
                df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

                # 6. RETURN FULL DATASET
                # We removed the .groupby() to preserve all dimensions like originName and gradeName.
                df = df.sort_values("time", ascending=False)
                
                return df

            except Exception as e:
                self.logger.error(f"Request Error for {symbol}: {e}")
                return pd.DataFrame()
            
if __name__ == "__main__":
    import asyncio
    from dotenv import load_dotenv
    
    # Load your .env file which should contain EIA_API_KEY
    load_dotenv()

    async def test_eia_fetcher():
        # 1. Initialize Fetcher with a dummy config
        config = {"database": {"target_schema": "eia"}}
        fetcher = EIAFetcher(config)

        # 2. Mock a row from your CSV for Natural Gas (Uses facets)
        ng_metadata = {
            "dataSymbol": "natural-gas/prod/sum",
            "instrumentType": "NG_PROD",
            "dataCol": "value",
            "facetId": "duoarea",
            "facetValue": "USA"
        }

        # 3. Mock a row for Crude Imports (No facets, uses 'quantity')
        crude_metadata = {
            "dataSymbol": "crude-oil-imports",
            "instrumentType": "IMPORTS",
            "dataCol": "quantity",
            "facetId": None,
            "facetValue": None
        }

        print("--- Testing Natural Gas Production (Filtered by USA) ---")
# Try a safe range from 2023 to ensure the API has finalized the data
        df_ng = await fetcher.fetch_data(ng_metadata, "2023-01", "2023-12")
        if not df_ng.empty:
            print(df_ng.head())
        else:
            print("Failed to fetch NG data. Checking API connection...")

        print("\n--- Testing Crude Oil Imports (Aggregated) ---")
        df_crude = await fetcher.fetch_data(crude_metadata, "2024-01", "2024-12")
        if not df_crude.empty:
            print(df_crude.head())
        else:
            print("Failed to fetch Crude data.")

    # Run the test
    asyncio.run(test_eia_fetcher())