import pandas as pd
import logging
from typing import Dict, Any
from src.modules.cleaner.cleaner import Cleaner

class EIACleaner(Cleaner):
    def __init__(self, config: Dict[str, Any]) -> None:
        # Call the base class init (which takes no arguments)
        super().__init__() 
        
        # Manually assign the config and logger to this instance
        self.config = config
        self.logger = logging.getLogger("EIACleaner")

    def validate_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        # Normalize names to match the Fetcher's output
        # Fetcher already outputs 'time', 'value', and 'series_id'
        rename_map = {"period": "time", "quantity": "value", "area-name": "series_id", "symbol": "series_id"}
        for old_col, new_col in rename_map.items():
            if old_col in data.columns and new_col not in data.columns:
                data = data.rename(columns={old_col: new_col})

        required = ["time", "value"]
        missing = [col for col in required if col not in data.columns]
        
        if missing:
            self.logger.error(f"EIA Data missing required fields: {missing}")
            raise ValueError(f"Missing required fields: {missing}")
            
        return data

    def handle_missing_data(self, data: pd.DataFrame) -> pd.DataFrame:
        # CRITICAL: Data MUST be sorted by time (ascending) for ffill to work correctly
        data = data.sort_values(by="time", ascending=True)

        if data["value"].isnull().any():
            method = self.config.get("missing_data", {}).get("method", "forward_fill")
            
            # Use series_id to ensure we don't leak data across different instruments/regions
            if "series_id" in data.columns:
                if method == "forward_fill":
                    data["value"] = data.groupby("series_id")["value"].ffill()
                elif method == "drop_nan":
                    data = data.dropna(subset=["value"])
                elif method == "zero_fill":
                    data["value"] = data.groupby("series_id")["value"].fillna(0)
            else:
                if method == "forward_fill":
                    data["value"] = data["value"].ffill()
                elif method == "drop_nan":
                    data = data.dropna(subset=["value"])
                elif method == "zero_fill":
                    data["value"] = data["value"].fillna(0)
                
        return data

    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        # 1. Convert core types
        data["time"] = pd.to_datetime(data["time"])
        data["value"] = pd.to_numeric(data["value"], errors='coerce')
        
        # 2. Final sort
        data = data.sort_values(by=["time"]).reset_index(drop=True)
        
        # 3. DYNAMIC COLUMN PRESERVATION
        # Instead of hardcoding what to keep, we keep EVERYTHING.
        # We just ensure the core columns exist.
        required_cols = ["time", "value", "series_id"]
        for col in required_cols:
            if col not in data.columns:
                self.logger.warning(f"Cleaner: Expected core column '{col}' missing!")

        # Return the whole DataFrame so 'originName', 'gradeName', etc. stay alive
        return data

# --- Test Block ---
if __name__ == "__main__":
    # Simulate data coming out of our EIAFetcher
    test_data = pd.DataFrame({
        "time": ["2024-01", "2024-01", "2024-02"],
        "series_id": ["Arkansas", "Colorado", "Colorado"],
        "value": [100.0, 200.0, None] 
    })

    config = {"missing_data": {"method": "forward_fill"}}
    cleaner = EIACleaner(config)

    try:
        # Use the base class 'clean' method which calls our overrides
        cleaned_df = cleaner.clean(test_data)
        print("--- Cleaned Data Output ---")
        print(cleaned_df)
        
        val = cleaned_df.iloc[2]['value']
        if val == 200.0:
            print("\n✅ SUCCESS: Colorado's Feb value filled from Jan.")
    except Exception as e:
        print(f"Cleanup Failed: {e}")