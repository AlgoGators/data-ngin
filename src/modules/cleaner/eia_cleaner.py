import pandas as pd
import logging
from typing import Dict, Any, List
from src.modules.cleaner.cleaner import Cleaner

class EIACleaner(Cleaner):
    """
    A Cleaner subclass for standardizing EIA (Energy Information Administration) data.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Args:
            config (Dict[str, Any]): Configuration settings, including 
                                     'missing_data_method' (ffill, drop, etc.)
        """
        self.config = config
        self.logger = logging.getLogger("EIACleaner")

    def validate_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Ensures 'time' and 'value' (or EIA's 'quantity') are present.
        """
        # EIA often uses 'period' or 'quantity' - we normalize them here
        if "period" in data.columns:
            data = data.rename(columns={"period": "time"})
        
        if "quantity" in data.columns:
            data = data.rename(columns={"quantity": "value"})

        required = ["time", "value"]
        missing = [col for col in required if col not in data.columns]
        
        if missing:
            self.logger.error(f"EIA Data missing required fields: {missing}")
            raise ValueError(f"Missing required fields: {missing}")
            
        return data

    def handle_missing_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Handles nulls in the 'value' column based on the config.
        """
        if data["value"].isnull().any():
            # Default to forward fill for fundamental data (last known stock level)
            method = self.config.get("missing_data", {}).get("method", "forward_fill")
            
            if method == "forward_fill":
                data["value"] = data["value"].ffill()
            elif method == "drop_nan":
                data = data.dropna(subset=["value"])
            elif method == "zero_fill":
                data["value"] = data["value"].fillna(0)
                
        return data

    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Standardizes timestamps and ensures numeric types.
        """
        # Convert timestamps (handles YYYY-MM and YYYY-MM-DD)
        data["time"] = pd.to_datetime(data["time"])
        
        # Ensure value is numeric
        data["value"] = pd.to_numeric(data["value"], errors='coerce')
        
        # Sort and clean up index
        data = data.sort_values(by="time").reset_index(drop=True)
        
        # Only return the columns needed for the database
        cols_to_keep = ["time", "value"]
        if "symbol" in data.columns:
            cols_to_keep.append("symbol")
            
        return data[cols_to_keep]
    

if __name__ == "__main__":
# 1. Setup mock data with a missing week
    raw_data = pd.DataFrame({
    "period": ["2024-01-03", "2024-01-10", "2024-01-24"], # Missing Jan 17th
    "quantity": [100.5, None, 105.2] # Has a None value
})

config = {"missing_data": {"method": "forward_fill"}}
cleaner = EIACleaner(config)

# 2. Run the clean() method (orchestrates validate -> handle -> transform)
cleaned_df = cleaner.clean(raw_data)
print("--- Cleaned Data ---")
print(cleaned_df)

# 3. Detect Gaps (using the base class method)
# 'W-WED' is the frequency for EIA Weekly Reports
gaps = cleaner.detect_time_gaps(cleaned_df, "time", freq="W-WED")

if not gaps.empty:
    print(f"\n--- Gaps Detected ---")
    print(gaps)
    cleaner.log_missing_data(gaps)