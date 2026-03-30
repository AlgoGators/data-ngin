import pandas as pd
import os
import logging
from typing import Dict, Any
from src.modules.loader.loader import Loader

class EIALoader(Loader):
    """
    A specialized Loader for EIA contracts that preserves all metadata 
    columns needed for API facets and data column mapping.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config=config)
        try:
            self.contract_path: str = config["loader"]["file_path"]
        except KeyError as e:
            logging.error(f"Missing required configuration key: {e}")
            raise KeyError(f"Missing required configuration key: {e}")

    def load_symbols(self) -> list:
        """
        Loads symbols as a list of dictionaries so each object 
        contains its own 'dataSymbol' and metadata.
        """
        if not os.path.exists(self.contract_path):
            logging.error(f"EIA Contract file not found at {self.contract_path}")
            raise FileNotFoundError(f"Contract file not found at {self.contract_path}")

        try:
            # 1. Read the CSV
            df = pd.read_csv(self.contract_path)
            
            # 2. Clean up whitespace and handle NaN values
            # Filling NaNs with None makes them easier to handle in the Fetcher logic
            df = df.where(pd.notnull(df), None)
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            
            # 3. Convert to a list of records
            # This results in: [{'dataSymbol': 'crude-oil-imports', 'dataCol': 'quantity', ...}, ...]
            symbol_list = df.to_dict(orient="records")

            logging.info(f"EIALoader successfully loaded {len(symbol_list)} complex symbols.")
            return symbol_list

        except Exception as e:
            logging.error(f"Failed to load EIA symbols: {e}")
            raise ValueError(f"Error processing {self.contract_path}: {e}")