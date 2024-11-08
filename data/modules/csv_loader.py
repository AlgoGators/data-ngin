import pandas as pd
import os
from typing import Dict
from data.modules.loader import Loader

class CSVLoader(Loader):
    """
    A Loader subclass that reads symbols and their asset types from a CSV file and validates them
    against the configuration settings. This loader is specific to CSV input sources.
    
    Attributes:
        contract_path (str): Path to the CSV file containing contract symbols and asset types.
    """
    
    def __init__(self, config_path: str = 'config/config.yaml', contract_path: str = 'contracts/contract.csv') -> None:
        """
        Initializes the CSVLoader with paths to the configuration file and the CSV file.
        
        Args:
            config_path (str): Path to the configuration file. Defaults to 'config/config.yaml'.
            contract_path (str): Path to the CSV file with symbols and asset types. Defaults to 'contracts/contract.csv'.
        """
        super().__init__(config_path)
        self.contract_path: str = contract_path

    def load_symbols(self) -> Dict[str, str]:
        """
        Loads symbols and their associated asset types from the CSV file specified in contract_path.
        
        Returns:
            Dict[str, str]: A dictionary where keys are symbols and values are asset types.
        
        Raises:
            FileNotFoundError: If the specified CSV file does not exist.
            ValueError: If required columns ('dataSymbol', 'instrumentType') are missing in the CSV.
        """
        if not os.path.exists(self.contract_path):
            raise FileNotFoundError(f"Contract file not found at {self.contract_path}")
        
        # Read symbols and asset types from CSV, assuming columns are 'dataSymbol' and 'instrumentType'
        contracts_df: pd.DataFrame = pd.read_csv(self.contract_path)
        
        if 'dataSymbol' not in contracts_df.columns or 'instrumentType' not in contracts_df.columns:
            raise ValueError("CSV file must contain 'dataSymbol' and 'instrumentType' columns.")
        
        # Create a dictionary mapping symbol to asset type
        return dict(zip(contracts_df['dataSymbol'], contracts_df['instrumentType']))
