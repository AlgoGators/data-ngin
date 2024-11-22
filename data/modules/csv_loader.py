import pandas as pd
import os
import logging
from typing import Dict, Any


class CSVLoader:
    """
    A Loader subclass that reads symbols and their asset types from a CSV file and validates them
    against the configuration settings.

    Attributes:
        contract_path (str): Path to the CSV file containing contract symbols and asset types.
        REQUIRED_COLUMNS (Dict[str, str]): Required column names in the CSV.
    """

    # Define expected column names as constants
    REQUIRED_COLUMNS: Dict[str, str] = {"dataSymbol": "Symbol identifier", "instrumentType": "Asset type"}

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the CSVLoader with paths to the configuration file and the CSV file.

        Args:
            config (Dict[str, Any]): Configuration settings.

        Raises:
            KeyError: If the 'file_path' key is missing from the configuration.
        """
        try:
            self.contract_path: str = config["loader"]["file_path"]
        except KeyError as e:
            logging.error(f"Missing required configuration key: {e}")
            raise KeyError(f"Missing required configuration key: {e}")

    def load_symbols(self) -> Dict[str, str]:
        """
        Loads symbols and their associated asset types from the CSV file specified in contract_path.

        Returns:
            Dict[str, str]: A dictionary where keys are symbols and values are asset types.

        Raises:
            FileNotFoundError: If the specified CSV file does not exist.
            ValueError: If required columns are missing or if the CSV has invalid content.
        """
        # Check if the CSV file exists
        if not os.path.exists(self.contract_path):
            logging.error(f"Contract file not found at {self.contract_path}")
            raise FileNotFoundError(f"Contract file not found at {self.contract_path}")

        # Read the CSV file
        try:
            contracts_df: pd.DataFrame = pd.read_csv(self.contract_path)
        except pd.errors.EmptyDataError:
            logging.error(f"The CSV file at {self.contract_path} is empty or unreadable.")
            raise ValueError(f"The CSV file at {self.contract_path} is empty or unreadable.")
        except pd.errors.ParserError as e:
            logging.error(f"Malformed CSV file at {self.contract_path}: {e}")
            raise ValueError(f"Malformed CSV file at {self.contract_path}: {e}")

        # Validate required columns
        missing_columns = [col for col in self.REQUIRED_COLUMNS if col not in contracts_df.columns]
        if missing_columns:
            logging.error(
                f"CSV file must contain the following columns: {list(self.REQUIRED_COLUMNS.keys())}. "
                f"Missing columns: {missing_columns}"
            )
            raise ValueError(
                f"CSV file must contain the following columns: {list(self.REQUIRED_COLUMNS.keys())}. "
                f"Missing columns: {missing_columns}"
            )

        # Ensure no duplicates or null values in 'dataSymbol'
        if contracts_df["dataSymbol"].duplicated().any():
            logging.error("Duplicate symbols found in 'dataSymbol' column.")
            raise ValueError("Duplicate symbols found in 'dataSymbol' column.")
        if contracts_df["dataSymbol"].isnull().any():
            logging.error("Null values found in 'dataSymbol' column.")
            raise ValueError("Null values found in 'dataSymbol' column.")

        # Normalize data: Strip whitespace and ensure consistent casing
        contracts_df["dataSymbol"] = contracts_df["dataSymbol"].str.strip()
        contracts_df["instrumentType"] = contracts_df["instrumentType"].str.upper()

        # Create a dictionary mapping symbols to their asset types
        symbol_dict: Dict[str, str] = dict(zip(contracts_df["dataSymbol"], contracts_df["instrumentType"]))

        logging.info(f"Successfully loaded {len(symbol_dict)} symbols from {self.contract_path}.")
        return symbol_dict
