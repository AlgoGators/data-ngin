import pandas as pd
from enum import Enum
from typing import Dict, Any
from data.modules.cleaner import Cleaner

class RequiredFields(Enum):
    """
    Enum representing the required fields for financial market data.

    Attributes:
        TIME (str): The timestamp of the data row.
        OPEN (str): The opening price.
        HIGH (str): The highest price.
        LOW (str): The lowest price.
        CLOSE (str): The closing price.
        VOLUME (str): The trading volume.
    """
    TIME = "time"
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOLUME = "volume"

class DatabentoCleaner(Cleaner):
    """
    A Cleaner subclass for standardizing Databento data.

    Methods:
        validate_fields: Ensure required fields are present in Databento data.
        handle_missing_data: Handle missing or corrupt data.
        transform_data: Standardize timestamps and field types.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the DatabentoCleaner with a configuration.

        Args:
            config (Dict[str, Any]): Configuration settings, including rules for missing data handling.
        """
        self.config = config

    def validate_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Validates required fields are present in the raw data.

        Args:
            data (pd.DataFrame): The raw data to validate.

        Returns:
            pd.DataFrame: The validated data.

        Raises:
            ValueError: If required fields are missing.
        """
        required_fields = [field.value for field in RequiredFields]
        missing_fields = [field for field in required_fields if field not in data.columns]
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")
        return data

    def handle_missing_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Handles missing or corrupt data by either dropping rows or filling values.

        Args:
            data (pd.DataFrame): The raw data with potential missing values.

        Returns:
            pd.DataFrame: The data after handling missing values.
        """
        # Drop rows with any missing values
        if self.config.get("drop_missing", True):
            data = data.dropna()
        else:
            # Fill missing values with default value of 0
            data = data.fillna({
                RequiredFields.OPEN.value: 0.0,
                RequiredFields.HIGH.value: 0.0,
                RequiredFields.LOW.value: 0.0,
                RequiredFields.CLOSE.value: 0.0,
                RequiredFields.VOLUME.value: 0
            })
        return data

    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms raw data into a standardized format.

        Args:
            data (pd.DataFrame): The raw data to transform.

        Returns:
            pd.DataFrame: The transformed, standardized data.
        """
        # Rename columns if needed
        if "date" in data.columns:
            data = data.rename(columns={"date": "time"})

        # Convert timestamps to UTC
        data["time"] = pd.to_datetime(data["time"]).dt.tz_localize("UTC")

        # Ensure correct data types
        data["open"] = data["open"].astype(float)
        data["high"] = data["high"].astype(float)
        data["low"] = data["low"].astype(float)
        data["close"] = data["close"].astype(float)
        data["volume"] = data["volume"].astype(int)

        # Sort the data by the time column
        data = data.sort_values(by="time")
        return data
