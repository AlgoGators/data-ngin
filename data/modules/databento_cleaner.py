import pandas as pd
from enum import Enum
from typing import Dict, Any, List
from data.modules.cleaner import Cleaner
import logging


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
        clean: Main entry point to validate, transform, and standardize raw data. 
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
        self.config: Dict[str, Any] = config
        self.logger: logging.Logger = logging.getLogger("DatabentoCleaner")

    def clean(self, data: pd.DataFrame) -> List[Dict[str, any]]:
        """
        Cleans and standardizes raw Databento data.

        Args:
            data (pd.DataFrame): Raw data from Databento API.

        Returns:
            List[Dict[str, Any]]: Cleaned data ready for insertion.

        Raises:
            ValueError: If data is empty or required fields are missing.
        """
        if data.empty:
            self.logger.error("Received empty DataFrame for cleaning.")
            raise ValueError("DataFrame is empty. Cannot clean data.")

        # Validate required fields
        self.logger.info("Validating required fields.")
        data = self.validate_fields(data)

        # Handle missing or corrupt data
        self.logger.info("Handling missing or corrupt data.")
        data = self.handle_missing_data(data)

        # Transform the data into the desired format
        self.logger.info("Transforming data.")
        data = self.transform_data(data)

        # Convert to a list of dictionaries for database insertion
        cleaned_data: List[Dict[str, Any]] = data.to_dict(orient="records")
        return cleaned_data

    

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

        # Reset index if needed
        if "ts_event" in data.index.names:
            data = data.reset_index()

        # Rename columns if needed
        if "ts_event" in data.columns:
            data: pd.DataFrame = data.rename(columns={"ts_event": "time"})

        required_fields: List[str] = [field.value for field in RequiredFields]
        missing_fields: List[str] = [field for field in required_fields if field not in data.columns]
        if missing_fields:
            logging.error(f"Missing required fields: {missing_fields}")
            raise ValueError(f"Missing required fields: {missing_fields}")
        return data

    def handle_missing_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Handles missing or corrupt data based on the configuration.

        Supported Methods:
            - forward_fill: Fill missing data forward.
            - backward_fill: Fill missing data backward.
            - interpolate: Interpolate missing data.
            - drop_nan: Drop rows with missing data.
            - zero_fill: Fill missing values with zeros.
            - mean_fill: Fill missing values with the column mean.
            - median_fill: Fill missing values with the column median.
            - custom_fill: Fill missing values with a custom value from the config.

        Args:
            data (pd.DataFrame): The raw data with potential missing values.

        Returns:
            pd.DataFrame: The data after handling missing values.
        """
        method_switch = {
            "drop_nan": lambda d: d.dropna(),
            "forward_fill": lambda d: d.fillna(method="ffill"),
            "backward_fill": lambda d: d.fillna(method="bfill"),
            "interpolate": lambda d: d.interpolate(),
            "zero_fill": lambda d: d.fillna(0),
            "mean_fill": lambda d: d.fillna(d.mean()),
            "median_fill": lambda d: d.fillna(d.median()),
            "custom_fill": lambda d: d.fillna(self.config["missing_data"].get("custom_value", 0)),
        }

        for method, action in method_switch.items():
            if self.config.get("missing_data", {}).get(method, "False") == "True":
                logging.info(f"Applying {method.replace('_', ' ')}.")
                data = action(data)

        return data

    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms raw data into a standardized format.

        Args:
            data (pd.DataFrame): The raw data to transform.

        Returns:
            pd.DataFrame: The transformed, standardized data.
        """
        logging.info("Starting data transformation.")

        # Convert timestamps to UTC
        logging.info("Converting timestamps to UTC.")
        if isinstance(data["time"].dtype, pd.DatetimeTZDtype):
            # If already tz-aware, use tz_convert
            data["time"] = data["time"].dt.tz_convert("UTC")
        else:
            # If not tz-aware, localize to UTC
            data["time"] = pd.to_datetime(data["time"]).dt.tz_localize("UTC")

        # Ensure correct data types
        logging.info("Converting data types for price and volume columns.")
        data["open"] = data["open"].astype(float)
        data["high"] = data["high"].astype(float)
        data["low"] = data["low"].astype(float)
        data["close"] = data["close"].astype(float)
        data["volume"] = data["volume"].astype(int)

        # Check for duplicates in the time column
        if data["time"].duplicated().any():
            logging.warning("Duplicate timestamps found in 'time' column. Consider deduplication.")

        # Sort the data by the time column
        logging.info("Sorting data by 'time' column.")
        data = data.sort_values(by="time")
        return data
