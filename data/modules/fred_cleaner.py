import pandas as pd
from typing import Dict, Any
from data.modules.cleaner import Cleaner
import logging


class FREDCleaner(Cleaner):
    """
    Cleaner for preprocessing FRED data before database insertion.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the FREDCleaner with a configuration.

        Args:
            config (Dict[str, Any]): Configuration dictionary.
        """
        self.logger = logging.getLogger("FREDCleaner")
        self.config = config or {}

    def validate_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Validates required fields in the data.

        Args:
            data (pd.DataFrame): The raw data.

        Returns:
            pd.DataFrame: Validated data.
        """
        required_columns = ["time", "index_name", "value", "metadata"]
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Data is missing required columns: {missing_columns}")
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
        # Get numeric columns
        numeric_columns = data.select_dtypes(include=['int64', 'float64']).columns

        method_switch = {
            "drop_nan": lambda d: d.dropna(),
            "forward_fill": lambda d: d.ffill(),
            "backward_fill": lambda d: d.bfill(),
            "interpolate": lambda d: d.infer_objects().interpolate(),
            "zero_fill": lambda d: d.fillna(0),
            "mean_fill": lambda d: d.fillna({col: d[col].mean() for col in numeric_columns}),
            "median_fill": lambda d: d.fillna({col: d[col].median() for col in numeric_columns}),
            "custom_fill": lambda d: d.fillna(self.config["missing_data"].get("custom_value", 0)),
        }

        for method, action in method_switch.items():
            if self.config.get("missing_data", {}).get(method, "False") == "True":
                logging.info(f"Applying {method.replace('_', ' ')}.")
                data = action(data)

        return data

    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Converts the data into the required format.

        Args:
            data (pd.DataFrame): The raw data.

        Returns:
            pd.DataFrame: Transformed data.
        """
        # Convert timestamps to UTC
        logging.info("Converting timestamps to UTC.")
        if isinstance(data["time"].dtype, pd.DatetimeTZDtype):
            # If already tz-aware, use tz_convert
            data["time"] = data["time"].dt.tz_convert("UTC")
        else:
            # If not tz-aware, localize to UTC
            data["time"] = pd.to_datetime(data["time"]).dt.tz_localize("UTC")

        return data[["time", "index_name", "value", "metadata"]]
