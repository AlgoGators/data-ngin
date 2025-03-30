from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Any
import logging


class Cleaner(ABC):
    """
    Abstract base class for Cleaner modules responsible for standardizing raw data
    and preparing it for database insertion.

    Methods:
        validate_fields: Ensure required fields are present and valid.
        handle_missing_data: Handle missing or corrupt data based on configuration.
        transform_data: Apply transformations to standardize data formats.
        clean: Main method to validate, handle, and transform raw data.
    """

    @abstractmethod
    def validate_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Validates that required fields are present and valid in the data.

        Args:
            data (pd.DataFrame): The raw data to validate. Must include required columns.

        Returns:
            pd.DataFrame: The validated data, with only rows meeting field requirements.

        Raises:
            ValueError: If required fields are missing or invalid.
        """
        pass

    @abstractmethod
    def handle_missing_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Handles missing or corrupt data in the provided dataset.

        Args:
            data (pd.DataFrame): The raw data with missing values.

        Returns:
            pd.DataFrame: The data after handling missing values (e.g., dropped rows or imputed values).

        Notes:
            - This method can be customized by subclasses to drop rows, fill missing values, or flag issues.
            (see data/modules/databento_cleaner.py for an example)
        """
        pass

    @abstractmethod
    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms raw data into a standardized format.

        Args:
            data (pd.DataFrame): The raw data to transform.

        Returns:
            pd.DataFrame: The transformed, standardized data.

        Notes:
            - Transformation could include renaming columns, changing data types, or formatting timestamps.
        """
        pass

#9
    def detect_time_gaps(self, data: pd.DataFrame, time_column: str, freq: str) -> List[pd.Timestamp]:
        """
        Detects missing time gaps in the time series data.

        Args:
            data (pd.DataFrame): The data to check for gaps.
            time_column (str): The column that contains the timestamp information.
            freq (str): The expected frequency of the timestamps (e.g., 'D' for daily).

        Returns:
            List[pd.Timestamp]: List of missing timestamps.
        """
        data[time_column] = pd.to_datetime(data[time_column])
        data = data.sort_values(by=time_column)
        time_range = pd.date_range(start=data[time_column].min(), end=data[time_column].max(), freq=freq)
        missing_timestamps = time_range.difference(data[time_column])

        return missing_timestamps

    def log_missing_data(self, missing_timestamps: List[pd.Timestamp]) -> None:
        """
        Logs the missing time gaps to a log file.

        Args:
            missing_timestamps (List[pd.Timestamp]): The list of missing timestamps to log.
        """
        logging.basicConfig(filename='data_quality.log', level=logging.INFO)
        for timestamp in missing_timestamps:
            logging.warning(f"Missing data at timestamp: {timestamp}")

    def clean(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Main method that orchestrates the cleaning process.

        Args:
            data (pd.DataFrame): The raw data to clean.

        Returns:
            pd.DataFrame: The cleaned data ready for database insertion.

        Process:
            1. Validate fields.
            2. Handle missing or corrupt data.
            3. Transform data into the desired format.
        """
        data: pd.DataFrame = self.validate_fields(data)
        data: pd.DataFrame = self.handle_missing_data(data)
        data: pd.DataFrame = self.transform_data(data)
        return data
