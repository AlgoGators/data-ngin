from abc import ABC, abstractmethod
import pandas as pd
from typing import List, Dict, Any

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
            data (pd.DataFrame): The raw data to validate.

        Returns:
            pd.DataFrame: The validated data.
        """
        pass

    @abstractmethod
    def handle_missing_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Handles missing or corrupt data in the provided dataset.

        Args:
            data (pd.DataFrame): The raw data with missing values.

        Returns:
            pd.DataFrame: The data after handling missing values.
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
        """
        pass

    def clean(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Main method that orchestrates the cleaning process.

        Args:
            data (pd.DataFrame): The raw data to clean.

        Returns:
            pd.DataFrame: The cleaned data ready for database insertion.
        """
        data = self.validate_fields(data)
        data = self.handle_missing_data(data)
        data = self.transform_data(data)
        return data
