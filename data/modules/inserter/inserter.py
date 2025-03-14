from abc import ABC, abstractmethod
from typing import List, Dict, Any


class Inserter(ABC):
    """
    Abstract base class for database inserters, defining methods for database
    connectivity and data insertion.

    Attributes:
        connection (Any): Placeholder for the database connection object.
    """

    def __init__(self, config: Dict[str, str]) -> None:
        """
        Initializes the Inserter base class. Subclasses should initialize a connection.
        """
        self.config: Dict[str, str] = config
        self.connection: Any = None

    @abstractmethod
    def connect(self) -> None:
        """
        Establishes a connection to the target database.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        pass

    @abstractmethod
    def insert_data(self, data: List[Dict[str, Any]], schema: str, table: str) -> None:
        """
        Inserts cleaned data into the target database.

        Args:
            data (List[Dict[str, Any]]): A list of dictionaries representing cleaned data rows.
            schema (str): The target schema in the database.
            table (str): The target table in the database.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        pass

#9
def log_insertion_status(self, success: bool, num_rows: int) -> None:
        """
        Logs the status of the data insertion.

        Args:
            success (bool): True if the insertion was successful, False otherwise.
            num_rows (int): The number of rows attempted to insert.
        """
        if success:
            self.logger.info(f"Successfully inserted {num_rows} rows into the database.")
        else:
            self.logger.error(f"Failed to insert {num_rows} rows into the database.")

def log_insertion_status(self, success: bool, num_rows: int) -> None:
        """
        Logs the status of the data insertion.

        Args:
            success (bool): True if the insertion was successful, False otherwise.
            num_rows (int): The number of rows attempted to insert.
        """
        if success:
            self.logger.info(f"Successfully inserted {num_rows} rows into the database.")
        else:
            self.logger.error(f"Failed to insert {num_rows} rows into the database.")

def validate_insertion(self, schema: str, table: str, expected_rows: int) -> bool:
        """
        Validates if the expected number of rows were inserted.

        Args:
            schema (str): The target schema in the database.
            table (str): The target table in the database.
            expected_rows (int): The expected number of rows to be inserted.

        Returns:
            bool: True if the validation passed (i.e., the correct number of rows was inserted).
        """
        try:
            # Placeholder: Implement query to check the number of rows in the target table
            query = f"SELECT COUNT(*) FROM {schema}.{table};"
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                actual_rows = result[0]
                
                if actual_rows == expected_rows:
                    self.logger.info(f"Validation passed: {actual_rows} rows inserted.")
                    return True
                else:
                    self.logger.warning(f"Validation failed: expected {expected_rows} rows, but found {actual_rows}.")
                    return False
        except Exception as e:
            self.logger.error(f"Error during insertion validation: {str(e)}")
            return False