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
