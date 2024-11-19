from abc import ABC, abstractmethod
from typing import List, Dict, Any


class Inserter(ABC):
    """
    Abstract base class for database inserters.

    Methods:
        connect: Establish a connection to the database.
        insert_data: Abstract method to insert data into the database.
    """

    @abstractmethod
    def connect(self) -> None:
        """
        Establishes a connection to the target database.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        pass

    @abstractmethod
    def insert_data(self, data: List[Dict[str, Any]]) -> None:
        """
        Inserts cleaned data into the target database.

        Args:
            data (List[Dict[str, Any]]): A list of dictionaries representing cleaned data rows.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        pass
