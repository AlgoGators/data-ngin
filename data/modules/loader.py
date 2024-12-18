import yaml
import os
from abc import ABC, abstractmethod
from typing import List, Dict, Any

class Loader(ABC):
    """
    Abstract base class for loaders that handle loading and validating symbols
    from various sources (e.g., CSV, JSON, APIs) based on configuration settings.
    
    Attributes:
        config (Dict[str, Any]): Configuration settings loaded from config.yaml.
    """
    
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initializes the Loader with a specified configuration file.
        
        Args:
            config (str): Config settings.
        """
        self.config: Dict[str, Any] = config

    @abstractmethod
    def load_symbols(self) -> Dict[str, str]:
        """
        Abstract method for loading symbols from a specific data source.
        This method must be implemented by subclasses.
        
        Returns:
            Dict[str, str]: A dictionary mapping each symbol to its asset type.
        """
        pass
