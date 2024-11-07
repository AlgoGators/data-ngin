import yaml
import os
from abc import ABC, abstractmethod
from typing import List, Dict, Any

class Loader(ABC):
    """
    Abstract base class for loaders that handle loading and validating symbols
    from various sources (e.g., CSV, JSON, APIs) based on configuration settings.
    
    Attributes:
        config_path (str): Path to the configuration file.
        config (Dict[str, Any]): Configuration settings loaded from config.yaml.
    """
    
    def __init__(self, config_path: str = 'config/config.yaml') -> None:
        """
        Initializes the Loader with a specified configuration file.
        
        Args:
            config_path (str): Path to the configuration file. Defaults to 'config/config.yaml'.
        """
        self.config_path: str = config_path
        self.config: Dict[str, Any] = self.load_config()

    def load_config(self) -> Dict[str, Any]:
        """
        Loads configuration settings from a YAML file.
        
        Returns:
            Dict[str, Any]: The loaded configuration as a dictionary.
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found at {self.config_path}")
        
        with open(self.config_path, 'r') as file:
            config: Dict[str, Any] = yaml.safe_load(file)
        
        return config

    @abstractmethod
    def load_symbols(self) -> Dict[str, str]:
        """
        Abstract method for loading symbols from a specific data source.
        This method must be implemented by subclasses.
        
        Returns:
            Dict[str, str]: A dictionary mapping each symbol to its asset type.
        """
        pass

    def validate_symbols(self, symbols: Dict[str, str]) -> List[str]:
        """
        Validates the loaded symbols against the supported asset types
        and datasets defined in the configuration file.
        
        Args:
            symbols (Dict[str, str]): A dictionary mapping symbols to their asset types.
        
        Returns:
            List[str]: A list of validated symbols that are supported by the configuration.
        """
        provider: Dict[str, Any] = self.config['providers']['databento']
        supported_assets: List[str] = provider.get('supported_assets', [])
        
        validated_symbols: List[str] = [
            symbol for symbol, asset_type in symbols.items()
            if self.is_supported(asset_type, supported_assets)
        ]
        
        for symbol in symbols:
            if symbol not in validated_symbols:
                print(f"Warning: {symbol} with asset type {symbols[symbol]} is not supported by the current configuration.")
        
        return validated_symbols

    def is_supported(self, asset_type: str, supported_assets: List[str]) -> bool:
        """
        Checks if an asset type is supported based on the configured asset types.
        
        Args:
            asset_type (str): The asset type of the symbol to check.
            supported_assets (List[str]): List of asset types supported by the provider.
        
        Returns:
            bool: True if the asset type is supported; False otherwise.
        """
        return asset_type in supported_assets

    def prepare_for_ingestion(self) -> List[Dict[str, Any]]:
        """
        Prepares ingestion jobs for each validated symbol based on the configuration settings.
        
        Returns:
            List[Dict[str, Any]]: A list of ingestion job dictionaries containing symbol, provider, and aggregation level.
        """
        symbols: Dict[str, str] = self.load_symbols()
        validated_symbols: List[str] = self.validate_symbols(symbols)
        
        ingestion_jobs: List[Dict[str, Any]] = [
            {
                "symbol": symbol,
                "asset_type": symbols[symbol],
                "provider": "databento",  
                "aggregation_level": "ohlcv-1d"  
            }
            for symbol in validated_symbols
        ]
        
        return ingestion_jobs
